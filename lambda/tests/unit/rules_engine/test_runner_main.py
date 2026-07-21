"""Tests for the rules-engine Fargate runner entry point
(`fargate/rules_engine/main.py`).

Focus: env-var marshaling into the event dict, `null`/empty handling,
and exit-code contract with the Step Functions state machine. The
actual validation loop (`rules_engine_rag.run_validation`) is stubbed
— its behavior is exercised by the other tests in this directory.
"""

import importlib.util
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[4]
_RUNNER_PATH = _REPO_ROOT / "fargate" / "rules_engine" / "main.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location(
        "rules_engine_runner_main", str(_RUNNER_PATH),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------- _plain_env / _parse_json_env ---------------------------------

class TestPlainEnv:
    def test_missing_returns_none(self, monkeypatch):
        monkeypatch.delenv("X", raising=False)
        runner = _load_runner()
        assert runner._plain_env("X") is None

    def test_empty_string_treated_as_absent(self, monkeypatch):
        monkeypatch.setenv("X", "")
        runner = _load_runner()
        assert runner._plain_env("X") is None

    def test_literal_null_treated_as_absent(self, monkeypatch):
        # States.JsonToString(null) yields the string "null".
        monkeypatch.setenv("X", "null")
        runner = _load_runner()
        assert runner._plain_env("X") is None

    def test_null_placeholder_treated_as_absent(self, monkeypatch):
        # EventBridge target inputs strip literal null, so we send the
        # placeholder "__NULL__" — it must round-trip to absent.
        monkeypatch.setenv("X", "__NULL__")
        runner = _load_runner()
        assert runner._plain_env("X") is None

    def test_json_encoded_null_placeholder_treated_as_absent(self, monkeypatch):
        # After States.JsonToString the placeholder is double-quoted.
        monkeypatch.setenv("X", '"__NULL__"')
        runner = _load_runner()
        assert runner._plain_env("X") is None

    def test_real_value_passthrough(self, monkeypatch):
        monkeypatch.setenv("X", "hello")
        runner = _load_runner()
        assert runner._plain_env("X") == "hello"


class TestParseJsonEnv:
    def test_missing_returns_none(self, monkeypatch):
        monkeypatch.delenv("X", raising=False)
        runner = _load_runner()
        assert runner._parse_json_env("X") is None

    def test_null_returns_none(self, monkeypatch):
        monkeypatch.setenv("X", "null")
        runner = _load_runner()
        assert runner._parse_json_env("X") is None

    def test_json_encoded_null_placeholder_returns_none(self, monkeypatch):
        # EventBridge inputs use "__NULL__" instead of literal null;
        # after `States.JsonToString` we see the double-quoted form.
        monkeypatch.setenv("X", '"__NULL__"')
        runner = _load_runner()
        assert runner._parse_json_env("X") is None

    def test_empty_list_returns_none(self, monkeypatch):
        monkeypatch.setenv("X", "[]")
        runner = _load_runner()
        assert runner._parse_json_env("X") is None

    def test_valid_list_returned(self, monkeypatch):
        monkeypatch.setenv("X", '["a", "b"]')
        runner = _load_runner()
        assert runner._parse_json_env("X") == ["a", "b"]

    def test_valid_object_returned(self, monkeypatch):
        monkeypatch.setenv("X", '{"days_back_from_today": [0]}')
        runner = _load_runner()
        assert runner._parse_json_env("X") == {"days_back_from_today": [0]}

    def test_json_encoded_string_returned(self, monkeypatch):
        # States.JsonToString("abc") yields "\"abc\"" — this is what the
        # runner sees for RUN_ID when the caller supplies one.
        monkeypatch.setenv("X", '"20260515-100000"')
        runner = _load_runner()
        assert runner._parse_json_env("X") == "20260515-100000"

    def test_bad_json_raises(self, monkeypatch):
        monkeypatch.setenv("X", "not{json")
        runner = _load_runner()
        with pytest.raises(ValueError):
            runner._parse_json_env("X")


# ---------- _build_event -------------------------------------------------

class TestBuildEvent:
    def test_org_id_required(self, monkeypatch):
        monkeypatch.delenv("ORG_ID", raising=False)
        runner = _load_runner()
        with pytest.raises(ValueError, match="ORG_ID"):
            runner._build_event()

    def test_minimal_event(self, monkeypatch):
        monkeypatch.setenv("ORG_ID", "test-org")
        for name in ("RUN_ID", "CATEGORIES", "DATES", "DATE_WINDOW"):
            monkeypatch.delenv(name, raising=False)
        runner = _load_runner()
        assert runner._build_event() == {"organization_id": "test-org"}

    def test_null_env_vars_absent_from_event(self, monkeypatch):
        # StartExecution path sends literal null, which JsonToString
        # turns into "null" in the container env.
        monkeypatch.setenv("ORG_ID", "test-org")
        monkeypatch.setenv("RUN_ID", "null")
        monkeypatch.setenv("CATEGORIES", "null")
        monkeypatch.setenv("DATES", "null")
        monkeypatch.setenv("DATE_WINDOW", "null")
        runner = _load_runner()
        assert runner._build_event() == {"organization_id": "test-org"}

    def test_null_placeholder_env_vars_absent_from_event(self, monkeypatch):
        # EventBridge-scheduled invocations use the "__NULL__"
        # placeholder because CDK's RuleTargetInput strips literal null.
        # After JsonToString the placeholder becomes '"__NULL__"'.
        monkeypatch.setenv("ORG_ID", "test-org")
        monkeypatch.setenv("RUN_ID", '"__NULL__"')
        monkeypatch.setenv("CATEGORIES", '"__NULL__"')
        monkeypatch.setenv("DATES", '"__NULL__"')
        # DATE_WINDOW arrives populated with the actual window from the
        # EventBridge input, so we keep it here.
        monkeypatch.setenv("DATE_WINDOW", '{"days_back_from_today": [0]}')
        runner = _load_runner()
        assert runner._build_event() == {
            "organization_id": "test-org",
            "date_window": {"days_back_from_today": [0]},
        }

    def test_all_env_vars_populated(self, monkeypatch):
        monkeypatch.setenv("ORG_ID", "test-org")
        monkeypatch.setenv("RUN_ID", '"20260515-100000"')
        monkeypatch.setenv("CATEGORIES", '["Billing", "Intake"]')
        monkeypatch.setenv("DATES", '["2026-05-12", "2026-05-13"]')
        monkeypatch.setenv("DATE_WINDOW", '{"days_back_from_today": [0]}')
        runner = _load_runner()
        assert runner._build_event() == {
            "organization_id": "test-org",
            "validation_run_id": "20260515-100000",
            "categories": ["Billing", "Intake"],
            "dates": ["2026-05-12", "2026-05-13"],
            "date_window": {"days_back_from_today": [0]},
        }


# ---------- main() exit codes -------------------------------------------

class TestMainExitCodes:
    def test_missing_org_id_exits_2(self, monkeypatch):
        monkeypatch.delenv("ORG_ID", raising=False)
        runner = _load_runner()
        assert runner.main() == 2

    def test_success_exits_0(self, monkeypatch):
        monkeypatch.setenv("ORG_ID", "test-org")
        for name in ("RUN_ID", "CATEGORIES", "DATES", "DATE_WINDOW", "MODE"):
            monkeypatch.delenv(name, raising=False)

        runner = _load_runner()
        monkeypatch.setattr(runner, "run_validation",
                            lambda event: {"status": "ok"})
        assert runner.main() == 0

    def test_run_validation_error_exits_1(self, monkeypatch):
        monkeypatch.setenv("ORG_ID", "test-org")
        for name in ("RUN_ID", "CATEGORIES", "DATES", "DATE_WINDOW", "MODE"):
            monkeypatch.delenv(name, raising=False)

        def _boom(event):
            raise RuntimeError("something went wrong")

        runner = _load_runner()
        monkeypatch.setattr(runner, "run_validation", _boom)
        assert runner.main() == 1
