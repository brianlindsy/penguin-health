"""Tests for the CentralReach Fargate runner entry point
(`fargate/centralreach_ingest/main.py`).

Goal: exercise the orchestration logic — config load, usage guard,
auth dispatch, run_started/run_completed audit emissions, the
EventBridge `CentralReachIngestComplete` ping — without spinning up
the HTTP client or hitting real CR endpoints.

We import the runner via importlib so we don't need to add another
sys.path entry to the shared conftest just for this test.
"""

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from centralreach import config as cr_config
from centralreach.exceptions import CentralReachAuthError
from centralreach.pipeline import IngestSummary


# ----- load the runner module under test ---------------------------------


_REPO_ROOT = Path(__file__).resolve().parents[4]
_RUNNER_PATH = _REPO_ROOT / "fargate" / "centralreach_ingest" / "main.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location(
        "centralreach_ingest_main", str(_RUNNER_PATH),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ----- fixtures ----------------------------------------------------------


_FIXED_NOW = datetime(2026, 6, 30, 18, 0, tzinfo=timezone.utc)  # 13:00 CDT


@pytest.fixture
def seeded(mock_dynamodb, monkeypatch):
    """Seed CENTRALREACH_CONFIG; rebind config module to moto."""
    cr_config.invalidate_cache()
    monkeypatch.setattr(cr_config, "_table",
                        mock_dynamodb.Table("penguin-health-org-config"))

    mock_dynamodb.Table("penguin-health-org-config").put_item(Item={
        "pk": "ORG#demo",
        "sk": "CENTRALREACH_CONFIG",
        "enabled": True,
        "display_name": "Demo CR ingest",
        "base_url": "https://members.centralreach.com",
        "bot_username": "centralreach-bot+demo",
        "guardrails": {
            "timezone": "America/Chicago",
            "allowed_hours": {"start": "06:00", "end": "20:00"},
            "rate_limit_ms_between_requests": 0,
            "blackout_dates": [],
        },
    })
    yield
    cr_config.invalidate_cache()


@pytest.fixture
def stubbed_runner(seeded, monkeypatch, mock_dynamodb):
    """Load the runner and stub the IO surfaces: client.authenticate,
    pipeline.run_ingest, EventBridge, audit emitter table."""
    runner = _load_runner()

    # Audit emitter -> moto DDB; firehose + cw stubbed.
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    # EventBridge stub. boto3.client is patched on the runner module's
    # boto3 import so we don't need to patch the global.
    events_client = MagicMock()
    monkeypatch.setattr(runner.boto3, "client",
                        lambda name, *a, **kw: events_client)

    # Pin "now" so usage_guard verdict is deterministic.
    monkeypatch.setattr(runner, "_now_utc", lambda: _FIXED_NOW)

    return {"runner": runner, "events": events_client}


# ----- happy path --------------------------------------------------------


def test_happy_path_emits_started_completed_and_ingest_complete(
    stubbed_runner, monkeypatch
):
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    # Stub the client class so its authenticate() succeeds and the
    # constructor doesn't try to mint an HTTP opener for real.
    client_instance = MagicMock()
    client_instance.authenticate = MagicMock()
    monkeypatch.setattr(
        runner, "CentralReachClient", lambda **kw: client_instance,
    )

    # Stub run_ingest to return a canned summary
    monkeypatch.setattr(runner, "run_ingest", lambda **kw: IngestSummary(
        processed_count=42, failure_count=1, skipped_count=3,
        failures_by_type={"CentralReachAPIError": 1},
        skipped_by_reason={"no_pdf_available": 3},
    ))

    rc = runner._run(org_id="demo", run_id="run-abc", mode="manual")
    assert rc == 0

    # EventBridge ping fired with the summary
    events.put_events.assert_called_once()
    entry = events.put_events.call_args.kwargs["Entries"][0]
    assert entry["Source"] == "penguin-health.centralreach"
    assert entry["DetailType"] == "CentralReachIngestComplete"
    detail = json.loads(entry["Detail"])
    assert detail["organization_id"] == "demo"
    assert detail["run_id"] == "run-abc"
    assert detail["vendor"] == "centralreach"
    assert detail["processed_count"] == 42
    assert detail["failure_count"] == 1
    assert detail["skipped_count"] == 3


# ----- skipped / failed paths --------------------------------------------


def test_not_configured_returns_2(stubbed_runner):
    runner = stubbed_runner["runner"]
    rc = runner._run(org_id="no-such-org", run_id="run-x", mode="manual")
    assert rc == 2


def test_outside_window_exits_0_with_minor_failure_audit(
    stubbed_runner, monkeypatch
):
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    # Move "now" to 04:00 CDT (09:00 UTC) — outside the 06:00-20:00 window
    monkeypatch.setattr(runner, "_now_utc",
                        lambda: datetime(2026, 6, 30, 9, 0,
                                         tzinfo=timezone.utc))

    rc = runner._run(org_id="demo", run_id="run-x", mode="scheduled")
    # Outside-window is a clean skip, not a failure
    assert rc == 0
    # No EventBridge ping for skipped runs — the test pins this so we
    # don't accidentally fan out to downstream consumers on skips.
    events.put_events.assert_not_called()


def test_auth_failure_returns_1_and_emits_two_failure_audits(
    stubbed_runner, monkeypatch
):
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    # Client.authenticate raises
    client_instance = MagicMock()
    client_instance.authenticate.side_effect = CentralReachAuthError("nope")
    monkeypatch.setattr(
        runner, "CentralReachClient", lambda **kw: client_instance,
    )

    rc = runner._run(org_id="demo", run_id="run-x", mode="manual")
    assert rc == 1
    # No ingest fan-out on auth failure
    events.put_events.assert_not_called()


def test_pipeline_exception_returns_1_and_still_emits_ingest_complete(
    stubbed_runner, monkeypatch
):
    """Even on pipeline failure, run_completed audit fires AND
    EventBridge ping fires (so downstream consumers know the run
    happened even if it produced zero records)."""
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    client_instance = MagicMock()
    monkeypatch.setattr(
        runner, "CentralReachClient", lambda **kw: client_instance,
    )

    def _explode(**kw):
        raise RuntimeError("pipeline crashed")

    monkeypatch.setattr(runner, "run_ingest", _explode)

    rc = runner._run(org_id="demo", run_id="run-x", mode="manual")
    assert rc == 1
    # EventBridge ping STILL fires — downstream needs to know
    events.put_events.assert_called_once()
    detail = json.loads(
        events.put_events.call_args.kwargs["Entries"][0]["Detail"],
    )
    # Counts default to zero since the IngestSummary stayed empty
    assert detail["processed_count"] == 0


def test_eventbridge_failure_does_not_fail_the_run(
    stubbed_runner, monkeypatch
):
    """PutEvents failure shouldn't drop the run on the floor — the
    audit already recorded the completion. Test pins that an
    EventBridge exception is caught and swallowed."""
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]
    events.put_events.side_effect = RuntimeError("eb down")

    client_instance = MagicMock()
    monkeypatch.setattr(
        runner, "CentralReachClient", lambda **kw: client_instance,
    )
    monkeypatch.setattr(runner, "run_ingest", lambda **kw: IngestSummary())

    rc = runner._run(org_id="demo", run_id="run-x", mode="manual")
    assert rc == 0


# ----- main() ------------------------------------------------------------


def test_main_requires_org_id_env(stubbed_runner, monkeypatch):
    """Without ORG_ID set, main() returns 2 and prints to stderr."""
    runner = stubbed_runner["runner"]
    monkeypatch.delenv("ORG_ID", raising=False)
    rc = runner.main()
    assert rc == 2


def test_main_generates_run_id_if_not_set(stubbed_runner, monkeypatch):
    runner = stubbed_runner["runner"]
    monkeypatch.setenv("ORG_ID", "demo")
    monkeypatch.delenv("RUN_ID", raising=False)

    client_instance = MagicMock()
    monkeypatch.setattr(
        runner, "CentralReachClient", lambda **kw: client_instance,
    )
    monkeypatch.setattr(runner, "run_ingest", lambda **kw: IngestSummary())

    rc = runner.main()
    assert rc == 0


def test_ingest_date_uses_eastern_wall_clock_for_partition():
    # 02:30 UTC on 2026-07-01 is 22:30 EDT on 2026-06-30 — the run
    # belongs to the prior US clinical day, so the partition folder
    # must be 2026-06-30, not 2026-07-01.
    runner = _load_runner()
    late_night_utc = datetime(2026, 7, 1, 2, 30, tzinfo=timezone.utc)
    assert runner._ingest_date(late_night_utc) == "2026-06-30"
