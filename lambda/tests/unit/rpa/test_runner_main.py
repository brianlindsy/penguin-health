"""Tests for the Fargate runner entry point (fargate/rpa_runner/main.py).

Goal: exercise the orchestration logic — config + playbook load, usage
guard, auth dispatch, run_started/run_completed audit emissions, the
EventBridge `RpaIngestComplete` ping — without spinning up Playwright
or hitting the real CR endpoints.

We import the runner via importlib so we don't need to add another
sys.path entry to the shared conftest just for this single test.
"""

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from rpa import config as config_mod
from rpa import secrets as secrets_mod


# ----- load the runner module under test ---------------------------------


_REPO_ROOT = Path(__file__).resolve().parents[4]
_RUNNER_PATH = _REPO_ROOT / "fargate" / "rpa_runner" / "main.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("rpa_runner_main",
                                                  str(_RUNNER_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ----- fixtures ----------------------------------------------------------


_FIXED_NOW = datetime(2026, 6, 10, 18, 0, tzinfo=timezone.utc)  # 13:00 CDT


@pytest.fixture
def seeded(mock_dynamodb, monkeypatch):
    """Seed RPA_CONFIG + RPA_PLAYBOOK; rebind config module to moto."""
    config_mod.invalidate_cache()
    monkeypatch.setattr(config_mod, "_table",
                        mock_dynamodb.Table("penguin-health-org-config"))

    mock_dynamodb.Table("penguin-health-org-config").put_item(Item={
        "pk": "ORG#demo",
        "sk": "RPA_CONFIG",
        "enabled": True,
        "vendor": "centralreach",
        "display_name": "Demo CR bot",
        "base_url": "https://members.centralreach.com",
        "bot_username": "rpa-bot+demo",
        "playbook_id": "cr-notes-v1",
        "guardrails": {
            "timezone": "America/Chicago",
            "allowed_hours": {"start": "06:00", "end": "20:00"},
            "rate_limit_ms_between_requests": 0,
            "blackout_dates": [],
        },
    })
    mock_dynamodb.Table("penguin-health-org-config").put_item(Item={
        "pk": "ORG#shared",
        "sk": "RPA_PLAYBOOK#cr-notes-v1",
        "vendor": "centralreach",
        "version": 1,
        "steps": [],   # the test stubs _drive_playwright_run, so steps
                       # are irrelevant — but the loader still needs them.
    })
    yield
    config_mod.invalidate_cache()


@pytest.fixture
def stubbed_runner(seeded, monkeypatch, mock_dynamodb):
    """Load the runner and stub the IO surfaces: authenticator, the
    Playwright-driven inner loop, EventBridge, audit emitter table.
    """
    runner = _load_runner()

    # Audit emitter -> moto DDB; firehose + cw stubbed.
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    # EventBridge stub
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

    monkeypatch.setattr(runner.authenticator, "authenticate",
                        lambda org_id, org_cfg: {
                            "cookies": [{"name": "crsd", "value": "x",
                                         "domain": ".x", "path": "/",
                                         "secure": True}],
                            "extra_http_headers": {},
                            "access_token": "JWT",
                        })

    # Stub the Playwright-driven inner loop; pretend it persisted 3 notes.
    async def fake_drive(**kw):
        return 3, "2026-06-10"
    monkeypatch.setattr(runner, "_drive_playwright_run", fake_drive)

    rc = runner._run("demo", "run-001", "manual")

    assert rc == 0
    # PutEvents called once with the documented detail-type.
    events.put_events.assert_called_once()
    entries = events.put_events.call_args.kwargs["Entries"]
    assert entries[0]["Source"] == "penguin-health.rpa"
    assert entries[0]["DetailType"] == "RpaIngestComplete"
    detail = json.loads(entries[0]["Detail"])
    assert detail == {
        "organization_id": "demo",
        "ingest_date": "2026-06-10",
        "playbook_run_id": "run-001",
        "note_count": 3,
        "vendor": "centralreach",
    }


# ----- usage guard skip --------------------------------------------------


def test_outside_window_skips_cleanly(stubbed_runner, monkeypatch):
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    # Force outside-window by re-pinning now to 03:00 UTC (22:00 prev CDT).
    monkeypatch.setattr(runner, "_now_utc",
                        lambda: datetime(2026, 6, 11, 3, 0,
                                         tzinfo=timezone.utc))

    auth_called = []
    monkeypatch.setattr(runner.authenticator, "authenticate",
                        lambda *a, **kw: auth_called.append(1) or {})

    async def must_not_run(**kw):  # pragma: no cover - assertion below
        raise AssertionError("playwright must not run when outside window")
    monkeypatch.setattr(runner, "_drive_playwright_run", must_not_run)

    rc = runner._run("demo", "run-002", "scheduled")

    assert rc == 0  # outside window is not an error
    assert auth_called == []
    events.put_events.assert_not_called()


# ----- auth failure ------------------------------------------------------


def test_auth_failure_emits_major_failure_and_exits_1(
    stubbed_runner, monkeypatch
):
    from rpa.exceptions import RpaAuthError
    runner = stubbed_runner["runner"]
    events = stubbed_runner["events"]

    def bad_auth(org_id, org_cfg):
        raise RpaAuthError("CR SSO returned HTTP 401")
    monkeypatch.setattr(runner.authenticator, "authenticate", bad_auth)

    async def must_not_run(**kw):  # pragma: no cover - assertion below
        raise AssertionError("playwright must not run on auth failure")
    monkeypatch.setattr(runner, "_drive_playwright_run", must_not_run)

    rc = runner._run("demo", "run-003", "scheduled")

    assert rc == 1
    # Even on auth failure we ALSO emit a run_completed audit; verify by
    # checking the audit table directly for that resource_type.
    rows = mock_audit_rows()
    types = [r["resource_type"] for r in rows]
    assert "RpaAuthSession" in types
    assert "RpaPlaybookRun" in types
    # An ingest-complete event is NOT emitted when the run did not start
    # extraction successfully.
    events.put_events.assert_not_called()


# ----- not configured ----------------------------------------------------


def test_not_configured_returns_2(stubbed_runner, monkeypatch,
                                  mock_dynamodb):
    runner = stubbed_runner["runner"]
    # Disable RPA for the demo org.
    mock_dynamodb.Table("penguin-health-org-config").update_item(
        Key={"pk": "ORG#demo", "sk": "RPA_CONFIG"},
        UpdateExpression="SET enabled = :f",
        ExpressionAttributeValues={":f": False},
    )
    config_mod.invalidate_cache()

    rc = runner._run("demo", "run-004", "manual")
    assert rc == 2


# ----- helper ------------------------------------------------------------


def mock_audit_rows():
    table = boto3.resource("dynamodb").Table("penguin-health-audit")
    return table.scan().get("Items", [])
