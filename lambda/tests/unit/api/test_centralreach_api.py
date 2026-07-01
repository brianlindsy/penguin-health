"""Tests for the CentralReach admin API.

Mirrors `test_rpa_api.py` exactly in shape — same shared fixtures
(mock_dynamodb, audit emitter), same MagicMock for Step Functions —
but exercises `centralreach_api` and the `/centralreach/*` URL paths.

Pins five contracts:
  1. get_config returns the redacted CENTRALREACH_CONFIG item
  2. trigger_run calls StartExecution with the org payload
  3. list_runs filters to the caller's org_id
  4. get_run reconstructs the execution ARN and returns describe output
  5. All four routes are gated by the Compliance Audit category
"""

import json
from unittest.mock import MagicMock

import pytest

import centralreach_api
from centralreach import config as cr_config


_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:111122223333:stateMachine:penguin-health-centralreach-ingest"
)
_EXECUTION_ARN_PREFIX = (
    "arn:aws:states:us-east-1:111122223333:execution:penguin-health-centralreach-ingest"
)


# ---- fixtures -----------------------------------------------------------


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("CENTRALREACH_STATE_MACHINE_ARN", _STATE_MACHINE_ARN)
    yield


@pytest.fixture
def sfn():
    client = MagicMock()
    centralreach_api._reset_for_tests(client=client)
    yield client
    centralreach_api._reset_for_tests()


@pytest.fixture
def cr_table(mock_dynamodb, monkeypatch):
    cr_config.invalidate_cache()
    monkeypatch.setattr(cr_config, "_table",
                        mock_dynamodb.Table("penguin-health-org-config"))
    yield mock_dynamodb.Table("penguin-health-org-config")
    cr_config.invalidate_cache()


@pytest.fixture(autouse=True)
def stub_audit(mock_dynamodb, monkeypatch):
    """Point the audit emitter at moto so the @audited decorator on
    every API handler doesn't blow up trying to write to real DDB."""
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())


def _seed_config(table, org_id="demo", *, enabled=True,
                 client_secret_leak=None):
    item = {
        "pk": f"ORG#{org_id}",
        "sk": "CENTRALREACH_CONFIG",
        "enabled": enabled,
        "display_name": "Demo CR ingest",
        "base_url": "https://members.centralreach.com",
        "bot_username": "centralreach-bot+demo",
        "guardrails": {
            "timezone": "America/Chicago",
            "allowed_hours": {"start": "06:00", "end": "20:00"},
            "rate_limit_ms_between_requests": 0,
            "blackout_dates": [],
        },
    }
    if client_secret_leak:
        item["client_secret"] = client_secret_leak
    table.put_item(Item=item)


def _event():
    return {
        "requestContext": {
            "authorizer": {"jwt": {"claims": {
                "email": "ops@example.com",
                "sub": "user-ops",
                "cognito:groups": "[Admins]",
            }}},
            "http": {"sourceIp": "127.0.0.1"},
        },
        "pathParameters": {},
        "body": None,
    }


def _authorize_pass_admin(event, org_id):
    return {
        "email": event["requestContext"]["authorizer"]["jwt"]["claims"]["email"],
        "sub": "user-ops",
        "groups": ["Admins"],
    }, None


def _authorize_pass_no_category(event, org_id):
    return {
        "email": event["requestContext"]["authorizer"]["jwt"]["claims"]["email"],
        "sub": "user-noperms",
        "groups": [],
    }, None


def _authorize_reject(event, org_id):
    return None, {"statusCode": 403, "body": json.dumps({"error": "nope"})}


# ---- GET /centralreach/config ------------------------------------------


def test_get_config_returns_redacted_item(env, sfn, cr_table):
    _seed_config(cr_table, client_secret_leak="should-never-surface")

    res = centralreach_api.get_config(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 200
    cfg = json.loads(res["body"])["config"]
    assert cfg["display_name"] == "Demo CR ingest"
    assert cfg["client_secret"] == "[REDACTED]"
    assert cfg["guardrails"]["allowed_hours"]["start"] == "06:00"


def test_get_config_404_when_not_configured(env, sfn, cr_table):
    res = centralreach_api.get_config(
        _event(), {"orgId": "no-such-org"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 404


def test_get_config_403_without_view_permission(env, sfn, cr_table):
    _seed_config(cr_table)
    res = centralreach_api.get_config(
        _event(), {"orgId": "demo"},
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    assert "Compliance Audit:view" in json.loads(res["body"])["error"]


def test_get_config_propagates_auth_error(env, sfn, cr_table):
    res = centralreach_api.get_config(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_reject,
    )
    assert res["statusCode"] == 403


# ---- POST /centralreach/run --------------------------------------------


def test_trigger_run_starts_execution_with_org_payload(env, sfn, cr_table):
    _seed_config(cr_table)
    sfn.start_execution.return_value = {
        "executionArn": f"{_EXECUTION_ARN_PREFIX}:run-xyz",
        "startDate": "2026-06-30T18:00:00Z",
    }

    res = centralreach_api.trigger_run(
        _event(), {"orgId": "demo"}, body=None,
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 202
    body = json.loads(res["body"])
    assert body["run_id"].startswith("run-")

    sfn.start_execution.assert_called_once()
    call = sfn.start_execution.call_args.kwargs
    assert call["stateMachineArn"] == _STATE_MACHINE_ARN
    sfn_input = json.loads(call["input"])
    assert sfn_input["organization_id"] == "demo"
    assert sfn_input["run_id"] == body["run_id"]
    assert sfn_input["mode"] == "manual"
    assert sfn_input["triggered_by"] == "ops@example.com"


def test_trigger_run_409_when_org_not_configured(env, sfn, cr_table):
    res = centralreach_api.trigger_run(
        _event(), {"orgId": "no-such-org"}, body=None,
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 409
    sfn.start_execution.assert_not_called()


def test_trigger_run_403_without_run_permission(env, sfn, cr_table):
    _seed_config(cr_table)
    res = centralreach_api.trigger_run(
        _event(), {"orgId": "demo"}, body=None,
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    assert "Compliance Audit:run" in json.loads(res["body"])["error"]
    sfn.start_execution.assert_not_called()


def test_trigger_run_no_state_machine_arn_env(env, sfn, cr_table, monkeypatch):
    _seed_config(cr_table)
    monkeypatch.delenv("CENTRALREACH_STATE_MACHINE_ARN")
    with pytest.raises(RuntimeError, match="env var is not set"):
        centralreach_api.trigger_run(
            _event(), {"orgId": "demo"}, body=None,
            authorize_fn=_authorize_pass_admin,
        )


# ---- GET /centralreach/runs --------------------------------------------


def test_list_runs_filters_to_caller_org(env, sfn, cr_table):
    sfn.list_executions.return_value = {
        "executions": [
            {"executionArn": f"{_EXECUTION_ARN_PREFIX}:run-a"},
            {"executionArn": f"{_EXECUTION_ARN_PREFIX}:run-b"},
            {"executionArn": f"{_EXECUTION_ARN_PREFIX}:run-c"},
        ],
    }

    def describe(executionArn):
        if executionArn.endswith(":run-a"):
            return {"executionArn": executionArn, "status": "SUCCEEDED",
                    "input": json.dumps({"organization_id": "demo",
                                          "run_id": "run-a", "mode": "scheduled"})}
        if executionArn.endswith(":run-b"):
            return {"executionArn": executionArn, "status": "RUNNING",
                    "input": json.dumps({"organization_id": "other-org",
                                          "run_id": "run-b", "mode": "manual"})}
        return {"executionArn": executionArn, "status": "FAILED",
                "input": json.dumps({"organization_id": "demo",
                                      "run_id": "run-c", "mode": "manual"})}

    sfn.describe_execution.side_effect = describe

    res = centralreach_api.list_runs(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    body = json.loads(res["body"])
    run_ids = sorted(it["run_id"] for it in body["items"])
    assert run_ids == ["run-a", "run-c"]


def test_list_runs_403_without_view_permission(env, sfn, cr_table):
    res = centralreach_api.list_runs(
        _event(), {"orgId": "demo"},
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    sfn.list_executions.assert_not_called()


def test_list_runs_honors_limit_param(env, sfn, cr_table):
    sfn.list_executions.return_value = {"executions": [
        {"executionArn": f"{_EXECUTION_ARN_PREFIX}:run-{i}"}
        for i in range(20)
    ]}
    sfn.describe_execution.side_effect = lambda executionArn: {
        "executionArn": executionArn,
        "status": "SUCCEEDED",
        "input": json.dumps({"organization_id": "demo",
                              "run_id": executionArn.rsplit(":", 1)[-1],
                              "mode": "scheduled"}),
    }

    evt = _event()
    evt["queryStringParameters"] = {"limit": "3"}
    res = centralreach_api.list_runs(
        evt, {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    body = json.loads(res["body"])
    assert len(body["items"]) == 3


# ---- GET /centralreach/runs/{runId} ------------------------------------


def test_get_run_returns_describe_result(env, sfn, cr_table):
    expected_arn = f"{_EXECUTION_ARN_PREFIX}:run-known"
    sfn.describe_execution.return_value = {
        "executionArn": expected_arn,
        "status": "SUCCEEDED",
        "startDate": "2026-06-30T18:00:00Z",
        "stopDate": "2026-06-30T18:02:00Z",
        "input": json.dumps({"organization_id": "demo",
                              "run_id": "run-known", "mode": "manual",
                              "triggered_by": "ops@example.com"}),
        "output": json.dumps({"processed_count": 12, "skipped_count": 3,
                               "failure_count": 0,
                               "ingest_date": "2026-06-30"}),
    }

    res = centralreach_api.get_run(
        _event(), {"orgId": "demo", "runId": "run-known"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    assert body["run_id"] == "run-known"
    assert body["status"] == "SUCCEEDED"
    assert body["output"]["processed_count"] == 12

    sfn.describe_execution.assert_called_once_with(executionArn=expected_arn)


def test_get_run_404_for_other_org_run(env, sfn, cr_table):
    """Defense in depth: even if caller knows the run_id, refuse to
    return runs belonging to other orgs."""
    sfn.describe_execution.return_value = {
        "executionArn": f"{_EXECUTION_ARN_PREFIX}:run-other",
        "status": "SUCCEEDED",
        "input": json.dumps({"organization_id": "other-org",
                              "run_id": "run-other",
                              "mode": "manual"}),
    }

    res = centralreach_api.get_run(
        _event(), {"orgId": "demo", "runId": "run-other"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 404
