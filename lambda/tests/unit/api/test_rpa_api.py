"""Tests for the RPA admin API.

Step Functions is stubbed via a MagicMock pinned to the module's
`_sfn_client`. RPA_CONFIG is seeded into moto DynamoDB through the
shared `mock_dynamodb` fixture. Permissions are exercised via the
`Compliance Audit` category (the same gate the rules engine uses).
"""

import json
from unittest.mock import MagicMock

import pytest

import rpa_api
from rpa import config as rpa_config


_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:111122223333:stateMachine:penguin-health-rpa-run"
)
_EXECUTION_ARN_PREFIX = (
    "arn:aws:states:us-east-1:111122223333:execution:penguin-health-rpa-run"
)


# ---- fixtures -----------------------------------------------------------


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("RPA_STATE_MACHINE_ARN", _STATE_MACHINE_ARN)
    yield


@pytest.fixture
def sfn():
    client = MagicMock()
    rpa_api._reset_for_tests(client=client)
    yield client
    rpa_api._reset_for_tests()


@pytest.fixture
def rpa_table(mock_dynamodb, monkeypatch):
    rpa_config.invalidate_cache()
    monkeypatch.setattr(rpa_config, "_table",
                        mock_dynamodb.Table("penguin-health-org-config"))
    yield mock_dynamodb.Table("penguin-health-org-config")
    rpa_config.invalidate_cache()


def _seed_config(table, org_id="demo", *, enabled=True,
                 client_secret_leak=None):
    item = {
        "pk": f"ORG#{org_id}",
        "sk": "RPA_CONFIG",
        "enabled": enabled,
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
    }
    if client_secret_leak:
        # Defensive scrub: even if someone foolishly puts this in DDB,
        # the API must redact it.
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
    """Authenticated user with no Compliance Audit permissions.

    Returning groups=[] makes is_org_admin / is_super_admin fall through;
    the category check then has no permission row and refuses.
    """
    return {
        "email": event["requestContext"]["authorizer"]["jwt"]["claims"]["email"],
        "sub": "user-noperms",
        "groups": [],
    }, None


def _authorize_reject(event, org_id):
    return None, {"statusCode": 403, "body": json.dumps({"error": "nope"})}


# ---- GET /rpa/config ----------------------------------------------------


def test_get_config_returns_redacted_item(env, sfn, rpa_table,
                                          mock_dynamodb, monkeypatch):
    # The audit emit fires inside the decorator; point it at moto so it
    # doesn't blow up.
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    _seed_config(rpa_table, client_secret_leak="should-never-surface")

    res = rpa_api.get_config(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    cfg = body["config"]
    assert cfg["vendor"] == "centralreach"
    assert cfg["playbook_id"] == "cr-notes-v1"
    # Any sensitive-looking key is scrubbed before return.
    assert cfg["client_secret"] == "[REDACTED]"
    # And the canonical happy-path values are untouched.
    assert cfg["guardrails"]["allowed_hours"]["start"] == "06:00"


def test_get_config_404_when_not_configured(env, sfn, rpa_table,
                                            mock_dynamodb, monkeypatch):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    res = rpa_api.get_config(
        _event(), {"orgId": "no-such-org"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 404


def test_get_config_403_when_no_compliance_audit_view(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    _seed_config(rpa_table)
    res = rpa_api.get_config(
        _event(), {"orgId": "demo"},
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    assert "Compliance Audit:view" in json.loads(res["body"])["error"]


def test_get_config_propagates_auth_error(env, sfn, rpa_table):
    res = rpa_api.get_config(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_reject,
    )
    assert res["statusCode"] == 403


# ---- POST /rpa/run -----------------------------------------------------


def test_trigger_run_starts_execution_with_org_payload(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    _seed_config(rpa_table)
    sfn.start_execution.return_value = {
        "executionArn": f"{_EXECUTION_ARN_PREFIX}:run-xyz",
        "startDate": "2026-06-12T18:00:00Z",
    }

    res = rpa_api.trigger_run(
        _event(), {"orgId": "demo"}, body=None,
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 202
    body = json.loads(res["body"])
    assert body["run_id"].startswith("run-")

    sfn.start_execution.assert_called_once()
    call = sfn.start_execution.call_args.kwargs
    assert call["stateMachineArn"] == _STATE_MACHINE_ARN
    assert call["name"] == body["run_id"]
    sfn_input = json.loads(call["input"])
    assert sfn_input["organization_id"] == "demo"
    assert sfn_input["run_id"] == body["run_id"]
    assert sfn_input["mode"] == "manual"
    assert sfn_input["triggered_by"] == "ops@example.com"


def test_trigger_run_refuses_when_org_not_configured(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    res = rpa_api.trigger_run(
        _event(), {"orgId": "no-such-org"}, body=None,
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 409
    sfn.start_execution.assert_not_called()


def test_trigger_run_403_without_run_permission(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    _seed_config(rpa_table)
    res = rpa_api.trigger_run(
        _event(), {"orgId": "demo"}, body=None,
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    assert "Compliance Audit:run" in json.loads(res["body"])["error"]
    sfn.start_execution.assert_not_called()


def test_trigger_run_no_state_machine_arn_env(env, sfn, rpa_table,
                                              monkeypatch,
                                              mock_dynamodb):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    _seed_config(rpa_table)
    monkeypatch.delenv("RPA_STATE_MACHINE_ARN")
    with pytest.raises(RuntimeError, match="env var is not set"):
        rpa_api.trigger_run(
            _event(), {"orgId": "demo"}, body=None,
            authorize_fn=_authorize_pass_admin,
        )


# ---- GET /rpa/runs -----------------------------------------------------


def test_list_runs_filters_to_caller_org(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    # Three executions in the shared state machine: two for demo, one for
    # another org. We must only return the demo ones.
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
                    "startDate": "2026-06-11T18:00:00Z",
                    "input": json.dumps({"organization_id": "demo",
                                          "run_id": "run-a", "mode": "scheduled"})}
        if executionArn.endswith(":run-b"):
            return {"executionArn": executionArn, "status": "RUNNING",
                    "startDate": "2026-06-12T18:00:00Z",
                    "input": json.dumps({"organization_id": "other-org",
                                          "run_id": "run-b", "mode": "manual"})}
        return {"executionArn": executionArn, "status": "FAILED",
                "startDate": "2026-06-12T17:00:00Z",
                "input": json.dumps({"organization_id": "demo",
                                      "run_id": "run-c", "mode": "manual"})}

    sfn.describe_execution.side_effect = describe

    res = rpa_api.list_runs(
        _event(), {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    run_ids = sorted(it["run_id"] for it in body["items"])
    assert run_ids == ["run-a", "run-c"]


def test_list_runs_403_without_view_permission(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    res = rpa_api.list_runs(
        _event(), {"orgId": "demo"},
        authorize_fn=_authorize_pass_no_category,
    )
    assert res["statusCode"] == 403
    sfn.list_executions.assert_not_called()


def test_list_runs_honors_limit_param(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

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
    res = rpa_api.list_runs(
        evt, {"orgId": "demo"}, authorize_fn=_authorize_pass_admin,
    )
    body = json.loads(res["body"])
    assert len(body["items"]) == 3


# ---- GET /rpa/runs/{runId} ---------------------------------------------


def test_get_run_returns_describe_result(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    expected_arn = f"{_EXECUTION_ARN_PREFIX}:run-known"
    sfn.describe_execution.return_value = {
        "executionArn": expected_arn,
        "status": "SUCCEEDED",
        "startDate": "2026-06-12T18:00:00Z",
        "stopDate": "2026-06-12T18:02:00Z",
        "input": json.dumps({"organization_id": "demo",
                              "run_id": "run-known", "mode": "manual",
                              "triggered_by": "ops@example.com"}),
        "output": json.dumps({"note_count": 12,
                               "ingest_date": "2026-06-12"}),
    }

    res = rpa_api.get_run(
        _event(), {"orgId": "demo", "runId": "run-known"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 200
    body = json.loads(res["body"])
    assert body["run_id"] == "run-known"
    assert body["status"] == "SUCCEEDED"
    assert body["output"]["note_count"] == 12

    # The handler must reconstruct the execution ARN deterministically.
    sfn.describe_execution.assert_called_once_with(executionArn=expected_arn)


def test_get_run_404_for_wrong_org(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    sfn.describe_execution.return_value = {
        "executionArn": f"{_EXECUTION_ARN_PREFIX}:run-other",
        "status": "SUCCEEDED",
        "input": json.dumps({"organization_id": "other-org",
                              "run_id": "run-other", "mode": "scheduled"}),
    }

    res = rpa_api.get_run(
        _event(), {"orgId": "demo", "runId": "run-other"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 404


def test_get_run_404_when_execution_missing(
    env, sfn, rpa_table, mock_dynamodb, monkeypatch
):
    from audit import emitter as emitter_mod
    monkeypatch.setattr(emitter_mod, "_table",
                        mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())

    from botocore.exceptions import ClientError
    sfn.describe_execution.side_effect = ClientError(
        {"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution"
    )

    res = rpa_api.get_run(
        _event(), {"orgId": "demo", "runId": "run-nope"},
        authorize_fn=_authorize_pass_admin,
    )
    assert res["statusCode"] == 404
