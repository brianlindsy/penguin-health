"""HTTP handlers for the RPA integration's admin surface.

Wired into admin_api.py's dispatch dict. Four endpoints:

    GET    /api/organizations/{orgId}/rpa/config
    POST   /api/organizations/{orgId}/rpa/run
    GET    /api/organizations/{orgId}/rpa/runs
    GET    /api/organizations/{orgId}/rpa/runs/{runId}

Scope is intentionally minimal. RPA is an ingestion path, not a UI
surface — extracted notes feed the rules engine via S3 and are reviewed
through the existing validation-results UI. The on-demand `run` endpoint
exists for engineers and operators to kick a manual pass; the listing
endpoints exist so an operator can see whether scheduled passes
succeeded without dropping into the Step Functions console.

Authorization:
  All four routes require the caller to have `view` or `run` on the
  `Compliance Audit` category for the org — RPA is a compliance-audit
  data source, so gating it on that category keeps the permission model
  consistent without inventing a new category for one integration.

The state-machine ARN is read from `RPA_STATE_MACHINE_ARN` (set by the
CDK component in `infra/components/rpa.py`). Per-org dispatch happens
inside the state machine, not in this Lambda — we pass `org_id` as the
SFN input.
"""

import json
import os
import uuid

import boto3
from botocore.exceptions import ClientError

import permissions as perms_module
from audit import audited
from rpa import config as rpa_config
from rpa.exceptions import RpaOrgNotConfigured


_STATE_MACHINE_ARN_ENV = "RPA_STATE_MACHINE_ARN"
_RPA_PERMISSION_CATEGORY = "Compliance Audit"
_RUNS_LIST_DEFAULT_LIMIT = 25
_RUNS_LIST_MAX_LIMIT = 100

# Lazy-cached; tests may rebind via _reset_for_tests.
_sfn_client = None


def _resolve_sfn():
    global _sfn_client
    if _sfn_client is None:
        _sfn_client = boto3.client("stepfunctions")
    return _sfn_client


def _reset_for_tests(client=None):
    global _sfn_client
    _sfn_client = client


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _ensure_can_view(claims, org_id):
    return perms_module.can_view_category(claims, org_id,
                                          _RPA_PERMISSION_CATEGORY)


def _ensure_can_run(claims, org_id):
    return perms_module.can_run_category(claims, org_id,
                                         _RPA_PERMISSION_CATEGORY)


def _state_machine_arn():
    arn = os.environ.get(_STATE_MACHINE_ARN_ENV)
    if not arn:
        raise RuntimeError(
            f"{_STATE_MACHINE_ARN_ENV} env var is not set; "
            "the RPA CDK component must wire it into this Lambda."
        )
    return arn


# Fields that should never be surfaced to the API. None of these live in
# the RPA_CONFIG item today, but we filter defensively in case a future
# rev. of the schema adds something the schema-validator misses.
_REDACT_KEYS_FROM_CONFIG = frozenset({
    "client_secret", "access_token", "refresh_token", "api_key",
    "token", "secret", "password",
})


def _scrub_config(item):
    """Deep-redact obviously-sensitive keys before returning RPA_CONFIG.

    Defense in depth — credentials live in Secrets Manager, not in the
    DDB item; if anything sensitive ever creeps in we drop it before
    sending. Operators wanting credentials use the Secrets Manager
    console directly.
    """
    if isinstance(item, dict):
        return {
            k: ("[REDACTED]" if k.lower() in _REDACT_KEYS_FROM_CONFIG
                else _scrub_config(v))
            for k, v in item.items()
        }
    if isinstance(item, list):
        return [_scrub_config(v) for v in item]
    return item


# ---- GET /api/organizations/{orgId}/rpa/config -------------------------


@audited(action="read", resource_type="RpaConfig",
         purpose_of_use="OPERATIONS", call_type="rpa_config_get")
def get_config(event, path_params, authorize_fn, **_):
    org_id = path_params.get("orgId")
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not _ensure_can_view(claims, org_id):
        return _response(403, {"error":
                               f"{_RPA_PERMISSION_CATEGORY}:view permission required"})
    try:
        item = rpa_config.load_rpa_config(org_id)
    except RpaOrgNotConfigured as e:
        return _response(404, {"error": str(e)})
    return _response(200, {"config": _scrub_config(item)})


# ---- POST /api/organizations/{orgId}/rpa/run ---------------------------


@audited(action="execute", resource_type="RpaPlaybookRun",
         purpose_of_use="OPERATIONS", call_type="rpa_run_trigger")
def trigger_run(event, path_params, body, authorize_fn, **_):
    org_id = path_params.get("orgId")
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not _ensure_can_run(claims, org_id):
        return _response(403, {"error":
                               f"{_RPA_PERMISSION_CATEGORY}:run permission required"})

    # Refuse to enqueue a run for an org that isn't configured / is
    # disabled. The runner itself would also catch this, but failing
    # fast at the API layer surfaces the misconfiguration before a
    # Fargate task spins up.
    try:
        rpa_config.load_rpa_config(org_id)
    except RpaOrgNotConfigured as e:
        return _response(409, {"error": str(e)})

    run_id = f"run-{uuid.uuid4()}"
    sfn = _resolve_sfn()
    try:
        execution = sfn.start_execution(
            stateMachineArn=_state_machine_arn(),
            # SFN execution names must be 1-80 chars; uuid+prefix fits.
            name=run_id,
            input=json.dumps({
                "organization_id": org_id,
                "run_id": run_id,
                "mode": "manual",
                "triggered_by": claims.get("email"),
            }),
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        return _response(502, {"error": f"StartExecution failed: {code}"})

    return _response(202, {
        "run_id": run_id,
        "execution_arn": execution.get("executionArn"),
        "started_at": execution.get("startDate"),
    })


# ---- GET /api/organizations/{orgId}/rpa/runs ---------------------------


@audited(action="read", resource_type="RpaPlaybookRun",
         purpose_of_use="OPERATIONS", call_type="rpa_runs_list")
def list_runs(event, path_params, authorize_fn, **_):
    org_id = path_params.get("orgId")
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not _ensure_can_view(claims, org_id):
        return _response(403, {"error":
                               f"{_RPA_PERMISSION_CATEGORY}:view permission required"})

    qs = event.get("queryStringParameters") or {}
    try:
        limit = min(int(qs.get("limit") or _RUNS_LIST_DEFAULT_LIMIT),
                    _RUNS_LIST_MAX_LIMIT)
    except (TypeError, ValueError):
        limit = _RUNS_LIST_DEFAULT_LIMIT

    status_filter = qs.get("status")  # optional: RUNNING, SUCCEEDED, FAILED, ...
    sfn = _resolve_sfn()
    kwargs = {
        "stateMachineArn": _state_machine_arn(),
        "maxResults": limit * 4,  # over-fetch; we filter to this org below
    }
    if status_filter:
        kwargs["statusFilter"] = status_filter

    try:
        resp = sfn.list_executions(**kwargs)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        return _response(502, {"error": f"ListExecutions failed: {code}"})

    # The state machine is shared across orgs; filter by execution name
    # prefix wouldn't help because we don't encode org_id in the name.
    # We fetch DescribeExecution input for each candidate up to `limit`.
    out = []
    for exec_item in resp.get("executions", []):
        if len(out) >= limit:
            break
        described = _describe_for_org(sfn, exec_item["executionArn"],
                                      org_id)
        if described is None:
            continue
        out.append(described)

    return _response(200, {"items": out})


def _describe_for_org(sfn, execution_arn, org_id):
    try:
        d = sfn.describe_execution(executionArn=execution_arn)
    except ClientError:
        return None
    raw_input = d.get("input") or "{}"
    try:
        parsed = json.loads(raw_input)
    except json.JSONDecodeError:
        return None
    if parsed.get("organization_id") != org_id:
        return None
    return {
        "run_id": parsed.get("run_id"),
        "execution_arn": d.get("executionArn"),
        "status": d.get("status"),
        "started_at": d.get("startDate"),
        "stopped_at": d.get("stopDate"),
        "mode": parsed.get("mode"),
        "triggered_by": parsed.get("triggered_by"),
    }


# ---- GET /api/organizations/{orgId}/rpa/runs/{runId} -------------------


@audited(action="read", resource_type="RpaPlaybookRun",
         purpose_of_use="OPERATIONS", call_type="rpa_run_get")
def get_run(event, path_params, authorize_fn, **_):
    org_id = path_params.get("orgId")
    run_id = path_params.get("runId")
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not _ensure_can_view(claims, org_id):
        return _response(403, {"error":
                               f"{_RPA_PERMISSION_CATEGORY}:view permission required"})

    # The SFN execution ARN is `${stateMachineArn}:${executionName}` and
    # we name executions `run-{uuid}`, so we can reconstruct it.
    sm_arn = _state_machine_arn()
    # An SFN state-machine ARN looks like
    #   arn:aws:states:REGION:ACCT:stateMachine:NAME
    # and an execution ARN looks like
    #   arn:aws:states:REGION:ACCT:execution:NAME:EXECUTION_NAME
    # We need to swap `stateMachine` -> `execution` and append :run_id.
    execution_arn = sm_arn.replace(":stateMachine:", ":execution:", 1) \
                          + ":" + run_id

    sfn = _resolve_sfn()
    try:
        d = sfn.describe_execution(executionArn=execution_arn)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        if code in {"ExecutionDoesNotExist", "InvalidArn"}:
            return _response(404, {"error": "run not found"})
        return _response(502, {"error": f"DescribeExecution failed: {code}"})

    # Org isolation: refuse to return executions belonging to a different
    # org even if the caller knows the run_id.
    try:
        parsed = json.loads(d.get("input") or "{}")
    except json.JSONDecodeError:
        parsed = {}
    if parsed.get("organization_id") != org_id:
        return _response(404, {"error": "run not found"})

    return _response(200, {
        "run_id": run_id,
        "execution_arn": d.get("executionArn"),
        "status": d.get("status"),
        "started_at": d.get("startDate"),
        "stopped_at": d.get("stopDate"),
        "mode": parsed.get("mode"),
        "triggered_by": parsed.get("triggered_by"),
        # SFN's `output` is the final state's payload; include verbatim
        # so an operator can see the runner's `result` summary fields
        # (note_count, ingest_date, vendor) without a console roundtrip.
        "output": _safe_parse_json(d.get("output")),
        "error": d.get("error"),
        "cause": d.get("cause"),
    })


def _safe_parse_json(maybe_json):
    if not maybe_json:
        return None
    try:
        return json.loads(maybe_json)
    except (TypeError, json.JSONDecodeError):
        return maybe_json
