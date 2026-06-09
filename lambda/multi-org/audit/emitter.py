"""`emit` — the single durable write path for every audit event.

Two writes per call:
  1. DynamoDB PutItem on penguin-health-audit (synchronous, fast). Uses
     ConditionExpression='attribute_not_exists(pk)' so retries are
     idempotent on the same event_id.
  2. Firehose PutRecord on penguin-health-audit (asynchronous, two
     retries with jitter). Firehose batches every 60s into the WORM S3
     bucket.

Failures are recorded but NEVER raise into the request path. The
decorator and explicit callers can therefore wrap PHI operations
unconditionally — a flaky audit substrate must not break a real request.

CloudWatch metrics:
  PenguinHealth/Audit AuditEmitFailure   — DDB write failed
  PenguinHealth/Audit FirehosePutFailure — Firehose put failed
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .schema import build_event

logger = logging.getLogger(__name__)


# 90 days, matching the TTL on the penguin-health-audit DDB table.
_NINETY_DAYS_SECONDS = 90 * 24 * 60 * 60

_TABLE_NAME = os.environ.get("AUDIT_TABLE_NAME", "penguin-health-audit")
_STREAM_NAME = os.environ.get("AUDIT_FIREHOSE_NAME", "penguin-health-audit")
_METRIC_NAMESPACE = "PenguinHealth/Audit"

# Lazily-resolved AWS clients. `_resolve_*` snapshots the client on first
# call and reuses it for the life of the process — Lambda warms reuse
# the HTTP connection. Late binding matters for tests (moto's mock
# context wraps boto3.client only while active) and for callers that
# monkeypatch the module-level handles in unit tests.
_table = None
_firehose = None
_cloudwatch = None


def _resolve_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(_TABLE_NAME)
    return _table


def _resolve_firehose():
    global _firehose
    if _firehose is None:
        _firehose = boto3.client("firehose")
    return _firehose


def _resolve_cloudwatch():
    global _cloudwatch
    if _cloudwatch is None:
        _cloudwatch = boto3.client("cloudwatch")
    return _cloudwatch


def _reset_for_tests():
    """Drop the cached AWS clients. Useful when a test wraps the emitter
    with a fresh moto context — the next call to `_resolve_*` will
    instantiate clients against the active boto3 session."""
    global _table, _firehose, _cloudwatch
    _table = None
    _firehose = None
    _cloudwatch = None


def emit(*,
         action: str,
         resource: dict,
         actor: dict,
         org_id: str,
         outcome: str = "success",
         purpose_of_use: str = "OPERATIONS",
         request_id: str | None = None,
         source_lambda: str | None = None,
         patient: dict | None = None,
         member_id: str | None = None,
         payer: dict | None = None,
         call_type: str | None = None,
         external_control_number: str | None = None,
         duration_ms: int | None = None,
         result: dict | None = None,
         http_status: int | None = None,
         error_class: str | None = None) -> str:
    """Write one audit event to DDB (sync) + Firehose (async). Returns
    the event_id so callers can echo it in HTTP responses or persist a
    link from a domain row.

    Never raises. Callers MUST NOT wrap this in try/except for the
    purpose of "handling audit failures" — failures are logged + emitted
    as CloudWatch metrics here.
    """
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    event_time = now.isoformat()

    event = build_event(
        event_id=event_id,
        event_time=event_time,
        action=action,
        outcome=outcome,
        purpose_of_use=purpose_of_use,
        org_id=org_id,
        actor=actor,
        resource=resource,
        source_lambda=source_lambda or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
        request_id=request_id,
        patient=patient,
        member_id=member_id,
        payer=payer,
        call_type=call_type,
        external_control_number=external_control_number,
        duration_ms=duration_ms,
        result=result,
        http_status=http_status,
        error_class=error_class,
    )

    _write_ddb(event_id, event_time, event, org_id)
    _write_firehose(event_id, event)
    return event_id


# ----- DDB write ----------------------------------------------------------

def _write_ddb(event_id: str, event_time: str, event: dict, org_id: str) -> None:
    """Synchronously persist to the hot mirror.

    Keys mirror stedi/audit.py exactly so `recent_checks_for_patient`
    keeps working after Phase 2 cutover:
      pk    = ORG#{org_id}
      sk    = AUDIT#{iso_ts}#{event_id}
      gsi1pk = PATIENT#{org_id}#{patient_hash}   (when patient present)
      gsi1sk = {iso_ts}
    """
    expires_at = int(datetime.now(timezone.utc).timestamp()) + _NINETY_DAYS_SECONDS
    item = {
        # Top-level DDB keys + searchable fields are stored as native
        # attributes; the full event lives under `event` for replay
        # (e.g. a future S3 backfill Lambda re-derives Firehose records
        # straight from these rows).
        "pk": f"ORG#{org_id}",
        "sk": f"AUDIT#{event_time}#{event_id}",
        "expires_at": expires_at,
        # Frequently-queried fields hoisted to top-level so DDB FilterExpression
        # and the existing Stedi dedup-query path don't need to crack `event`.
        "event_id": event_id,
        "event_time": event_time,
        "org_id": org_id,
        "action": event.get("action"),
        "outcome": event.get("outcome"),
        "agent_email": event.get("agent_email"),
        "agent_id": event.get("agent_id"),
        "resource_type": event.get("resource_type"),
        "resource_id": event.get("resource_id"),
        "call_type": event.get("call_type"),
        "patient_hash": event.get("patient_hash"),
        "patient_first_initial": event.get("patient_first_initial"),
        "patient_last_initial": event.get("patient_last_initial"),
        "patient_dob": event.get("patient_dob"),
        "member_id_last4": event.get("member_id_last4"),
        "payer_id": event.get("payer_id"),
        "payer_name": event.get("payer_name"),
        # Snapshot of the full event for replay / debugging.
        "event": event,
    }
    if event.get("patient_hash"):
        item["gsi1pk"] = f"PATIENT#{org_id}#{event['patient_hash']}"
        item["gsi1sk"] = event_time

    try:
        _resolve_table().put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            # event_id collision — only happens on retry, and the existing
            # row already contains the same event. Not an error.
            return
        _report_failure("AuditEmitFailure", event_id, code or "ClientError")
    except Exception as e:  # noqa: BLE001 — emit must never raise
        _report_failure("AuditEmitFailure", event_id, type(e).__name__)


# ----- Firehose write -----------------------------------------------------

_FIREHOSE_MAX_RETRIES = 2
_FIREHOSE_BASE_DELAY_SEC = 0.1


def _write_firehose(event_id: str, event: dict) -> None:
    """Best-effort async ship to the WORM archive. Two retries on
    transient errors, then drop + record a metric. The DDB row is the
    durability guarantee — a missed Firehose put is reconstructible.
    """
    # Firehose's JSON deserializer needs a trailing newline OR the
    # AppendDelimiterToRecord processor (configured in the CDK component)
    # to split records. We include the newline ourselves as a belt-and-
    # suspenders so the records remain parseable even if the processor
    # is misconfigured later.
    payload = (json.dumps(event, default=str) + "\n").encode("utf-8")

    last_err: str | None = None
    for attempt in range(_FIREHOSE_MAX_RETRIES + 1):
        try:
            _resolve_firehose().put_record(
                DeliveryStreamName=_STREAM_NAME,
                Record={"Data": payload},
            )
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code") or "ClientError"
            last_err = code
            # Permanent errors (missing stream, IAM denied) won't recover
            # from a retry. Fail fast — retrying would only inflate the
            # latency of the host operation.
            if code in (
                "ResourceNotFoundException",
                "AccessDeniedException",
                "ValidationException",
            ):
                break
        except Exception as e:  # noqa: BLE001
            last_err = type(e).__name__
        # Linear-with-jitter retry; Firehose throttling is rare and a
        # single retry usually resolves it.
        if attempt < _FIREHOSE_MAX_RETRIES:
            time.sleep(_FIREHOSE_BASE_DELAY_SEC * (attempt + 1)
                       + random.uniform(0, 0.05))

    _report_failure("FirehosePutFailure", event_id, last_err or "Unknown")


# ----- Failure reporting --------------------------------------------------

def _report_failure(metric_name: str, event_id: str, error_class: str) -> None:
    """Best-effort: log + emit a CloudWatch metric. Swallow any error
    from CloudWatch itself — losing the metric is worse than crashing
    the request, but only marginally."""
    logger.error(
        "audit_failure metric=%s event_id=%s err=%s",
        metric_name, event_id, error_class,
    )
    try:
        _resolve_cloudwatch().put_metric_data(
            Namespace=_METRIC_NAMESPACE,
            MetricData=[{
                "MetricName": metric_name,
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "ErrorClass", "Value": error_class},
                ],
            }],
        )
    except Exception:  # noqa: BLE001
        # CloudWatch failure during audit failure — log and move on. The
        # ERROR-level log line above is the audit-of-last-resort.
        logger.exception("audit_metric_emit_failed metric=%s", metric_name)
