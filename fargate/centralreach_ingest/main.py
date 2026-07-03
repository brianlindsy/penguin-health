"""Fargate task entry point for one CentralReach ingest run.

Reads `ORG_ID`, `RUN_ID`, and `MODE` from the environment (set by the
Step Functions state machine that invokes this task), then:

  1. Loads CENTRALREACH_CONFIG from DynamoDB.
  2. Calls usage_guard — clean abort if outside window or blackout date.
  3. Emits `run_started` audit event.
  4. Authenticates against CR (per `centralreach.auth.Authenticator`).
  5. Constructs a `CentralReachClient` and drives `pipeline.run_ingest`.
  6. Each persisted note emits its own `read ClinicalNote` audit via
     `result_writer.persist_note`.
  7. Emits `run_completed` audit + an EventBridge `IngestComplete`
     event so downstream consumers (rules engine, future materializers)
     can act on the new files without coupling to the runner.

Failure modes are all explicit: each `except` either re-raises after
emitting a failure audit, or emits a `minor-failure` audit and exits 0
(for usage-guard outside-window aborts — they're expected, not errors).

PHI handling:
  * The PDF bytes are written to S3 only (encrypted via the per-org
    bucket's KMS configuration). They never enter logs.
  * Patient identifiers pass through to the result_writer where the
    audit emitter slims them to hash + initials + last-4. Raw values
    live in-memory and in the encrypted S3 payload only.
  * The bot's session cookies + CSRF token are in-memory only — they
    do not persist between Fargate task invocations.

Auth gap (PR A's PlaceholderAuthenticator):
  The runner currently wires `PlaceholderAuthenticator`, which raises
  CentralReachAuthError at startup. See
  `docs/centralreach-api-integration.md` Open Questions for the auth
  flow that needs to land before this task can run end-to-end.
  Replace the import of PlaceholderAuthenticator with the real one
  in `_make_authenticator` below when the auth gap resolves.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3

from audit import SystemPrincipal, emit as audit_emit

from centralreach import config as cr_config
from centralreach import usage_guard
from centralreach.auth import Authenticator, OAuthAuthenticator
from centralreach.client import CentralReachClient
from centralreach.config import CentralReachOrgNotConfigured
from centralreach.exceptions import (
    CentralReachAuthError,
    CentralReachError,
)
from centralreach.parameters import resolve_date_range
from centralreach.pipeline import IngestSummary, run_ingest
from centralreach.rate_limiter import RateLimiter
from centralreach.usage_guard import CentralReachOutsideWindow


_EVENTBRIDGE_SOURCE = "penguin-health.centralreach"
_INGEST_COMPLETE_DETAIL_TYPE = "CentralReachIngestComplete"
_VENDOR = "centralreach"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ingest_date(dt: datetime) -> str:
    # Partition folder tracks the org's clinical day, not wall-clock UTC:
    # a cron firing just after 00:00 UTC still belongs to the prior US day.
    # Eastern matches `parameters._yesterday_eastern` — revisit when a
    # non-Eastern org lands.
    return dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _actor(org_id: str) -> dict:
    actor = SystemPrincipal(f"centralreach-ingest+{org_id}").as_actor()
    actor["user_agent"] = f"centralreach-ingest/{org_id}"
    actor["agent_groups"] = ["centralreach_bot"]
    return actor


def _resource_run(run_id: str, org_id: str) -> dict:
    return {"type": "CentralReachIngestRun", "id": run_id, "org": org_id}


def _resource_auth(org_id: str) -> dict:
    return {"type": "CentralReachAuthSession", "id": org_id, "org": org_id}


def _emit_ingest_complete(*, org_id: str, ingest_date: str,
                          run_id: str, summary: IngestSummary) -> None:
    boto3.client("events").put_events(Entries=[{
        "Source": _EVENTBRIDGE_SOURCE,
        "DetailType": _INGEST_COMPLETE_DETAIL_TYPE,
        "Detail": json.dumps({
            "organization_id": org_id,
            "ingest_date": ingest_date,
            "run_id": run_id,
            "vendor": _VENDOR,
            "processed_count": summary.processed_count,
            "failure_count": summary.failure_count,
            "skipped_count": summary.skipped_count,
            "failures_by_type": summary.failures_by_type,
            "skipped_by_reason": summary.skipped_by_reason,
        }),
    }])


def _make_authenticator(
    org_cfg: dict, *, tz_offset_minutes: int,
) -> Authenticator:
    """Construct the authenticator for this run.

    Returns CR's documented `client_credentials` OAuth flow. The
    per-org `vendor_settings.centralreach` subtree (if present) can
    swap the SSO + legacy-auth URLs to a sandbox tenant; default
    URLs are documented prod endpoints. See `centralreach.auth` for
    the known limitation around per-user resource access.
    """
    vendor_settings = (org_cfg.get("vendor_settings") or {})
    vendor_cfg = vendor_settings.get("centralreach") or {}
    return OAuthAuthenticator(
        vendor_cfg=vendor_cfg,
        tz_offset_minutes=tz_offset_minutes,
    )


def _utc_offset_minutes_for_tz(tz_name: str) -> int:
    """Return the current UTC offset for `tz_name` in minutes.

    The runner uses this for the `_utcOffsetMinutes` field on CR
    request bodies AND for the `tzoffset` cookie on the session. CR
    rejects mismatched values, so a single source ensures they stay
    consistent.

    Note: this captures the offset at task startup. A run that spans
    a DST transition will still use the startup offset. For daily
    runs of ~30 minutes that's fine.
    """
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo(tz_name))
    offset = now.utcoffset()
    if offset is None:
        return 0
    return int(offset.total_seconds() // 60)


def _run(org_id: str, run_id: str, mode: str) -> int:
    started_at = _now_utc()

    # Step 1: load config
    try:
        org_cfg = cr_config.load_centralreach_config(org_id)
    except CentralReachOrgNotConfigured as e:
        print(f"centralreach-ingest: not configured: {e}", file=sys.stderr)
        return 2

    actor = _actor(org_id)

    # Step 2: usage guard. Outside-window is NOT an error — it's a
    # clean skip; the next scheduled run picks up the work.
    try:
        usage_guard.check_or_raise(org_cfg, started_at)
    except CentralReachOutsideWindow as e:
        audit_emit(
            action="execute",
            resource=_resource_run(run_id, org_id),
            actor=actor,
            org_id=org_id,
            outcome="minor-failure",
            purpose_of_use="OPERATIONS",
            call_type="centralreach_ingest_run",
            external_control_number=run_id,
            error_class="CentralReachOutsideWindow",
            result={"vendor": _VENDOR, "reason": str(e), "mode": mode},
        )
        print(f"centralreach-ingest: skipped (outside window): {e}",
              file=sys.stderr)
        return 0

    # Step 3: run_started
    audit_emit(
        action="execute",
        resource=_resource_run(run_id, org_id),
        actor=actor,
        org_id=org_id,
        purpose_of_use="OPERATIONS",
        call_type="centralreach_ingest_run_started",
        external_control_number=run_id,
        result={"vendor": _VENDOR, "mode": mode},
    )

    # Step 4: build client + authenticate
    rate_limit_ms = int(
        org_cfg.get("guardrails", {}).get(
            "rate_limit_ms_between_requests", 1500,
        )
    )
    rate_limiter = RateLimiter(min_ms=rate_limit_ms)

    tz_name = org_cfg["guardrails"]["timezone"]
    utc_offset_minutes = _utc_offset_minutes_for_tz(tz_name)

    client = CentralReachClient(
        org_id=org_id,
        authenticator=_make_authenticator(
            org_cfg, tz_offset_minutes=utc_offset_minutes,
        ),
        rate_limiter=rate_limiter,
        base_url=org_cfg.get("base_url") or "https://members.centralreach.com",
    )

    try:
        client.authenticate()
    except CentralReachAuthError as e:
        audit_emit(
            action="execute",
            resource=_resource_auth(org_id),
            actor=actor,
            org_id=org_id,
            outcome="major-failure",
            purpose_of_use="OPERATIONS",
            call_type="centralreach_auth",
            external_control_number=run_id,
            error_class=type(e).__name__,
        )
        audit_emit(
            action="execute",
            resource=_resource_run(run_id, org_id),
            actor=actor,
            org_id=org_id,
            outcome="major-failure",
            purpose_of_use="OPERATIONS",
            call_type="centralreach_ingest_run",
            external_control_number=run_id,
            error_class=type(e).__name__,
        )
        print(f"centralreach-ingest: auth failed: {type(e).__name__}: {e}",
              file=sys.stderr)
        return 1

    # Steps 5+6: drive ingest, persist each yielded note
    summary = IngestSummary()
    outcome = "success"
    error_class: str | None = None
    ingest_date = _ingest_date(started_at)

    try:
        date_range = resolve_date_range()
        summary = run_ingest(
            client=client,
            org_id=org_id,
            date_range=date_range,
            actor=actor,
            utc_offset_minutes=utc_offset_minutes,
            ingest_run_id=run_id,
        )
    except CentralReachError as e:
        outcome = "major-failure"
        error_class = type(e).__name__
        print(f"centralreach-ingest: error: {type(e).__name__}: {e}",
              file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
    except Exception as e:  # noqa: BLE001
        outcome = "serious-failure"
        error_class = type(e).__name__
        print(f"centralreach-ingest: unexpected error: {type(e).__name__}: {e}",
              file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

    # Step 7: run_completed audit + EventBridge ingest-complete
    duration_ms = int((_now_utc() - started_at).total_seconds() * 1000)
    audit_emit(
        action="execute",
        resource=_resource_run(run_id, org_id),
        actor=actor,
        org_id=org_id,
        outcome=outcome,
        purpose_of_use="OPERATIONS",
        call_type="centralreach_ingest_run_completed",
        external_control_number=run_id,
        duration_ms=duration_ms,
        error_class=error_class,
        result={
            "vendor": _VENDOR,
            "processed_count": summary.processed_count,
            "failure_count": summary.failure_count,
            "skipped_count": summary.skipped_count,
            "ingest_date": ingest_date,
            "mode": mode,
        },
    )

    # Emit even on partial failure — downstream consumers will use the
    # counts to know what's available; an empty ingest is still a
    # signal worth publishing.
    try:
        _emit_ingest_complete(
            org_id=org_id, ingest_date=ingest_date,
            run_id=run_id, summary=summary,
        )
    except Exception as e:  # noqa: BLE001
        # Don't fail the run if EventBridge is having a bad day; the
        # audit already records the completion.
        print(f"centralreach-ingest: warning: PutEvents failed: "
              f"{type(e).__name__}", file=sys.stderr)

    return 0 if outcome == "success" else 1


def main() -> int:
    org_id = os.environ.get("ORG_ID")
    run_id = os.environ.get("RUN_ID") or str(uuid.uuid4())
    mode = os.environ.get("MODE", "scheduled")
    if not org_id:
        print("centralreach-ingest: ORG_ID env var is required",
              file=sys.stderr)
        return 2
    return _run(org_id, run_id, mode)


if __name__ == "__main__":
    raise SystemExit(main())
