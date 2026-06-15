"""Fargate task entry point for one RPA run.

Reads `ORG_ID`, `RUN_ID`, and `MODE` from the environment (set by the
Step Functions state machine that invokes this task), then:

  1. Loads RPA_CONFIG + playbook from DynamoDB.
  2. Calls usage_guard — clean abort if outside window or blackout date.
  3. Emits `run_started` audit event.
  4. Authenticates against the vendor (per-vendor module dispatched
     from RPA_CONFIG.vendor).
  5. Launches Playwright + drives the playbook engine.
  6. For each yielded note: persists JSON to per-org S3 + emits a
     `read ClinicalNote` audit event.
  7. Emits `run_completed` audit + an EventBridge `RpaIngestComplete`
     event so downstream consumers (rules engine, future materializers)
     can act on the new files without coupling to the runner.

Failure modes are all explicit: each `except` either re-raises after
emitting a failure audit, or emits a `minor-failure` audit and exits 0
(for usage-guard outside-window aborts — they're expected, not errors).

PHI handling:
  * The note body and HTML are written to S3 only (encrypted via the
    per-org bucket's KMS configuration). They never enter logs.
  * Patient identifiers pass through to the result_writer where the
    audit emitter slims them to hash + initials + last-4. Raw values
    live in-memory and in the encrypted S3 payload only.
  * The bot's access token + session cookie are in-memory only —
    they do not persist between Fargate task invocations.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import boto3

from audit import SystemPrincipal, emit as audit_emit

from rpa import authenticator, config, usage_guard
from rpa.exceptions import (
    RpaAuthError,
    RpaError,
    RpaOrgNotConfigured,
    RpaOutsideWindow,
    RpaPlaybookError,
    RpaPlaybookNotFound,
    RpaUnsupportedVendor,
)
from rpa.playbook_engine import execute as run_playbook
from rpa.playbook_engine_playwright import PlaywrightPage
from rpa.rate_limiter import RateLimiter
from rpa.result_writer import persist_note


_DEFAULT_NAV_TIMEOUT_MS = 15_000
_EVENTBRIDGE_SOURCE = "penguin-health.rpa"
_INGEST_COMPLETE_DETAIL_TYPE = "RpaIngestComplete"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _ingest_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _actor(org_id: str, vendor: str, playbook_id: str,
           playbook_version: int) -> dict:
    actor = SystemPrincipal(f"rpa-runner+{org_id}").as_actor()
    actor["user_agent"] = (
        f"rpa-runner/{vendor}/playbook={playbook_id}@v{playbook_version}"
    )
    actor["agent_groups"] = ["rpa_bot"]
    return actor


def _resource_run(run_id: str, org_id: str) -> dict:
    return {"type": "RpaPlaybookRun", "id": run_id, "org": org_id}


def _resource_auth(org_id: str) -> dict:
    return {"type": "RpaAuthSession", "id": org_id, "org": org_id}


def _emit_ingest_complete(*, org_id: str, ingest_date: str,
                          playbook_run_id: str, note_count: int,
                          vendor: str) -> None:
    boto3.client("events").put_events(Entries=[{
        "Source": _EVENTBRIDGE_SOURCE,
        "DetailType": _INGEST_COMPLETE_DETAIL_TYPE,
        "Detail": json.dumps({
            "organization_id": org_id,
            "ingest_date": ingest_date,
            "playbook_run_id": playbook_run_id,
            "note_count": note_count,
            "vendor": vendor,
        }),
    }])


# ----- core run loop -----------------------------------------------------


async def _drive_playwright_run(
    *,
    org_id: str,
    org_cfg: dict,
    playbook: dict,
    auth_session: dict,
    run_id: str,
    actor: dict,
    rate_limiter: RateLimiter,
    now: datetime,
):
    """Launch Playwright, set cookies + headers from the auth session,
    run the playbook, and persist each yielded note. Returns the count
    of persisted notes.
    """
    # Defer the import so the unit-test environment doesn't need
    # Playwright installed.
    from playwright.async_api import async_playwright  # type: ignore

    persisted = 0
    ingest_date = _ingest_date(now)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                extra_http_headers=auth_session.get("extra_http_headers") or {},
            )
            if auth_session.get("cookies"):
                await context.add_cookies(_normalize_cookies(
                    auth_session["cookies"]))
            page = await context.new_page()
            page.set_default_navigation_timeout(_DEFAULT_NAV_TIMEOUT_MS)

            adapter = PlaywrightPage(page)
            async for extraction in run_playbook(
                playbook, adapter, rate_limiter=rate_limiter,
            ):
                persist_note(
                    extraction=extraction,
                    org_id=org_id,
                    vendor=org_cfg["vendor"],
                    playbook_run_id=run_id,
                    captured_at=_iso(_now_utc()),
                    ingest_date=ingest_date,
                    captured_at_compact=_compact(_now_utc()),
                    actor=actor,
                )
                persisted += 1
        finally:
            await browser.close()

    return persisted, ingest_date


def _normalize_cookies(cookies: list[dict]) -> list[dict]:
    """Playwright requires `url` or (`domain` + `path`). Our authenticator
    returns the latter; pass through unchanged but drop falsy domain entries
    so Playwright's validator doesn't reject them.
    """
    out = []
    for c in cookies:
        if not c.get("domain"):
            continue
        out.append({
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path") or "/",
            "secure": bool(c.get("secure")),
        })
    return out


def _run(org_id: str, run_id: str, mode: str) -> int:
    started_at = _now_utc()

    # Step 1: load config + playbook
    try:
        org_cfg = config.load_rpa_config(org_id)
    except RpaOrgNotConfigured as e:
        print(f"rpa-runner: not configured: {e}", file=sys.stderr)
        return 2
    try:
        playbook = config.load_playbook(org_id, org_cfg["playbook_id"])
    except RpaPlaybookNotFound as e:
        print(f"rpa-runner: playbook missing: {e}", file=sys.stderr)
        return 2

    vendor = org_cfg["vendor"]
    playbook_id = org_cfg["playbook_id"]
    playbook_version = int(playbook.get("version") or 1)

    actor = _actor(org_id, vendor, playbook_id, playbook_version)

    # Step 2: usage guard. Outside-window is NOT an error — it's a
    # clean skip; the next scheduled run will pick up the work.
    try:
        usage_guard.check_or_raise(org_cfg, started_at)
    except RpaOutsideWindow as e:
        audit_emit(
            action="execute",
            resource=_resource_run(run_id, org_id),
            actor=actor,
            org_id=org_id,
            outcome="minor-failure",
            purpose_of_use="OPERATIONS",
            call_type="rpa_playbook_run",
            external_control_number=run_id,
            error_class="RpaOutsideWindow",
            result={"vendor": vendor, "reason": str(e), "mode": mode},
        )
        print(f"rpa-runner: skipped (outside window): {e}", file=sys.stderr)
        return 0

    # Step 3: run_started
    audit_emit(
        action="execute",
        resource=_resource_run(run_id, org_id),
        actor=actor,
        org_id=org_id,
        purpose_of_use="OPERATIONS",
        call_type="rpa_playbook_run_started",
        external_control_number=run_id,
        result={"vendor": vendor, "mode": mode},
    )

    # Step 4: authenticate
    try:
        auth_session = authenticator.authenticate(org_id, org_cfg)
    except (RpaAuthError, RpaUnsupportedVendor) as e:
        audit_emit(
            action="execute",
            resource=_resource_auth(org_id),
            actor=actor,
            org_id=org_id,
            outcome="major-failure",
            purpose_of_use="OPERATIONS",
            call_type="rpa_auth",
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
            call_type="rpa_playbook_run",
            external_control_number=run_id,
            error_class=type(e).__name__,
        )
        print(f"rpa-runner: auth failed: {type(e).__name__}", file=sys.stderr)
        return 1

    rate_limiter = RateLimiter(
        min_ms=int(org_cfg.get("guardrails", {}).get(
            "rate_limit_ms_between_requests", 0)),
    )

    # Steps 5+6: drive Playwright, persist each yielded note
    persisted = 0
    outcome = "success"
    error_class = None
    ingest_date = _ingest_date(started_at)
    try:
        persisted, ingest_date = asyncio.run(_drive_playwright_run(
            org_id=org_id,
            org_cfg=org_cfg,
            playbook=playbook,
            auth_session=auth_session,
            run_id=run_id,
            actor=actor,
            rate_limiter=rate_limiter,
            now=started_at,
        ))
    except RpaPlaybookError as e:
        outcome = "major-failure"
        error_class = "RpaPlaybookError"
        print(f"rpa-runner: playbook error: {type(e).__name__}",
              file=sys.stderr)
    except RpaError as e:
        outcome = "major-failure"
        error_class = type(e).__name__
        print(f"rpa-runner: rpa error: {type(e).__name__}", file=sys.stderr)
    except Exception as e:  # noqa: BLE001
        outcome = "serious-failure"
        error_class = type(e).__name__
        print(f"rpa-runner: unexpected error: {type(e).__name__}",
              file=sys.stderr)

    # Step 7: run_completed audit + EventBridge ingest-complete
    duration_ms = int((_now_utc() - started_at).total_seconds() * 1000)
    audit_emit(
        action="execute",
        resource=_resource_run(run_id, org_id),
        actor=actor,
        org_id=org_id,
        outcome=outcome,
        purpose_of_use="OPERATIONS",
        call_type="rpa_playbook_run_completed",
        external_control_number=run_id,
        duration_ms=duration_ms,
        error_class=error_class,
        result={"vendor": vendor, "note_count": persisted,
                "ingest_date": ingest_date, "mode": mode},
    )

    # Emit even on partial failure — downstream consumers will use the
    # note_count to know what's available; an empty ingest is still a
    # signal worth publishing.
    try:
        _emit_ingest_complete(
            org_id=org_id, ingest_date=ingest_date,
            playbook_run_id=run_id, note_count=persisted, vendor=vendor,
        )
    except Exception as e:  # noqa: BLE001
        # Don't fail the run if EventBridge is having a bad day; the
        # audit already records the completion.
        print(f"rpa-runner: warning: PutEvents failed: {type(e).__name__}",
              file=sys.stderr)

    return 0 if outcome == "success" else 1


def main() -> int:
    org_id = os.environ.get("ORG_ID")
    run_id = os.environ.get("RUN_ID") or str(uuid.uuid4())
    mode = os.environ.get("MODE", "scheduled")
    if not org_id:
        print("rpa-runner: ORG_ID env var is required", file=sys.stderr)
        return 2
    return _run(org_id, run_id, mode)


if __name__ == "__main__":
    raise SystemExit(main())
