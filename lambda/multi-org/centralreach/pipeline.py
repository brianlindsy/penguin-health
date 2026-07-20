"""Per-entry orchestration for the centralreach ingest pipeline.

`run_ingest()` is the public entry point. It iterates billing entries
in the requested date range and processes each one independently —
a failure or skip on one entry does not abort the rest of the run.

Per-entry walk (matches the design doc's "Per-entry pipeline" section):

  preview = get_preview(entry.Id)
  if not preview.has_pdf_available:
      skip with reason="no_pdf_available"
  else:
      file_id = preview.first_accessible_file.id
      resource, pdf_bytes = fetch_pdf_bytes(file_id, ...)
      pdf_s3_key = write_pdf(pdf_bytes, ...)
      record = build_record(entry, preview, pdf_s3_key, ...)
      persist_note(record, identity, ...)

Failure mode handling:
  * `PdfNotAvailable` from `resources.get_resource_url` -> skip with
    reason="no_pdf_url" (the bot is authenticated but CR refused to
    issue a presigned URL — most often access-control on the file)
  * `CentralReachError` subclasses -> failure with the exception type
  * Any other exception -> failure with the exception type, captured
    and continues to the next entry rather than aborting the run
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

from .client import CentralReachClient
from .exceptions import CentralReachError
from . import ingest_cursor
from .list_query import BillingEntry, paginate_billing_entries
from .narrative_extractor import NarrativeExtractionError, extract_narrative
from .note_fields_extractor import (
    NoteFieldsExtractionError,
    extract_note_fields,
)
from .parameters import DateRange
from .pdf_storage import write_pdf
from .preview import PreviewResponse, get_preview
from .record_builder import build_record
from .resources import PdfNotAvailable, fetch_pdf_bytes
from .result_writer import IdentityForAudit, persist_note


@dataclass(frozen=True)
class IngestSummary:
    """Per-run summary the runner emits to the run_completed audit
    event. Distinguishes failure (real problem) from skip (expected
    behavior — unsigned/draft entries normally exist on any given
    day)."""

    processed_count: int = 0
    failure_count: int = 0
    skipped_count: int = 0
    failures_by_type: dict[str, int] = field(default_factory=dict)
    skipped_by_reason: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class _RunCounters:
    """Mutable per-run counters. Folded into an `IngestSummary` at
    end-of-run."""

    processed: list[int]
    failures_by_type: Counter
    skipped_by_reason: Counter


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _ingest_date(dt: datetime) -> str:
    # Partition folder tracks the org's clinical day, not wall-clock UTC:
    # a cron firing just after 00:00 UTC still belongs to the prior US day.
    # Eastern matches `parameters._today_eastern` — revisit when a
    # non-Eastern org lands.
    return dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _emit_dedupe_skip_audit(
    *,
    org_id: str,
    source_record_id: str,
    ingest_run_id: str,
    actor: dict,
    audit_emit_fn: Callable | None,
) -> None:
    """Audit an entry the runner intentionally skipped because a
    prior run already ingested it. Emitted for compliance visibility
    — the seen-vs-processed counts wouldn't otherwise reconcile.

    Contains only ids + org, no patient identity or note content.
    """
    emit = audit_emit_fn if audit_emit_fn is not None else _default_audit_emit
    emit(
        action="execute",
        resource={
            "type": "CentralReachBillingEntry",
            "id": source_record_id,
            "org": org_id,
        },
        actor=actor,
        org_id=org_id,
        purpose_of_use="OPERATIONS",
        call_type="centralreach_ingest_dedupe_skip",
        external_control_number=ingest_run_id,
    )


def _default_audit_emit(**kwargs: Any) -> None:
    """Lazy import so pipeline stays importable in contexts that don't
    ship the audit module (CLI helpers, some tests)."""
    from audit import emit as audit_emit
    audit_emit(**kwargs)


def _process_one(
    *,
    entry: BillingEntry,
    client: CentralReachClient,
    org_id: str,
    ingest_run_id: str,
    actor: dict,
    utc_offset_minutes: int,
    now_fn: Callable[[], datetime],
    counters: _RunCounters,
    s3_client: Any | None,
    audit_emit_fn: Callable | None,
    cursor_table: Any | None,
    dedupe_enabled: bool,
    force_reingest: bool,
) -> None:
    """Process a single billing entry. Records skip or failure into
    counters; does not raise."""
    source_record_id = str(entry.id)

    # Ingest-cursor dedupe: skip entries already ingested by a prior
    # run before touching CR or Bedrock. Zero CR API calls on the skip
    # path — the whole point is to save PDF fetches + Claude spend on
    # the wide-lookback window. `force_reingest` is the operator
    # escape hatch for manual reprocessing.
    if dedupe_enabled and not force_reingest:
        if ingest_cursor.has_ingested(
            org_id, source_record_id, table=cursor_table,
        ):
            counters.skipped_by_reason["already_ingested"] += 1
            _emit_dedupe_skip_audit(
                org_id=org_id,
                source_record_id=source_record_id,
                ingest_run_id=ingest_run_id,
                actor=actor,
                audit_emit_fn=audit_emit_fn,
            )
            return

    try:
        preview = get_preview(client, entry.id)
    except CentralReachError as e:
        counters.failures_by_type[type(e).__name__] += 1
        return
    except Exception as e:
        # Anything not modeled as a CentralReachError still gets
        # captured rather than aborting the run.
        counters.failures_by_type[type(e).__name__] += 1
        return

    if not preview.has_pdf_available:
        counters.skipped_by_reason["no_pdf_available"] += 1
        return

    file_resource = preview.first_accessible_file
    assert file_resource is not None  # has_pdf_available guarantees

    try:
        _resource, pdf_bytes = fetch_pdf_bytes(
            client, file_resource.id,
            utc_offset_minutes=utc_offset_minutes,
        )
    except PdfNotAvailable:
        # 200 + success=false from CR. Skip rather than fail — the
        # bot session reached the endpoint but CR refused to issue a
        # presigned URL, most often an access-control case the
        # pipeline can't recover from.
        counters.skipped_by_reason["no_pdf_url"] += 1
        return
    except CentralReachError as e:
        counters.failures_by_type[type(e).__name__] += 1
        return

    # Extract the clinical narrative once per record via Bedrock. The
    # text-based rules (1, 2, 3) read this from `record.text`; rule 11
    # still routes through the PDF document block at eval time because
    # the data it asks about (charts/percentages) isn't in the prose.
    try:
        narrative_text = extract_narrative(
            pdf_bytes,
            org_id=org_id,
            ingest_run_id=ingest_run_id,
        )
    except NarrativeExtractionError:
        counters.skipped_by_reason["narrative_extract_failed"] += 1
        return

    # Extract the five structured fields the provider wrote on the
    # note (location, billed time, signature names, supervisor
    # presence). Distinct from the values CR's API returned — rules
    # 6/7 cross-check the two sources.
    try:
        note_fields = extract_note_fields(
            pdf_bytes,
            org_id=org_id,
            ingest_run_id=ingest_run_id,
        )
    except NoteFieldsExtractionError:
        counters.skipped_by_reason["note_fields_extract_failed"] += 1
        return

    now = now_fn()
    ingest_date = _ingest_date(now)
    captured_at = _iso(now)
    captured_at_compact = _compact(now)

    try:
        pdf_s3_key = write_pdf(
            org_id=org_id,
            source_record_id=source_record_id,
            template_id=preview.template_id,
            ingest_date=ingest_date,
            captured_at_compact=captured_at_compact,
            pdf_bytes=pdf_bytes,
            s3_client=s3_client,
        )

        record = build_record(
            entry=entry,
            preview=preview,
            pdf_s3_key=pdf_s3_key,
            preview_file_id=file_resource.id,
            narrative_text=narrative_text,
            note_fields=note_fields,
            org_id=org_id,
            ingest_run_id=ingest_run_id,
            captured_at=captured_at,
        )

        identity = IdentityForAudit(
            first_name=entry.client_first_name,
            last_name=entry.client_last_name,
            client_id=str(entry.client_id),
        )

        persist_kwargs: dict[str, Any] = {
            "record": record,
            "identity": identity,
            "captured_at_compact": captured_at_compact,
            "ingest_date": ingest_date,
            "actor": actor,
        }
        if s3_client is not None:
            persist_kwargs["s3_client"] = s3_client
        if audit_emit_fn is not None:
            persist_kwargs["audit_emit_fn"] = audit_emit_fn

        result = persist_note(**persist_kwargs)
    except Exception as e:
        counters.failures_by_type[type(e).__name__] += 1
        return

    if dedupe_enabled:
        # Only mark on the success path. Failures/skips must not write
        # a cursor row — the next run needs to retry them.
        ingest_cursor.mark_ingested(
            org_id, source_record_id,
            ingest_run_id=ingest_run_id,
            pdf_s3_key=pdf_s3_key,
            record_s3_key=result["s3_key"],
            now_iso=_iso(now),
            table=cursor_table,
        )

    counters.processed.append(entry.id)


def run_ingest(
    *,
    client: CentralReachClient,
    org_id: str,
    date_range: DateRange,
    actor: dict,
    utc_offset_minutes: int,
    ingest_run_id: str | None = None,
    entries: Iterable[BillingEntry] | None = None,
    now_fn: Callable[[], datetime] = _utc_now,
    s3_client: Any | None = None,
    audit_emit_fn: Callable | None = None,
    cursor_table: Any | None = None,
    dedupe_enabled: bool | None = None,
    force_reingest: bool | None = None,
) -> IngestSummary:
    """Run the centralreach ingest pipeline.

    Args:
      * `client`: an authenticated `CentralReachClient`
      * `org_id`: drives the per-org bucket and audit org_id
      * `date_range`: from `parameters.resolve_date_range()`
      * `actor`: the system-principal actor dict for audit events
      * `utc_offset_minutes`: the org's tz, in minutes — passed into
        every CR request body (`_utcOffsetMinutes`) and matches the
        `tzoffset` cookie
      * `ingest_run_id`: optional override (e.g. from EventBridge);
        a fresh uuid4 is generated if not provided
      * `entries`: optional iterable for testing — defaults to the
        paginated list query results for the date range
      * `now_fn`, `s3_client`, `audit_emit_fn`, `cursor_table`: test
        injection points
      * `dedupe_enabled` / `force_reingest`: resolved from
        `CENTRALREACH_INGEST_DEDUPE_ENABLED` /
        `CENTRALREACH_FORCE_REINGEST` env vars when not supplied.
        When dedupe is on, entries previously marked in the ingest-
        cursor table are skipped without any CR API or Bedrock calls.
        `force_reingest` overrides the skip so an operator can
        reprocess entries without deleting cursor rows.

    Returns an `IngestSummary` the caller emits to the run_completed
    audit event.

    Does not emit `run_started` or `run_completed` events itself —
    that's the runner's responsibility (those events bracket auth +
    ingest + EventBridge fan-out, which is broader than this
    function's scope).
    """
    if ingest_run_id is None:
        ingest_run_id = str(uuid.uuid4())

    if dedupe_enabled is None:
        dedupe_enabled = ingest_cursor.is_enabled()
    if force_reingest is None:
        force_reingest = ingest_cursor.is_force_reingest()

    if entries is None:
        entries = paginate_billing_entries(
            client,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            utc_offset_minutes=utc_offset_minutes,
        )

    counters = _RunCounters(
        processed=[],
        failures_by_type=Counter(),
        skipped_by_reason=Counter(),
    )

    for entry in entries:
        # Voided/deleted entries are tracked on the list but should
        # not be ingested — they were withdrawn by the org and don't
        # have a meaningful clinical state.
        if entry.is_void or entry.is_deleted:
            counters.skipped_by_reason["void_or_deleted"] += 1
            continue

        _process_one(
            entry=entry,
            client=client,
            org_id=org_id,
            ingest_run_id=ingest_run_id,
            actor=actor,
            utc_offset_minutes=utc_offset_minutes,
            now_fn=now_fn,
            counters=counters,
            s3_client=s3_client,
            audit_emit_fn=audit_emit_fn,
            cursor_table=cursor_table,
            dedupe_enabled=dedupe_enabled,
            force_reingest=force_reingest,
        )

    return IngestSummary(
        processed_count=len(counters.processed),
        failure_count=sum(counters.failures_by_type.values()),
        skipped_count=sum(counters.skipped_by_reason.values()),
        failures_by_type=dict(counters.failures_by_type),
        skipped_by_reason=dict(counters.skipped_by_reason),
    )
