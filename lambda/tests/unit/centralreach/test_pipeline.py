"""Tests for centralreach.pipeline — end-to-end orchestration.

The pipeline composes list_query → preview → resources →
narrative_extractor → record_builder → result_writer per the design
doc. These tests inject fakes at the client boundary AND stub out the
Bedrock narrative extraction (the production path calls Bedrock once
per entry between PDF fetch and record build).

Pins seven contracts:
  1. Signed entry with an accessible file -> processed (extract called)
  2. Unsigned/draft entry (preview.has_pdf_available=False) -> skip
  3. PdfNotAvailable from resources -> skip with `no_pdf_url`
  4. Voided/deleted entry from list -> skip with `void_or_deleted`
  5. Preview API error -> failure (counted by exception type)
  6. Persist failure doesn't kill the rest of the run
  7. NarrativeExtractionError -> skip with `narrative_extract_failed`
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import boto3
import pytest
from moto import mock_aws

from centralreach import pipeline as pipeline_mod
from centralreach.exceptions import CentralReachAPIError
from centralreach.list_query import BillingEntry
from centralreach.narrative_extractor import NarrativeExtractionError
from centralreach.note_fields_extractor import NoteFieldsExtractionError
from centralreach.parameters import DateRange
from centralreach.pipeline import IngestSummary, run_ingest
from centralreach.preview import (
    PreviewBetterNote,
    PreviewFile,
    PreviewResponse,
)
from centralreach.resources import PdfNotAvailable, ResourceUrlResponse


_ACTOR = {
    "agent_type": "system",
    "agent_id": "centralreach-ingest",
    "agent_email": None,
    "agent_groups": [],
    "client_ip": None,
    "user_agent": "centralreach-ingest/demo",
}


def _fixed_now():
    return datetime(2026, 6, 30, 12, 0, tzinfo=timezone.utc)


def _make_entry(**overrides) -> BillingEntry:
    defaults = {
        "id": 502614593,
        "date_of_service": "2026-06-28T17:00:00.0000000",
        "client_id": 5678,
        "client_first_name": "Jane",
        "client_last_name": "Doe",
        "provider_id": 9012,
        "provider_first_name": "Ann",
        "provider_last_name": "Smith",
        "procedure_code_string": "97155: Treatment Planning - BCBA",
        "procedure_code_id": 3456,
        "location": "10: Home",
        "time_worked_in_mins": 75,
        "units_of_service": 5,
        "date_time_from": "2026-06-28T17:00:00.0000000",
        "date_time_to": "2026-06-28T18:15:00.0000000",
        "creation_date": "2026-06-28T16:55:00.0000000",
        "is_void": False,
        "is_deleted": False,
        "is_locked": False,
        "timezone": "America/Chicago",
        # Remaining list-item columns — orchestration tests don't
        # inspect them, so bland defaults are fine.
        "voided_date": "",
        "deleted_date": "",
        "last_paid": "",
        "last_billed": "",
        "first_billed_date": "",
        "modified_date": "",
        "schedule_date": "",
        "authorization_id": 0,
        "authorization_resource_id": 0,
        "service_location_id": 0,
        "calc_type": 0,
        "drive_time_minutes": 0,
        "mileage": 0.0,
        "labels": "",
        "code_labels": "",
        "resource_count": 0,
        "payor_id": 0,
        "payor_insurance_id": 0,
        "payor_name": "",
        "rate_client": 0.0,
        "rate_client_agreed": 0.0,
        "rate_client_drive_hourly": 0.0,
        "rate_client_drive_mileage": 0.0,
        "invoiced": 0,
        "payments_made": 0,
        "exported": 0,
        "claims": 0,
        "claims_exported": 0,
        "group_count": 0,
        "group_id": 0,
        "client_charges": 0.0,
        "client_charges_agreed": 0.0,
        "drive_time_charges": 0.0,
        "mileage_charges": 0.0,
        "client_charges_total": 0.0,
        "client_charges_total_agreed": 0.0,
        "amount_owed": 0.0,
        "amount_owed_agreed": 0.0,
        "amount_paid": 0.0,
        "amount_adjustment": 0.0,
        "copay_owed": 0.0,
        "copay_amount": 0.0,
        "tasks": 0,
        "tasks_completed": 0,
        "show_agreed": False,
        "schedule_course": 0,
        "schedule_auth": 0,
        "schedule_code": 0,
        "schedule_ordinal": 0,
        "time_worked_from_utc_offset": "",
        "time_zone_abbr": "",
    }
    return BillingEntry(**{**defaults, **overrides})


def _make_preview(**overrides) -> PreviewResponse:
    defaults = {
        "billing_entry_id": 502614593,
        "provider_full_name": "Ann Smith, BCBA",
        "provider_signature_present": True,
        "signed_at": "2026-06-28T22:59:32.0000000Z",
        "files": (
            PreviewFile(id=8901, name="x.pdf",
                        is_archived=False, has_access=True),
        ),
        "better_notes": (
            PreviewBetterNote(id=1357, template_id=113875, name="x"),
        ),
        "raw": {},
    }
    return PreviewResponse(**{**defaults, **overrides})


_DRAFT_PREVIEW = PreviewResponse(
    billing_entry_id=502614593,
    provider_full_name="",
    provider_signature_present=False,
    signed_at=None,
    files=(),
    better_notes=(),
    raw={},
)


_SUCCESS_RESOURCE_PAYLOAD = {
    "result": "ok",
    "success": True,
    "url": "https://s3.amazonaws.com/docs.centralreach.com/x.pdf",
    "fileName": "x.pdf",
    "cacheExpires": "06/29/2026 02:22:47",
}

_FAILURE_RESOURCE_PAYLOAD = {
    "result": "ok",
    "success": False,
}


# ----- fake client ---------------------------------------------------------


class FakeClient:
    """Composed of stubs for `get_json`, `post_json`, `get_bytes`.

    Each method routes by URL pattern. Tests inject per-entry-id
    responses for preview/resourceurl; download returns canned bytes.
    """

    def __init__(
        self,
        *,
        preview_by_entry_id: dict[int, dict | Exception] | None = None,
        resourceurl_by_resource_id: dict[int, dict | Exception] | None = None,
        download_bytes: bytes = b"%PDF-1.4\nfake\n%%EOF",
    ):
        self._preview = preview_by_entry_id or {}
        self._resourceurl = resourceurl_by_resource_id or {}
        self._download_bytes = download_bytes
        self.calls: list[str] = []

    def get_json(self, path: str) -> dict:
        self.calls.append(("GET", path))
        # Path shape: /crxapi/billing/billing-entries/{id}/preview
        if "/billing-entries/" in path and path.endswith("/preview"):
            entry_id = int(path.split("/")[-2])
            response = self._preview.get(entry_id)
            if isinstance(response, Exception):
                raise response
            if response is None:
                raise CentralReachAPIError(f"no preview scripted for {entry_id}")
            return response
        raise CentralReachAPIError(f"unscripted GET {path}")

    def post_json(self, path: str, body=None) -> dict:
        self.calls.append(("POST", path, body))
        if path == "/api/?resources.getresourceurl":
            resource_id = body["resourceId"]
            response = self._resourceurl.get(resource_id)
            if isinstance(response, Exception):
                raise response
            if response is None:
                raise CentralReachAPIError(
                    f"no resourceurl scripted for {resource_id}",
                )
            return response
        raise CentralReachAPIError(f"unscripted POST {path}")

    def get_bytes(self, url: str) -> bytes:
        self.calls.append(("GET_BYTES", url))
        return self._download_bytes


# ----- fixtures ------------------------------------------------------------


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


@pytest.fixture(autouse=True)
def stub_narrative_extract(monkeypatch):
    """Stub out the Bedrock narrative extraction by default.

    Production calls `extract_narrative` once per entry between PDF
    fetch and record build. The pipeline tests focus on orchestration,
    not Bedrock; the narrative-extractor module has its own dedicated
    tests for the Bedrock contract.
    """
    monkeypatch.setattr(
        pipeline_mod, "extract_narrative",
        lambda pdf_bytes, **_: "extracted narrative text",
    )
    # Stub the note-fields extractor too — same rationale. Returns a
    # fully-populated NoteFields; individual tests override for the
    # skip-on-failure and partial-extraction cases.
    from centralreach.note_fields_extractor import NoteFields
    _stub_fields = NoteFields(
        provider_location="10: Telehealth",
        provider_billed_time="75 minutes",
        provider_billed="Ann Smith, BCBA",
        provider_signature_name="Ann Smith, BCBA",
        supervisor_name="Dr. Jane Doe",
        supervisor_signature_names=("Dr. Jane Doe",),
    )
    monkeypatch.setattr(
        pipeline_mod, "extract_note_fields",
        lambda pdf_bytes, **_: _stub_fields,
    )


@pytest.fixture
def audit_collector():
    """Captures audit events instead of emitting them."""
    events: list[dict] = []

    def collect(**kw):
        events.append(kw)
        return "fake-event-id"

    return events, collect


# ----- happy path ----------------------------------------------------------


def test_signed_entry_with_accessible_file_is_processed(s3, audit_collector):
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    raw_preview_payload = _build_raw_preview_from(preview)
    client = FakeClient(
        preview_by_entry_id={entry.id: raw_preview_payload},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary == IngestSummary(
        processed_count=1,
        failure_count=0,
        skipped_count=0,
        failures_by_type={},
        skipped_by_reason={},
    )
    assert len(events) == 1  # one audit per processed entry


# ----- skip cases ----------------------------------------------------------


def test_unsigned_draft_entry_is_skipped_no_pdf_available(s3, audit_collector):
    """Preview's `files: []` -> skip with no_pdf_available."""
    events, collector = audit_collector
    entry = _make_entry()
    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(_DRAFT_PREVIEW)},
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 0
    assert summary.skipped_count == 1
    assert summary.skipped_by_reason == {"no_pdf_available": 1}
    assert events == []  # no audit on skip


def test_pdf_not_available_is_skipped_no_pdf_url(s3, audit_collector):
    """Preview says the file is there, but CR returns success: false
    on the resourceurl call. Skip with no_pdf_url."""
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _FAILURE_RESOURCE_PAYLOAD,
        },
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 0
    assert summary.skipped_count == 1
    assert summary.skipped_by_reason == {"no_pdf_url": 1}


def test_voided_entry_skipped_without_calling_preview(s3, audit_collector):
    """`is_void: true` short-circuits before any per-entry API call."""
    events, collector = audit_collector
    entry = _make_entry(is_void=True)
    client = FakeClient()  # no scripted responses needed

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.skipped_by_reason == {"void_or_deleted": 1}
    assert client.calls == []  # no preview, no resourceurl


def test_deleted_entry_skipped_without_calling_preview(s3, audit_collector):
    events, collector = audit_collector
    entry = _make_entry(is_deleted=True)
    client = FakeClient()

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )
    assert summary.skipped_by_reason == {"void_or_deleted": 1}


# ----- failure handling ----------------------------------------------------


def test_preview_api_error_is_failure_not_skip(s3, audit_collector):
    events, collector = audit_collector
    entry = _make_entry()
    client = FakeClient(
        preview_by_entry_id={entry.id: CentralReachAPIError("boom")},
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.failure_count == 1
    assert summary.failures_by_type == {"CentralReachAPIError": 1}
    assert summary.processed_count == 0
    assert summary.skipped_count == 0


def test_one_failure_does_not_block_other_entries(s3, audit_collector):
    """First entry fails on preview; second entry succeeds. Summary
    reflects both."""
    events, collector = audit_collector
    failing = _make_entry(id=1)
    succeeding = _make_entry(id=2)
    succeeding_preview = _make_preview(billing_entry_id=2)

    client = FakeClient(
        preview_by_entry_id={
            1: CentralReachAPIError("entry 1 broken"),
            2: _build_raw_preview_from(succeeding_preview),
        },
        resourceurl_by_resource_id={
            succeeding_preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[failing, succeeding],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 1
    assert summary.failure_count == 1
    assert len(events) == 1  # audit only for the succeeding entry


def test_mixed_run_with_processed_skipped_failed_all_counted(s3, audit_collector):
    events, collector = audit_collector
    processed = _make_entry(id=1)
    draft = _make_entry(id=2)
    voided = _make_entry(id=3, is_void=True)
    failing = _make_entry(id=4)

    processed_preview = _make_preview(billing_entry_id=1)

    client = FakeClient(
        preview_by_entry_id={
            1: _build_raw_preview_from(processed_preview),
            2: _build_raw_preview_from(_DRAFT_PREVIEW),
            4: CentralReachAPIError("entry 4 broken"),
        },
        resourceurl_by_resource_id={
            processed_preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[processed, draft, voided, failing],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 1
    assert summary.failure_count == 1
    assert summary.skipped_count == 2
    assert summary.failures_by_type == {"CentralReachAPIError": 1}
    assert summary.skipped_by_reason == {
        "no_pdf_available": 1,
        "void_or_deleted": 1,
    }


# ----- narrative extraction -----------------------------------------------


def test_narrative_extract_failure_is_skipped(s3, audit_collector, monkeypatch):
    """Bedrock failure during narrative extraction must skip the entry
    (not fail the whole run). The skipped reason is
    `narrative_extract_failed` so ops can monitor extraction health
    separately from the rest of the pipeline."""
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    def boom(pdf_bytes, **_):
        raise NarrativeExtractionError("bedrock said no")

    monkeypatch.setattr(pipeline_mod, "extract_narrative", boom)

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 0
    assert summary.skipped_count == 1
    assert summary.skipped_by_reason == {"narrative_extract_failed": 1}
    assert events == []


def test_narrative_text_is_passed_to_record_builder(s3, audit_collector,
                                                     monkeypatch):
    """The narrative returned by `extract_narrative` must end up in
    `record.text`. Rule 1's narrative_hash and rules 2/3's text-path
    eval both depend on this.
    """
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    monkeypatch.setattr(
        pipeline_mod, "extract_narrative",
        lambda pdf_bytes, **_: "provider observed task initiation latency",
    )

    captured: list = []
    real_persist = pipeline_mod.persist_note

    def spy_persist(**kw):
        captured.append(kw["record"])
        return real_persist(**kw)

    monkeypatch.setattr(pipeline_mod, "persist_note", spy_persist)

    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert len(captured) == 1
    assert captured[0].text == "provider observed task initiation latency"
    assert "narrative_hash" in captured[0].extracted_fields


def test_note_fields_extract_failure_is_skipped(s3, audit_collector,
                                                 monkeypatch):
    """Bedrock failure during the note-fields extraction skips the
    entry (not fails the whole run). `note_fields_extract_failed` is
    tracked separately from `narrative_extract_failed` so ops can
    monitor the two Bedrock extraction stages independently."""
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    def boom(pdf_bytes, **_):
        raise NoteFieldsExtractionError("bedrock returned garbage")

    monkeypatch.setattr(pipeline_mod, "extract_note_fields", boom)

    summary = run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert summary.processed_count == 0
    assert summary.skipped_count == 1
    assert summary.skipped_by_reason == {"note_fields_extract_failed": 1}
    assert events == []


def test_note_fields_flow_through_to_record_extracted_fields(
    s3, audit_collector, monkeypatch,
):
    """The five fields from `extract_note_fields` must land on the
    persisted record under their `note_*` keys. Downstream rules
    depend on this — the whole point of extraction is that the rule
    engine sees the values without re-invoking Bedrock."""
    from centralreach.note_fields_extractor import NoteFields
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()

    monkeypatch.setattr(
        pipeline_mod, "extract_note_fields",
        lambda pdf_bytes, **_: NoteFields(
            provider_location="Clinic - Room 3",
            provider_billed_time="1.25 hours",
            provider_billed="A. Smith, BCBA",
            provider_signature_name="A. Smith",
            supervisor_name="Dr. Doe",
            supervisor_signature_names=("Dr. Doe", "J. Roe, BCBA-D"),
        ),
    )

    captured: list = []
    real_persist = pipeline_mod.persist_note

    def spy_persist(**kw):
        captured.append(kw["record"])
        return real_persist(**kw)

    monkeypatch.setattr(pipeline_mod, "persist_note", spy_persist)

    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        ingest_run_id="run-abc",
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    assert len(captured) == 1
    ef = captured[0].extracted_fields
    assert ef["note_provider_location"] == "Clinic - Room 3"
    assert ef["note_provider_billed_time"] == "1.25 hours"
    assert ef["note_provider_billed"] == "A. Smith, BCBA"
    assert ef["note_provider_signature_name"] == "A. Smith"
    assert ef["note_supervisor_name"] == "Dr. Doe"
    # Both supervisor signers flow through — a rule that expects "J.
    # Roe, BCBA-D" matches the second entry.
    assert ef["note_supervisor_signature_names"] == [
        "Dr. Doe", "J. Roe, BCBA-D",
    ]


# ----- ingest_run_id behavior ----------------------------------------------


def test_ingest_run_id_generated_when_not_provided(s3, audit_collector):
    """A uuid4 is generated if the caller doesn't pass one. The
    generated id appears on every audit event's external_control_number
    so a whole run's events can be queried together."""
    events, collector = audit_collector
    entry = _make_entry()
    preview = _make_preview()
    client = FakeClient(
        preview_by_entry_id={entry.id: _build_raw_preview_from(preview)},
        resourceurl_by_resource_id={
            preview.files[0].id: _SUCCESS_RESOURCE_PAYLOAD,
        },
    )

    run_ingest(
        client=client,
        org_id="demo",
        date_range=DateRange(start_date="2026-06-28", end_date="2026-06-28"),
        actor=_ACTOR,
        utc_offset_minutes=300,
        # No ingest_run_id passed
        entries=[entry],
        now_fn=_fixed_now,
        s3_client=s3,
        audit_emit_fn=collector,
    )

    # uuid4 is 36 chars in canonical form; audit carries it
    assert len(events) == 1
    assert len(events[0]["external_control_number"]) == 36


# ----- helpers -------------------------------------------------------------


def _build_raw_preview_from(preview: PreviewResponse) -> dict:
    """Reconstruct the raw JSON the CR preview endpoint would have
    returned for this `PreviewResponse`. The pipeline calls
    `get_preview` which expects raw JSON, so we hand back the shape
    `PreviewResponse.from_json` would parse from."""
    return {
        "fields": {
            "providerSignature": "data:image/png;base64,X" if preview.provider_signature_present else "",
            "providerSignatureName": preview.provider_full_name,
            "providerSignatureCreationDate": preview.signed_at,
            "providerName": preview.provider_full_name,
        },
        "files": [
            {
                "id": f.id, "name": f.name,
                "isArchived": f.is_archived, "hasAccess": f.has_access,
            }
            for f in preview.files
        ],
        "betterNotes": [
            {"id": n.id, "templateId": n.template_id, "name": n.name}
            for n in preview.better_notes
        ],
        "result": "OK",
        "failed": False,
    }


def test_ingest_date_uses_eastern_wall_clock_for_partition():
    # 02:30 UTC on 2026-07-01 is 22:30 EDT on 2026-06-30 — the run
    # belongs to the prior US clinical day, so the partition folder
    # must be 2026-06-30, not 2026-07-01.
    late_night_utc = datetime(2026, 7, 1, 2, 30, tzinfo=timezone.utc)
    assert pipeline_mod._ingest_date(late_night_utc) == "2026-06-30"
