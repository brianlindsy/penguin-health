"""Tests for centralreach.record_builder.

Pins seven contracts:
  1. End-to-end: BillingEntry + PreviewResponse + pdf_s3_key → record
  2. Patient identity stays out of extracted_fields
  3. Visit date is sliced from DateOfService's first 10 chars
  4. Procedure code splits into service_code + note_type
  5. Provider preference: preview's signature name > entry's provider
  6. Signature fields surface as booleans, never bytes
  7. Narrative text flows to record.text and narrative_hash is computed
     from it (Rule 1 dedup signal)
"""

from __future__ import annotations

from centralreach.list_query import BillingEntry
from centralreach.note_fields_extractor import NoteFields
from centralreach.preview import (
    PreviewBetterNote,
    PreviewFile,
    PreviewResponse,
)
from centralreach.record import SOURCE, narrative_hash
from centralreach.record_builder import (
    _initials,
    _split_procedure_code,
    _visit_date,
    build_record,
)


_NARRATIVE = (
    "Session focused on tact training. Learner demonstrated "
    "independent responding at 80% across 10 trials."
)


def _make_note_fields(**overrides) -> NoteFields:
    defaults = {
        "provider_location": "10: Telehealth Provided in Patient's Home",
        "provider_billed_time": "75 minutes",
        "provider_billed": "Ann Smith, BCBA",
        "provider_signature_name": "Ann Smith, BCBA",
        "supervisor_name": "Dr. Jane Doe",
        "supervisor_signature_name": "Dr. Jane Doe",
    }
    return NoteFields(**{**defaults, **overrides})


_EMPTY_NOTE_FIELDS = NoteFields(
    provider_location=None,
    provider_billed_time=None,
    provider_billed=None,
    provider_signature_name=None,
    supervisor_name=None,
    supervisor_signature_name=None,
)


# ----- helpers --------------------------------------------------------------


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
        "location": "10: Telehealth Provided in Patient's Home",
        "time_worked_in_mins": 75,
        "units_of_service": 5,
        "date_time_from": "2026-06-28T17:00:00.0000000",
        "date_time_to": "2026-06-28T18:15:00.0000000",
        "creation_date": "2026-06-28T16:55:00.0000000",
        "is_void": False,
        "is_deleted": False,
        "is_locked": False,
        "timezone": "America/Chicago",
        # Remaining CR list-item columns — mostly zeros/empties for
        # the test defaults; individual tests override the ones they
        # care about.
        "voided_date": "",
        "deleted_date": "",
        "last_paid": "",
        "last_billed": "",
        "first_billed_date": "",
        "modified_date": "2026-06-28T22:32:03",
        "schedule_date": "2026-06-28T00:00:00",
        "authorization_id": 1111,
        "authorization_resource_id": 2222,
        "service_location_id": 50,
        "calc_type": 1,
        "drive_time_minutes": 0,
        "mileage": 0.0,
        "labels": "",
        "code_labels": "code labels",
        "resource_count": 1,
        "payor_id": 3333,
        "payor_insurance_id": 4444,
        "payor_name": "payor name",
        "rate_client": 115.0,
        "rate_client_agreed": 47.21,
        "rate_client_drive_hourly": 0.0,
        "rate_client_drive_mileage": 0.0,
        "invoiced": 0,
        "payments_made": 0,
        "exported": 0,
        "claims": 0,
        "claims_exported": 0,
        "group_count": 1,
        "group_id": 5555,
        "client_charges": 230.0,
        "client_charges_agreed": 94.42,
        "drive_time_charges": 0.0,
        "mileage_charges": 0.0,
        "client_charges_total": 230.0,
        "client_charges_total_agreed": 94.42,
        "amount_owed": 230.0,
        "amount_owed_agreed": 94.42,
        "amount_paid": 0.0,
        "amount_adjustment": 0.0,
        "copay_owed": 0.0,
        "copay_amount": 0.0,
        "tasks": 0,
        "tasks_completed": 0,
        "show_agreed": True,
        "schedule_course": 6666,
        "schedule_auth": 7777,
        "schedule_code": 8888,
        "schedule_ordinal": 1,
        "time_worked_from_utc_offset": "-300",
        "time_zone_abbr": "CDT",
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


# ----- _initials -----------------------------------------------------------


def test_initials_basic():
    assert _initials("Jane", "Doe") == "JD"


def test_initials_empty_inputs_yield_questionmarks():
    assert _initials("", "") == "??"


def test_initials_handles_whitespace():
    assert _initials("  jane  ", "  doe  ") == "JD"


# ----- _visit_date ---------------------------------------------------------


def test_visit_date_slices_first_ten_chars():
    """CR's DateOfService is a full ISO timestamp. The encounter
    validator wants just the YYYY-MM-DD prefix."""
    assert _visit_date("2026-06-28T17:00:00.0000000") == "2026-06-28"


def test_visit_date_already_date_only():
    assert _visit_date("2026-06-28") == "2026-06-28"


def test_visit_date_empty_returns_empty():
    """Returns "" rather than None — the dataclass validator will
    reject an empty string with its YYYY-MM-DD regex, which is what
    we want (loud failure at construction)."""
    assert _visit_date("") == ""
    assert _visit_date(None) == ""


# ----- _split_procedure_code -----------------------------------------------


def test_split_procedure_code_normal_shape():
    code, name = _split_procedure_code("97155: Treatment Planning - BCBA")
    assert code == "97155"
    assert name == "Treatment Planning - BCBA"


def test_split_procedure_code_handles_extra_whitespace():
    code, name = _split_procedure_code("  97155 :  Treatment Planning  ")
    # The split treats the FIRST `: ` as the separator; trailing
    # whitespace gets stripped.
    assert code == "97155"
    assert name == "Treatment Planning"


def test_split_procedure_code_no_separator_keeps_whole_as_note_type():
    """If CR ever sends a code-only or name-only string, the record
    builder should keep the whole thing as note_type rather than
    guess. service_code is empty so a downstream consumer can detect
    the unusual shape."""
    code, name = _split_procedure_code("Family Treatment Guidance")
    assert code == ""
    assert name == "Family Treatment Guidance"


def test_split_procedure_code_empty():
    assert _split_procedure_code("") == ("", "")
    assert _split_procedure_code(None) == ("", "")


# ----- build_record end-to-end ---------------------------------------------


def test_build_record_basic_shape():
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/2026-06-28/20260628T220000Z__502614593.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )

    assert rec.source == SOURCE
    assert rec.source_record_id == "502614593"
    assert rec.vendor == "centralreach"
    assert rec.org_id == "demo"
    assert rec.ingest_run_id == "run-abc"
    assert rec.captured_at == "2026-06-28T22:00:00Z"

    assert rec.text == _NARRATIVE
    assert rec.body_html is None


def test_build_record_patient_fields():
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.patient.source_patient_id == "5678"
    assert rec.patient.initials == "JD"
    assert len(rec.patient.patient_hash) == 64


def test_build_record_encounter_fields():
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.encounter.visit_date == "2026-06-28"
    assert rec.encounter.provider_display == "Ann Smith, BCBA"
    assert rec.encounter.note_type == "Treatment Planning - BCBA"


def test_build_record_provider_falls_back_to_entry_when_preview_empty():
    """If preview's providerSignatureName is empty (unsigned draft),
    fall back to the entry's provider first+last names."""
    preview = _make_preview(
        provider_full_name="",
        provider_signature_present=False,
        signed_at=None,
    )
    rec = build_record(
        entry=_make_entry(),
        preview=preview,
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.encounter.provider_display == "Ann Smith"


def test_build_record_extracted_fields():
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/2026-06-28/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    assert ef["pdf_s3_key"] == "pdfs/2026-06-28/x.pdf"
    assert ef["preview_file_id"] == 8901
    assert ef["text_source"] == "pdf_bedrock_extracted"
    assert ef["narrative_hash"] == narrative_hash(_NARRATIVE)
    assert ef["template_id"] == 113875
    assert ef["service_code"] == "97155"
    assert ef["provider_signature"] is True
    assert ef["supervisor_signature"] is True
    assert ef["supervisor_name"] == "Ann Smith, BCBA"
    assert ef["signed_at"] == "2026-06-28T22:59:32.0000000Z"
    # List-endpoint values only appear under the `billing_list_` prefix
    # (no short-name aliases). Duplicate contract is pinned in
    # `test_build_record_no_short_name_list_endpoint_aliases` below.
    assert ef["billing_list_location"] == "10: Telehealth Provided in Patient's Home"
    assert ef["billing_list_time_worked_in_mins"] == 75
    assert ef["billing_list_units_of_service"] == 5
    assert ef["billing_list_date_time_from"] == "2026-06-28T17:00:00.0000000"
    assert ef["billing_list_date_time_to"] == "2026-06-28T18:15:00.0000000"
    assert ef["billing_list_creation_date"] == "2026-06-28T16:55:00.0000000"
    # Bedrock-extracted, from the rendered PDF (note the `note_` prefix
    # marks these as note-derived, distinct from the API-derived fields
    # above with the same semantic meaning).
    assert ef["note_provider_location"] == "10: Telehealth Provided in Patient's Home"
    assert ef["note_provider_billed_time"] == "75 minutes"
    assert ef["note_provider_billed"] == "Ann Smith, BCBA"
    assert ef["note_provider_signature_name"] == "Ann Smith, BCBA"
    assert ef["note_supervisor_name"] == "Dr. Jane Doe"
    assert ef["note_supervisor_signature_name"] == "Dr. Jane Doe"


def test_build_record_omits_note_fields_when_extractor_returned_none():
    """The extractor returns None for each field that isn't present
    on the note. The builder must omit the key rather than emit an
    explicit null so a rule can distinguish "not present on the note"
    from a false match against an empty string."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_EMPTY_NOTE_FIELDS,
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    for key in (
        "note_provider_location",
        "note_provider_billed_time",
        "note_provider_billed",
        "note_provider_signature_name",
        "note_supervisor_name",
        "note_supervisor_signature_name",
    ):
        assert key not in rec.extracted_fields


def test_build_record_supervisor_name_and_signature_name_land_separately():
    """The two supervisor fields cover different sections of the note:
    `note_supervisor_name` is the attribution (e.g. header
    "Supervisor: Jane Doe"), `note_supervisor_signature_name` is what
    was signed at the bottom. Rules that cross-check the two must see
    both values on the record independently."""
    note_fields = _make_note_fields(
        supervisor_name="Dr. Jane Doe",
        supervisor_signature_name="J. Roe, BCBA-D",
    )
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=note_fields,
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    assert ef["note_supervisor_name"] == "Dr. Jane Doe"
    assert ef["note_supervisor_signature_name"] == "J. Roe, BCBA-D"


def test_build_record_note_fields_are_prefixed_and_do_not_shadow_api_fields():
    """Cross-check: `location` (from CR API) and
    `note_provider_location` (from PDF) can differ. Both must land
    on the record; the note_ prefix keeps them separate so a rule
    can compare them."""
    note_fields = _make_note_fields(provider_location="Clinic — Room 12")
    rec = build_record(
        entry=_make_entry(location="10: Telehealth"),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=note_fields,
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    assert ef["billing_list_location"] == "10: Telehealth"
    assert ef["note_provider_location"] == "Clinic — Room 12"


def test_build_record_surfaces_billing_list_fields_from_entry():
    """Every non-identity column on the CR list-item lands on the
    record under the `billing_list_*` prefix — the canonical form
    for list-endpoint values. Rules reference these prefixed names
    directly; the "no short-name aliases" contract is pinned in
    `test_build_record_no_short_name_list_endpoint_aliases` below."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    # Representative sample across every parsed type.
    assert ef["billing_list_procedure_code_id"] == 3456
    assert ef["billing_list_authorization_id"] == 1111
    assert ef["billing_list_service_location_id"] == 50
    assert ef["billing_list_payor_name"] == "payor name"
    assert ef["billing_list_rate_client"] == 115.0
    assert ef["billing_list_client_charges_total_agreed"] == 94.42
    assert ef["billing_list_show_agreed"] is True
    assert ef["billing_list_time_zone_abbr"] == "CDT"


def test_build_record_no_short_name_list_endpoint_aliases():
    """List-endpoint values live only under `billing_list_*`. The
    ingest layer used to emit short-name aliases (`billed_minutes`,
    `billed_start`, `location`, etc.) alongside the prefixed form;
    those aliases have been removed. Rules reference the
    `billing_list_*` name directly. This test catches a regression
    that reintroduces a short-name alias."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    # Prefixed form is present.
    assert ef["billing_list_time_worked_in_mins"] == 75
    assert ef["billing_list_date_time_from"] == "2026-06-28T17:00:00.0000000"
    assert ef["billing_list_date_time_to"] == "2026-06-28T18:15:00.0000000"
    assert ef["billing_list_creation_date"] == "2026-06-28T16:55:00.0000000"
    assert ef["billing_list_location"] == "10: Telehealth Provided in Patient's Home"
    assert ef["billing_list_units_of_service"] == 5
    # No short-name aliases for list-endpoint values.
    removed_aliases = {
        "billed_minutes",
        "billed_start",
        "billed_end",
        "entry_created_at",
        "location",
        "units_of_service",
    }
    assert not (removed_aliases & set(ef.keys()))


def test_build_record_billing_list_fields_do_not_leak_patient_identity():
    """`billing_list_*` dumps every column from the entry, but
    identity columns (client_id, first/last name, provider names)
    are intentionally kept out — patient PHI is confined to
    `patient.patient_hash` / `patient.initials`, and workforce PII
    to `encounter.provider_display`. The record dataclass would
    also reject these keys."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    forbidden = {
        "billing_list_client_id",
        "billing_list_client_first_name",
        "billing_list_client_last_name",
        "billing_list_provider_id",
        "billing_list_provider_first_name",
        "billing_list_provider_last_name",
    }
    assert not (forbidden & set(rec.extracted_fields.keys()))


def test_build_record_billing_list_dump_covers_all_mapped_fields():
    """Belt-and-suspenders: every entry attribute in
    `_BILLING_LIST_MAP` becomes an `billing_list_<name>` key. If a
    future refactor drops one, this catches it."""
    from centralreach.record_builder import _BILLING_LIST_MAP
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    for attr in _BILLING_LIST_MAP:
        assert f"billing_list_{attr}" in rec.extracted_fields


def test_build_record_emits_empty_string_verbatim_for_absent_cr_time_fields():
    """When CR sent empty strings for DateTimeFrom / DateTimeTo /
    CreationDate, the record emits the `billing_list_*` keys with
    empty-string values (no omit-on-empty shim). Rules 4/5 SKIP
    cleanly on empties because the datetime parser rejects `""` —
    the SKIP message says "Could not parse datetime" rather than
    "field not found," but both are the right operator-visible
    signal that this record can't be evaluated."""
    entry = _make_entry(
        date_time_from="", date_time_to="", creation_date="",
    )
    rec = build_record(
        entry=entry,
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    assert ef["billing_list_date_time_from"] == ""
    assert ef["billing_list_date_time_to"] == ""
    assert ef["billing_list_creation_date"] == ""


def test_build_record_extracted_fields_omits_signature_for_unsigned():
    """An unsigned/draft entry has provider_signature_present=False
    and no signed_at. The signed_at key should not appear in
    extracted_fields rather than being explicitly None."""
    preview = _make_preview(
        provider_signature_present=False,
        signed_at=None,
        provider_full_name="",
    )
    rec = build_record(
        entry=_make_entry(),
        preview=preview,
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    ef = rec.extracted_fields
    assert "signed_at" not in ef
    assert "supervisor_name" not in ef
    assert ef["provider_signature"] is False


def test_build_record_does_not_leak_patient_identity_into_extracted_fields():
    """Defensive — the record dataclass would reject identity keys,
    but a pin here catches a regression at the builder level rather
    than at construction. The builder must not emit first_name,
    last_name, dob, or other identity-bearing keys."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    forbidden = {
        "first_name", "last_name", "first", "last", "name",
        "dob", "date_of_birth", "birth_date", "ssn",
    }
    assert not (forbidden & set(rec.extracted_fields.keys()))


def test_build_record_populates_narrative_text_and_hash():
    """Rule 1 (narrative_hash_unique) consumes
    `extracted_fields.narrative_hash`, and rules 2/3 read
    `record.text` directly. Both must come from the Bedrock-extracted
    narrative passed in at ingest time."""
    text = "  Provider observed task initiation latency of 4s.  "
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=text,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.text == text
    assert rec.extracted_fields["narrative_hash"] == narrative_hash(text)
    assert rec.extracted_fields["text_source"] == "pdf_bedrock_extracted"


def test_build_record_narrative_hash_normalizes_whitespace_and_case():
    """`narrative_hash` collapses whitespace and lowercases; two
    cosmetically different narratives with the same prose must
    produce the same hash so Rule 1 can detect them as duplicates."""
    a = "Session focused on tact training."
    b = "  session   focused  on tact training.  "
    rec_a = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=a,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    rec_b = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=b,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert (
        rec_a.extracted_fields["narrative_hash"]
        == rec_b.extracted_fields["narrative_hash"]
    )


def test_build_record_surfaces_preview_file_id():
    """The CR file id (from preview.files, chosen by the pipeline via
    `first_accessible_file`) is stored on the record so the document
    validation UI can deep-link to the file screen in CentralReach.
    The builder writes the caller's value verbatim — it does not
    re-derive it from preview.files, since the pipeline already made
    the pick (and may have preferred the second file per the
    MM/DD/YYYY heuristic)."""
    rec = build_record(
        entry=_make_entry(),
        preview=_make_preview(),
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=424242,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.extracted_fields["preview_file_id"] == 424242


def test_build_record_handles_missing_template_id():
    """If preview.template_id is None (no betterNotes), the field
    surfaces as None in extracted_fields — explicit rather than
    omitted, so ops queries can group by 'no template' explicitly."""
    preview = _make_preview(better_notes=())
    rec = build_record(
        entry=_make_entry(),
        preview=preview,
        pdf_s3_key="pdfs/x.pdf",
        preview_file_id=8901,
        narrative_text=_NARRATIVE,
        note_fields=_make_note_fields(),
        org_id="demo",
        ingest_run_id="run-abc",
        captured_at="2026-06-28T22:00:00Z",
    )
    assert rec.extracted_fields["template_id"] is None
