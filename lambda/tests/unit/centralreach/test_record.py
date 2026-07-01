"""Tests for centralreach.record — dataclass validators and hashing.

Pins five contracts:
  1. patient_hash uses first|last|ClientId (not DOB)
  2. patient_hash is deterministic across reorderings/whitespace
  3. Record requires text OR pdf_s3_key, not both absent
  4. Record's forbidden-keys rejection works for identity fields in
     extracted_fields
  5. source field must be exactly "centralreach.api"
"""

from __future__ import annotations

import pytest

from centralreach.record import (
    CentralReachEncounter,
    CentralReachNoteRecord,
    CentralReachPatient,
    SOURCE,
    narrative_hash,
    patient_hash_from_client_id,
)


_VALID_HASH = "a" * 64


# ----- patient_hash --------------------------------------------------------


def test_patient_hash_uses_client_id_not_dob():
    """Pinned: third arg is the per-org ClientId. Two records with
    same first/last but different ClientId hash differently."""
    h1 = patient_hash_from_client_id("Jane", "Doe", 12345)
    h2 = patient_hash_from_client_id("Jane", "Doe", 67890)
    assert h1 != h2


def test_patient_hash_normalizes_whitespace_and_case():
    h1 = patient_hash_from_client_id("Jane", "Doe", 12345)
    h2 = patient_hash_from_client_id("  JANE  ", "doe", 12345)
    assert h1 == h2


def test_patient_hash_accepts_int_and_str_client_id():
    """Real call sites pass str(entry.client_id); test that both work
    and produce the same hash."""
    h1 = patient_hash_from_client_id("Jane", "Doe", 12345)
    h2 = patient_hash_from_client_id("Jane", "Doe", "12345")
    assert h1 == h2


def test_patient_hash_handles_empty_inputs():
    """All-empty input still returns a valid hex hash (it's the hash
    of '||'), not None or an exception."""
    h = patient_hash_from_client_id("", "", "")
    assert len(h) == 64


def test_patient_hash_returns_lowercase_hex():
    h = patient_hash_from_client_id("Jane", "Doe", 12345)
    assert h == h.lower()
    assert all(c in "0123456789abcdef" for c in h)


# ----- narrative_hash ------------------------------------------------------


def test_narrative_hash_normalizes_whitespace():
    """Pin: deterministic_evaluator uses this to detect duplicate
    narratives across documents. Two narratives differing only in
    whitespace must produce the same hash."""
    h1 = narrative_hash("BCBA met with caregiver to review goals.")
    h2 = narrative_hash("  BCBA   met with caregiver   to review goals.  ")
    assert h1 == h2


def test_narrative_hash_lowercases():
    h1 = narrative_hash("BCBA met")
    h2 = narrative_hash("bcba MET")
    assert h1 == h2


def test_narrative_hash_empty_input():
    assert len(narrative_hash("")) == 64
    assert len(narrative_hash(None)) == 64
    assert narrative_hash("") == narrative_hash(None)


# ----- CentralReachPatient -------------------------------------------------


def test_patient_dataclass_rejects_bad_hash():
    with pytest.raises(ValueError, match="patient_hash"):
        CentralReachPatient(
            patient_hash="too short",
            source_patient_id="12345",
            initials="JD",
        )


def test_patient_dataclass_rejects_empty_source_patient_id():
    with pytest.raises(ValueError, match="source_patient_id"):
        CentralReachPatient(
            patient_hash=_VALID_HASH,
            source_patient_id="",
            initials="JD",
        )


def test_patient_dataclass_rejects_empty_initials():
    with pytest.raises(ValueError, match="initials"):
        CentralReachPatient(
            patient_hash=_VALID_HASH,
            source_patient_id="12345",
            initials="",
        )


# ----- CentralReachEncounter -----------------------------------------------


def test_encounter_rejects_visit_date_without_iso_prefix():
    """`visit_date` must start with YYYY-MM-DD. CR's
    `DateOfService` is an ISO timestamp; the record builder slices
    the first 10 chars. If that slicing produces something else
    (CR omitted DateOfService), the encounter rejects."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        CentralReachEncounter(
            visit_date="6/28/2026",
            provider_display="Provider",
            note_type="x",
        )


def test_encounter_accepts_iso_date_only():
    e = CentralReachEncounter(
        visit_date="2026-06-28",
        provider_display="Provider",
        note_type="x",
    )
    assert e.visit_date == "2026-06-28"


def test_encounter_accepts_iso_datetime():
    """Full ISO is also accepted — `_ISO_DATE` regex only checks the
    prefix."""
    e = CentralReachEncounter(
        visit_date="2026-06-28T17:00:00Z",
        provider_display="Provider",
        note_type="x",
    )
    assert e.visit_date.startswith("2026-06-28")


# ----- CentralReachNoteRecord ----------------------------------------------


def _make_patient() -> CentralReachPatient:
    return CentralReachPatient(
        patient_hash=_VALID_HASH,
        source_patient_id="12345",
        initials="JD",
    )


def _make_encounter() -> CentralReachEncounter:
    return CentralReachEncounter(
        visit_date="2026-06-28",
        provider_display="Provider",
        note_type="Family Treatment Guidance – BCBA",
    )


def _make_record(**overrides) -> CentralReachNoteRecord:
    defaults = {
        "schema_version": 1,
        "source": SOURCE,
        "source_record_id": "999",
        "captured_at": "2026-06-28T22:00:00Z",
        "ingest_run_id": "run-abc",
        "vendor": "centralreach",
        "org_id": "demo",
        "patient": _make_patient(),
        "encounter": _make_encounter(),
        "text": None,
        "body_html": None,
        "extracted_fields": {"pdf_s3_key": "pdfs/2026-06-28/x.pdf"},
    }
    return CentralReachNoteRecord(**{**defaults, **overrides})


def test_record_happy_path_pdf_strategy():
    rec = _make_record()
    assert rec.source == SOURCE
    assert rec.text is None
    assert rec.extracted_fields["pdf_s3_key"] == "pdfs/2026-06-28/x.pdf"


def test_record_rejects_when_both_text_and_pdf_missing():
    """Either text or pdf_s3_key must be set."""
    with pytest.raises(ValueError, match="text.*pdf_s3_key"):
        _make_record(extracted_fields={})


def test_record_accepts_text_path_with_no_pdf():
    """Future HTML strategy: text populated, pdf_s3_key absent. The
    dataclass accepts; the rules engine reads `text` directly."""
    rec = _make_record(text="narrative goes here", extracted_fields={})
    assert rec.text == "narrative goes here"
    assert "pdf_s3_key" not in rec.extracted_fields


def test_record_rejects_wrong_schema_version():
    with pytest.raises(ValueError, match="schema_version"):
        _make_record(schema_version=2)


def test_record_rejects_wrong_source():
    """Pinned: any record bearing `source: centralreach.api` must
    actually be one. Defends against accidental cross-module misuse."""
    with pytest.raises(ValueError, match="source"):
        _make_record(source="rpa.centralreach")


def test_record_rejects_empty_source_record_id():
    with pytest.raises(ValueError, match="source_record_id"):
        _make_record(source_record_id="")


@pytest.mark.parametrize("forbidden_key", [
    "first_name", "last_name", "name", "first", "last",
    "dob", "date_of_birth", "birth_date", "ssn",
])
def test_record_rejects_forbidden_identity_keys_in_extracted_fields(
    forbidden_key,
):
    """Identity-bearing keys in extracted_fields would sidestep the
    patient hash. Reject at construction."""
    with pytest.raises(ValueError, match="forbidden keys"):
        _make_record(extracted_fields={
            "pdf_s3_key": "pdfs/x.pdf",
            forbidden_key: "value",
        })


def test_record_to_json_dict_includes_all_fields():
    rec = _make_record()
    out = rec.to_json_dict()
    assert out["source"] == SOURCE
    assert out["text"] is None
    assert out["patient"]["patient_hash"] == _VALID_HASH
    assert out["encounter"]["visit_date"] == "2026-06-28"
    assert out["extracted_fields"]["pdf_s3_key"] == "pdfs/2026-06-28/x.pdf"
