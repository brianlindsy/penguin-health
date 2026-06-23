"""Tests for rpa.record — the on-disk JSON shape.

Focus: required fields, schema_version pinning, patient_hash format,
and the forbidden-keys gate that prevents raw PHI (names, DOB) from
leaking into extracted_fields.
"""

import pytest

from audit.schema import patient_hash
from rpa.record import RpaEncounter, RpaNoteRecord, RpaPatient


def _valid_patient() -> RpaPatient:
    return RpaPatient(
        patient_hash=patient_hash("Jane", "Doe", "1990-01-02"),
        source_patient_id="MRN-12345",
        initials="JD",
    )


def _valid_encounter() -> RpaEncounter:
    return RpaEncounter(
        visit_date="2026-06-08",
        provider_display="Dr. Alice Smith",
        note_type="Progress Note",
    )


def _valid_record(**overrides) -> RpaNoteRecord:
    defaults = dict(
        schema_version=1,
        source="rpa.credible",
        source_record_id="note-abc-123",
        captured_at="2026-06-10T14:00:00Z",
        playbook_run_id="run-xyz",
        vendor="credible",
        org_id="demo",
        patient=_valid_patient(),
        encounter=_valid_encounter(),
        text="Patient presents in stable condition.",
        body_html="<p>Patient presents in stable condition.</p>",
        extracted_fields={"session_duration_minutes": 60},
    )
    defaults.update(overrides)
    return RpaNoteRecord(**defaults)


def test_valid_record_round_trips_to_dict():
    rec = _valid_record()
    d = rec.to_json_dict()
    assert d["schema_version"] == 1
    assert d["source"] == "rpa.credible"
    assert d["source_record_id"] == "note-abc-123"
    assert d["text"] == "Patient presents in stable condition."
    # Nested dataclasses come through as dicts, not instances.
    assert isinstance(d["patient"], dict)
    assert d["patient"]["patient_hash"] == _valid_patient().patient_hash
    assert isinstance(d["encounter"], dict)


def test_patient_hash_must_be_64_char_lowercase_hex():
    with pytest.raises(ValueError, match="64-char"):
        RpaPatient(patient_hash="too-short", source_patient_id="MRN", initials="JD")
    with pytest.raises(ValueError, match="64-char"):
        # Uppercase rejected — sha256 hexdigest() lowercases.
        RpaPatient(patient_hash="A" * 64, source_patient_id="MRN", initials="JD")


def test_patient_requires_source_id_and_initials():
    with pytest.raises(ValueError, match="source_patient_id"):
        RpaPatient(
            patient_hash="a" * 64, source_patient_id="", initials="JD"
        )
    with pytest.raises(ValueError, match="initials"):
        RpaPatient(
            patient_hash="a" * 64, source_patient_id="MRN", initials=""
        )


def test_encounter_visit_date_must_start_yyyy_mm_dd():
    with pytest.raises(ValueError, match="visit_date"):
        RpaEncounter(visit_date="06/08/2026", provider_display="x", note_type="y")
    # Both date-only and full iso are accepted.
    RpaEncounter(visit_date="2026-06-08", provider_display="x", note_type="y")
    RpaEncounter(
        visit_date="2026-06-08T09:30:00Z", provider_display="x", note_type="y"
    )


def test_schema_version_pinned_to_1():
    with pytest.raises(ValueError, match="schema_version"):
        _valid_record(schema_version=2)


def test_source_must_start_with_rpa_dot():
    with pytest.raises(ValueError, match="source must start with 'rpa."):
        _valid_record(source="sftp.demo")


def test_source_record_id_required():
    with pytest.raises(ValueError, match="source_record_id"):
        _valid_record(source_record_id="")


def test_text_required():
    with pytest.raises(ValueError, match="text is required"):
        _valid_record(text="")


@pytest.mark.parametrize(
    "bad_key",
    ["first_name", "last_name", "first", "last", "name",
     "dob", "date_of_birth", "birth_date", "ssn"],
)
def test_extracted_fields_rejects_raw_phi_identity_keys(bad_key):
    with pytest.raises(ValueError, match="forbidden keys present"):
        _valid_record(extracted_fields={bad_key: "Jane Doe"})


def test_extracted_fields_allows_clinical_data():
    # Things rules 4–13 need (signed_at, billed_duration, locations, supervisors)
    # are NOT identity fields and must be allowed.
    rec = _valid_record(extracted_fields={
        "signed_at": "2026-06-08T10:35:00Z",
        "billed_start": "2026-06-08T09:00:00Z",
        "billed_end": "2026-06-08T10:30:00Z",
        "billed_duration_minutes": 90,
        "billed_location": "Office",
        "note_location": "Office",
        "provider_billed": "Dr. Alice Smith",
        "provider_signature": "A. Smith, LCSW",
        "supervisor_name": "Dr. Bob Jones",
        "supervisor_signature": "B. Jones, MD",
        "note_created_at": "2026-06-08T11:00:00Z",
    })
    d = rec.to_json_dict()
    assert d["extracted_fields"]["billed_duration_minutes"] == 90


class TestNarrativeHash:
    """Stable key for the supportive-care 'individualized narrative' rule."""

    def test_deterministic(self):
        from rpa.record import narrative_hash
        assert narrative_hash("Client engaged.") == narrative_hash("Client engaged.")

    def test_whitespace_collapse(self):
        from rpa.record import narrative_hash
        assert narrative_hash("Client   engaged.") == narrative_hash("Client engaged.")

    def test_case_insensitive(self):
        from rpa.record import narrative_hash
        assert narrative_hash("CLIENT engaged.") == narrative_hash("client engaged.")

    def test_punctuation_preserved(self):
        """Two narratives differing only in trailing punctuation must hash differently."""
        from rpa.record import narrative_hash
        assert narrative_hash("Client engaged") != narrative_hash("Client engaged.")

    def test_distinct_narratives_differ(self):
        from rpa.record import narrative_hash
        assert narrative_hash("Client engaged in DTT.") != narrative_hash("Client engaged in NET.")

    def test_empty_and_none(self):
        from rpa.record import narrative_hash
        h = narrative_hash("")
        assert len(h) == 64
        assert narrative_hash(None) == h

    def test_output_shape(self):
        from rpa.record import narrative_hash
        h = narrative_hash("Some narrative.")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)
