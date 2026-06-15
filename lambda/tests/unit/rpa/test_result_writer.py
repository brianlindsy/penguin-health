"""Tests for rpa.result_writer — extraction dict -> RpaNoteRecord JSON in S3.

The audit emitter is replaced with a capture stub so we can assert the
exact PHI-safe shape (hash + last4, no name/DOB/note body) without
depending on the audit DDB schema in this test.
"""

import json
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from audit.schema import patient_hash as compute_patient_hash
from rpa import result_writer


SAMPLE_EXTRACTION = {
    "source_record_id": "note-abc-123",
    "first_name": "Jane",
    "last_name": "Doe",
    "dob": "1990-01-02",
    "source_patient_id": "MRN-12345",
    "visit_date": "2026-06-08",
    "provider_display": "Dr. Alice Smith",
    "note_type": "Progress Note",
    "text": "Patient presents in stable condition.",
    "body_html": "<p>Patient presents in stable condition.</p>",
    "extracted_fields": {
        "signed_at": "2026-06-08T10:35:00Z",
        "billed_duration_minutes": 60,
        "supervisor_name": "Dr. Bob Jones",
    },
}

ACTOR = {
    "agent_type": "system",
    "agent_id": "rpa-runner",
    "agent_email": None,
    "agent_groups": [],
    "client_ip": None,
    "user_agent": "rpa-runner/credible/playbook=credible-notes-v3@v3",
}


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


def test_persist_note_writes_json_to_per_org_bucket(s3):
    audit_events = []

    def fake_audit(**kw):
        audit_events.append(kw)
        return "fake-event-id"

    out = result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-xyz",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=fake_audit,
    )

    expected_key = "data/2026-06-10/20260610T140000Z__note-abc-123.json"
    assert out == {
        "s3_bucket": "penguin-health-demo",
        "s3_key": expected_key,
        "source_record_id": "note-abc-123",
        "patient_hash": compute_patient_hash("Jane", "Doe", "1990-01-02"),
    }

    obj = s3.get_object(Bucket="penguin-health-demo", Key=expected_key)
    body = json.loads(obj["Body"].read())

    assert obj["ContentType"] == "application/json"
    assert body["schema_version"] == 1
    assert body["source"] == "rpa.credible"
    assert body["source_record_id"] == "note-abc-123"
    assert body["text"] == "Patient presents in stable condition."
    assert body["patient"]["initials"] == "JD"
    assert body["patient"]["patient_hash"] == out["patient_hash"]
    assert body["patient"]["source_patient_id"] == "MRN-12345"  # in encrypted payload, OK


def test_audit_call_carries_raw_identity_for_emitter_to_slim(s3):
    """The writer hands raw identity to the trusted audit module which
    derives the hash + initials and persists ONLY those — the boundary
    is at audit.emitter, not at the writer. This test pins the contract:
    raw fields go in; the emitter test (`test_audit_emission.py`) verifies
    only slimmed forms come out."""
    audit_events = []

    def fake_audit(**kw):
        audit_events.append(kw)
        return "fake-event-id"

    result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-xyz",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=fake_audit,
    )

    assert len(audit_events) == 1
    ev = audit_events[0]
    assert ev["action"] == "read"
    assert ev["resource"]["type"] == "ClinicalNote"
    assert ev["resource"]["id"] == "note-abc-123"
    assert ev["resource"]["org"] == "demo"
    assert ev["org_id"] == "demo"
    assert ev["purpose_of_use"] == "OPERATIONS"
    assert ev["call_type"] == "rpa_note_extraction"
    assert ev["external_control_number"] == "run-xyz"
    assert ev["patient"] == {
        "first_name": "Jane",
        "last_name": "Doe",
        "dob": "1990-01-02",
    }
    assert ev["member_id"] == "MRN-12345"
    # The note body and HTML are NEVER passed to the emitter — they live
    # only in the encrypted S3 payload.
    serialized = json.dumps(ev)
    assert "Patient presents" not in serialized
    assert "<p>" not in serialized


def test_persist_note_round_trips_record_through_rpa_note_record_validation(s3):
    # If extraction had a forbidden identity key in extracted_fields, the
    # record constructor must reject it — the writer must NOT silently
    # strip it.
    bad = {**SAMPLE_EXTRACTION,
           "extracted_fields": {"first_name": "Jane"}}
    with pytest.raises(ValueError, match="forbidden keys"):
        result_writer.persist_note(
            extraction=bad,
            org_id="demo",
            vendor="credible",
            playbook_run_id="run-xyz",
            captured_at="2026-06-10T14:00:00Z",
            ingest_date="2026-06-10",
            captured_at_compact="20260610T140000Z",
            actor=ACTOR,
            s3_client=s3,
            audit_emit_fn=lambda **kw: "x",
        )


def test_initials_handles_missing_name_fragments():
    assert result_writer._initials("", "Doe") == "?D"
    assert result_writer._initials("Jane", "") == "J?"
    assert result_writer._initials("", "") == "??"
    # Trims whitespace
    assert result_writer._initials("  jane ", " doe ") == "JD"


def test_last4_handles_short_values():
    assert result_writer._last4("MRN-12345") == "2345"
    assert result_writer._last4("12") == "12"
    assert result_writer._last4("") == ""
