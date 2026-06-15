"""End-to-end audit emission for the RPA writer path.

Uses the real `audit.emit` (against a moto-backed `penguin-health-audit`
table) to prove that the writer produces a single, PHI-safe audit row
per persisted note. Mirrors lambda/tests/unit/audit/test_emitter.py's
fixture pattern so the realistic emitter path is exercised.
"""

from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from audit import emit as audit_emit, patient_hash
from rpa import result_writer


SAMPLE_EXTRACTION = {
    "source_record_id": "note-pii-test",
    "first_name": "Jane",
    "last_name": "Doe",
    "dob": "1990-01-02",
    "source_patient_id": "MRN-CONFIDENTIAL-12345",
    "visit_date": "2026-06-08",
    "provider_display": "Dr. Alice Smith",
    "note_type": "Progress Note",
    "text": "The patient described symptoms in confidence today.",
    "body_html": "<p>The patient described symptoms in confidence today.</p>",
    "extracted_fields": {"signed_at": "2026-06-08T10:35:00Z"},
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
def emitter(mock_dynamodb, monkeypatch):
    """Wire `audit.emit` to the moto-backed audit table; stub Firehose +
    CloudWatch the same way audit's own test suite does."""
    from audit import emitter as emitter_mod
    table = mock_dynamodb.Table("penguin-health-audit")
    monkeypatch.setattr(emitter_mod, "_table", table)
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())
    return table


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


def test_one_audit_row_per_persisted_note_no_phi_leakage(emitter, s3):
    result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-pii-test",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=audit_emit,  # the real emitter
    )

    # Look the event up via the patient GSI — proves the row landed with
    # the canonical hash key derived from name+dob.
    expected_hash = patient_hash("Jane", "Doe", "1990-01-02")
    rows = emitter.query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={
            ":p": f"PATIENT#demo#{expected_hash}",
        },
    )["Items"]
    assert len(rows) == 1, f"expected exactly one audit row, got {len(rows)}"

    row = rows[0]
    # Hoisted top-level DDB fields (see lambda/multi-org/audit/emitter.py
    # `_write_ddb` line 156).
    assert row["org_id"] == "demo"
    assert row["agent_id"] == "rpa-runner"
    assert row["resource_type"] == "ClinicalNote"
    assert row["resource_id"] == "note-pii-test"
    assert row["call_type"] == "rpa_note_extraction"
    assert row["patient_first_initial"] == "J"
    assert row["patient_last_initial"] == "D"
    assert row["member_id_last4"] == "2345"

    # Fields that live only inside the nested `event` blob.
    event = row["event"]
    assert event["purpose_of_use"] == "OPERATIONS"
    assert event["external_control_number"] == "run-pii-test"

    # Forbidden raw fields never persist at the top level.
    forbidden_keys = {"first_name", "last_name", "dob", "source_patient_id",
                      "ssn", "patient_name", "text", "body_html"}
    leaked = forbidden_keys & set(row.keys())
    assert leaked == set(), f"raw PHI leaked into audit row: {leaked}"

    # Nor inside the nested event.
    leaked_in_event = forbidden_keys & set(event.keys())
    assert leaked_in_event == set(), (
        f"raw PHI leaked into nested event: {leaked_in_event}"
    )

    # NOTE on DOB: the audit substrate deliberately persists `patient_dob`
    # at the top level (lambda/multi-org/audit/emitter.py:179 and
    # schema.py:181) for join-with-hash queries. It's a system-wide policy
    # — Stedi and FHIR audit rows do the same thing. So we explicitly do
    # NOT assert `1990-01-02 not in serialized` here; instead we pin the
    # exact location to `patient_dob` and assert nothing else leaks.
    assert row["patient_dob"] == "1990-01-02"

    # The entire serialized row must not contain the note body, full
    # name, or full MRN.
    serialized = str(row)
    assert "Jane" not in serialized
    assert "Doe" not in serialized
    assert "MRN-CONFIDENTIAL-12345" not in serialized
    assert "in confidence today" not in serialized
    assert "<p>" not in serialized


def test_only_last4_of_source_patient_id_lands_in_audit(emitter, s3):
    result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-last4",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=audit_emit,
    )

    expected_hash = patient_hash("Jane", "Doe", "1990-01-02")
    rows = emitter.query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={
            ":p": f"PATIENT#demo#{expected_hash}",
        },
    )["Items"]
    row = rows[0]
    # The emitter slims `patient.source_patient_id_last4` to `member_id_last4`-
    # style fields when relevant. Just verify the only patient-id-like data
    # in the row is the 4-char tail, not the full string.
    serialized = str(row)
    assert "2345" in serialized
    assert "12345" not in serialized  # the full last-5 portion would imply
    # the full id slipped through somehow; only last-4 is allowed.


def test_two_notes_emit_two_distinct_audit_rows(emitter, s3):
    for i in range(2):
        extraction = {**SAMPLE_EXTRACTION,
                      "source_record_id": f"note-{i}"}
        result_writer.persist_note(
            extraction=extraction,
            org_id="demo",
            vendor="credible",
            playbook_run_id="run-multi",
            captured_at="2026-06-10T14:00:00Z",
            ingest_date="2026-06-10",
            captured_at_compact=f"20260610T14000{i}Z",
            actor=ACTOR,
            s3_client=s3,
            audit_emit_fn=audit_emit,
        )

    expected_hash = patient_hash("Jane", "Doe", "1990-01-02")
    rows = emitter.query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={
            ":p": f"PATIENT#demo#{expected_hash}",
        },
    )["Items"]
    assert len(rows) == 2
    resource_ids = sorted(r["resource_id"] for r in rows)
    assert resource_ids == ["note-0", "note-1"]
