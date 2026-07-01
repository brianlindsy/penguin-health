"""Tests for centralreach.result_writer.

Pins five contracts:
  1. Record JSON lands at `data/{date}/{ts}__{id}.json` on per-org bucket
  2. Returned dict surfaces bucket, key, source_record_id, patient_hash
  3. Audit event shape matches the new `centralreach_note_ingest` call_type
  4. Audit `patient.dob` slot carries the ClientId (for hash consistency)
  5. Note body / PHI never appears in the audit event
"""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from centralreach.record import (
    CentralReachEncounter,
    CentralReachNoteRecord,
    CentralReachPatient,
    SOURCE,
    patient_hash_from_client_id,
)
from centralreach.result_writer import IdentityForAudit, persist_note


_HASH = patient_hash_from_client_id("Jane", "Doe", 5678)

_ACTOR = {
    "agent_type": "system",
    "agent_id": "centralreach-ingest",
    "agent_email": None,
    "agent_groups": [],
    "client_ip": None,
    "user_agent": "centralreach-ingest/demo@run-abc",
}


def _make_record(**overrides) -> CentralReachNoteRecord:
    defaults = {
        "schema_version": 1,
        "source": SOURCE,
        "source_record_id": "502614593",
        "captured_at": "2026-06-28T22:00:00Z",
        "ingest_run_id": "run-abc",
        "vendor": "centralreach",
        "org_id": "demo",
        "patient": CentralReachPatient(
            patient_hash=_HASH,
            source_patient_id="5678",
            initials="JD",
        ),
        "encounter": CentralReachEncounter(
            visit_date="2026-06-28",
            provider_display="Ann Smith, BCBA",
            note_type="Treatment Planning - BCBA",
        ),
        "text": None,
        "body_html": None,
        "extracted_fields": {
            "pdf_s3_key": "pdfs/2026-06-28/20260628T220000Z__502614593.pdf",
            "template_id": 113875,
            "service_code": "97155",
            "location": "10: Telehealth Provided in Patient's Home",
            "billed_minutes": 75,
        },
    }
    return CentralReachNoteRecord(**{**defaults, **overrides})


def _make_identity() -> IdentityForAudit:
    return IdentityForAudit(
        first_name="Jane",
        last_name="Doe",
        client_id="5678",
    )


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


# ----- S3 write -----------------------------------------------------------


def test_persist_note_writes_record_json_to_per_org_bucket(s3):
    out = persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **_: "fake-event-id",
    )

    expected_key = "data/2026-06-28/20260628T220000Z__502614593.json"
    assert out == {
        "s3_bucket": "penguin-health-demo",
        "s3_key": expected_key,
        "source_record_id": "502614593",
        "patient_hash": _HASH,
    }

    obj = s3.get_object(Bucket="penguin-health-demo", Key=expected_key)
    body = json.loads(obj["Body"].read())
    assert body["source"] == SOURCE
    assert body["text"] is None
    assert body["extracted_fields"]["pdf_s3_key"].startswith("pdfs/")


def test_persist_note_uses_record_org_id_for_bucket(s3):
    """Bucket name is derived from the record's `org_id`, not a
    separately-passed argument."""
    # Override org and pre-create that bucket too
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-other-org")

        record = _make_record(org_id="other-org")
        out = persist_note(
            record=record,
            identity=_make_identity(),
            captured_at_compact="20260628T220000Z",
            ingest_date="2026-06-28",
            actor=_ACTOR,
            s3_client=client,
            audit_emit_fn=lambda **_: "ok",
        )

        assert out["s3_bucket"] == "penguin-health-other-org"
        listing = client.list_objects_v2(Bucket="penguin-health-other-org")
        assert listing["KeyCount"] == 1


def test_persist_note_writes_content_type_application_json(s3):
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **_: "ok",
    )
    obj = s3.get_object(
        Bucket="penguin-health-demo",
        Key="data/2026-06-28/20260628T220000Z__502614593.json",
    )
    assert obj["ContentType"] == "application/json"


# ----- Audit emission ------------------------------------------------------


def test_audit_event_has_centralreach_call_type(s3):
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    assert len(events) == 1
    event = events[0]
    assert event["call_type"] == "centralreach_note_ingest"
    assert event["action"] == "read"
    assert event["purpose_of_use"] == "OPERATIONS"


def test_audit_event_carries_ingest_run_id_as_external_control_number(s3):
    events = []
    persist_note(
        record=_make_record(ingest_run_id="run-xyz-42"),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    assert events[0]["external_control_number"] == "run-xyz-42"


def test_audit_event_passes_identity_to_emitter_for_hashing(s3):
    """Pinned: the audit emitter receives raw first/last/client_id so
    it can derive the hash + initials inside its own boundary. The
    audit module is responsible for ensuring raw values are dropped
    before they enter the audit table or Firehose stream."""
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    patient = events[0]["patient"]
    assert patient == {
        "first_name": "Jane",
        "last_name": "Doe",
        # client_id occupies the `dob` slot for hash consistency.
        # See result_writer.py module docstring for the rationale.
        "dob": "5678",
    }


def test_audit_event_member_id_is_source_patient_id(s3):
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    assert events[0]["member_id"] == "5678"


def test_audit_result_includes_diagnostic_fields(s3):
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    result = events[0]["result"]
    assert result["vendor"] == "centralreach"
    assert result["visit_date"] == "2026-06-28"
    assert result["note_type"] == "Treatment Planning - BCBA"
    assert result["s3_key"] == "data/2026-06-28/20260628T220000Z__502614593.json"
    assert result["pdf_s3_key"].startswith("pdfs/")
    assert result["template_id"] == 113875


def test_audit_result_omits_body_or_pdf_bytes(s3):
    """Defense in depth: the audit result block must not contain any
    PHI body content. Only metadata: ids, paths, dates."""
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    result = events[0]["result"]
    forbidden = {"text", "body_html", "pdf_bytes", "narrative"}
    assert not (forbidden & set(result.keys()))


def test_audit_resource_block(s3):
    events = []
    persist_note(
        record=_make_record(),
        identity=_make_identity(),
        captured_at_compact="20260628T220000Z",
        ingest_date="2026-06-28",
        actor=_ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: events.append(kw),
    )
    resource = events[0]["resource"]
    assert resource == {
        "type": "ClinicalNote",
        "id": "502614593",
        "org": "demo",
    }
