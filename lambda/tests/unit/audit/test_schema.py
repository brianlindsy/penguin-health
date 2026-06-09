"""Tests for audit.schema — pure functions, no AWS."""

from audit import patient_hash
from audit.schema import (
    OUTCOME_MAJOR_FAILURE,
    OUTCOME_MINOR_FAILURE,
    OUTCOME_SERIOUS_FAILURE,
    OUTCOME_SUCCESS,
    SCHEMA_VERSION,
    build_event,
    outcome_for_status,
    slim_result,
)


def test_patient_hash_is_case_insensitive_and_stable():
    h1 = patient_hash("John", "Doe", "19800101")
    h2 = patient_hash("JOHN", "doe", "19800101")
    h3 = patient_hash("  john  ", " DOE ", "19800101")
    assert h1 == h2 == h3
    # And matches the stedi hash exactly so dedup queries survive cutover.
    from stedi import audit as stedi_audit
    assert h1 == stedi_audit.patient_hash("John", "Doe", "19800101")


def test_outcome_for_status_buckets():
    assert outcome_for_status(200) == OUTCOME_SUCCESS
    assert outcome_for_status(299) == OUTCOME_SUCCESS
    assert outcome_for_status(404) == OUTCOME_MINOR_FAILURE
    assert outcome_for_status(429) == OUTCOME_MINOR_FAILURE
    assert outcome_for_status(500) == OUTCOME_SERIOUS_FAILURE
    assert outcome_for_status(503) == OUTCOME_SERIOUS_FAILURE
    assert outcome_for_status(None) == OUTCOME_SUCCESS
    # Major-failure is reserved for caught exceptions; outcome_for_status
    # never returns it from a status code.
    assert outcome_for_status(599) == OUTCOME_SERIOUS_FAILURE


def test_slim_result_drops_pii_payload():
    out = slim_result({
        "status": "active",
        "active": True,
        "plan": {
            "name": "Aetna PPO",
            "effective_date": "20240101",
            "expiration_date": "20251231",
        },
        "auth_required": True,
        # These would be PHI if kept — slim_result must drop them.
        "subscriber": {"first_name": "Jane", "last_name": "Doe"},
        "raw_response_body": "PHI PHI PHI",
    })
    assert out == {
        "status": "active",
        "active": True,
        "plan_name": "Aetna PPO",
        "effective_date": "20240101",
        "expiration_date": "20251231",
        "auth_required": True,
    }


def test_build_event_redacts_member_id_to_last4():
    event = build_event(
        event_id="evt-1",
        event_time="2026-06-08T12:00:00+00:00",
        action="read",
        outcome=OUTCOME_SUCCESS,
        purpose_of_use="ELIGIBILITY",
        org_id="org-1",
        actor={
            "agent_type": "human",
            "agent_id": "u@x.com",
            "agent_email": "u@x.com",
            "agent_groups": [],
            "client_ip": "10.0.0.1",
            "user_agent": "curl/8",
        },
        resource={"type": "Coverage", "id": "enc-123", "org": "org-1"},
        member_id="ABCDEFGH12345",  # full id arrives; only last 4 persisted
    )
    assert event["member_id_last4"] == "2345"
    # Nothing called "member_id" or "ssn" should leak through.
    assert "member_id" not in event
    assert "ssn" not in event
    assert event["schema_version"] == SCHEMA_VERSION


def test_build_event_initial_extraction():
    event = build_event(
        event_id="evt-2",
        event_time="2026-06-08T12:00:00+00:00",
        action="read",
        outcome=OUTCOME_SUCCESS,
        purpose_of_use="ELIGIBILITY",
        org_id="org-1",
        actor={"agent_type": "human"},
        resource={"type": "Coverage"},
        patient={"first_name": "jane", "last_name": "doe", "dob": "19800101"},
    )
    assert event["patient_first_initial"] == "J"
    assert event["patient_last_initial"] == "D"
    assert event["patient_dob"] == "19800101"
    # patient_hash must be the deterministic sha256 — not the raw name.
    assert event["patient_hash"] == patient_hash("jane", "doe", "19800101")
    assert "first_name" not in event
    assert "last_name" not in event


def test_build_event_drops_member_id_under_4_chars():
    event = build_event(
        event_id="e",
        event_time="2026-06-08T12:00:00+00:00",
        action="read",
        outcome=OUTCOME_SUCCESS,
        purpose_of_use="ELIGIBILITY",
        org_id="o",
        actor={"agent_type": "system"},
        resource={"type": "Coverage"},
        member_id="ABC",
    )
    assert event["member_id_last4"] is None
