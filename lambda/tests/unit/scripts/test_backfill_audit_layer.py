"""Tests for scripts/backfill_audit_layer.py.

The script mirrors legacy Stedi AUDIT# rows into the new penguin-health-audit
table. Tests use the moto-backed fixtures from conftest and exercise the
mirror logic directly (not subprocess).
"""

import os
import sys
from datetime import datetime, timezone

import pytest

# Import the script as a module. The file lives at <repo>/scripts/, which
# isn't on sys.path by default. Add it ad-hoc for tests.
_SCRIPTS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "scripts"
)
sys.path.insert(0, _SCRIPTS_DIR)
import backfill_audit_layer  # noqa: E402


@pytest.fixture
def source_and_dest(mock_dynamodb):
    return (
        mock_dynamodb.Table("penguin-health-stedi"),
        mock_dynamodb.Table("penguin-health-audit"),
    )


def _seed_legacy_row(source_table, *, org_id, user_email, ts,
                     request_id, patient_first, patient_last, dob,
                     payer_id="AETNA", payer_name="Aetna"):
    """Write a legacy AUDIT# row in the shape stedi.audit.write_audit
    produces. Mirror logic must consume this exact shape."""
    from audit.schema import patient_hash
    p_hash = patient_hash(patient_first, patient_last, dob)
    source_table.put_item(Item={
        "pk": f"ORG#{org_id}",
        "sk": f"AUDIT#{ts}#{request_id}",
        "gsi1pk": f"PATIENT#{org_id}#{p_hash}",
        "gsi1sk": ts,
        "request_id": request_id,
        "user_email": user_email,
        "requested_at": ts,
        "call_type": "eligibility",
        "patient_hash": p_hash,
        "patient_first_initial": patient_first[:1].upper(),
        "patient_last_initial": patient_last[:1].upper(),
        "patient_dob": dob,
        "client_ip": "10.0.0.1",
        "member_id_last4": "1234",
        "payer_id": payer_id,
        "payer_name": payer_name,
        "stedi_control_number": f"CTRL-{request_id}",
        "duration_ms": 250,
        "result_status": "active",
        "result_summary": {"status": "active", "active": True,
                           "plan_name": "PPO"},
        "expires_at": int(datetime.now(timezone.utc).timestamp()) + 1_000_000,
    })


def test_mirror_row_translates_legacy_shape_to_new_schema(source_and_dest):
    source, dest = source_and_dest
    now_iso = datetime.now(timezone.utc).isoformat()
    _seed_legacy_row(source, org_id="org-1", user_email="u@x.com",
                     ts=now_iso, request_id="req-1",
                     patient_first="Jane", patient_last="Doe", dob="19800101")
    [row] = list(backfill_audit_layer.query_audit_rows(source, "org-1", "1970"))
    result = backfill_audit_layer.mirror_row(dest, row, dry_run=False)
    assert result == "written"

    items = dest.query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(items) == 1
    mirrored = items[0]
    assert mirrored["event_id"] == "req-1"
    assert mirrored["agent_email"] == "u@x.com"
    assert mirrored["call_type"] == "eligibility"
    assert mirrored["resource_type"] == "Coverage"
    assert mirrored["patient_first_initial"] == "J"
    # gsi1 keys are populated so post-cutover queries hit the GSI.
    assert mirrored["gsi1pk"].startswith("PATIENT#org-1#")
    assert mirrored["gsi1sk"] == now_iso
    # Full event preserved for future Firehose replay.
    assert mirrored["event"]["external_control_number"] == "CTRL-req-1"
    assert mirrored["event"]["_backfilled_from"] == "penguin-health-stedi"


def test_mirror_row_is_idempotent(source_and_dest):
    source, dest = source_and_dest
    now_iso = datetime.now(timezone.utc).isoformat()
    _seed_legacy_row(source, org_id="org-1", user_email="u@x.com",
                     ts=now_iso, request_id="req-1",
                     patient_first="Jane", patient_last="Doe", dob="19800101")
    [row] = list(backfill_audit_layer.query_audit_rows(source, "org-1", "1970"))
    assert backfill_audit_layer.mirror_row(dest, row, dry_run=False) == "written"
    # Same row again → duplicate, no extra DDB item.
    assert backfill_audit_layer.mirror_row(dest, row, dry_run=False) == "duplicate"
    items = dest.query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(items) == 1


def test_mirror_row_dry_run_does_not_write(source_and_dest):
    source, dest = source_and_dest
    now_iso = datetime.now(timezone.utc).isoformat()
    _seed_legacy_row(source, org_id="org-1", user_email="u@x.com",
                     ts=now_iso, request_id="req-1",
                     patient_first="Jane", patient_last="Doe", dob="19800101")
    [row] = list(backfill_audit_layer.query_audit_rows(source, "org-1", "1970"))
    assert backfill_audit_layer.mirror_row(dest, row, dry_run=True) == "written"
    items = dest.query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert items == []


def test_list_orgs_returns_only_orgs_with_audit_rows(source_and_dest):
    source, _ = source_and_dest
    now_iso = datetime.now(timezone.utc).isoformat()
    _seed_legacy_row(source, org_id="org-a", user_email="u@x.com",
                     ts=now_iso, request_id="req-1",
                     patient_first="A", patient_last="A", dob="19800101")
    _seed_legacy_row(source, org_id="org-b", user_email="u@x.com",
                     ts=now_iso, request_id="req-2",
                     patient_first="B", patient_last="B", dob="19800101")
    # Drop a non-AUDIT row that should NOT surface.
    source.put_item(Item={
        "pk": "ORG#org-c",
        "sk": "USAGE#2026-06-08",
        "count": 5,
    })
    orgs = sorted(backfill_audit_layer.list_orgs(source))
    assert orgs == ["org-a", "org-b"]


def test_query_audit_rows_filters_by_since(source_and_dest):
    source, _ = source_and_dest
    old = (datetime.now(timezone.utc).replace(year=2020)).isoformat()
    recent = datetime.now(timezone.utc).isoformat()
    _seed_legacy_row(source, org_id="org-1", user_email="u@x.com",
                     ts=old, request_id="old",
                     patient_first="J", patient_last="D", dob="19800101")
    _seed_legacy_row(source, org_id="org-1", user_email="u@x.com",
                     ts=recent, request_id="new",
                     patient_first="J", patient_last="D", dob="19800101")
    since = (datetime.now(timezone.utc).replace(year=2024)).isoformat()
    request_ids = [r["request_id"] for r
                   in backfill_audit_layer.query_audit_rows(source, "org-1", since)]
    assert request_ids == ["new"]
