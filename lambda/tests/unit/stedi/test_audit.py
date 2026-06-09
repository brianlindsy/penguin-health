"""Tests for stedi.audit — atomic daily cap + audit row writes against the
real moto-backed DynamoDB table."""

import pytest

from stedi import audit as audit_module
from stedi.exceptions import StediDailyCapExceeded


@pytest.fixture
def audit(mock_dynamodb, monkeypatch):
    """Rebind both DDB tables to moto so write_audit (new layer) and
    `reserve_capacity` (legacy stedi table for USAGE# rows) both work,
    and so `recent_checks_for_patient` queries the new audit table."""
    monkeypatch.setattr(audit_module, '_table',
                        mock_dynamodb.Table('penguin-health-stedi'))
    monkeypatch.setattr(audit_module, '_audit_table',
                        mock_dynamodb.Table('penguin-health-audit'))
    # The new emitter writes to penguin-health-audit; wire it up too so
    # write_audit's audit.emit() lands in the moto table.
    from audit import emitter as emitter_mod
    from unittest.mock import MagicMock
    monkeypatch.setattr(emitter_mod, '_table',
                        mock_dynamodb.Table('penguin-health-audit'))
    monkeypatch.setattr(emitter_mod, '_firehose', MagicMock())
    monkeypatch.setattr(emitter_mod, '_cloudwatch', MagicMock())
    return audit_module


def test_reserve_capacity_increments_until_cap(audit):
    assert audit.reserve_capacity('org-1', daily_cap=3) == 1
    assert audit.reserve_capacity('org-1', daily_cap=3) == 2
    assert audit.reserve_capacity('org-1', daily_cap=3) == 3
    with pytest.raises(StediDailyCapExceeded):
        audit.reserve_capacity('org-1', daily_cap=3)


def test_cap_is_per_org(audit):
    audit.reserve_capacity('org-1', daily_cap=1)
    # Other org's counter is independent
    assert audit.reserve_capacity('org-2', daily_cap=1) == 1


def test_write_audit_persists_no_ssn_and_member_last4_only(audit):
    rid = audit.write_audit(
        org_id='org-1', user_email='ur@example.com', call_type='eligibility',
        patient={'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101'},
        result={'status': 'active', 'active': True,
                'plan': {'name': 'Aetna PPO', 'effective_date': '20240101'}},
        client_ip='10.0.0.1', member_id='ABC123456789',
        payer={'id': 'AETNA', 'name': 'Aetna'},
        stedi_control_number='CTRL-1', duration_ms=523,
    )
    assert rid

    # Query gsi1 by patient hash on the NEW audit table to confirm the
    # denormalized fields are correct after Phase 3 cutover.
    table = audit._audit_table
    p_hash = audit.patient_hash('John', 'Doe', '19800101')
    rows = table.query(
        IndexName='gsi1',
        KeyConditionExpression='gsi1pk = :p',
        ExpressionAttributeValues={':p': f'PATIENT#org-1#{p_hash}'},
    )['Items']
    assert len(rows) == 1
    row = rows[0]
    assert row['member_id_last4'] == '6789'
    assert 'ssn' not in row  # never persisted
    assert 'member_id' not in row  # full member id never persisted either
    assert row['payer_name'] == 'Aetna'
    # client_ip lives under the `event` snapshot, not top-level, on the
    # new schema. Either is fine — the OCR-relevant guarantee is that
    # the value is queryable.
    assert row['event']['client_ip'] == '10.0.0.1'


def test_recent_checks_orders_newest_first(audit):
    for i in range(3):
        audit.write_audit(
            org_id='org-1', user_email=f'u{i}@x.com', call_type='eligibility',
            patient={'first_name': 'J', 'last_name': 'D', 'dob': '19800101'},
            result={'status': 'active'}, client_ip='10.0.0.1',
        )
    rows = audit.recent_checks_for_patient('org-1', 'J', 'D', '19800101', limit=10)
    assert len(rows) == 3
    # newest first → user_emails should be u2, u1, u0
    assert rows[0]['user_email'] == 'u2@x.com'
    assert rows[2]['user_email'] == 'u0@x.com'


def test_patient_hash_is_case_insensitive():
    h1 = audit_module.patient_hash('John', 'Doe', '19800101')
    h2 = audit_module.patient_hash('JOHN', 'doe', '19800101')
    assert h1 == h2


def test_write_audit_writes_to_new_layer_only(audit, mock_dynamodb, monkeypatch):
    """Phase 3 cutover: write_audit no longer writes the legacy AUDIT# row on
    penguin-health-stedi; it only emits through the new audit layer to
    penguin-health-audit (+ Firehose, mocked here)."""
    from audit import emitter as emitter_mod
    from unittest.mock import MagicMock
    monkeypatch.setattr(emitter_mod, '_table',
                        mock_dynamodb.Table('penguin-health-audit'))
    monkeypatch.setattr(emitter_mod, '_firehose', MagicMock())
    monkeypatch.setattr(emitter_mod, '_cloudwatch', MagicMock())

    audit.write_audit(
        org_id='org-1', user_email='u@x.com', call_type='eligibility',
        patient={'first_name': 'Jane', 'last_name': 'Doe', 'dob': '19800101'},
        result={'status': 'active', 'active': True},
        client_ip='10.0.0.1', member_id='ABCDEF1234',
        payer={'id': 'AETNA', 'name': 'Aetna'},
    )

    # Legacy table is empty — no more AUDIT# rows are written there.
    legacy = audit._table.query(
        KeyConditionExpression='pk = :p',
        ExpressionAttributeValues={':p': 'ORG#org-1'},
    )['Items']
    assert legacy == []

    # New table has the event.
    new = mock_dynamodb.Table('penguin-health-audit').query(
        KeyConditionExpression='pk = :p',
        ExpressionAttributeValues={':p': 'ORG#org-1'},
    )['Items']
    assert len(new) == 1
    assert new[0]['agent_email'] == 'u@x.com'
    assert new[0]['call_type'] == 'eligibility'
    assert new[0]['member_id_last4'] == '1234'
