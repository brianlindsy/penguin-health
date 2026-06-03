"""Tests for the encounter-keyed eligibility worklist API.

Exercises the three endpoints (list / resolve / rerun) end-to-end against
moto-backed DynamoDB. RBAC + JWT shape match the existing api tests.
"""

import json

import pytest

import eligibility_worklist_api as worklist_api
from stedi import audit as audit_module
from stedi import config as stedi_config_module


# ---- fixtures -----------------------------------------------------------

@pytest.fixture
def stedi_table(mock_dynamodb, monkeypatch):
    table = mock_dynamodb.Table('penguin-health-stedi')
    monkeypatch.setattr(worklist_api, '_stedi_table', table)
    monkeypatch.setattr(audit_module, '_table', table)
    return table


@pytest.fixture
def authorized_event():
    return {
        'requestContext': {
            'authorizer': {'jwt': {'claims': {
                'email': 'ur@example.com',
                'sub': 'user-1',
                'cognito:groups': '[Admins]',
            }}},
            'http': {'sourceIp': '127.0.0.1'},
        },
        'pathParameters': {},
        'body': None,
    }


def _authorize_fn_pass(event, org_id):
    """Mirror what `admin_api.authorize_request` normalizes the claims into
    — `groups` populated from `cognito:groups` so permissions.is_super_admin
    sees Admins."""
    raw = event['requestContext']['authorizer']['jwt']['claims']
    return {
        'email': raw.get('email'),
        'sub': raw.get('sub'),
        'groups': ['Admins'],
    }, None


def _seed_encounter(table, org_id, encounter_id, *,
                    last_updated='2026-06-01T10:00:00Z',
                    result_status='verified',
                    submitted=None, payer=None, resolution=None):
    table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': f'ENCOUNTER_ITEM#{encounter_id}',
        'gsi1pk': f'ENCOUNTER_ITEM#{org_id}',
        'gsi1sk': last_updated,
        'encounter_id': encounter_id,
        'encounter_class': 'IMP',
        'encounter_status': 'in-progress',
        'encounter_lastUpdated': last_updated,
        'patient_hash': f'hash-{encounter_id}',
        'patient_first_initial': 'J',
        'patient_last_initial': 'D',
        'submitted_demographics': submitted or {
            'first_name': 'Jane', 'last_name': 'Doe', 'dob': '19850712',
        },
        'payer_demographics': payer,
        'corrected_demographics': None,
        'rerun_history': [],
        'result_status': result_status,
        'result_summary': {'payer_name': 'Aetna', 'plan_name': 'PPO', 'active': True},
        'audit_ids': [],
        'resolution': resolution or {'state': 'unresolved'},
        'expires_at': 9999999999,
    })


# ---- list_encounters ----------------------------------------------------

def test_list_encounters_returns_rows_newest_first(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'enc-001', last_updated='2026-06-01T10:00:00Z')
    _seed_encounter(stedi_table, 'demo', 'enc-002', last_updated='2026-06-01T11:00:00Z')
    _seed_encounter(stedi_table, 'demo', 'enc-003', last_updated='2026-06-01T09:00:00Z')

    res = worklist_api.list_encounters(
        authorized_event, {'orgId': 'demo'}, authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert len(body['items']) == 3
    # Newest first.
    assert [it['encounter_id'] for it in body['items']] == ['enc-002', 'enc-001', 'enc-003']


def test_list_encounters_counts_by_status(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'a', result_status='verified')
    _seed_encounter(stedi_table, 'demo', 'b', result_status='discrepancy')
    _seed_encounter(stedi_table, 'demo', 'c', result_status='no_coverage',
                    resolution={'state': 'resolved'})
    _seed_encounter(stedi_table, 'demo', 'd', result_status='no_coverage')

    res = worklist_api.list_encounters(
        authorized_event, {'orgId': 'demo'}, authorize_fn=_authorize_fn_pass,
    )
    counts = json.loads(res['body'])['counts']
    assert counts['total'] == 4
    assert counts['verified'] == 1
    assert counts['discrepancy'] == 1
    assert counts['no_coverage'] == 2
    assert counts['resolved'] == 1
    # discrepancy (1) + unresolved no_coverage (1) = 2 attention rows.
    # The resolved no_coverage does NOT count toward attention.
    assert counts['attention'] == 2


def test_list_encounters_empty_org(stedi_table, authorized_event):
    res = worklist_api.list_encounters(
        authorized_event, {'orgId': 'demo'}, authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['items'] == []
    assert body['counts']['total'] == 0


def test_list_encounters_requires_view_permission(stedi_table, authorized_event):
    def deny(event, org_id):
        return event['requestContext']['authorizer']['jwt']['claims'], {
            'statusCode': 403, 'body': '{}',
        }
    res = worklist_api.list_encounters(
        authorized_event, {'orgId': 'demo'}, authorize_fn=deny,
    )
    assert res['statusCode'] == 403


# ---- resolve_encounter --------------------------------------------------

def test_resolve_encounter_marks_resolved_with_note(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'enc-1', result_status='discrepancy')

    res = worklist_api.resolve_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={'state': 'resolved', 'note': 'Confirmed via payer portal'},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['resolution']['state'] == 'resolved'
    assert body['resolution']['note'] == 'Confirmed via payer portal'
    assert body['resolution']['resolved_by'] == 'ur@example.com'
    assert body['resolution']['resolved_at']
    # `action` is no longer part of the resolution shape.
    assert 'action' not in body['resolution']

    # Persisted.
    row = stedi_table.get_item(Key={'pk': 'ORG#demo', 'sk': 'ENCOUNTER_ITEM#enc-1'})['Item']
    assert row['resolution']['state'] == 'resolved'


def test_resolve_encounter_resolves_without_note(stedi_table, authorized_event):
    """Note is optional — body with just state=resolved is fine."""
    _seed_encounter(stedi_table, 'demo', 'enc-1', result_status='no_coverage')

    res = worklist_api.resolve_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={'state': 'resolved'},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['resolution']['state'] == 'resolved'
    assert body['resolution']['note'] is None


def test_resolve_encounter_404_when_missing(stedi_table, authorized_event):
    res = worklist_api.resolve_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'nope'},
        body={'state': 'resolved'},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 404


def test_resolve_encounter_rejects_bad_state(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'enc-1')
    res = worklist_api.resolve_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={'state': 'banana'},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 400


# ---- rerun_encounter ----------------------------------------------------

def test_rerun_encounter_updates_row(stedi_table, authorized_event, mock_dynamodb, monkeypatch):
    _seed_encounter(stedi_table, 'demo', 'enc-1', result_status='no_coverage')

    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#demo', 'sk': 'STEDI_CONFIG',
        'enabled': True, 'demo_mode': True, 'census_enabled': True,
        'provider': {'npi': '1999999984', 'organization_name': 'Demo'},
        'daily_cap': 100,
    })
    stedi_config_module.invalidate_cache()

    # Stub orchestrator.verify so we don't depend on the discovery fixtures.
    fake_result = {
        'path': 'direct',
        'primary_coverage': {
            'status': 'active', 'active': True,
            'payer': {'name': 'Aetna', 'id': '60054'},
            'plan': {'name': 'Aetna PPO', 'effective_date': '20260101'},
            'subscriber': {'member_id': 'MEM-001', 'first_name': 'Jane', 'last_name': 'Doe'},
            'service_type_status': 'covered',
            'service_types': [],
        },
        'secondary_coverages': [],
        'discovery_review_needed': [],
        'discrepancies': [],
        'recent_check': None,
        'audit_ids': ['audit-1'],
        'copy_block': '',
    }
    monkeypatch.setattr(worklist_api.orchestrator, 'verify', lambda *a, **kw: fake_result)
    # Avoid the secrets-manager call inside client_factory.
    monkeypatch.setattr(worklist_api.client_factory, 'build_client',
                        lambda *a, **kw: object())

    res = worklist_api.rerun_encounter(
        authorized_event,
        {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={'first_name': 'Jane', 'last_name': 'Doe', 'dob': '19850712',
              'member_id': 'MEM-001'},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['item']['result_status'] == 'verified'
    assert body['item']['rerun_history']
    assert body['item']['rerun_history'][-1]['previous_status'] == 'no_coverage'
    assert body['item']['rerun_history'][-1]['new_status'] == 'verified'
    assert body['item']['corrected_demographics'] == {
        'first_name': 'Jane', 'last_name': 'Doe', 'dob': '19850712',
        'member_id': 'MEM-001',
    }


def test_rerun_encounter_requires_at_least_one_field(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'enc-1')
    res = worklist_api.rerun_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={},
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 400


def test_rerun_encounter_validates_dob_shape(stedi_table, authorized_event):
    _seed_encounter(stedi_table, 'demo', 'enc-1', submitted={
        'first_name': 'Jane', 'last_name': 'Doe',  # missing dob
    })
    res = worklist_api.rerun_encounter(
        authorized_event, {'orgId': 'demo', 'encounterId': 'enc-1'},
        body={'dob': '1985-07-12'},  # not YYYYMMDD
        authorize_fn=_authorize_fn_pass,
    )
    assert res['statusCode'] == 400


# ---- unread_count_for_org -----------------------------------------------

def test_unread_count_counts_attention_only(stedi_table):
    _seed_encounter(stedi_table, 'demo', 'a', result_status='verified')
    _seed_encounter(stedi_table, 'demo', 'b', result_status='discrepancy')
    _seed_encounter(stedi_table, 'demo', 'c', result_status='no_coverage',
                    resolution={'state': 'resolved', 'resolved_by': 'ur@example.com'})
    _seed_encounter(stedi_table, 'demo', 'd', result_status='review_needed')

    # 1 discrepancy + 1 review_needed; resolved no_coverage doesn't count.
    assert worklist_api.unread_count_for_org('demo') == 2


def test_unread_count_returns_zero_on_error(monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("DDB down")
    monkeypatch.setattr(worklist_api, '_query_encounters', boom)
    assert worklist_api.unread_count_for_org('demo') == 0
