"""HTTP-handler tests for the morning-census endpoints.

Exercises get_latest_run / list_runs / resolve_item against the moto
DynamoDB table, with a synthetic CENSUS_RUN# + CENSUS_ITEM# row written
directly (no need to call the actual census runner)."""

from datetime import datetime, timezone
import pytest

import census_api


def _seed_run_and_item(stedi_table, *, org_id='demo', run_id='r1', run_date='2026-05-25',
                       result_status='discrepancy', patient_hash='hash1',
                       resolution_state='unresolved'):
    expires_at = int(datetime.now(timezone.utc).timestamp()) + 90 * 86400
    stedi_table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': f'CENSUS_RUN#{run_date}#{run_id}',
        'gsi1pk': f'CENSUS_RUN#{org_id}',
        'gsi1sk': f'{run_date}#{run_id}',
        'run_id': run_id,
        'org_id': org_id,
        'run_date': run_date,
        'started_at': '2026-05-25T11:00:00+00:00',
        'completed_at': '2026-05-25T11:00:42+00:00',
        'status': 'complete',
        'source': 'demo_roster',
        'total': 1, 'verified': 0, 'discrepancy': 1, 'no_coverage': 0,
        'review_needed': 0, 'pediatric_no_info': 0, 'service_type_denied': 0, 'error': 0,
        'expires_at': expires_at,
    })
    stedi_table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': f'CENSUS_ITEM#{run_date}#{run_id}#{patient_hash}',
        'run_id': run_id,
        'run_date': run_date,
        'patient_hash': patient_hash,
        'patient_first_name': 'Linda',
        'patient_last_name': 'Sandbox',
        'patient_dob': '19620818',
        'submitted_demographics': {
            'first_name': 'Linda', 'last_name': 'Sandbox',
            'dob': '19620818', 'postal_code': '33606',
        },
        'corrected_demographics': None,
        'payer_demographics': None,
        'rerun_history': [],
        'result_status': result_status,
        'result_summary': {'payer_name': 'Humana', 'plan_name': 'Humana POS',
                           'member_id_last4': '0002', 'discrepancies': ['was Cigna']},
        'audit_ids': ['aud-1'],
        'resolution': {'state': resolution_state, 'action': None, 'note': None,
                       'resolved_by': None, 'resolved_at': None, 'rerun_audit_id': None},
        'expires_at': expires_at,
    })


@pytest.fixture
def stedi_table(mock_dynamodb, monkeypatch):
    table = mock_dynamodb.Table('penguin-health-stedi')
    monkeypatch.setattr(census_api, '_stedi_table', table)
    return table


@pytest.fixture
def super_admin_event():
    return {
        'requestContext': {
            'http': {'sourceIp': '10.0.0.1'},
            'authorizer': {'jwt': {'claims': {
                'email': 'admin@example.com',
                'cognito:groups': '[Admins]',
                'sub': 'admin-sub',
            }}},
        },
    }


def _authorize_fn(event, org_id=None):
    """Stub matching admin_api.authorize_request's signature."""
    import permissions as perms
    claims_raw = event['requestContext']['authorizer']['jwt']['claims']
    claims = {
        'email': claims_raw.get('email'),
        'groups': ['Admins'] if 'Admins' in claims_raw.get('cognito:groups', '') else [],
        'organization_id': claims_raw.get('custom:organization_id'),
    }
    perms.invalidate_cache()
    return claims, None


def test_get_latest_run_returns_run_and_items(stedi_table, super_admin_event):
    _seed_run_and_item(stedi_table)
    res = census_api.get_latest_run(
        event=super_admin_event,
        path_params={'orgId': 'demo'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 200
    import json
    body = json.loads(res['body'])
    assert body['run']['run_id'] == 'r1'
    assert len(body['items']) == 1
    assert body['items'][0]['result_status'] == 'discrepancy'


def test_get_latest_run_returns_empty_when_no_runs(stedi_table, super_admin_event):
    res = census_api.get_latest_run(
        event=super_admin_event,
        path_params={'orgId': 'demo'},
        authorize_fn=_authorize_fn,
    )
    import json
    body = json.loads(res['body'])
    assert body['run'] is None
    assert body['items'] == []


def test_list_runs(stedi_table, super_admin_event):
    _seed_run_and_item(stedi_table, run_id='r1', run_date='2026-05-23')
    _seed_run_and_item(stedi_table, run_id='r2', run_date='2026-05-25')
    res = census_api.list_runs(
        event={**super_admin_event, 'queryStringParameters': None},
        path_params={'orgId': 'demo'},
        authorize_fn=_authorize_fn,
    )
    import json
    body = json.loads(res['body'])
    assert len(body['runs']) == 2
    # Newest first
    assert body['runs'][0]['run_id'] == 'r2'


def test_resolve_item_sets_resolved_state(stedi_table, super_admin_event):
    _seed_run_and_item(stedi_table, run_id='r1', patient_hash='hash1')
    res = census_api.resolve_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'r1', 'patientHash': 'hash1'},
        body={'state': 'resolved', 'action': 'Verified via portal', 'note': 'all good'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 200
    import json
    body = json.loads(res['body'])
    assert body['resolution']['state'] == 'resolved'
    assert body['resolution']['action'] == 'Verified via portal'
    assert body['resolution']['resolved_by'] == 'admin@example.com'
    assert body['resolution']['resolved_at'] is not None

    # Confirm persisted
    refetched = stedi_table.get_item(Key={
        'pk': 'ORG#demo',
        'sk': 'CENSUS_ITEM#2026-05-25#r1#hash1',
    })['Item']
    assert refetched['resolution']['state'] == 'resolved'


def test_resolve_item_rejects_invalid_state(stedi_table, super_admin_event):
    _seed_run_and_item(stedi_table)
    res = census_api.resolve_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'r1', 'patientHash': 'hash1'},
        body={'state': 'whatever'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 400


def test_resolve_item_404_when_not_found(stedi_table, super_admin_event):
    res = census_api.resolve_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'nope', 'patientHash': 'nope'},
        body={'state': 'resolved', 'action': 'Acknowledged'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 404


def test_unread_count_for_org_counts_attention_rows(stedi_table):
    _seed_run_and_item(stedi_table, run_id='r1', patient_hash='h1',
                       result_status='discrepancy', resolution_state='unresolved')
    # Add 2 more items by reusing same run.
    expires_at = int(datetime.now(timezone.utc).timestamp()) + 90 * 86400
    stedi_table.put_item(Item={
        'pk': 'ORG#demo',
        'sk': 'CENSUS_ITEM#2026-05-25#r1#h2',
        'run_id': 'r1', 'run_date': '2026-05-25',
        'patient_hash': 'h2',
        'result_status': 'verified',
        'result_summary': {},
        'resolution': {'state': 'unresolved'},
        'expires_at': expires_at,
    })
    stedi_table.put_item(Item={
        'pk': 'ORG#demo',
        'sk': 'CENSUS_ITEM#2026-05-25#r1#h3',
        'run_id': 'r1', 'run_date': '2026-05-25',
        'patient_hash': 'h3',
        'result_status': 'no_coverage',
        'result_summary': {},
        'resolution': {'state': 'resolved'},  # resolved → doesn't count
        'expires_at': expires_at,
    })
    count = census_api.unread_count_for_org('demo')
    # h1 (discrepancy/unresolved) only — h2 is verified, h3 is resolved.
    assert count == 1


# ---- rerun_census_item ----

def _seed_org_config(mock_dynamodb):
    """Set up a demo_mode STEDI_CONFIG so rerun_census_item can find
    org config and use the demo client."""
    org_table = mock_dynamodb.Table('penguin-health-org-config')
    org_table.put_item(Item={
        'pk': 'ORG#demo', 'sk': 'STEDI_CONFIG',
        'organization_id': 'demo', 'enabled': True, 'demo_mode': True,
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
    })
    # Bust the lru_cache on stedi.config.load_stedi_config so the rerun
    # handler reads the fresh row.
    from stedi import config as stedi_config
    stedi_config.invalidate_cache()


def test_rerun_returns_400_when_no_corrections_provided(stedi_table, super_admin_event, mock_dynamodb):
    _seed_run_and_item(stedi_table)
    _seed_org_config(mock_dynamodb)
    res = census_api.rerun_census_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'r1', 'patientHash': 'hash1'},
        body={},  # no fields
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 400


def test_rerun_returns_404_when_item_missing(stedi_table, super_admin_event, mock_dynamodb):
    _seed_org_config(mock_dynamodb)
    res = census_api.rerun_census_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'nope', 'patientHash': 'nope'},
        body={'last_name': 'Other'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 404


def test_rerun_with_demo_mode_returns_canned_data_and_appends_history(
    stedi_table, super_admin_event, mock_dynamodb
):
    """Seed a row for Linda Sandbox, rerun with a tweaked postal_code,
    and confirm the item is updated with corrected_demographics +
    rerun_history + a refreshed result_summary."""
    _seed_org_config(mock_dynamodb)
    # Compute Linda's real patient_hash so the seeded row matches what
    # demo_fixtures.lookup() expects.
    from stedi import audit as stedi_audit
    patient_hash = stedi_audit.patient_hash('Linda', 'Sandbox', '19620818')
    _seed_run_and_item(stedi_table, patient_hash=patient_hash)

    res = census_api.rerun_census_item(
        event=super_admin_event,
        path_params={'orgId': 'demo', 'runId': 'r1', 'patientHash': patient_hash},
        body={'postal_code': '33607'},  # corrected ZIP
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 200, res['body']
    import json
    body = json.loads(res['body'])
    item = body['item']
    assert item['corrected_demographics'] == {'postal_code': '33607'}
    assert len(item['rerun_history']) == 1
    entry = item['rerun_history'][0]
    assert entry['corrected_fields'] == ['postal_code']
    assert entry['rerun_by'] == 'admin@example.com'
    # Result summary was rebuilt — should reflect a fresh Stedi result
    # (Linda's scenario produces a Humana primary).
    assert item['result_summary']['payer_name']
    # New status reflects the actual fresh classification.
    assert item['result_status'] in (
        'verified', 'discrepancy', 'no_coverage', 'review_needed', 'service_type_denied',
    )


def test_rerun_requires_eligibility_run_permission(stedi_table, mock_dynamodb):
    """A member without Eligibility:run gets 403, even though the seed row
    exists."""
    _seed_org_config(mock_dynamodb)
    _seed_run_and_item(stedi_table)
    member_event = {
        'requestContext': {
            'http': {'sourceIp': '10.0.0.1'},
            'authorizer': {'jwt': {'claims': {
                'email': 'nobody@example.com',
                'cognito:groups': '[]',  # not in Admins
                'custom:organization_id': 'demo',
                'sub': 'nobody-sub',
            }}},
        },
    }
    res = census_api.rerun_census_item(
        event=member_event,
        path_params={'orgId': 'demo', 'runId': 'r1', 'patientHash': 'hash1'},
        body={'postal_code': '00000'},
        authorize_fn=_authorize_fn,
    )
    assert res['statusCode'] == 403
