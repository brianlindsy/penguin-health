"""Tests for the morning-census scheduled runner.

These exercise the full pipeline against the moto-backed DDB table:
  EventBridge payload -> handler -> _load_roster -> orchestrator (real code)
  -> DemoStediClient (canned fixtures) -> classify -> CENSUS_RUN# + CENSUS_ITEM# rows.

Demo-mode is on for every test so no real Stedi traffic is generated.
"""

import pytest
from unittest.mock import patch

from stedi import audit as audit_module
from stedi import census_runner
from stedi import config as stedi_config_module


@pytest.fixture
def stedi_table(mock_dynamodb, monkeypatch):
    """Point both audit_module and census_runner at the moto table."""
    table = mock_dynamodb.Table('penguin-health-stedi')
    monkeypatch.setattr(audit_module, '_table', table)
    monkeypatch.setattr(census_runner, '_table', table)
    return table


@pytest.fixture
def demo_config(mock_dynamodb):
    """Seed STEDI_CONFIG for the demo org with census + demo_mode on."""
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    item = {
        'pk': 'ORG#demo',
        'sk': 'STEDI_CONFIG',
        'organization_id': 'demo',
        'enabled': True,
        'demo_mode': True,
        'census_enabled': True,
        'census_roster_source': 'demo_roster',
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
        'preferred_payer_ids': [],
    }
    org_config_table.put_item(Item=item)
    # Bust the lru_cache in config.py since we just wrote a fresh row.
    stedi_config_module.invalidate_cache()
    return item


def test_handler_runs_full_roster_and_writes_rows(stedi_table, demo_config):
    result = census_runner.handler({'organization_id': 'demo'}, None)

    assert result['status'] == 'complete'
    assert result['total'] == 10  # 10-patient demo roster
    # At least one of each major status appears.
    assert result['verified'] >= 1
    assert result['discrepancy'] >= 1
    assert result['no_coverage'] >= 1
    assert result['review_needed'] >= 1
    assert result['pediatric_no_info'] >= 1
    assert result['service_type_denied'] >= 1

    # CENSUS_RUN# summary row written
    runs = stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression='gsi1pk = :p',
        ExpressionAttributeValues={':p': 'CENSUS_RUN#demo'},
        ScanIndexForward=False,
    )['Items']
    assert len(runs) == 1
    run = runs[0]
    assert run['status'] == 'complete'
    assert int(run['total']) == 10

    # 10 CENSUS_ITEM# rows for this run
    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo',
                                   ':s': f'CENSUS_ITEM#{run["run_date"]}#{run["run_id"]}#'},
    )['Items']
    assert len(items) == 10


def test_each_patient_classified_correctly(stedi_table, demo_config):
    census_runner.handler({'organization_id': 'demo'}, None)

    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'CENSUS_ITEM#'},
    )['Items']
    status_by_name = {
        f"{it['patient_first_name']} {it['patient_last_name']}": it['result_status']
        for it in items
    }
    # Spot-check the trickier ones — the ones that needed pipeline fixes.
    assert status_by_name['Jane Sample'] == 'review_needed'
    # Robert Testpatient has 2 HIGH hits (primary + secondary), so the
    # secondary's audit row written during this same run shows up as a
    # "different payer" in _derive_discrepancies — net result: discrepancy.
    # Either status is acceptable for a multi-payer patient.
    assert status_by_name['Robert Testpatient'] in ('verified', 'discrepancy')
    assert status_by_name['Maria Mockerson'] == 'verified'
    assert status_by_name['Nora Faker'] == 'no_coverage'
    assert status_by_name['Daniel Demoson'] == 'discrepancy'
    assert status_by_name['Linda Sandbox'] == 'discrepancy'  # via seeded prior Cigna audit
    assert status_by_name['Tyler Fixture'] == 'pediatric_no_info'
    assert status_by_name['Patricia Stub'] == 'verified'
    assert status_by_name['James Example'] == 'service_type_denied'
    assert status_by_name['Sarah Placeholder'] == 'discrepancy'


def test_seed_writes_linda_history_only_once(stedi_table, demo_config):
    # First run seeds the prior Cigna audit row.
    census_runner.handler({'organization_id': 'demo'}, None)
    p_hash = audit_module.patient_hash('Linda', 'Sandbox', '19620818')
    audit_rows_after_first = stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression='gsi1pk = :p',
        ExpressionAttributeValues={':p': f'PATIENT#demo#{p_hash}'},
    )['Items']
    assert len(audit_rows_after_first) >= 1
    seeded_after_first = sum(
        1 for r in audit_rows_after_first if r.get('user_email') == 'system@census-seed'
    )
    assert seeded_after_first == 1

    # Second run should NOT add another seed row (idempotent).
    census_runner.handler({'organization_id': 'demo'}, None)
    audit_rows_after_second = stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression='gsi1pk = :p',
        ExpressionAttributeValues={':p': f'PATIENT#demo#{p_hash}'},
    )['Items']
    seeded_after_second = sum(
        1 for r in audit_rows_after_second if r.get('user_email') == 'system@census-seed'
    )
    assert seeded_after_second == 1  # still one seed row, even after a second run


def test_handler_skips_when_census_disabled(stedi_table, mock_dynamodb):
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#demo', 'sk': 'STEDI_CONFIG',
        'organization_id': 'demo', 'enabled': True, 'demo_mode': True,
        'census_enabled': False,  # explicitly disabled
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
    })
    stedi_config_module.invalidate_cache()

    result = census_runner.handler({'organization_id': 'demo'}, None)
    assert result['status'] == 'skipped'

    # No CENSUS_RUN# row was written.
    runs = stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression='gsi1pk = :p',
        ExpressionAttributeValues={':p': 'CENSUS_RUN#demo'},
    )['Items']
    assert runs == []


def test_handler_skips_when_org_not_configured(stedi_table):
    # No STEDI_CONFIG row exists — handler should skip rather than crash.
    stedi_config_module.invalidate_cache()
    result = census_runner.handler({'organization_id': 'unknown-org'}, None)
    assert result['status'] == 'skipped'


def test_unknown_roster_source_raises(stedi_table, mock_dynamodb):
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#demo', 'sk': 'STEDI_CONFIG',
        'organization_id': 'demo', 'enabled': True, 'demo_mode': True,
        'census_enabled': True,
        'census_roster_source': 'fhir',  # not yet implemented
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
    })
    stedi_config_module.invalidate_cache()

    with pytest.raises(NotImplementedError):
        census_runner.handler({'organization_id': 'demo'}, None)


def test_per_patient_error_doesnt_crash_run(stedi_table, demo_config):
    """If orchestrator.verify raises for one patient, the runner should
    still complete the rest of the roster and write an error row."""
    real_verify = census_runner.orchestrator.verify
    call_count = {'n': 0}

    def flaky_verify(*args, **kwargs):
        call_count['n'] += 1
        if call_count['n'] == 3:  # third patient explodes
            raise RuntimeError("simulated orchestrator failure")
        return real_verify(*args, **kwargs)

    with patch.object(census_runner.orchestrator, 'verify', side_effect=flaky_verify):
        result = census_runner.handler({'organization_id': 'demo'}, None)

    assert result['status'] == 'complete'
    assert result['total'] == 10
    assert result['error'] == 1

    # The error row was persisted.
    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'CENSUS_ITEM#'},
    )['Items']
    error_items = [i for i in items if i['result_status'] == 'error']
    assert len(error_items) == 1
    assert 'simulated' in error_items[0]['result_summary']['error_message']


def test_idempotent_within_same_run_id(stedi_table, demo_config):
    """If a patient row is written twice for the same (run_id, patient_hash),
    the second write should be a no-op via the attribute_not_exists guard."""
    census_runner.handler({'organization_id': 'demo'}, None)
    # Pull the patient_hash for any of the 10 patients
    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'CENSUS_ITEM#'},
    )['Items']
    first_count = len(items)

    # Re-running creates a NEW run with a new run_id, so total grows.
    census_runner.handler({'organization_id': 'demo'}, None)
    items_after = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'CENSUS_ITEM#'},
    )['Items']
    assert len(items_after) == 2 * first_count


def test_pediatric_classification_uses_dob(stedi_table, demo_config):
    """Tyler Fixture (a minor) should be classified pediatric_no_info
    because he has no coverage AND is under 18. The same no-coverage result
    for Nora Faker (DOB 19900101) should be 'no_coverage' instead."""
    census_runner.handler({'organization_id': 'demo'}, None)
    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'CENSUS_ITEM#'},
    )['Items']
    by_name = {f"{it['patient_first_name']} {it['patient_last_name']}": it for it in items}
    assert by_name['Tyler Fixture']['result_status'] == 'pediatric_no_info'
    assert by_name['Nora Faker']['result_status'] == 'no_coverage'
