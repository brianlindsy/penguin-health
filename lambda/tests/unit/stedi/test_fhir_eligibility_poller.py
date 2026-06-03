"""Tests for the FHIR-polling-triggered eligibility runner.

Exercises the full pipeline against the moto-backed DDB table using
demo_mode so we never hit the real FHIR or Stedi networks. For the
non-demo branch we monkeypatch the lazily-imported fhir.fhir_query module.
"""

import sys
import types
from datetime import datetime, timezone

import pytest

from stedi import audit as audit_module
from stedi import config as stedi_config_module
from stedi import demo_fixtures
from stedi import fhir_eligibility_poller as poller
from stedi.exceptions import StediBadRequest, StediDailyCapExceeded


@pytest.fixture
def stedi_table(mock_dynamodb, monkeypatch):
    table = mock_dynamodb.Table('penguin-health-stedi')
    monkeypatch.setattr(audit_module, '_table', table)
    monkeypatch.setattr(poller, '_table', table)
    return table


@pytest.fixture
def demo_config(mock_dynamodb, stedi_table):
    """STEDI_CONFIG for the demo org with census + demo_mode on.

    Also seeds the FHIR_POLL_CURSOR at the unix epoch so the demo encounter
    stream (whose lastUpdated timestamps are pinned to 2026-01-15) is
    always 'after the cursor' regardless of when the test runs.
    """
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    item = {
        'pk': 'ORG#demo',
        'sk': 'STEDI_CONFIG',
        'organization_id': 'demo',
        'enabled': True,
        'demo_mode': True,
        'census_enabled': True,
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
        'preferred_payer_ids': [],
        'encounter_filter': {
            'class_codes': ['IMP'],
            'statuses': ['in-progress'],
        },
    }
    org_config_table.put_item(Item=item)
    stedi_config_module.invalidate_cache()
    # Pin cursor before the demo epoch so the stream is fully visible.
    stedi_table.put_item(Item={
        'pk': 'ORG#demo',
        'sk': 'FHIR_POLL_CURSOR',
        'last_updated_iso': '1970-01-01T00:00:00Z',
        'updated_at': '1970-01-01T00:00:00Z',
        'last_poll_status': 'complete',
        'last_processed': 0,
    })
    return item


# ---- handler-level guards -----------------------------------------------

def test_handler_raises_without_org_id():
    with pytest.raises(StediBadRequest):
        poller.handler({}, None)


def test_handler_skips_when_org_disabled(stedi_table, mock_dynamodb):
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#paused',
        'sk': 'STEDI_CONFIG',
        'enabled': False,
        'census_enabled': True,
        'provider': {'npi': '1999999984', 'organization_name': 'X'},
        'daily_cap': 100,
    })
    stedi_config_module.invalidate_cache()
    result = poller.handler({'organization_id': 'paused'}, None)
    assert result['status'] == 'skipped'
    assert 'disabled' in result['reason'].lower()


def test_handler_skips_when_census_disabled(stedi_table, mock_dynamodb):
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#opt-out',
        'sk': 'STEDI_CONFIG',
        'enabled': True,
        'census_enabled': False,
        'provider': {'npi': '1999999984', 'organization_name': 'X'},
        'daily_cap': 100,
    })
    stedi_config_module.invalidate_cache()
    result = poller.handler({'organization_id': 'opt-out'}, None)
    assert result['status'] == 'skipped'
    assert result['reason'] == 'census_enabled is false'


# ---- demo-mode end-to-end ----------------------------------------------

def test_demo_mode_processes_canned_stream(stedi_table, demo_config):
    result = poller.handler({'organization_id': 'demo'}, None)

    assert result['status'] == 'complete'
    # 10-patient CENSUS_ROSTER -> 10 demo encounters on the first run.
    assert result['processed'] == 10

    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'ENCOUNTER_ITEM#'},
    )['Items']
    assert len(items) == 10
    # All items have the canonical fields the UI will read.
    for it in items:
        assert it['encounter_class'] == 'IMP'
        assert it['encounter_status'] == 'in-progress'
        assert it['encounter_id'].startswith('enc-')
        assert it['gsi1pk'] == 'ENCOUNTER_ITEM#demo'
        assert 'result_summary' in it
        assert 'resolution' in it


def test_cursor_row_written_and_advances(stedi_table, demo_config):
    poller.handler({'organization_id': 'demo'}, None)

    cursor = stedi_table.get_item(
        Key={'pk': 'ORG#demo', 'sk': 'FHIR_POLL_CURSOR'}
    )['Item']
    assert cursor['last_poll_status'] == 'complete'
    assert int(cursor['last_processed']) == 10
    # Cursor watermark equals the last encounter's lastUpdated.
    last_encounter_iso = demo_fixtures.ENCOUNTER_STREAM[-1]['meta']['lastUpdated']
    assert cursor['last_updated_iso'] == last_encounter_iso


def test_second_call_processes_zero(stedi_table, demo_config):
    poller.handler({'organization_id': 'demo'}, None)
    result = poller.handler({'organization_id': 'demo'}, None)
    assert result['status'] == 'complete'
    assert result['processed'] == 0


def test_idempotent_writes_when_cursor_reset(stedi_table, demo_config):
    """If cursor is reset / DDB write retried, the encounter_id-keyed row
    write is conditional and the second write is silently skipped."""
    poller.handler({'organization_id': 'demo'}, None)
    # Reset cursor to force reprocessing.
    stedi_table.delete_item(Key={'pk': 'ORG#demo', 'sk': 'FHIR_POLL_CURSOR'})
    result = poller.handler({'organization_id': 'demo'}, None)
    # The poller iterates and skips on ConditionalCheckFailed; processed
    # counter still increments since we wrap the put_item, not the call.
    assert result['status'] == 'complete'
    items = stedi_table.query(
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p': 'ORG#demo', ':s': 'ENCOUNTER_ITEM#'},
    )['Items']
    # Still only 10 distinct rows — no duplicates.
    assert len(items) == 10


# ---- error-path coverage ------------------------------------------------

def test_encounter_without_subject_writes_error_row(stedi_table, demo_config, monkeypatch):
    bad_encounter = {
        'resourceType': 'Encounter',
        'id': 'enc-bad-1',
        'status': 'in-progress',
        'class': {'code': 'IMP'},
        'meta': {'lastUpdated': '2099-01-01T00:00:00Z'},
        # subject.reference missing
    }
    monkeypatch.setattr(
        demo_fixtures, 'encounter_stream_after',
        lambda cursor: iter([bad_encounter]),
    )

    result = poller.handler({'organization_id': 'demo'}, None)
    # The error row counts toward processed since we wrote it; the per-
    # status counter separately tracks the failure.
    assert result['processed'] == 1
    assert result['error'] == 1

    row = stedi_table.get_item(
        Key={'pk': 'ORG#demo', 'sk': 'ENCOUNTER_ITEM#enc-bad-1'}
    )['Item']
    assert row['result_status'] == 'error'
    assert row['result_summary']['error_kind'] == 'missing_subject_reference'


def test_patient_not_found_writes_error_row(stedi_table, demo_config, monkeypatch):
    orphan_encounter = {
        'resourceType': 'Encounter',
        'id': 'enc-orphan-1',
        'status': 'in-progress',
        'class': {'code': 'IMP'},
        'subject': {'reference': 'Patient/does-not-exist'},
        'meta': {'lastUpdated': '2099-01-01T00:00:00Z'},
    }
    monkeypatch.setattr(
        demo_fixtures, 'encounter_stream_after',
        lambda cursor: iter([orphan_encounter]),
    )

    result = poller.handler({'organization_id': 'demo'}, None)
    assert result['error'] == 1
    row = stedi_table.get_item(
        Key={'pk': 'ORG#demo', 'sk': 'ENCOUNTER_ITEM#enc-orphan-1'}
    )['Item']
    assert row['result_summary']['error_kind'] == 'patient_not_found'


def test_daily_cap_mid_batch_halts_and_holds_cursor(stedi_table, demo_config, monkeypatch):
    """When the Stedi daily cap fires mid-poll, the poller must stop and
    leave the cursor at the last successfully-processed encounter."""
    from stedi import orchestrator
    call_state = {'count': 0}

    def fake_verify(*args, **kwargs):
        call_state['count'] += 1
        # Succeed on the first 3 encounters, then trip the cap.
        if call_state['count'] <= 3:
            return {
                'path': 'direct',
                'primary_coverage': None,
                'secondary_coverages': [],
                'discovery_review_needed': [],
                'discrepancies': [],
                'recent_check': None,
                'audit_ids': [],
                'copy_block': '',
            }
        raise StediDailyCapExceeded("synthetic")

    monkeypatch.setattr(orchestrator, 'verify', fake_verify)
    # Same module reference inside the poller.
    monkeypatch.setattr(poller, 'orchestrator', orchestrator)

    result = poller.handler({'organization_id': 'demo'}, None)
    assert result['status'] == 'cap_exceeded'
    assert result['processed'] == 3
    assert result['no_coverage'] == 3

    cursor = stedi_table.get_item(
        Key={'pk': 'ORG#demo', 'sk': 'FHIR_POLL_CURSOR'}
    )['Item']
    assert cursor['last_poll_status'] == 'cap_exceeded'
    # Cursor pinned at the 3rd encounter's lastUpdated, NOT the last one.
    third_iso = demo_fixtures.ENCOUNTER_STREAM[2]['meta']['lastUpdated']
    last_iso = demo_fixtures.ENCOUNTER_STREAM[-1]['meta']['lastUpdated']
    assert cursor['last_updated_iso'] == third_iso
    assert cursor['last_updated_iso'] < last_iso


# ---- non-demo (real-FHIR) branch ---------------------------------------

class _FakeFhirQuery:
    """In-process stand-in for the fhir.fhir_query module — captures the
    search params and returns canned Patient resources for get_resource."""

    def __init__(self, encounters, patients_by_id):
        self.encounters = encounters
        self.patients_by_id = patients_by_id
        self.last_search_params = None

    def search(self, org_id, resource_type, params, *, max_results=None, max_pages=None):
        assert resource_type == 'Encounter'
        self.last_search_params = dict(params)
        for e in self.encounters:
            yield e

    def get_resource(self, org_id, resource_type, resource_id):
        assert resource_type == 'Patient'
        return self.patients_by_id[resource_id]


def _install_fake_fhir(monkeypatch, fake):
    """The poller does `from fhir import fhir_query` lazily. Inject a fake
    `fhir` package with `fhir_query` + `exceptions` modules so the import
    succeeds without the real lambda/multi-org/fhir/ package on PATH."""
    fhir_pkg = types.ModuleType('fhir')
    exceptions_mod = types.ModuleType('fhir.exceptions')

    class _FhirNotFound(Exception):
        pass

    exceptions_mod.FhirNotFound = _FhirNotFound
    fhir_pkg.fhir_query = fake
    fhir_pkg.exceptions = exceptions_mod
    monkeypatch.setitem(sys.modules, 'fhir', fhir_pkg)
    monkeypatch.setitem(sys.modules, 'fhir.fhir_query', fake)
    monkeypatch.setitem(sys.modules, 'fhir.exceptions', exceptions_mod)


def test_real_branch_passes_encounter_filter_to_search(stedi_table, mock_dynamodb, monkeypatch):
    """The encounter_filter on STEDI_CONFIG flows through to the FHIR search
    params verbatim, along with the cursor + sort + count."""
    org_config_table = mock_dynamodb.Table('penguin-health-org-config')
    org_config_table.put_item(Item={
        'pk': 'ORG#real',
        'sk': 'STEDI_CONFIG',
        'enabled': True,
        'demo_mode': False,
        'census_enabled': True,
        'provider': {'npi': '1999999984', 'organization_name': 'X'},
        'daily_cap': 100,
        'encounter_filter': {
            'class_codes': ['IMP', 'EMER'],
            'type_codes': ['BH'],
            'statuses': ['arrived', 'in-progress'],
        },
    })
    stedi_config_module.invalidate_cache()

    # Bypass Secrets Manager — real-branch verify() isn't reached anyway
    # because the fake search returns zero encounters.
    from stedi import client_factory
    monkeypatch.setattr(client_factory, 'build_client', lambda *a, **kw: object())

    fake = _FakeFhirQuery(encounters=[], patients_by_id={})
    _install_fake_fhir(monkeypatch, fake)

    result = poller.handler({'organization_id': 'real'}, None)
    assert result['status'] == 'complete'
    assert result['processed'] == 0
    params = fake.last_search_params
    assert params is not None
    assert params['class'] == ['IMP', 'EMER']
    assert params['type'] == ['BH']
    assert params['status'] == ['arrived', 'in-progress']
    assert params['_sort'] == '_lastUpdated'
    assert params['_count'] == 50
    assert params['_lastUpdated'].startswith('gt')


# ---- pure-function helpers ----------------------------------------------

@pytest.mark.parametrize("reference,expected", [
    ("Patient/123", "123"),
    ("123", "123"),
    ("http://example.org/fhir/Patient/abc-9", "abc-9"),
    ("urn:uuid:abc-9", "abc-9"),
    ("#contained-1", "contained-1"),
    ("", None),
    (None, None),
])
def test_parse_patient_id(reference, expected):
    assert poller._parse_patient_id(reference) == expected


def test_build_encounter_params_minimal():
    params = poller._build_encounter_params({}, '2026-01-01T00:00:00Z')
    assert params['_lastUpdated'] == 'gt2026-01-01T00:00:00Z'
    assert params['_sort'] == '_lastUpdated'
    assert params['_count'] == 50
    assert 'class' not in params
    assert 'type' not in params
    assert 'status' not in params
