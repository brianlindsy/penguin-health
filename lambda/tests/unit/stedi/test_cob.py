"""Tests for the Coordination-of-Benefits integration.

Covers:
  - cob_transformer (pure response → normalized ranking)
  - orchestrator gating (cob_enabled flag + ≥2-active-coverages condition)
  - demo client COB fixture wiring
"""

import pytest
from unittest.mock import MagicMock

from stedi import cob_transformer
from stedi import demo_fixtures
from stedi.demo_client import DemoStediClient


# ---- cob_transformer ----------------------------------------------------

def _aetna(active=True):
    return {
        'status': 'active' if active else 'inactive',
        'payer': {'name': 'Aetna', 'id': '60054'},
        'subscriber': {'member_id': 'AET-1'},
        'active': active,
    }


def _medicaid(active=True):
    return {
        'status': 'active' if active else 'inactive',
        'payer': {'name': 'Sunshine Medicaid', 'id': '68068'},
        'subscriber': {'member_id': 'MED-1'},
        'active': active,
    }


def test_cob_transform_subobject_shape_confirms_input_order():
    """Stedi returns primary/secondary blocks; if order matches our input,
    status is 'no_change' so the UI knows not to flag a discrepancy."""
    response = {
        'cobId': 'cob-1',
        'result': {
            'primaryCoverage': {'tradingPartnerServiceId': '60054',
                                'payer': {'name': 'Aetna'}},
            'secondaryCoverage': {'tradingPartnerServiceId': '68068',
                                  'payer': {'name': 'Sunshine'}},
            'reason': 'Commercial is primary; Medicaid is payer of last resort.',
        },
    }
    cob = cob_transformer.transform(response, input_coverages=[_aetna(), _medicaid()])
    assert cob['checked'] is True
    assert cob['status'] == 'no_change'
    assert cob['primary_payer_id'] == '60054'
    assert cob['primary_payer_name'] == 'Aetna'
    assert [r['rank'] for r in cob['rankings']] == ['primary', 'secondary']
    assert 'last resort' in cob['reason']
    assert cob['cob_id'] == 'cob-1'


def test_cob_transform_flags_reorder():
    """When COB's ranking disagrees with input order, status is 'ok'."""
    response = {
        'cobId': 'cob-2',
        'result': {
            # COB puts Medicaid primary even though we sent Aetna first.
            'primaryCoverage': {'tradingPartnerServiceId': '68068',
                                'payer': {'name': 'Sunshine'}},
            'secondaryCoverage': {'tradingPartnerServiceId': '60054',
                                  'payer': {'name': 'Aetna'}},
        },
    }
    cob = cob_transformer.transform(response, input_coverages=[_aetna(), _medicaid()])
    assert cob['status'] == 'ok'
    assert cob['primary_payer_id'] == '68068'


def test_cob_transform_accepts_array_shape():
    """Stedi may return `coverages: [{rank, payer}, ...]` instead of the
    sub-object shape. Both should normalize identically."""
    response = {
        'cobId': 'cob-3',
        'coverages': [
            {'rank': 'primary', 'tradingPartnerServiceId': '60054',
             'payer': {'name': 'Aetna'}},
            {'rank': 'secondary', 'tradingPartnerServiceId': '68068',
             'payer': {'name': 'Sunshine'}},
        ],
    }
    cob = cob_transformer.transform(response, input_coverages=[_aetna(), _medicaid()])
    assert cob['status'] == 'no_change'
    assert cob['primary_payer_id'] == '60054'


def test_cob_transform_empty_response_is_no_signal():
    cob = cob_transformer.transform({}, input_coverages=[_aetna(), _medicaid()])
    assert cob['checked'] is True
    assert cob['status'] == 'no_signal'
    assert cob['primary_payer_id'] is None
    assert cob['rankings'] == []


# ---- orchestrator gating ------------------------------------------------

@pytest.fixture
def stedi_table_for_orchestrator(mock_dynamodb, monkeypatch):
    """Point audit module at moto so reserve_capacity works."""
    from stedi import audit as audit_module
    table = mock_dynamodb.Table('penguin-health-stedi')
    monkeypatch.setattr(audit_module, '_table', table)
    return table


def _verify_input(first='Robert', last='Testpatient', dob='19780214'):
    return {
        'first_name': first, 'last_name': last, 'dob': dob,
        'gender': 'M', 'ssn_last4': '8812',
        'address1': '44 Oak Ridge Dr', 'city': 'Pensacola',
        'state': 'FL', 'postal_code': '32503',
    }


def _org_config(cob_enabled=False):
    return {
        'enabled': True,
        'demo_mode': True,
        'cob_enabled': cob_enabled,
        'provider': {'npi': '1999999984', 'organization_name': 'Demo'},
        'daily_cap': 100,
    }


def test_orchestrator_skips_cob_when_flag_off(stedi_table_for_orchestrator):
    """cob_enabled=False -> verify() never calls check_coordination_of_benefits,
    and cob_check.checked is False."""
    from stedi import orchestrator
    client = DemoStediClient(real_client=None)
    spy = MagicMock(wraps=client.check_coordination_of_benefits)
    client.check_coordination_of_benefits = spy

    result = orchestrator.verify(
        _verify_input(), org_id='demo', org_config=_org_config(cob_enabled=False),
        stedi_client=client, client_ip=None, user_email='test@example',
    )

    assert spy.call_count == 0
    assert result['cob_check'] == {'checked': False}


def test_orchestrator_calls_cob_when_two_active_coverages(stedi_table_for_orchestrator):
    """cob_enabled=True + Robert (Aetna + Medicaid both active) → COB fires
    and the demo fixture is returned."""
    from stedi import orchestrator
    client = DemoStediClient(real_client=None)
    spy = MagicMock(wraps=client.check_coordination_of_benefits)
    client.check_coordination_of_benefits = spy

    result = orchestrator.verify(
        _verify_input(), org_id='demo', org_config=_org_config(cob_enabled=True),
        stedi_client=client, client_ip=None, user_email='test@example',
    )

    assert spy.call_count == 1
    cob = result['cob_check']
    assert cob['checked'] is True
    assert cob['status'] == 'no_change'  # demo fixture confirms our input order
    assert cob['primary_payer_id'] == '60054'  # Aetna
    assert 'last resort' in (cob.get('reason') or '').lower()


def test_orchestrator_skips_cob_for_single_coverage(stedi_table_for_orchestrator):
    """A patient with only one HIGH hit shouldn't trigger COB even with
    cob_enabled=True — there's nothing to coordinate."""
    from stedi import orchestrator
    client = DemoStediClient(real_client=None)
    spy = MagicMock(wraps=client.check_coordination_of_benefits)
    client.check_coordination_of_benefits = spy

    # Linda Sandbox: single Humana hit.
    result = orchestrator.verify(
        _verify_input('Linda', 'Sandbox', '19620818'),
        org_id='demo', org_config=_org_config(cob_enabled=True),
        stedi_client=client, client_ip=None, user_email='test@example',
    )

    assert spy.call_count == 0
    assert result['cob_check']['checked'] is False


def test_orchestrator_cob_reorder_appends_discrepancy(stedi_table_for_orchestrator, monkeypatch):
    """When COB returns status='ok' (reorder), verify() should both
    re-rank primary_coverage AND append a discrepancy string."""
    from stedi import orchestrator
    client = DemoStediClient(real_client=None)

    # Override Robert's fixture to put Medicaid primary.
    reordered_cob = {
        'cobId': 'cob-flip',
        'result': {
            'primaryCoverage': {'tradingPartnerServiceId': '68068',
                                'payer': {'name': 'Sunshine Medicaid'}},
            'secondaryCoverage': {'tradingPartnerServiceId': '60054',
                                  'payer': {'name': 'Aetna'}},
            'reason': 'Synthetic test: payer-of-record system put Medicaid primary.',
        },
    }
    monkeypatch.setattr(client, 'check_coordination_of_benefits',
                        lambda _payload: reordered_cob)

    result = orchestrator.verify(
        _verify_input(), org_id='demo', org_config=_org_config(cob_enabled=True),
        stedi_client=client, client_ip=None, user_email='test@example',
    )

    assert result['cob_check']['status'] == 'ok'
    # primary_coverage was re-anchored on Medicaid.
    assert result['primary_coverage']['payer']['id'] == '68068'
    # Discrepancy was appended.
    assert any('COB' in d for d in result['discrepancies'])


# ---- demo client --------------------------------------------------------

def test_demo_client_serves_cob_fixture_for_robert():
    client = DemoStediClient(real_client=None)
    payload = {
        'subscriber': {'firstName': 'Robert', 'lastName': 'Testpatient',
                       'dateOfBirth': '19780214'},
    }
    response = client.check_coordination_of_benefits(payload)
    assert response['cobId'] == 'demo-cob-robert-001'


def test_demo_client_returns_empty_cob_for_unknown_patient():
    """An unknown patient (no fixture) and no real client → empty response,
    not a crash. Keeps the demo self-contained."""
    client = DemoStediClient(real_client=None)
    payload = {
        'subscriber': {'firstName': 'Ghost', 'lastName': 'Unknown',
                       'dateOfBirth': '20000101'},
    }
    response = client.check_coordination_of_benefits(payload)
    assert response['cobId'] == 'demo-cob-empty'
    assert response['result'] == {}


def test_robert_demo_fixture_has_cob_attached():
    """Sanity check on demo_fixtures.SCENARIOS — Robert must have a `cob`
    key or the orchestrator gating tests above would be exercising the
    no-fixture fallthrough instead of the real demo path."""
    scenario = demo_fixtures.lookup('Robert', 'Testpatient', '19780214')
    assert scenario is not None
    assert scenario.get('cob') is not None
