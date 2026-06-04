"""Unit tests for the Stedi orchestrator.

The orchestrator takes injected client + audit module so these tests
don't need to touch DynamoDB or the Stedi network. Audit module is mocked
where the test focuses on path logic; integration with the real audit
module is covered in test_audit.py.
"""

import pytest
from unittest.mock import MagicMock

from stedi import orchestrator
from stedi.exceptions import StediBadRequest, StediDailyCapExceeded


@pytest.fixture
def org_config():
    return {
        'organization_id': 'test-org',
        'enabled': True,
        'provider': {'npi': '1234567890', 'organization_name': 'Test Provider'},
        'daily_cap': 200,
        'preferred_payer_ids': ['60054'],
    }


@pytest.fixture
def fake_audit():
    """Stub audit module: counter is just an in-memory int; recent_checks empty."""
    audit = MagicMock()
    audit.reserve_capacity.return_value = 1
    audit.recent_check_summary.return_value = None
    audit.recent_checks_for_patient.return_value = []
    audit.write_audit.side_effect = lambda **kw: kw.get('request_id') or 'req-stub'
    return audit


def _eligibility_response(payer_id='60054', payer_name='Aetna', active=True,
                          member_id='ABC123', plan_name='Aetna PPO'):
    return {
        'controlNumber': 'CTRL-1',
        'tradingPartnerServiceId': payer_id,
        'subscriber': {
            'firstName': 'JOHN', 'lastName': 'DOE', 'memberId': member_id,
            'dateOfBirth': '19800101', 'groupNumber': 'GRP-1',
        },
        'planInformation': {'planName': plan_name},
        'planDateInformation': {'planBegin': '20240101', 'planEnd': '20241231'},
        'benefitsInformation': [
            {'code': '1' if active else '6', 'name': 'Active Coverage' if active else 'Inactive',
             'serviceTypeCodes': ['30']},
        ],
    }


def _discovery_response(items=None):
    return {
        'coveragesFound': len(items or []),
        'discoveryId': 'disc-1',
        'items': items or [],
        'errors': [],
    }


def _discovery_item(payer_id='60054', payer_name='Aetna', confidence='HIGH', member_id='ABC123'):
    return {
        'confidence': {'level': confidence, 'reason': 'name+dob+ssn match'},
        'tradingPartnerServiceId': payer_id,
        'payer': {'name': payer_name},
        'subscriber': {'firstName': 'JOHN', 'lastName': 'DOE', 'memberId': member_id, 'groupNumber': 'G1'},
    }


# ---- path A: direct ------------------------------------------------------

def test_direct_path_uses_eligibility_only(org_config, fake_audit):
    client = MagicMock()
    client.check_eligibility.return_value = _eligibility_response()
    client.check_insurance_discovery.return_value = _discovery_response()  # should not be called

    result = orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101',
         'member_id': 'ABC123', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['path'] == 'direct'
    assert result['primary_coverage']['status'] == 'active'
    assert client.check_eligibility.call_count == 1
    assert client.check_insurance_discovery.call_count == 0
    assert fake_audit.reserve_capacity.call_count == 1
    assert fake_audit.write_audit.call_count == 1


# ---- path C: discovery-first --------------------------------------------

def test_discovery_first_with_single_high_hit(org_config, fake_audit):
    client = MagicMock()
    client.check_insurance_discovery.return_value = _discovery_response([
        _discovery_item(payer_id='60054', member_id='ABC123', confidence='HIGH'),
    ])
    client.check_eligibility.return_value = _eligibility_response(payer_id='60054')

    result = orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101', 'ssn': '123456789'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['path'] == 'discovery_first'
    assert 'Aetna' in result['primary_coverage']['payer']['name']
    assert result['primary_coverage']['status'] == 'active'
    assert client.check_insurance_discovery.call_count == 1
    assert client.check_eligibility.call_count == 1
    assert fake_audit.reserve_capacity.call_count == 2  # discovery + eligibility


def test_discovery_first_caps_at_three_high_hits(org_config, fake_audit):
    client = MagicMock()
    client.check_insurance_discovery.return_value = _discovery_response([
        _discovery_item(payer_id='60054', member_id='M1'),
        _discovery_item(payer_id='CIGNA', member_id='M2'),
        _discovery_item(payer_id='UHC', member_id='M3'),
        _discovery_item(payer_id='HUMANA', member_id='M4'),  # should be ignored (cap=3)
    ])
    client.check_eligibility.return_value = _eligibility_response()

    result = orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert client.check_eligibility.call_count == 3  # not 4
    assert result['primary_coverage'] is not None
    assert len(result['secondary_coverages']) == 2  # one active becomes primary


def test_discovery_returns_review_needed_only(org_config, fake_audit):
    client = MagicMock()
    client.check_insurance_discovery.return_value = _discovery_response([
        _discovery_item(confidence='REVIEW_NEEDED', member_id='X1'),
    ])

    result = orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['primary_coverage'] is None
    assert len(result['discovery_review_needed']) == 1
    assert client.check_eligibility.call_count == 0


# ---- daily cap -----------------------------------------------------------

def test_daily_cap_raises_before_stedi_call(org_config, fake_audit):
    fake_audit.reserve_capacity.side_effect = StediDailyCapExceeded("cap reached")
    client = MagicMock()

    with pytest.raises(StediDailyCapExceeded):
        orchestrator.verify(
            {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101',
             'member_id': 'M1', 'payer_id': '60054'},
            org_id='test-org', org_config=org_config, stedi_client=client,
            client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
        )

    assert client.check_eligibility.call_count == 0
    assert client.check_insurance_discovery.call_count == 0


# ---- input validation ----------------------------------------------------

def test_missing_required_field_raises_bad_request(org_config, fake_audit):
    with pytest.raises(StediBadRequest):
        orchestrator.verify(
            {'first_name': 'John', 'last_name': '', 'dob': '19800101'},
            org_id='test-org', org_config=org_config, stedi_client=MagicMock(),
            client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
        )


def test_missing_npi_in_config_raises(org_config, fake_audit):
    org_config['provider'] = {}  # no npi
    with pytest.raises(StediBadRequest):
        orchestrator.verify(
            {'first_name': 'J', 'last_name': 'D', 'dob': '19800101'},
            org_id='test-org', org_config=org_config, stedi_client=MagicMock(),
            client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
        )


def test_missing_organization_name_in_config_raises(org_config, fake_audit):
    org_config['provider'] = {'npi': '1234567890'}  # has npi but no org name
    with pytest.raises(StediBadRequest):
        orchestrator.verify(
            {'first_name': 'J', 'last_name': 'D', 'dob': '19800101'},
            org_id='test-org', org_config=org_config, stedi_client=MagicMock(),
            client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
        )


def test_outbound_payload_includes_provider_organization_name(org_config, fake_audit):
    """Stedi rejects requests without provider.organizationName (or lastName)
    with a 400. Pin that we always send it."""
    client = MagicMock()
    client.check_eligibility.return_value = _eligibility_response()
    orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101',
         'member_id': 'ABC123', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
    )
    sent_payload = client.check_eligibility.call_args[0][0]
    assert sent_payload['provider']['npi'] == '1234567890'
    assert sent_payload['provider']['organizationName'] == 'Test Provider'


# ---- discrepancies -------------------------------------------------------

def test_primary_changed_discrepancy(org_config, fake_audit):
    # Audit history shows a different payer in the lookback window.
    fake_audit.recent_checks_for_patient.return_value = [
        {'payer_name': 'Cigna', 'requested_at': '2026-05-01T10:00:00+00:00'},
    ]
    client = MagicMock()
    client.check_eligibility.return_value = _eligibility_response(payer_id='60054', payer_name='Aetna')

    result = orchestrator.verify(
        {'first_name': 'John', 'last_name': 'Doe', 'dob': '19800101',
         'member_id': 'A1', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
    )

    assert any('Cigna' in d and 'Aetna' in d for d in result['discrepancies'])


def test_recent_inactivation_discrepancy(org_config, fake_audit):
    from datetime import date, timedelta
    client = MagicMock()
    five_days_ago = (date.today() - timedelta(days=5)).strftime('%Y%m%d')
    client.check_eligibility.return_value = {
        'controlNumber': 'X',
        'tradingPartnerServiceId': '60054',
        'subscriber': {'firstName': 'J', 'lastName': 'D', 'memberId': 'A1'},
        'planInformation': {'planName': 'Aetna PPO'},
        'planDateInformation': {'planBegin': '20240101', 'planEnd': five_days_ago},
        'benefitsInformation': [{'code': '6', 'name': 'Inactive', 'serviceTypeCodes': ['30']}],
    }

    result = orchestrator.verify(
        {'first_name': 'J', 'last_name': 'D', 'dob': '19800101',
         'member_id': 'A1', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
    )

    assert any('terminated' in d.lower() for d in result['discrepancies'])


# ---- grace-period discrepancy -------------------------------------------

def _grace_primary(payer_id='68069', *, paid_through=None, has_code_5=False,
                   additional_info_texts=None):
    """Build the minimum primary_coverage shape _grace_period_discrepancy
    inspects. Keeps tests focused on policy, not transformer output."""
    return {
        'payer': {'id': payer_id, 'name': 'Ambetter Centene'},
        'plan': {},
        'grace_period_signals': {
            'paid_through': paid_through,
            'has_code_5': has_code_5,
            'additional_info_texts': additional_info_texts or [],
        },
    }


def test_grace_period_fires_for_ambetter_with_past_paid_through():
    from datetime import date, timedelta
    today = date.today()
    paid_through = (today - timedelta(days=10)).strftime('%Y%m%d')
    primary = _grace_primary(payer_id='68069', paid_through=paid_through)

    result = orchestrator._grace_period_discrepancy(primary, today)

    assert result is not None
    assert result.startswith('Grace period risk:')
    iso = (today - timedelta(days=10)).isoformat()
    assert iso in result
    assert 'before today' in result


def test_grace_period_fires_for_cenpatico_on_code_5():
    from datetime import date
    primary = _grace_primary(payer_id='68068', has_code_5=True)

    result = orchestrator._grace_period_discrepancy(primary, date.today())

    assert result is not None
    assert 'code 5' in result
    assert 'Active – Pending Investigation' in result


def test_grace_period_fires_for_free_text_match():
    from datetime import date
    primary = _grace_primary(
        payer_id='68069',
        additional_info_texts=['member is in grace period for non-payment.'],
    )

    result = orchestrator._grace_period_discrepancy(primary, date.today())

    assert result is not None
    assert 'free-text indicates grace-period status' in result


def test_grace_period_does_not_fire_for_non_gated_payer():
    """Even with all three signals tripped, a non-Ambetter/Cenpatico
    payer must not get the grace-period discrepancy — Stedi confirmed
    these signals are unreliable elsewhere."""
    from datetime import date, timedelta
    paid_through = (date.today() - timedelta(days=10)).strftime('%Y%m%d')
    primary = _grace_primary(
        payer_id='60054',  # Aetna
        paid_through=paid_through,
        has_code_5=True,
        additional_info_texts=['grace period notice'],
    )

    assert orchestrator._grace_period_discrepancy(primary, date.today()) is None


def test_grace_period_does_not_fire_when_paid_through_today_or_future():
    """Equal-day paid-through means coverage is current; future means
    the member is paid ahead. Neither flags as delinquent."""
    from datetime import date, timedelta
    today = date.today()
    primary_today = _grace_primary(
        payer_id='68069', paid_through=today.strftime('%Y%m%d'),
    )
    primary_future = _grace_primary(
        payer_id='68069',
        paid_through=(today + timedelta(days=30)).strftime('%Y%m%d'),
    )

    assert orchestrator._grace_period_discrepancy(primary_today, today) is None
    assert orchestrator._grace_period_discrepancy(primary_future, today) is None


def test_grace_period_discrepancy_omits_raw_free_text():
    """PHI guard: a payer could in principle stuff member-specific info
    into the additionalInformation description. The discrepancy string
    must never echo the raw text — only a generic 'free-text indicates'
    marker."""
    from datetime import date
    primary = _grace_primary(
        payer_id='68069',
        additional_info_texts=['member ssn 1234 in grace period notes'],
    )

    result = orchestrator._grace_period_discrepancy(primary, date.today())

    assert result is not None
    assert 'free-text' in result
    assert 'ssn' not in result.lower()
    assert '1234' not in result


# ---- copy block + recent-check dedup ------------------------------------

def test_copy_block_included(org_config, fake_audit):
    client = MagicMock()
    client.check_eligibility.return_value = _eligibility_response()
    result = orchestrator.verify(
        {'first_name': 'J', 'last_name': 'D', 'dob': '19800101',
         'member_id': 'ABC123', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
    )
    assert 'PRIMARY INSURANCE' in result['copy_block']
    assert 'Aetna' in result['copy_block']


def test_recent_check_attached_when_in_window(org_config, fake_audit):
    fake_audit.recent_check_summary.return_value = {
        'checked_by': 'kayla@example.com',
        'checked_at': '2026-05-22T08:00:00+00:00',
        'payer_name': 'Aetna',
        'result_status': 'active',
    }
    client = MagicMock()
    client.check_eligibility.return_value = _eligibility_response()
    result = orchestrator.verify(
        {'first_name': 'J', 'last_name': 'D', 'dob': '19800101',
         'member_id': 'A1', 'payer_id': '60054'},
        org_id='test-org', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='u@example.com', audit=fake_audit,
    )
    assert result['recent_check']['checked_by'] == 'kayla@example.com'
