"""End-to-end tests for demo_mode routing: orchestrator -> DemoStediClient
-> demo_fixtures, with no Stedi network involvement.

Updated for V2: synthetic-surname patient roster (Sample, Testpatient,
Mockerson, Faker, etc.) and the (member_id, payer_id) direct-path fallback.
"""

import pytest
from unittest.mock import MagicMock

from stedi import demo_fixtures, orchestrator
from stedi.demo_client import DemoStediClient


@pytest.fixture
def org_config():
    return {
        'organization_id': 'demo',
        'enabled': True,
        'demo_mode': True,
        'provider': {'npi': '1999999984', 'organization_name': 'Provider Name'},
        'daily_cap': 200,
        'preferred_payer_ids': [],
    }


@pytest.fixture
def fake_audit():
    audit = MagicMock()
    audit.reserve_capacity.return_value = 1
    audit.recent_check_summary.return_value = None
    audit.recent_checks_for_patient.return_value = []
    audit.write_audit.side_effect = lambda **kw: kw.get('request_id') or 'req-stub'
    return audit


@pytest.fixture
def demo_client():
    """DemoStediClient with a real-client backend that should never be called
    when fixtures match — assert that's true."""
    real = MagicMock()
    real.check_eligibility = MagicMock(side_effect=AssertionError("real client called for matched scenario"))
    real.check_insurance_discovery = MagicMock(side_effect=AssertionError("real client called for matched scenario"))
    return DemoStediClient(real_client=real)


def test_robert_testpatient_returns_two_high_hits_and_primary_active(org_config, fake_audit, demo_client):
    """The 'messy multi-payer' scenario should produce both Aetna and FL
    Medicaid coverages, with one promoted to primary."""
    result = orchestrator.verify(
        {'first_name': 'Robert', 'last_name': 'Testpatient', 'dob': '19780214'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['path'] == 'discovery_first'
    assert result['primary_coverage'] is not None
    assert result['primary_coverage']['status'] == 'active'
    assert len(result['secondary_coverages']) == 1
    payer_names = sorted([result['primary_coverage']['payer']['name']]
                          + [c['payer']['name'] for c in result['secondary_coverages']])
    assert any('Aetna' in n for n in payer_names)
    assert any('Medicaid' in n or 'Sunshine' in n for n in payer_names)


def test_jane_sample_dependent_returns_review_needed_only(org_config, fake_audit, demo_client):
    """Dependent with last-name mismatch is REVIEW_NEEDED, so no follow-up
    eligibility call and no primary coverage."""
    result = orchestrator.verify(
        {'first_name': 'Jane', 'last_name': 'Sample', 'dob': '20010925'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['primary_coverage'] is None
    assert len(result['discovery_review_needed']) == 1
    item = result['discovery_review_needed'][0]
    assert 'SMITH SAMPLE' in (item.get('confidence_reason') or '').upper() or \
           'mismatch' in (item.get('confidence_reason') or '').lower()


def test_maria_mockerson_medicare_active_direct_path(org_config, fake_audit, demo_client):
    """Maria is a direct-path patient — orchestrator should hit eligibility
    only (no discovery call) when member_id + payer_id are supplied."""
    result = orchestrator.verify(
        {'first_name': 'Maria', 'last_name': 'Mockerson', 'dob': '19550630',
         'member_id': '1AB2-CD3-EF45', 'payer_id': '09101'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['path'] == 'direct'
    assert result['primary_coverage'] is not None
    assert result['primary_coverage']['status'] == 'active'
    assert 'Medicare' in result['primary_coverage']['payer']['name']


def test_nora_faker_no_coverage(org_config, fake_audit, demo_client):
    result = orchestrator.verify(
        {'first_name': 'Nora', 'last_name': 'Faker', 'dob': '19900101'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['primary_coverage'] is None
    assert result['secondary_coverages'] == []
    assert result['discovery_review_needed'] == []


def test_daniel_demoson_recently_inactivated(org_config, fake_audit, demo_client):
    """Aetna terminated 12 days ago — eligibility shows inactive and
    _derive_discrepancies fires 'plan terminated within 30 days'."""
    result = orchestrator.verify(
        {'first_name': 'Daniel', 'last_name': 'Demoson', 'dob': '19850712'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['primary_coverage'] is not None
    assert result['primary_coverage']['status'] == 'inactive'
    assert any('terminated' in d.lower() for d in result['discrepancies'])


def test_patricia_stub_direct_path_with_auth_required(org_config, fake_audit, demo_client):
    """Direct-path patient (Sunshine member ID on file) with auth required
    for inpatient BH."""
    result = orchestrator.verify(
        {'first_name': 'Patricia', 'last_name': 'Stub', 'dob': '19710505',
         'member_id': 'SUNSHINE-003', 'payer_id': '68068'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert result['path'] == 'direct'
    assert result['primary_coverage']['active'] is True
    assert result['primary_coverage']['auth_required'] is True


def test_james_example_service_type_denied(org_config, fake_audit, demo_client):
    """Cigna is active overall (code 1 for service-type 30) but inpatient
    BH (45/MH/AI) is explicitly non-covered (code I)."""
    result = orchestrator.verify(
        {'first_name': 'James', 'last_name': 'Example', 'dob': '19831120'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    cov = result['primary_coverage']
    assert cov is not None
    assert cov['status'] == 'active'
    assert cov['service_type_status'] == 'not_covered'


def test_unknown_patient_falls_through_to_real_client(org_config, fake_audit):
    """An unrecognized patient in demo mode should still try the real
    Stedi client (so a demo-mode org can also test real patients)."""
    real = MagicMock()
    real.check_insurance_discovery.return_value = {
        'coveragesFound': 0, 'discoveryId': 'real-1', 'status': 'COMPLETE',
        'items': [], 'errors': [],
    }
    client = DemoStediClient(real_client=real)

    orchestrator.verify(
        {'first_name': 'Unknown', 'last_name': 'Person', 'dob': '19850101'},
        org_id='demo', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert real.check_insurance_discovery.call_count == 1


def test_unknown_patient_with_no_real_client_returns_empty_discovery(org_config, fake_audit):
    """When demo_mode is on and there's no real Stedi client wired in
    (client_factory.build_client passes real_client=None), an unrecognized
    patient should NOT crash — it should return an empty discovery
    response so UR can rerun with corrected demographics."""
    client = DemoStediClient(real_client=None)

    result = orchestrator.verify(
        {'first_name': 'Mistyped', 'last_name': 'Patient', 'dob': '19000101'},
        org_id='demo', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    # No primary coverage, no review items — clean "no coverage found".
    assert result['primary_coverage'] is None
    assert result['secondary_coverages'] == []
    assert result['discovery_review_needed'] == []


def test_direct_path_with_no_real_client_returns_empty_eligibility(org_config, fake_audit):
    """Direct-path verify (member_id + payer) for an unknown patient
    with no real client should also gracefully return an empty
    eligibility response, not crash on `NoneType.check_eligibility`."""
    client = DemoStediClient(real_client=None)

    result = orchestrator.verify(
        {'first_name': 'Mistyped', 'last_name': 'Patient', 'dob': '19000101',
         'member_id': 'BOGUS-999', 'payer_id': '99999'},
        org_id='demo', org_config=org_config, stedi_client=client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    # Eligibility returned an empty benefits array; orchestrator surfaces
    # that as no_coverage (no benefits → status 'no_coverage').
    assert result['primary_coverage'] is not None
    assert result['primary_coverage']['status'] == 'no_coverage'


def test_copy_block_renders_for_demo_scenario(org_config, fake_audit, demo_client):
    result = orchestrator.verify(
        {'first_name': 'Robert', 'last_name': 'Testpatient', 'dob': '19780214'},
        org_id='demo', org_config=org_config, stedi_client=demo_client,
        client_ip='10.0.0.1', user_email='ur@example.com', audit=fake_audit,
    )

    assert 'PRIMARY INSURANCE' in result['copy_block']
    assert 'INSURANCE #2' in result['copy_block']


# ---- direct-path eligibility lookup ------------------------------------

def test_direct_eligibility_lookup_by_member_id():
    """ELIGIBILITY_DIRECT_SCENARIOS is hit when UR types a member ID into
    the verify form for a patient who exists in the demo roster."""
    response = demo_fixtures.lookup_direct_eligibility('AETNA12345', '60054')
    assert response is not None
    assert response['tradingPartnerServiceId'] == '60054'
    assert response['subscriber']['memberId'] == 'AETNA12345'


def test_direct_eligibility_lookup_misses_for_unknown_member():
    assert demo_fixtures.lookup_direct_eligibility('NOT-A-REAL-ID', '60054') is None


def test_demo_client_uses_direct_lookup_when_name_misses(org_config):
    """If the patient name isn't in SCENARIOS but the (member_id, payer_id)
    is in ELIGIBILITY_DIRECT_SCENARIOS, DemoStediClient should still return
    the canned response without calling real Stedi."""
    real = MagicMock()
    real.check_eligibility = MagicMock(side_effect=AssertionError("should not be called"))
    client = DemoStediClient(real_client=real)

    # Different name from any SCENARIOS entry, but a known direct-path member ID.
    payload = {
        'provider': {'npi': '1999999984', 'organizationName': 'Provider Name'},
        'subscriber': {'firstName': 'AnonName', 'lastName': 'AnonName',
                       'dateOfBirth': '20000101', 'memberId': 'AETNA12345'},
        'tradingPartnerServiceId': '60054',
    }
    response = client.check_eligibility(payload)
    assert response['tradingPartnerServiceId'] == '60054'
    assert response['subscriber']['memberId'] == 'AETNA12345'


# ---- demo_fixtures sanity checks ----------------------------------------

def test_lookup_is_case_insensitive_on_name():
    s1 = demo_fixtures.lookup('robert', 'testpatient', '19780214')
    s2 = demo_fixtures.lookup('ROBERT', 'Testpatient', '19780214')
    assert s1 is not None
    assert s1 == s2


def test_lookup_returns_none_on_dob_mismatch():
    assert demo_fixtures.lookup('Robert', 'Testpatient', '19000101') is None


def test_lookup_returns_none_for_unknown_patient():
    assert demo_fixtures.lookup('Some', 'Stranger', '19900101') is None


def test_list_scenarios_returns_full_roster():
    scenarios = demo_fixtures.list_scenarios()
    assert len(scenarios) == 11
    names = {(s['first_name'], s['last_name']) for s in scenarios}
    expected = {
        ('Jane', 'Sample'), ('Robert', 'Testpatient'), ('Maria', 'Mockerson'),
        ('Nora', 'Faker'), ('Daniel', 'Demoson'), ('Linda', 'Sandbox'),
        ('Tyler', 'Fixture'), ('Patricia', 'Stub'), ('James', 'Example'),
        ('Sarah', 'Placeholder'), ('Karen', 'Examplez'),
    }
    assert names == expected


def test_census_roster_matches_scenarios():
    """Every patient in CENSUS_ROSTER must have a corresponding SCENARIOS
    entry so the census runner doesn't fall through to real Stedi."""
    for patient in demo_fixtures.CENSUS_ROSTER:
        key = (patient['first_name'].lower(), patient['last_name'].lower())
        assert key in demo_fixtures.SCENARIOS, f"missing fixture for {key}"
        scenario = demo_fixtures.SCENARIOS[key]
        assert scenario['expected_dob'] == patient['dob']
