"""Tests for eligibility + discovery transformers — pure functions, no AWS."""

from stedi import eligibility_transformer, discovery_transformer


def test_eligibility_active_with_known_payer():
    response = {
        'controlNumber': 'X',
        'tradingPartnerServiceId': '60054',
        'subscriber': {'firstName': 'JOHN', 'lastName': 'DOE',
                       'memberId': 'A1', 'dateOfBirth': '19800101',
                       'groupNumber': 'G1'},
        'planInformation': {'planName': 'Aetna PPO'},
        'planDateInformation': {'planBegin': '20240101', 'planEnd': '20241231'},
        'benefitsInformation': [
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['30']},
            {'code': 'B', 'name': 'Co-Payment', 'benefitAmount': '25.00',
             'serviceTypeCodes': ['45'], 'authOrCertIndicator': 'Y'},
        ],
    }
    result = eligibility_transformer.transform(response)
    assert result['active'] is True
    assert result['status'] == 'active'
    assert 'Aetna' in result['payer']['name']
    assert result['payer']['payer_name_unknown'] is False
    assert result['subscriber']['member_id'] == 'A1'
    assert result['plan']['name'] == 'Aetna PPO'
    assert result['auth_required'] is True
    assert any(c['amount'] == '25.00' for c in result['copays'])


def test_eligibility_inactive():
    response = {
        'tradingPartnerServiceId': 'CIGNA',
        'subscriber': {'memberId': 'C1'},
        'planInformation': {},
        'planDateInformation': {'planEnd': '20240301'},
        'benefitsInformation': [{'code': '6', 'name': 'Inactive', 'serviceTypeCodes': ['30']}],
    }
    result = eligibility_transformer.transform(response)
    assert result['status'] == 'inactive'
    assert result['active'] is False


def test_service_types_breakdown_lists_one_entry_per_code_with_labels_and_auth():
    """The service_types array surfaces exactly what the payer returned,
    one row per distinct serviceTypeCode, with human-readable labels and
    rolled-up auth/copay detail. Order matches first appearance."""
    response = {
        'tradingPartnerServiceId': '60054',
        'subscriber': {'memberId': 'A1'},
        'planInformation': {},
        'planDateInformation': {},
        'benefitsInformation': [
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['30']},
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['45'],
             'authOrCertIndicator': 'Y'},
            {'code': 'B', 'name': 'Co-Payment', 'benefitAmount': '50.00',
             'serviceTypeCodes': ['45'], 'inPlanNetworkIndicatorCode': 'Y'},
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['MH']},
            {'code': 'I', 'name': 'Non-Covered', 'serviceTypeCodes': ['AI']},
            # 35 is dental — not in our inpatient-BH set but should still
            # be surfaced verbatim so UR sees the full payer response.
            {'code': 'I', 'name': 'Non-Covered', 'serviceTypeCodes': ['35']},
        ],
    }
    result = eligibility_transformer.transform(response)
    by_code = {st['code']: st for st in result['service_types']}

    assert set(by_code.keys()) == {'30', '45', 'MH', 'AI', '35'}
    # First-appearance order preserved.
    assert [st['code'] for st in result['service_types']] == ['30', '45', 'MH', 'AI', '35']

    assert by_code['30']['status'] == 'covered'
    assert by_code['30']['label'] == 'Health Benefit Plan Coverage'

    assert by_code['45']['status'] == 'covered'
    assert by_code['45']['label'] == 'Hospital - Inpatient'
    assert by_code['45']['auth_required'] is True
    assert by_code['45']['copays'] == [{'amount': '50.00', 'in_or_out_of_network': 'Y'}]

    assert by_code['MH']['status'] == 'covered'
    assert by_code['MH']['label'] == 'Mental Health'

    assert by_code['AI']['status'] == 'not_covered'
    assert by_code['AI']['label'] == 'Substance Abuse'

    assert by_code['35']['status'] == 'not_covered'
    assert by_code['35']['label'] == 'Dental Care'


def test_service_types_breakdown_handles_unknown_code_with_pass_through_label():
    """Service-type codes the X12 spec includes but we haven't put in
    SERVICE_TYPE_LABELS should still appear, with the raw code as label."""
    response = {
        'tradingPartnerServiceId': '60054',
        'subscriber': {},
        'planInformation': {},
        'planDateInformation': {},
        'benefitsInformation': [
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['XX']},
        ],
    }
    result = eligibility_transformer.transform(response)
    assert len(result['service_types']) == 1
    st = result['service_types'][0]
    assert st['code'] == 'XX'
    assert st['label'] == 'XX'  # passes through raw when label unknown
    assert st['status'] == 'covered'


def test_service_types_breakdown_active_wins_over_inactive_for_same_code():
    """If a payer returns both a code-1 and a code-I line for the same
    service type, treat it as covered (the EB01 spec doesn't strictly
    forbid this; some payers do it when there's a benefit limit)."""
    response = {
        'tradingPartnerServiceId': '60054',
        'subscriber': {},
        'planInformation': {},
        'planDateInformation': {},
        'benefitsInformation': [
            {'code': 'I', 'name': 'Non-Covered', 'serviceTypeCodes': ['MH']},
            {'code': '1', 'name': 'Active Coverage', 'serviceTypeCodes': ['MH']},
        ],
    }
    result = eligibility_transformer.transform(response)
    [st] = result['service_types']
    assert st['code'] == 'MH'
    assert st['status'] == 'covered'


def test_eligibility_unknown_payer_flagged():
    response = {
        'tradingPartnerServiceId': 'OBSCURE_PAYER',
        'subscriber': {},
        'planInformation': {},
        'planDateInformation': {},
        'benefitsInformation': [],
    }
    result = eligibility_transformer.transform(response)
    assert result['payer']['payer_name_unknown'] is True
    assert result['payer']['id'] == 'OBSCURE_PAYER'


def test_discovery_partitions_high_vs_review_needed():
    response = {
        'coveragesFound': 2,
        'discoveryId': 'd-1',
        'items': [
            {'confidence': {'level': 'HIGH', 'reason': 'exact match'},
             'tradingPartnerServiceId': '60054',
             'payer': {'name': 'Aetna'},
             'subscriber': {'memberId': 'A1'}},
            {'confidence': {'level': 'REVIEW_NEEDED', 'reason': 'name fuzzy'},
             'tradingPartnerServiceId': 'CIGNA',
             'payer': {'name': 'Cigna'},
             'subscriber': {'memberId': 'C1'}},
        ],
    }
    result = discovery_transformer.transform(response)
    assert len(result['high_confidence']) == 1
    assert len(result['review_needed']) == 1
    assert 'Aetna' in result['high_confidence'][0]['payer']['name']
    # 'CIGNA' is not a real Stedi ID; lookup_by_id returns a stub
    # {id: 'CIGNA', name: 'CIGNA', payer_name_unknown: True}.
    assert result['review_needed'][0]['payer']['payer_name_unknown'] is True
    assert result['review_needed'][0]['payer']['id'] == 'CIGNA'
    assert result['discovery_id'] == 'd-1'
