"""Tests for eligibility + discovery transformers — pure functions, no AWS."""

from stedi import eligibility_transformer, discovery_transformer


def test_eligibility_active_with_known_payer():
    response = {
        'controlNumber': 'X',
        'tradingPartnerServiceId': 'AETNA',
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
    assert result['payer']['name'] == 'Aetna'
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
             'tradingPartnerServiceId': 'AETNA',
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
    assert result['high_confidence'][0]['payer']['name'] == 'Aetna'
    assert result['review_needed'][0]['payer']['name'] == 'Cigna'
    assert result['discovery_id'] == 'd-1'
