"""Tests for stedi.stedi_client — focused on SSN redaction and the
X-Forwarded-For pass-through. Network layer is mocked via urllib."""

import io
import json
from unittest.mock import patch, MagicMock

from stedi import stedi_client


def test_redact_for_logging_scrubs_ssn_and_member_id():
    payload = {
        'subscriber': {
            'ssn': '123456789',
            'memberId': 'ABC123456789',
            'firstName': 'John',
        },
        'provider': {'npi': '1234567890'},
    }
    safe = stedi_client.redact_for_logging(payload)
    assert safe['subscriber']['ssn'] == '***REDACTED***'
    assert safe['subscriber']['memberId'].endswith('6789')
    assert safe['subscriber']['memberId'].startswith('*')
    assert safe['subscriber']['firstName'] == 'John'  # untouched
    assert safe['provider']['npi'] == '1234567890'   # untouched
    # Original is not mutated
    assert payload['subscriber']['ssn'] == '123456789'


def test_client_forwards_client_ip_as_x_forwarded_for():
    captured = {}

    def fake_urlopen(request, timeout):
        captured['headers'] = dict(request.headers)
        captured['body'] = request.data
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"controlNumber": "ABC"}'
        mock_response.status = 200
        mock_response.__enter__ = lambda self: self
        mock_response.__exit__ = lambda *a: None
        return mock_response

    client = stedi_client.StediClient('test-key', client_ip='10.1.2.3')
    with patch.object(stedi_client.urllib.request, 'urlopen', side_effect=fake_urlopen):
        result = client.check_eligibility({'provider': {'npi': '1'}, 'subscriber': {}, 'tradingPartnerServiceId': 'AETNA'})
    # urllib lowercases the header names
    assert captured['headers'].get('X-forwarded-for') == '10.1.2.3'
    assert captured['headers'].get('Authorization') == 'Key test-key'
    assert result == {'controlNumber': 'ABC'}
