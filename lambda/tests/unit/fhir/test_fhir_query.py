import io
import json
import os
import sys
import urllib.error
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest
import boto3
import jwt
from cryptography.hazmat.primitives import serialization


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org'))


KMS_ALIAS = 'alias/penguin-health-fhir-demo'


def _rebind_kms_clients_to_mock():
    """Swap the module-level KMS clients in the signer + resolver to one
    bound to the active moto session. Necessary because both modules cache
    a boto3.client at import time, before moto's mock_aws() is in scope."""
    from fhir import kms_resolver, kms_signer
    mocked = boto3.client('kms', region_name='us-east-1')
    kms_signer._kms = mocked
    kms_resolver._kms = mocked
    kms_resolver.reset_cache_for_tests()


@pytest.fixture
def kms_key(mock_dynamodb):
    """Provision a KMS asymmetric RSA_4096 key in moto, attach the demo
    alias to it, and return (alias, key_arn, derived_public_key). The
    resolver derives the kid from the public key at runtime, so we don't
    expose a precomputed kid here."""
    _rebind_kms_clients_to_mock()
    kms = boto3.client('kms', region_name='us-east-1')
    response = kms.create_key(
        KeySpec='RSA_4096', KeyUsage='SIGN_VERIFY',
        Description='test fhir signing key',
    )
    key_id = response['KeyMetadata']['KeyId']
    arn = response['KeyMetadata']['Arn']
    kms.create_alias(AliasName=KMS_ALIAS, TargetKeyId=key_id)
    pub = kms.get_public_key(KeyId=key_id)
    public_key = serialization.load_der_public_key(pub['PublicKey'])
    return {
        'alias': KMS_ALIAS,
        'arn': arn,
        'public_key': public_key,
    }


@pytest.fixture
def seeded_demo_config(mock_dynamodb, kms_key):
    table = mock_dynamodb.Table('penguin-health-org-config')

    table.put_item(Item={
        'pk': 'ORG#demo',
        'sk': 'FHIR_CONFIG',
        'gsi1pk': 'FHIR_CONFIG',
        'gsi1sk': 'ORG#demo',
        'organization_id': 'demo',
        'vendor': 'credible',
        'base_url': 'https://fhir.example.test',
        'token_url': 'https://sts.example.test/connect/token',
        'auth_type': 'oauth2_client_credentials',
        'client_authentication': 'private_key_jwt',
        'scopes': [],
        'client_id': 'cid',
        'kms_alias': kms_key['alias'],
        'page_size': 100,
        'concurrency': 4,
        'enabled': True,
        'fhir_mappings': {
            'encounter': {
                'source_table': 'charts_demo',
                'source_column': 'service_id_1',
                'fhir_lookup': 'by_id',
            }
        },
    })

    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    from fhir import kms_resolver
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()
    kms_resolver.reset_cache_for_tests()
    yield
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()
    kms_resolver.reset_cache_for_tests()


def _http_response(status, body, headers=None):
    response = MagicMock()
    response.status = status
    response.read.return_value = json.dumps(body).encode('utf-8') if not isinstance(body, bytes) else body
    response.__enter__ = lambda self_: self_
    response.__exit__ = lambda *_: None
    return response


def _http_error(status, body=b''):
    return urllib.error.HTTPError(
        url='http://example.test', code=status, msg='err', hdrs={}, fp=io.BytesIO(body)
    )


def _make_sequence_urlopen(items):
    """Wrap a sequence of HTTP responses/errors as a urlopen replacement.
    Exceptions are raised (mimicking real urlopen 4xx/5xx behavior); responses are returned."""
    it = iter(items)

    def fake_urlopen(request, timeout=None):
        item = next(it)
        if isinstance(item, BaseException):
            raise item
        return item

    return fake_urlopen


def _token_response(token='tok-1', expires_in=3600):
    return _http_response(200, {'access_token': token, 'expires_in': expires_in, 'token_type': 'Bearer'})


def test_get_resource_happy_path(seeded_demo_config):
    from fhir import get_resource

    encounter = {'resourceType': 'Encounter', 'id': 'enc-1', 'status': 'finished'}
    calls = [_token_response(), _http_response(200, encounter)]
    with patch('urllib.request.urlopen', side_effect=calls):
        result = get_resource('demo', 'Encounter', 'enc-1')
    assert result == encounter


def test_get_resource_404_raises_not_found(seeded_demo_config):
    from fhir import FhirNotFound, get_resource

    calls = [_token_response(), _http_error(404)]
    with patch('urllib.request.urlopen', side_effect=calls):
        with pytest.raises(FhirNotFound):
            get_resource('demo', 'Encounter', 'missing')


def _capture_token_request():
    """Helper: returns (fake_urlopen, captured_dict). The fake responds with
    a 200 token to STS calls and a 200 Encounter to FHIR calls, while
    capturing the form body of the first STS call."""
    captured = {}

    def fake_urlopen(request, timeout=None):
        if request.full_url.startswith('https://sts'):
            captured.setdefault('data', request.data)
            return _token_response()
        return _http_response(200, {'resourceType': 'Encounter', 'id': 'enc-1'})

    return fake_urlopen, captured


def _parse_form(body_bytes):
    return dict(urllib.parse.parse_qsl(body_bytes.decode('utf-8')))


def test_token_request_uses_private_key_jwt_assertion(seeded_demo_config, kms_key):
    """Token request body sends a signed JWT assertion, NOT client_secret."""
    from fhir import get_resource

    fake_urlopen, captured = _capture_token_request()
    with patch('urllib.request.urlopen', side_effect=fake_urlopen):
        get_resource('demo', 'Encounter', 'enc-1')

    form = _parse_form(captured['data'])
    assert form['grant_type'] == 'client_credentials'
    assert form['client_assertion_type'] == (
        'urn:ietf:params:oauth:client-assertion-type:jwt-bearer'
    )
    assert 'client_assertion' in form
    # Critical: client_secret must NEVER appear in this flow.
    assert 'client_secret' not in form
    assert 'scope' not in form


def test_client_assertion_jwt_claims_and_header(seeded_demo_config, kms_key):
    """The assertion JWT must have the right header (RS384 + kid) and the
    right claim set (iss, sub, aud, iat, exp, jti) and verify against our
    public key."""
    from fhir import get_resource

    fake_urlopen, captured = _capture_token_request()
    with patch('urllib.request.urlopen', side_effect=fake_urlopen):
        get_resource('demo', 'Encounter', 'enc-1')

    form = _parse_form(captured['data'])
    assertion = form['client_assertion']

    header = jwt.get_unverified_header(assertion)
    assert header['alg'] == 'RS384'
    # The kid is derived by kms_resolver from the live public key; verify
    # the assertion's kid matches what we'd compute ourselves.
    from fhir.kms_resolver import resolve_alias
    expected_kid = resolve_alias(kms_key['alias'])['kid']
    assert header['kid'] == expected_kid
    assert header['typ'] == 'JWT'

    claims = jwt.decode(
        assertion,
        kms_key['public_key'],
        algorithms=['RS384'],
        audience='https://sts.example.test/connect/token',
    )
    assert claims['iss'] == 'cid'
    assert claims['sub'] == 'cid'
    assert claims['aud'] == 'https://sts.example.test/connect/token'
    assert claims['exp'] > claims['iat']
    assert (claims['exp'] - claims['iat']) <= 600  # short-lived
    assert isinstance(claims['jti'], str) and len(claims['jti']) > 0


def test_each_assertion_has_unique_jti(seeded_demo_config):
    """Replay protection: every token request gets a fresh `jti`."""
    from fhir import get_resource

    captured_data = []

    def fake_urlopen(request, timeout=None):
        if request.full_url.startswith('https://sts'):
            captured_data.append(request.data)
            return _token_response(expires_in=1)  # force re-auth each call
        return _http_response(200, {'resourceType': 'Encounter', 'id': 'enc-1'})

    with patch('urllib.request.urlopen', side_effect=fake_urlopen), \
         patch('time.time', side_effect=lambda real=__import__('time').time: real()):
        # Force two token requests by invalidating between calls
        get_resource('demo', 'Encounter', 'enc-1')
        from fhir.fhir_query import get_client
        get_client('demo')._invalidate_token()
        get_resource('demo', 'Encounter', 'enc-2')

    assert len(captured_data) == 2
    assertions = [_parse_form(d)['client_assertion'] for d in captured_data]
    jtis = [
        jwt.decode(a, options={'verify_signature': False})['jti']
        for a in assertions
    ]
    assert jtis[0] != jtis[1], f"jti must be unique per request, got {jtis}"


def test_token_request_includes_scope_when_configured(seeded_demo_config, mock_dynamodb):
    table = mock_dynamodb.Table('penguin-health-org-config')
    table.update_item(
        Key={'pk': 'ORG#demo', 'sk': 'FHIR_CONFIG'},
        UpdateExpression='SET scopes = :s',
        ExpressionAttributeValues={':s': ['system/Encounter.read', 'system/Patient.read']},
    )
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()

    from fhir import get_resource
    fake_urlopen, captured = _capture_token_request()
    with patch('urllib.request.urlopen', side_effect=fake_urlopen):
        get_resource('demo', 'Encounter', 'enc-1')

    form = _parse_form(captured['data'])
    assert form['scope'] == 'system/Encounter.read system/Patient.read'


def test_token_cached_across_calls(seeded_demo_config):
    from fhir import get_resource

    call_log = []

    def fake_urlopen(request, timeout=None):
        if request.full_url.startswith('https://sts'):
            call_log.append('token')
            return _token_response()
        call_log.append('fhir')
        return _http_response(200, {'resourceType': 'Encounter', 'id': 'enc'})

    with patch('urllib.request.urlopen', side_effect=fake_urlopen):
        get_resource('demo', 'Encounter', 'a')
        get_resource('demo', 'Encounter', 'b')
        get_resource('demo', 'Encounter', 'c')

    assert call_log.count('token') == 1
    assert call_log.count('fhir') == 3


def test_401_triggers_single_refresh_then_succeeds(seeded_demo_config):
    from fhir import get_resource

    encounter = {'resourceType': 'Encounter', 'id': 'enc-1'}
    fake = _make_sequence_urlopen([
        _token_response('tok-1'),
        _http_error(401),
        _token_response('tok-2'),
        _http_response(200, encounter),
    ])

    with patch('urllib.request.urlopen', side_effect=fake):
        result = get_resource('demo', 'Encounter', 'enc-1')
    assert result == encounter


def test_401_after_refresh_raises_auth_error(seeded_demo_config):
    from fhir import FhirAuthError, get_resource

    fake = _make_sequence_urlopen([
        _token_response('tok-1'),
        _http_error(401),
        _token_response('tok-2'),
        _http_error(401),
    ])

    with patch('urllib.request.urlopen', side_effect=fake):
        with pytest.raises(FhirAuthError):
            get_resource('demo', 'Encounter', 'enc-1')


def test_429_retries_then_fails(seeded_demo_config):
    from fhir import FhirRateLimited, get_resource

    fake = _make_sequence_urlopen([
        _token_response(),
        _http_error(429),
        _http_error(429),
        _http_error(429),
    ])

    with patch('urllib.request.urlopen', side_effect=fake), \
         patch('time.sleep'):
        with pytest.raises(FhirRateLimited):
            get_resource('demo', 'Encounter', 'enc-1')


def test_5xx_retries_then_fails_with_upstream_error(seeded_demo_config):
    from fhir import FhirUpstreamError, get_resource

    fake = _make_sequence_urlopen([
        _token_response(),
        _http_error(502),
        _http_error(503),
        _http_error(500),
    ])

    with patch('urllib.request.urlopen', side_effect=fake), \
         patch('time.sleep'):
        with pytest.raises(FhirUpstreamError):
            get_resource('demo', 'Encounter', 'enc-1')


def test_search_paginates_via_next_link(seeded_demo_config):
    from fhir import search

    page1 = {
        'resourceType': 'Bundle',
        'entry': [{'resource': {'resourceType': 'Encounter', 'id': 'e1'}}],
        'link': [{'relation': 'next', 'url': 'https://fhir.example.test/Encounter?page=2'}],
    }
    page2 = {
        'resourceType': 'Bundle',
        'entry': [{'resource': {'resourceType': 'Encounter', 'id': 'e2'}}],
        'link': [],
    }

    calls = iter([_token_response(), _http_response(200, page1), _http_response(200, page2)])
    with patch('urllib.request.urlopen', side_effect=lambda req, timeout=None: next(calls)):
        ids = [r['id'] for r in search('demo', 'Encounter', {'patient': 'p-1'})]
    assert ids == ['e1', 'e2']


def test_search_respects_max_results(seeded_demo_config):
    from fhir import search

    page1 = {
        'resourceType': 'Bundle',
        'entry': [
            {'resource': {'resourceType': 'Encounter', 'id': 'e1'}},
            {'resource': {'resourceType': 'Encounter', 'id': 'e2'}},
            {'resource': {'resourceType': 'Encounter', 'id': 'e3'}},
        ],
        'link': [{'relation': 'next', 'url': 'https://fhir.example.test/Encounter?page=2'}],
    }
    calls = iter([_token_response(), _http_response(200, page1)])
    with patch('urllib.request.urlopen', side_effect=lambda req, timeout=None: next(calls)):
        ids = [r['id'] for r in search('demo', 'Encounter', {}, max_results=2)]
    assert ids == ['e1', 'e2']


def test_search_respects_max_pages(seeded_demo_config):
    from fhir import search

    page = lambda i: {
        'resourceType': 'Bundle',
        'entry': [{'resource': {'resourceType': 'Encounter', 'id': f'e{i}'}}],
        'link': [{'relation': 'next', 'url': f'https://fhir.example.test/Encounter?page={i+1}'}],
    }
    calls = iter([_token_response(), _http_response(200, page(1)), _http_response(200, page(2))])
    with patch('urllib.request.urlopen', side_effect=lambda req, timeout=None: next(calls)):
        ids = [r['id'] for r in search('demo', 'Encounter', {}, max_pages=2)]
    assert ids == ['e1', 'e2']


def test_unconfigured_org_raises_before_any_http(mock_dynamodb):
    from fhir import FhirOrgNotConfigured, get_resource
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirOrgNotConfigured):
            get_resource('not-configured', 'Encounter', 'x')


def _put_fhir_config(mock_dynamodb, **overrides):
    """Helper: write a minimally-valid FHIR_CONFIG item with optional overrides."""
    item = {
        'pk': f'ORG#{overrides["organization_id"]}',
        'sk': 'FHIR_CONFIG',
        'gsi1pk': 'FHIR_CONFIG',
        'gsi1sk': f'ORG#{overrides["organization_id"]}',
        'vendor': 'credible',
        'base_url': 'https://fhir.example.test',
        'token_url': 'https://sts.example.test/connect/token',
        'auth_type': 'oauth2_client_credentials',
        'scopes': [],
        'client_id': 'cid',
        'kms_alias': 'alias/penguin-health-fhir-demo',
        'enabled': True,
    }
    item.update(overrides)
    mock_dynamodb.Table('penguin-health-org-config').put_item(Item=item)


def _reset_caches():
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    from fhir import kms_resolver
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()
    kms_resolver.reset_cache_for_tests()


def test_disabled_org_raises(mock_dynamodb):
    from fhir import FhirOrgNotConfigured, get_resource

    _put_fhir_config(mock_dynamodb, organization_id='disabled', enabled=False)
    _reset_caches()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirOrgNotConfigured):
            get_resource('disabled', 'Encounter', 'x')


def test_unknown_vendor_raises(mock_dynamodb):
    from fhir import FhirOrgNotConfigured, get_resource

    _put_fhir_config(
        mock_dynamodb,
        organization_id='weirdvendor',
        vendor='epic-or-something',
    )
    _reset_caches()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirOrgNotConfigured):
            get_resource('weirdvendor', 'Encounter', 'x')


def test_missing_client_id_raises_auth_error(mock_dynamodb):
    """FHIR_CONFIG without client_id should fail loudly before any HTTP."""
    from fhir import FhirAuthError, get_resource

    _put_fhir_config(mock_dynamodb, organization_id='noclient', client_id=None)
    # Remove the client_id we just wrote (put_item would have included it)
    mock_dynamodb.Table('penguin-health-org-config').update_item(
        Key={'pk': 'ORG#noclient', 'sk': 'FHIR_CONFIG'},
        UpdateExpression='REMOVE client_id',
    )
    _reset_caches()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirAuthError) as exc_info:
            get_resource('noclient', 'Encounter', 'x')
    assert 'client_id' in str(exc_info.value) or 'kms_alias' in str(exc_info.value)


def test_missing_kms_alias_raises_auth_error(mock_dynamodb):
    """FHIR_CONFIG without kms_alias should fail loudly before any HTTP."""
    from fhir import FhirAuthError, get_resource

    _put_fhir_config(mock_dynamodb, organization_id='noalias')
    mock_dynamodb.Table('penguin-health-org-config').update_item(
        Key={'pk': 'ORG#noalias', 'sk': 'FHIR_CONFIG'},
        UpdateExpression='REMOVE kms_alias',
    )
    _reset_caches()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirAuthError) as exc_info:
            get_resource('noalias', 'Encounter', 'x')
    assert 'client_id' in str(exc_info.value) or 'kms_alias' in str(exc_info.value)


def test_alias_pointing_at_nothing_raises_auth_error(mock_dynamodb):
    """kms_alias references a name that doesn't exist in KMS → FhirAuthError."""
    from fhir import FhirAuthError, get_resource

    _rebind_kms_clients_to_mock()
    _put_fhir_config(
        mock_dynamodb,
        organization_id='ghost',
        kms_alias='alias/penguin-health-fhir-this-alias-does-not-exist',
    )
    _reset_caches()

    with patch('urllib.request.urlopen', side_effect=AssertionError('must not be called')):
        with pytest.raises(FhirAuthError) as exc_info:
            get_resource('ghost', 'Encounter', 'x')
    assert 'alias' in str(exc_info.value).lower() or 'not found' in str(exc_info.value).lower()
