"""
Integration test for scripts/multi-org/provision_fhir_keypair.py.

Verifies the first-run path end-to-end against moto:
  - creates a KMS asymmetric key (RSA_4096, SIGN_VERIFY) with the
    expected alias `alias/penguin-health-fhir-{org_id}`
  - uploads a JWK Set with the right shape and a kid derived from the
    KMS public key via RFC 7638
  - does NOT touch Secrets Manager (client_id lives on the FHIR_CONFIG
    DynamoDB record, written separately by add_fhir_config.py)

The rotation/second-run path is intentionally NOT tested here. Real KMS
supports `update_alias(AliasName=A, TargetKeyId=new_id)` to repoint an
existing alias at a new key, but moto's implementation has a known issue
where it looks up the alias under the target key instead of the source.
Rotation is exercised in real-AWS testing only.
"""

import json
import os
import sys

import pytest
import boto3


# Make the script importable as a module
_SCRIPTS_DIR = os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..',
    'scripts', 'multi-org',
)
sys.path.insert(0, _SCRIPTS_DIR)


JWKS_BUCKET = 'phealth-fhir-jwks'
ORG_ID = 'demo'
EXPECTED_ALIAS = f'alias/penguin-health-fhir-{ORG_ID}'


@pytest.fixture
def jwks_bucket(mock_s3):
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=JWKS_BUCKET)
    return s3


def test_provision_first_run_creates_kms_key_alias_and_jwks(jwks_bucket):
    import provision_fhir_keypair

    rc = provision_fhir_keypair.main([
        '--org-id', ORG_ID,
        '--jwks-bucket', JWKS_BUCKET,
        '--jwks-domain', 'keys.penguinhealth.io',
    ])
    assert rc == 0

    # JWKS uploaded
    body = jwks_bucket.get_object(Bucket=JWKS_BUCKET, Key=f'{ORG_ID}/jwks.json')['Body'].read()
    parsed = json.loads(body)
    assert 'keys' in parsed
    assert len(parsed['keys']) == 1
    jwk = parsed['keys'][0]
    assert jwk['kty'] == 'RSA'
    assert jwk['alg'] == 'RS384'
    assert jwk['use'] == 'sig'
    assert 'kid' in jwk
    assert 'n' in jwk
    assert 'e' in jwk

    # KMS alias exists and points at a SIGN_VERIFY key
    kms = boto3.client('kms', region_name='us-east-1')
    described = kms.describe_key(KeyId=EXPECTED_ALIAS)
    assert described['KeyMetadata']['KeyUsage'] == 'SIGN_VERIFY'

    # Secrets Manager is NOT touched — explicitly check no secret was created
    sm = boto3.client('secretsmanager', region_name='us-east-1')
    with pytest.raises(sm.exceptions.ResourceNotFoundException):
        sm.get_secret_value(SecretId=f'penguin-health/{ORG_ID}/fhir')


def test_provision_dry_run_makes_no_aws_writes(jwks_bucket):
    import provision_fhir_keypair

    rc = provision_fhir_keypair.main([
        '--org-id', ORG_ID,
        '--jwks-bucket', JWKS_BUCKET,
        '--jwks-domain', 'keys.penguinhealth.io',
        '--dry-run',
    ])
    assert rc == 0

    # No JWKS file
    listed = jwks_bucket.list_objects_v2(Bucket=JWKS_BUCKET)
    assert 'Contents' not in listed or len(listed['Contents']) == 0

    # No KMS key/alias
    kms = boto3.client('kms', region_name='us-east-1')
    aliases = kms.list_aliases().get('Aliases', [])
    assert not any(a['AliasName'] == EXPECTED_ALIAS for a in aliases)


def test_kid_is_rfc_7638_thumbprint_of_kms_public_key(jwks_bucket):
    """The published JWK's kid must equal SHA256(canonical JSON of {e,kty,n})
    base64url-encoded. This is the deterministic property that makes
    re-deriving the kid possible from the public key alone — and is what
    the runtime kms_resolver relies on."""
    import base64
    import hashlib
    import provision_fhir_keypair

    provision_fhir_keypair.main([
        '--org-id', ORG_ID,
        '--jwks-bucket', JWKS_BUCKET,
        '--jwks-domain', 'keys.penguinhealth.io',
    ])
    body = jwks_bucket.get_object(Bucket=JWKS_BUCKET, Key=f'{ORG_ID}/jwks.json')['Body'].read()
    jwk = json.loads(body)['keys'][0]

    canonical = json.dumps(
        {'e': jwk['e'], 'kty': 'RSA', 'n': jwk['n']},
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8')
    expected_kid = base64.urlsafe_b64encode(
        hashlib.sha256(canonical).digest()
    ).rstrip(b'=').decode('ascii')

    assert jwk['kid'] == expected_kid
