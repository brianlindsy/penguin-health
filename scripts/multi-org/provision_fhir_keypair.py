#!/usr/bin/env python3
"""
Provision a per-org KMS-backed signing key for Credible (or any other
FHIR vendor that requires private_key_jwt client authentication).

What this does, in order:

1. Create a fresh KMS asymmetric key (RSA_4096, SIGN_VERIFY) and point
   the alias `alias/penguin-health-fhir-{org_id}` at it. The private key
   is generated inside KMS and never leaves AWS.
2. Fetch the public half via `kms:GetPublicKey` and build a JWK from it
   (RFC 7517). The `kid` is the RFC 7638 JWK thumbprint — deterministic
   from the public key bytes.
3. Upload the JWK as a JWK Set to the public JWKS bucket at
   `{org_id}/jwks.json`. CloudFront fronts it.
4. Print the JWKS URL to register with the FHIR vendor.

This script does NOT touch DynamoDB or Secrets Manager. The FHIR_CONFIG
record stores `client_id` + `kms_alias`; the Lambda resolves the alias
to a key ARN + kid at runtime. After provisioning, run
`add_fhir_config.py` to write the FHIR_CONFIG record.

Rotation flow: rerun this script. It creates a NEW KMS key (KMS
asymmetric keys cannot be rotated in place), repoints the alias to the
new key, and overwrites the JWK Set. See TODO at the bottom for the
overlap-aware variant that keeps both old + new JWKs published during
the vendor cache TTL.

Usage:
    python scripts/multi-org/provision_fhir_keypair.py \\
        --org-id demo \\
        --jwks-bucket phealth-fhir-jwks \\
        --jwks-domain keys.penguinhealth.io
"""

import argparse
import base64
import hashlib
import json
import sys

import boto3
from cryptography.hazmat.primitives import serialization


KEY_SPEC = 'RSA_4096'
KEY_USAGE = 'SIGN_VERIFY'
SIGNING_ALG = 'RS384'


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def _int_to_b64url(value: int) -> str:
    byte_length = (value.bit_length() + 7) // 8
    return _b64url(value.to_bytes(byte_length, 'big'))


def public_jwk_from_kms_key(public_key_der: bytes, *, alg=SIGNING_ALG) -> dict:
    """Convert KMS's SubjectPublicKeyInfo DER public key to a JWK + RFC 7638 kid."""
    public_key = serialization.load_der_public_key(public_key_der)
    numbers = public_key.public_numbers()
    n_b64 = _int_to_b64url(numbers.n)
    e_b64 = _int_to_b64url(numbers.e)
    jwk = {
        'kty': 'RSA',
        'use': 'sig',
        'alg': alg,
        'n': n_b64,
        'e': e_b64,
    }
    canonical = json.dumps(
        {'e': e_b64, 'kty': 'RSA', 'n': n_b64},
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8')
    jwk['kid'] = _b64url(hashlib.sha256(canonical).digest())
    return jwk


def parse_args(argv):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--org-id', required=True)
    p.add_argument('--jwks-bucket', required=True,
                   help='S3 bucket hosting JWKS (provisioned by the JwksHosting CDK construct).')
    p.add_argument('--jwks-domain', required=True,
                   help='Public hostname CloudFront serves the JWKS from.')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true',
                   help="Print what would be done; don't touch AWS.")
    return p.parse_args(argv)


def _jwks_key(org_id):
    return f'{org_id}/jwks.json'


def _kms_alias(org_id):
    return f'alias/penguin-health-fhir-{org_id}'


def _find_alias(kms, alias):
    """Return the TargetKeyId for `alias` if it exists, else None."""
    paginator = kms.get_paginator('list_aliases')
    for page in paginator.paginate():
        for entry in page.get('Aliases', []):
            if entry.get('AliasName') == alias:
                return entry.get('TargetKeyId')
    return None


def create_kms_key(kms, org_id, dry_run):
    alias = _kms_alias(org_id)

    if dry_run:
        print(f"[dry-run] would create KMS key with alias {alias} ({KEY_SPEC}, {KEY_USAGE})")
        return None, None

    # Always create a new key. Rotation semantics: the caller invoking
    # this script is explicitly asking for a new keypair.
    response = kms.create_key(
        Description=f'Penguin Health FHIR signing key for {org_id}',
        KeyUsage=KEY_USAGE,
        KeySpec=KEY_SPEC,
        Tags=[
            {'TagKey': 'Project', 'TagValue': 'penguin-health'},
            {'TagKey': 'OrgId', 'TagValue': org_id},
            {'TagKey': 'Purpose', 'TagValue': 'fhir-private-key-jwt'},
        ],
    )
    key_id = response['KeyMetadata']['KeyId']
    key_arn = response['KeyMetadata']['Arn']

    # Point the alias at the new key. List first so we know whether to
    # create-vs-update — both are valid against real KMS, but moto has a
    # known issue with update_alias against a fresh target key.
    existing = _find_alias(kms, alias)
    if existing is None:
        kms.create_alias(AliasName=alias, TargetKeyId=key_id)
        print(f"Created KMS alias {alias} -> {key_arn}")
    else:
        kms.update_alias(AliasName=alias, TargetKeyId=key_id)
        print(
            f"Updated KMS alias {alias} -> {key_arn} "
            f"(was pointing at {existing})"
        )

    return key_id, key_arn


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    kms = boto3.client('kms', region_name=args.region)

    key_id, key_arn = create_kms_key(kms, args.org_id, args.dry_run)

    if args.dry_run:
        print()
        print(f"org_id:       {args.org_id}")
        print(f"alias:        {_kms_alias(args.org_id)}")
        print(f"jwks s3:      s3://{args.jwks_bucket}/{_jwks_key(args.org_id)}")
        print(f"jwks url:     https://{args.jwks_domain}/{_jwks_key(args.org_id)}")
        print()
        print("[dry-run] no AWS writes performed.")
        return 0

    pub_response = kms.get_public_key(KeyId=key_id)
    public_key_der = pub_response['PublicKey']
    jwk = public_jwk_from_kms_key(public_key_der)
    jwk_set_body = json.dumps({'keys': [jwk]}, indent=2).encode('utf-8')

    s3_key = _jwks_key(args.org_id)
    s3 = boto3.client('s3', region_name=args.region)
    s3.put_object(
        Bucket=args.jwks_bucket,
        Key=s3_key,
        Body=jwk_set_body,
        ContentType='application/json',
        CacheControl='public, max-age=300',
    )

    print()
    print(f"org_id:       {args.org_id}")
    print(f"alias:        {_kms_alias(args.org_id)}")
    print(f"kms key arn:  {key_arn}")
    print(f"kid:          {jwk['kid']}")
    print(f"jwks s3:      s3://{args.jwks_bucket}/{s3_key}")
    print(f"jwks url:     https://{args.jwks_domain}/{s3_key}")
    print()
    print("Next steps:")
    print(f"  1. Give the FHIR vendor this JWKS URL: https://{args.jwks_domain}/{s3_key}")
    print(f"  2. Run add_fhir_config.py for org={args.org_id} with "
          f"--client-id <from-vendor> --jwks-url https://{args.jwks_domain}/{s3_key}")
    print(f"  3. Smoke-test by calling fhir.get_resource('{args.org_id}', ...)")
    return 0


# TODO(rotation): when rerunning to rotate, this script should:
#   1. Fetch the existing public JWK at s3://{bucket}/{org_id}/jwks.json
#   2. Append the NEW JWK alongside it (so the JWK Set has both)
#   3. Wait for the vendor's cache TTL to expire (manual coordination)
#   4. Remove the old JWK + schedule-delete the old KMS key
# Today the script just overwrites the file with the single new JWK, so the
# initial provision works but rotation needs an overlap-aware update first.


if __name__ == '__main__':
    sys.exit(main())
