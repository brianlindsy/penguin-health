#!/usr/bin/env python3
"""
Seed (or update) a per-org `FHIR_CONFIG` record on the
`penguin-health-org-config` DynamoDB table. The materializer Lambda reads
this record to know which FHIR vendor to call, which KMS key signs its
JWT assertions, and which Athena column on `charts_{org_id}` holds the
encounter identifier to look up.

Prerequisite: the org's KMS signing key + JWKS file must already be
provisioned via `provision_fhir_keypair.py`. That script creates a KMS
asymmetric key (RSA_4096, SIGN_VERIFY) under the alias
`alias/penguin-health-fhir-{org_id}` and uploads the public JWK to the
JWKS-hosting bucket. Pass the resulting JWKS URL and the `client_id`
Qualifacts issued to this script.

Authentication is OAuth2 client_credentials + private_key_jwt (RS384),
with the private key in KMS. There are no secrets stored anywhere
(Secrets Manager is not used) — the KMS alias is the single source of
truth for "which key signs requests for this org," and the Lambda
resolves the alias to a key ARN + kid at cold start.

Usage examples:

    # Demo (Credible sandbox)
    python scripts/multi-org/add_fhir_config.py \\
        --org-id demo \\
        --vendor credible \\
        --base-url https://fhir.cbhstg4.crediblebh.com \\
        --token-url https://sts-duende.cbhstg4.crediblebh.com/connect/token \\
        --client-id <client_id from Qualifacts> \\
        --jwks-url https://keys.penguinhealth.io/demo/jwks.json \\
        --source-column service_id_1

Re-running this script overwrites the existing FHIR_CONFIG item — that's
the intended way to rotate base URLs, change the JWKS URL, or flip
`--disabled`.
"""

import argparse
import sys

import boto3


TABLE_NAME = 'penguin-health-org-config'
DEFAULT_RESOURCE_TYPES = ['Encounter']  # v1 only materializes Encounter
KMS_ALIAS_PREFIX = 'alias/penguin-health-fhir-'


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--org-id', required=True,
                   help='Organization identifier, e.g. "demo".')
    p.add_argument('--vendor', required=True, choices=['credible'],
                   help='FHIR vendor. Only "credible" is supported in v1.')
    p.add_argument('--base-url', required=True,
                   help='FHIR R4 base URL (no trailing slash needed).')
    p.add_argument('--token-url', required=True,
                   help='OAuth2 token endpoint URL.')
    p.add_argument('--client-id', required=True,
                   help='client_id issued by the FHIR vendor (Qualifacts). '
                        'Embedded in the iss/sub claims of the JWT assertion '
                        'we sign with our KMS key.')
    p.add_argument('--jwks-url', required=True,
                   help='Public JWKS URL registered with the FHIR vendor. '
                        'Stored on the config for observability; the FHIR '
                        'client itself does not fetch it.')
    p.add_argument('--kms-alias', default=None,
                   help='KMS alias name pointing at the org\'s signing key. '
                        f'Defaults to "{KMS_ALIAS_PREFIX}{{org_id}}", which '
                        'is what provision_fhir_keypair.py creates.')
    p.add_argument('--source-column', required=True,
                   help='Column on charts_{org_id} that holds the encounter '
                        'identifier (e.g. "service_id_1", "clientvisit_id").')
    p.add_argument('--scopes', default='',
                   help='Space-separated OAuth scopes. Default empty = '
                        'send no scope param (Credible accepts this).')
    p.add_argument('--page-size', type=int, default=100,
                   help='FHIR _count page size. Default 100.')
    p.add_argument('--concurrency', type=int, default=4,
                   help='Max in-flight FHIR HTTP requests per org. Default 4.')
    p.add_argument('--disabled', action='store_true',
                   help='Write the record with enabled=False. Use to '
                        'pause an org without deleting its config.')
    p.add_argument('--region', default='us-east-1',
                   help='AWS region. Default us-east-1.')
    p.add_argument('--dry-run', action='store_true',
                   help='Print the item that would be written; do not write.')
    return p.parse_args(argv)


def build_item(args):
    scopes = [s for s in args.scopes.split(' ') if s.strip()]
    kms_alias = args.kms_alias or f'{KMS_ALIAS_PREFIX}{args.org_id}'
    return {
        'pk': f'ORG#{args.org_id}',
        'sk': 'FHIR_CONFIG',
        'gsi1pk': 'FHIR_CONFIG',
        'gsi1sk': f'ORG#{args.org_id}',
        'organization_id': args.org_id,
        'vendor': args.vendor,
        'base_url': args.base_url.rstrip('/'),
        'token_url': args.token_url,
        'auth_type': 'oauth2_client_credentials',
        'client_authentication': 'private_key_jwt',
        'signing_alg': 'RS384',
        'scopes': scopes,
        'client_id': args.client_id,
        'kms_alias': kms_alias,
        'jwks_url': args.jwks_url,
        'page_size': args.page_size,
        'concurrency': args.concurrency,
        'enabled': not args.disabled,
        'resource_types': DEFAULT_RESOURCE_TYPES,
        'fhir_mappings': {
            'encounter': {
                'source_table': f'charts_{args.org_id.replace("-", "_")}',
                'source_column': args.source_column,
                'fhir_lookup': 'by_id',
            },
        },
    }


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    item = build_item(args)

    if args.dry_run:
        import json
        print(json.dumps(item, indent=2, default=str))
        return 0

    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)
    print(
        f"Wrote FHIR_CONFIG for org={args.org_id} "
        f"(vendor={args.vendor}, enabled={not args.disabled}, "
        f"source_column={args.source_column})"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
