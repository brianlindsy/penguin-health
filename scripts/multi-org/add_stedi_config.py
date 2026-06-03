#!/usr/bin/env python3
"""Seed (or update) a per-org STEDI_CONFIG record on penguin-health-org-config.

The admin API reads this item to know:
  - the provider NPI to put on every Stedi request (10-digit string)
  - the per-day Stedi transaction cap (cost guardrail)
  - which payers to surface in the UI dropdown by default
  - whether the feature is enabled for the org at all

Prerequisite: the shared Stedi API key must already be in Secrets Manager
at `penguin-health/stedi/api-key` (one key for all orgs — Stedi billing is
account-level, not org-level).

Usage:
    python scripts/multi-org/add_stedi_config.py \\
        --org-id catholic-charities-multi-org \\
        --npi 1234567890 \\
        --organization-name "Catholic Charities" \\
        --daily-cap 200 \\
        --preferred-payers SUNSHINE_STATE,CARELON_BH,BCBSFL_LUCET,AETNA,MEDICARE

Re-running this script overwrites the existing STEDI_CONFIG item.
"""

import argparse
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = 'penguin-health-org-config'


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--org-id', required=True)
    p.add_argument('--npi', required=True,
                   help='10-digit National Provider Identifier for the org.')
    p.add_argument('--organization-name', required=True,
                   help='Provider organization name (sent on Stedi requests).')
    p.add_argument('--daily-cap', type=int, required=True,
                   help='Max Stedi transactions per day for this org. '
                        'Each discovery counts as 1; each eligibility counts as 1.')
    p.add_argument('--preferred-payers', default='',
                   help='Comma-separated payer IDs to feature in the UI dropdown. '
                        'See lambda/multi-org/stedi/payer_registry.py for valid IDs.')
    p.add_argument('--disabled', action='store_true',
                   help='Write with enabled=False to pause without deleting.')
    p.add_argument('--demo-mode', action='store_true',
                   help='Route verify-patient calls to canned fixtures instead '
                        'of Stedi. Lets you exercise the full UI workflow '
                        '(discovery + eligibility + UI) without spending Stedi '
                        'transactions. See lambda/multi-org/stedi/demo_fixtures.py '
                        'for the patient list. Never enable for a production org.')
    p.add_argument('--census-enabled', action='store_true',
                   help='Opt the org into automated eligibility verification. '
                        'When set, the fhir_eligibility_poller Lambda polls the '
                        "org's FHIR API every ~15 minutes for new encounters "
                        'matching --encounter-filter-* and runs orchestrator.verify '
                        'for each. (The legacy flag name is kept on the DDB item '
                        'for backward compatibility.)')
    p.add_argument('--encounter-filter-class-codes', default='',
                   help='Comma-separated FHIR Encounter.class codes the poller '
                        'should match (e.g. "IMP,EMER"). Empty = no class filter.')
    p.add_argument('--encounter-filter-type-codes', default='',
                   help='Comma-separated FHIR Encounter.type codes the poller '
                        'should match. Empty = no type filter.')
    p.add_argument('--encounter-filter-statuses', default='',
                   help='Comma-separated FHIR Encounter.status values to match '
                        '(e.g. "planned,arrived,in-progress"). Empty = no status filter.')
    p.add_argument('--cob-enabled', action='store_true',
                   help='Opt the org into a Stedi /coordination-of-benefits call '
                        'when discovery + eligibility return ≥2 active coverages. '
                        'One extra Stedi transaction per check; off by default.')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true')
    return p.parse_args(argv)


def build_item(args):
    if not (args.npi.isdigit() and len(args.npi) == 10):
        raise SystemExit(f"--npi must be a 10-digit string, got {args.npi!r}")
    if args.daily_cap <= 0:
        raise SystemExit(f"--daily-cap must be positive, got {args.daily_cap}")
    preferred = [p.strip() for p in args.preferred_payers.split(',') if p.strip()]
    now = datetime.now(timezone.utc).isoformat()
    item = {
        'pk': f'ORG#{args.org_id}',
        'sk': 'STEDI_CONFIG',
        'gsi1pk': 'STEDI_CONFIG',
        'gsi1sk': f'ORG#{args.org_id}',
        'organization_id': args.org_id,
        'enabled': not args.disabled,
        'provider': {
            'npi': args.npi,
            'organization_name': args.organization_name,
        },
        'daily_cap': args.daily_cap,
        'preferred_payer_ids': preferred,
        'demo_mode': args.demo_mode,
        'census_enabled': args.census_enabled,
        'cob_enabled': args.cob_enabled,
        'created_at': now,
        'updated_at': now,
    }
    encounter_filter = {}
    if args.encounter_filter_class_codes:
        encounter_filter['class_codes'] = [
            c.strip() for c in args.encounter_filter_class_codes.split(',') if c.strip()
        ]
    if args.encounter_filter_type_codes:
        encounter_filter['type_codes'] = [
            c.strip() for c in args.encounter_filter_type_codes.split(',') if c.strip()
        ]
    if args.encounter_filter_statuses:
        encounter_filter['statuses'] = [
            s.strip() for s in args.encounter_filter_statuses.split(',') if s.strip()
        ]
    if encounter_filter:
        item['encounter_filter'] = encounter_filter
    return item


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
        f"Wrote STEDI_CONFIG for org={args.org_id} "
        f"(enabled={not args.disabled}, daily_cap={args.daily_cap}, "
        f"demo_mode={args.demo_mode}, census_enabled={args.census_enabled}, "
        f"cob_enabled={args.cob_enabled}, "
        f"encounter_filter={item.get('encounter_filter') or {}}, "
        f"preferred_payers={item['preferred_payer_ids']})"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
