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
                   help='Opt the org into the morning-census auto-run. Without '
                        'this flag, the scheduled census_runner Lambda will skip '
                        'this org. The EventBridge schedule itself is defined in '
                        'infra/components/audit_engine.py based on infra/config.py.')
    p.add_argument('--census-roster-source', default='demo_roster',
                   choices=['demo_roster', 'sftp', 'fhir'],
                   help='Where the morning roster comes from. Only "demo_roster" '
                        'is implemented; "sftp" and "fhir" raise NotImplementedError. '
                        'Default: demo_roster.')
    p.add_argument('--census-schedule-cron', default=None,
                   help='Cron expression for the morning EventBridge fire, in '
                        'AWS Schedule format (e.g. "0 11 * * ? *" = 6am ET in UTC). '
                        'Stored on the config for documentation; the actual '
                        'schedule is defined in infra (CDK redeploy required to '
                        'change). If omitted, falls back to infra default.')
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
        'census_roster_source': args.census_roster_source,
        'created_at': now,
        'updated_at': now,
    }
    if args.census_schedule_cron:
        item['census_schedule_cron'] = args.census_schedule_cron
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
        f"census_roster_source={args.census_roster_source}, "
        f"preferred_payers={item['preferred_payer_ids']})"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
