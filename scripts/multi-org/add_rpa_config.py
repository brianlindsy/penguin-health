#!/usr/bin/env python3
"""Seed (or update) a per-org RPA_CONFIG record on penguin-health-org-config.

The Fargate RPA runner reads this item to know:
  - which vendor's authenticator to dispatch (`vendor`)
  - the playbook to drive (`playbook_id`, resolved via load_playbook)
  - the timezone-aware run window, blackout dates, and rate limit
  - any per-vendor URL/scope overrides under `vendor_settings.{vendor}`

Prerequisites:
  1. The bot's client_id + client_secret must already be in Secrets Manager
     at `penguin-health/rpa/{org_id}/credentials` as JSON
     `{"client_id": "...", "client_secret": "..."}`. Provision separately:

        aws secretsmanager create-secret \\
            --name penguin-health/rpa/{org_id}/credentials \\
            --secret-string '{"client_id":"...","client_secret":"..."}'

  2. A playbook item must exist (run `seed_rpa_playbook.py` first).

Usage:
    python scripts/multi-org/add_rpa_config.py \\
        --org-id demo \\
        --vendor centralreach \\
        --display-name "Demo CR clinical-notes bot" \\
        --base-url https://members.centralreach.com \\
        --bot-username "rpa-bot+demo" \\
        --playbook-id cr-notes-v1 \\
        --timezone America/Chicago \\
        --allowed-hours-start 06:00 \\
        --allowed-hours-end 20:00 \\
        --rate-limit-ms 1500 \\
        --blackout-dates 2026-12-25,2027-01-01

Re-running this script overwrites the existing RPA_CONFIG item.

Enabling the EventBridge schedule is a separate step — add an entry to
`_PER_ORG_SCHEDULES` in `infra/components/rpa.py` and redeploy.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = 'penguin-health-org-config'

_VALID_VENDORS = ('centralreach',)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--org-id', required=True)
    p.add_argument('--vendor', required=True, choices=_VALID_VENDORS,
                   help='Vendor key; must match a module under '
                        'lambda/multi-org/rpa/authenticators/.')
    p.add_argument('--display-name', required=True,
                   help='Human-friendly name shown in the runs UI.')
    p.add_argument('--base-url', required=True,
                   help='Vendor portal root used by playbook navigate steps.')
    p.add_argument('--bot-username', required=True,
                   help='Informational; the vendor identifies the bot by '
                        'client_id, not username. Helpful for ops.')
    p.add_argument('--playbook-id', required=True,
                   help='Refers to RPA_PLAYBOOK#{playbook_id}. Use '
                        'seed_rpa_playbook.py to create one.')
    # Guardrails
    p.add_argument('--timezone', default='America/Chicago',
                   help='IANA timezone for allowed-hours + blackout-date '
                        'calculations (e.g. "America/New_York"). DST handled '
                        'automatically via Python zoneinfo.')
    p.add_argument('--allowed-hours-start', default='06:00',
                   help='Local time the bot may start running, HH:MM.')
    p.add_argument('--allowed-hours-end', default='20:00',
                   help='Local time after which the bot stops. Overnight '
                        'windows are supported by setting end < start '
                        '(e.g. start=22:00 end=06:00).')
    p.add_argument('--rate-limit-ms', type=int, default=1500,
                   help='Minimum delay between Playwright actions, in ms. '
                        'Tune downward only after watching multiple runs '
                        "against the vendor's portal without rate-limit errors.")
    p.add_argument('--blackout-dates', default='',
                   help='Comma-separated YYYY-MM-DD dates (local) on which '
                        'the bot must skip its schedule entirely.')
    # Per-vendor overrides
    p.add_argument('--cr-scope', default=None,
                   help='[centralreach only] override the SSO scope. The '
                        "authenticator defaults to 'cr-api' per CR's docs.")
    p.add_argument('--cr-sandbox', action='store_true',
                   help='[centralreach only] target the sandbox tenant URLs '
                        'instead of the documented prod endpoints. Use only '
                        'when CR has explicitly provisioned a sandbox client_id.')
    p.add_argument('--disabled', action='store_true',
                   help='Write with enabled=False to pause without deleting.')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true')
    return p.parse_args(argv)


def _parse_hhmm(value, *, field):
    try:
        hh, mm = value.split(':')
        h, m = int(hh), int(mm)
        if not (0 <= h < 24 and 0 <= m < 60):
            raise ValueError
    except (ValueError, AttributeError):
        raise SystemExit(f"--{field} must be HH:MM in 24-hour time, got {value!r}")
    return value


def _parse_blackouts(value):
    out = []
    for raw in value.split(','):
        s = raw.strip()
        if not s:
            continue
        try:
            datetime.strptime(s, '%Y-%m-%d')
        except ValueError:
            raise SystemExit(
                f"--blackout-dates entry must be YYYY-MM-DD, got {s!r}"
            )
        out.append(s)
    return out


def build_item(args):
    _parse_hhmm(args.allowed_hours_start, field='allowed-hours-start')
    _parse_hhmm(args.allowed_hours_end, field='allowed-hours-end')
    if args.rate_limit_ms < 0:
        raise SystemExit(f"--rate-limit-ms must be non-negative, got {args.rate_limit_ms}")

    now = datetime.now(timezone.utc).isoformat()
    item = {
        'pk': f'ORG#{args.org_id}',
        'sk': 'RPA_CONFIG',
        'gsi1pk': 'RPA_CONFIG',
        'gsi1sk': f'ORG#{args.org_id}',
        'organization_id': args.org_id,
        'enabled': not args.disabled,
        'vendor': args.vendor,
        'display_name': args.display_name,
        'base_url': args.base_url,
        'bot_username': args.bot_username,
        'playbook_id': args.playbook_id,
        'guardrails': {
            'timezone': args.timezone,
            'allowed_hours': {
                'start': args.allowed_hours_start,
                'end': args.allowed_hours_end,
            },
            'rate_limit_ms_between_requests': args.rate_limit_ms,
            'blackout_dates': _parse_blackouts(args.blackout_dates),
        },
        'created_at': now,
        'updated_at': now,
    }

    vendor_settings = {}
    if args.vendor == 'centralreach':
        cr = {}
        if args.cr_scope:
            cr['scope'] = args.cr_scope
        if args.cr_sandbox:
            # The actual sandbox URLs are provisioned per CR account; record
            # the intent here so the authenticator's base_overrides take
            # effect. Engineers can edit the item directly to point at the
            # exact sandbox hostnames CR issues.
            cr['base_overrides'] = {
                'sso_token_url':
                    'https://sandbox-login.centralreach.com/connect/token',
                'legacy_auth_url':
                    'https://sandbox-members.centralreach.com/api/?framework.authtoken',
            }
        if cr:
            vendor_settings['centralreach'] = cr
    if vendor_settings:
        item['vendor_settings'] = vendor_settings

    return item


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    item = build_item(args)

    if args.dry_run:
        print(json.dumps(item, indent=2, default=str))
        return 0

    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)
    print(
        f"Wrote RPA_CONFIG for org={args.org_id} "
        f"(enabled={not args.disabled}, vendor={args.vendor}, "
        f"playbook_id={args.playbook_id}, "
        f"tz={args.timezone}, "
        f"hours={args.allowed_hours_start}-{args.allowed_hours_end}, "
        f"rate_limit_ms={args.rate_limit_ms}, "
        f"blackouts={item['guardrails']['blackout_dates']}, "
        f"vendor_settings={item.get('vendor_settings') or {}})"
    )
    print(
        "\nReminders:"
        f"\n  1. Credentials secret must exist at "
        f"penguin-health/rpa/{args.org_id}/credentials"
        f"\n  2. Playbook must exist (run seed_rpa_playbook.py if not)"
        "\n  3. Enable the EventBridge schedule by adding an entry to "
        "_PER_ORG_SCHEDULES in infra/components/rpa.py and redeploying."
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
