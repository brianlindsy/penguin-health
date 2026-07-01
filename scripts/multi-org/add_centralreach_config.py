#!/usr/bin/env python3
"""Seed (or update) a per-org CENTRALREACH_CONFIG record on
penguin-health-org-config.

The CentralReach Fargate ingest runner reads this item to discover:
  - the org's timezone, allowed-hours window, blackout dates
  - the per-request rate limit
  - the CR portal base URL (rarely overridden)

Prerequisites:
  1. The bot's credentials must already be in Secrets Manager at
     `penguin-health/centralreach/{org_id}/credentials` as JSON. The
     payload shape depends on the auth flow the runner uses (see
     `docs/centralreach-api-integration.md` Open Questions). For
     OAuth client_credentials:

        aws secretsmanager create-secret \\
            --name penguin-health/centralreach/{org_id}/credentials \\
            --secret-string '{"client_id":"...","client_secret":"..."}'

Usage:
    python scripts/multi-org/add_centralreach_config.py \\
        --org-id demo \\
        --display-name "Demo CR clinical-notes ingest" \\
        --base-url https://members.centralreach.com \\
        --bot-username "centralreach-bot+demo" \\
        --timezone America/Chicago \\
        --allowed-hours-start 06:00 \\
        --allowed-hours-end 20:00 \\
        --rate-limit-ms 1500 \\
        --blackout-dates 2026-12-25,2027-01-01

Re-running this script overwrites the existing CENTRALREACH_CONFIG
item.

Enabling the EventBridge schedule is a separate step — add an entry
to `_PER_ORG_SCHEDULES` in `infra/components/centralreach.py` and
redeploy.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = 'penguin-health-org-config'


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--org-id', required=True)
    p.add_argument('--display-name', required=True,
                   help='Human-friendly name shown in the runs UI.')
    p.add_argument('--base-url',
                   default='https://members.centralreach.com',
                   help='Vendor portal root used by the HTTP client.')
    p.add_argument('--bot-username', required=True,
                   help='Informational; the vendor identifies the bot by '
                        'credentials, not username. Helpful for ops.')
    # Guardrails
    p.add_argument('--timezone', default='America/Chicago',
                   help='IANA timezone for allowed-hours + blackout-date '
                        'calculations (e.g. "America/New_York"). DST handled '
                        'automatically via Python zoneinfo. The same value '
                        'drives the `tzoffset` cookie and `_utcOffsetMinutes` '
                        'on every CR request.')
    p.add_argument('--allowed-hours-start', default='06:00',
                   help='Local time the bot may start running, HH:MM.')
    p.add_argument('--allowed-hours-end', default='20:00',
                   help='Local time after which the bot stops. Overnight '
                        'windows are supported by setting end < start '
                        '(e.g. start=22:00 end=06:00).')
    p.add_argument('--rate-limit-ms', type=int, default=1500,
                   help='Minimum delay between HTTP requests, in ms. '
                        'Tune downward only after watching multiple runs '
                        "against the vendor's portal without rate-limit "
                        "errors.")
    p.add_argument('--blackout-dates', default='',
                   help='Comma-separated YYYY-MM-DD dates (local) on which '
                        'the bot must skip its schedule entirely.')
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
        raise SystemExit(f"--{field} must be HH:MM in 24-hour time, "
                         f"got {value!r}")
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
        raise SystemExit(
            f"--rate-limit-ms must be non-negative, "
            f"got {args.rate_limit_ms}"
        )

    now = datetime.now(timezone.utc).isoformat()
    return {
        'pk': f'ORG#{args.org_id}',
        'sk': 'CENTRALREACH_CONFIG',
        'gsi1pk': 'CENTRALREACH_CONFIG',
        'gsi1sk': f'ORG#{args.org_id}',
        'organization_id': args.org_id,
        'enabled': not args.disabled,
        'display_name': args.display_name,
        'base_url': args.base_url,
        'bot_username': args.bot_username,
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
        f"Wrote CENTRALREACH_CONFIG for org={args.org_id} "
        f"(enabled={not args.disabled}, "
        f"tz={args.timezone}, "
        f"hours={args.allowed_hours_start}-{args.allowed_hours_end}, "
        f"rate_limit_ms={args.rate_limit_ms}, "
        f"blackouts={item['guardrails']['blackout_dates']})"
    )
    print(
        "\nReminders:"
        f"\n  1. Credentials secret must exist at "
        f"penguin-health/centralreach/{args.org_id}/credentials"
        "\n  2. Enable the EventBridge schedule by adding an entry to "
        "_PER_ORG_SCHEDULES in infra/components/centralreach.py and "
        "redeploying."
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
