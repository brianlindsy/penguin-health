#!/usr/bin/env python3
"""Seed (or update) a per-org UI_DISPLAY_FIELDS record on
penguin-health-org-config.

The rules-engine reads this item to project each document's raw
`field_values` into canonical UI field names at validation time. The
admin UI (and downstream analytics slim projection) then read those
canonical names without needing to know the org's source-side shape.

Missing item, or item with an empty `mappings` object, means "no
projection" — the UI falls back to reading `field_values` directly.

Usage:
    python scripts/multi-org/seed_ui_display_fields.py \\
        --org-id supportive-care \\
        --preset supportive-care

    # Or pass explicit mappings on the command line:
    python scripts/multi-org/seed_ui_display_fields.py \\
        --org-id my-org \\
        --map employee_name=provider_display \\
        --map date=visit_date \\
        --map program=billing_list_procedure_code_string

Re-running overwrites the existing item.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = 'penguin-health-org-config'


# Canonical UI field names the admin UI knows how to render. Anything
# not in this list still gets written (the config is a free-form dict),
# but the UI's FIELD_LABELS won't have a nice label for it. Kept in sync
# with admin-ui/src/pages/ValidationRunDetailPage.jsx's FIELD_LABELS.
KNOWN_UI_FIELDS = frozenset({
    'service_id',
    'date',
    'program',
    'service_type',
    'diagnosis_code',
    'bed_day_diagnosis_code',
    'cpt_code',
    'rate',
    'employee_name',
    'document_id',
    'payer_description',
})


# Presets for orgs whose source-field shape is well-known. Adding a new
# preset here lets one command seed the mapping instead of listing 6-10
# --map pairs. Keys are the org_id used at seed time.
PRESETS: dict[str, dict[str, str]] = {
    # centralreach ingest lands these keys in `field_values` via
    # field_extractor.extract_fields_from_json_record — the top-level
    # `source_record_id` plus flattened `encounter.*` and the raw
    # `extracted_fields.billing_list_*` columns.
    'supportive-care': {
        'service_id': 'source_record_id',
        'employee_name': 'provider_display',
        'date': 'visit_date',
        'program': 'billing_list_procedure_code_string',
        'service_type': 'note_type',
        'cpt_code': 'billing_list_procedure_code_id',
        'rate': 'billing_list_rate_client',
        'payer_description': 'billing_list_payor_name',
    },
}


def parse_args(argv):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--org-id', required=True)
    p.add_argument(
        '--preset', choices=sorted(PRESETS),
        help='Use a built-in mapping preset. Combines with --map (explicit '
             'entries override preset entries).',
    )
    p.add_argument(
        '--map', dest='maps', action='append', default=[],
        metavar='CANONICAL=SOURCE',
        help='Add one mapping entry, e.g. --map employee_name=provider_display. '
             'Repeatable.',
    )
    p.add_argument(
        '--clear', action='store_true',
        help='Write with an empty mappings dict (turns projection off '
             'without deleting the item).',
    )
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true')
    return p.parse_args(argv)


def _parse_map_entries(entries):
    """Turn ['a=b', 'c=d'] into {'a': 'b', 'c': 'd'}. Raises on bad shape."""
    out = {}
    for raw in entries:
        if '=' not in raw:
            raise SystemExit(
                f"--map entry must be CANONICAL=SOURCE, got {raw!r}"
            )
        canonical, source = raw.split('=', 1)
        canonical, source = canonical.strip(), source.strip()
        if not canonical or not source:
            raise SystemExit(
                f"--map entry has empty name or value: {raw!r}"
            )
        out[canonical] = source
    return out


def build_item(args):
    if args.clear:
        mappings = {}
    else:
        mappings = dict(PRESETS[args.preset]) if args.preset else {}
        mappings.update(_parse_map_entries(args.maps))
        if not mappings:
            raise SystemExit(
                "No mappings specified. Pass --preset, one or more --map "
                "entries, or --clear to write an empty mapping."
            )

    unknown = sorted(set(mappings) - KNOWN_UI_FIELDS)
    if unknown:
        # Not fatal — orgs can define custom canonical names — but a
        # visible warning keeps typos from silently disappearing.
        print(
            f"Warning: canonical name(s) not in KNOWN_UI_FIELDS "
            f"(UI has no built-in label): {unknown}"
        )

    now = datetime.now(timezone.utc).isoformat()
    return {
        'pk': f'ORG#{args.org_id}',
        'sk': 'UI_DISPLAY_FIELDS',
        'organization_id': args.org_id,
        'mappings': mappings,
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
        f"Wrote UI_DISPLAY_FIELDS for org={args.org_id} "
        f"({len(item['mappings'])} mappings): "
        f"{sorted(item['mappings'])}"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
