#!/usr/bin/env python3
"""Delete demo-generated rows from the penguin-health-stedi table.

Targets a single org's demo data: encounter items, the FHIR-poll cursor,
and the seeded audit rows. Lets you reset the demo to a clean slate
between screenshots without nuking the whole table.

What gets deleted under pk=ORG#{org-id}:
  - ENCOUNTER_ITEM#…          (fhir_eligibility_poller items)
  - FHIR_POLL_CURSOR          (force the next poll to start from now-1h)
  - AUDIT#… rows where user_email is one of:
      system@census-seed       (Linda Sandbox's seeded prior-Cigna row)
      system@fhir-poller       (FHIR poller verify audits)

What is NEVER deleted:
  - STEDI_CONFIG / FHIR_CONFIG (on the org-config table, not this one)
  - USAGE#yyyy-mm-dd daily counters (90-day TTL handles cleanup)
  - Any AUDIT# row written by a real user_email (preserves real audit trail)

Usage:
    # Preview only — no writes.
    python scripts/multi-org/cleanup_demo_stedi.py --org-id demo --dry-run

    # Actually delete.
    python scripts/multi-org/cleanup_demo_stedi.py --org-id demo

    # Skip the AUDIT# rows (keep history, only drop run/item/cursor rows).
    python scripts/multi-org/cleanup_demo_stedi.py --org-id demo --keep-audits
"""

import argparse
import sys

import boto3


TABLE_NAME = 'penguin-health-stedi'

# user_email values that mark a row as system-generated for the demo.
# A real audit row written by a real UR staffer (e.g. ur@org.com) is
# preserved — we only delete rows we know we wrote ourselves.
SYSTEM_USER_EMAILS = frozenset({
    'system@census-seed',
    'system@fhir-poller',
})

# sk prefixes that are always demo/system-generated and safe to delete.
SAFE_PREFIXES = (
    'ENCOUNTER_ITEM#',
)

# Exact sk values that are demo-specific bookkeeping rows.
SAFE_EXACT_SKS = frozenset({
    'FHIR_POLL_CURSOR',
})


def parse_args(argv):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--org-id', required=True,
                   help='Organization id whose rows to delete (e.g. "demo").')
    p.add_argument('--keep-audits', action='store_true',
                   help='Keep AUDIT# rows even if user_email marks them as '
                        'system-generated. Useful if you want the dedup window '
                        'to remember prior checks across cleanups.')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true',
                   help='List what would be deleted; do not write.')
    return p.parse_args(argv)


def _should_delete(item, *, keep_audits):
    """Return True iff this row is safe to remove under the rules above."""
    sk = item.get('sk', '')
    if sk in SAFE_EXACT_SKS:
        return True
    if any(sk.startswith(prefix) for prefix in SAFE_PREFIXES):
        return True
    if sk.startswith('AUDIT#') and not keep_audits:
        return item.get('user_email') in SYSTEM_USER_EMAILS
    return False


def _classify(item):
    """Short label used in the dry-run summary."""
    sk = item['sk']
    if sk == 'FHIR_POLL_CURSOR':
        return 'cursor'
    if sk.startswith('ENCOUNTER_ITEM#'):
        return 'encounter_item'
    if sk.startswith('AUDIT#'):
        return f"audit ({item.get('user_email', '?')})"
    return 'other'


def _iter_org_rows(table, org_id):
    """Yield every row under pk=ORG#{org_id}, following pagination."""
    pk = f'ORG#{org_id}'
    kwargs = {
        'KeyConditionExpression': 'pk = :p',
        'ExpressionAttributeValues': {':p': pk},
        # Only the fields we need for the delete decision.
        'ProjectionExpression': 'pk, sk, user_email',
    }
    while True:
        response = table.query(**kwargs)
        yield from response.get('Items', [])
        last = response.get('LastEvaluatedKey')
        if not last:
            return
        kwargs['ExclusiveStartKey'] = last


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(TABLE_NAME)

    to_delete = []
    skipped = 0
    by_class = {}

    for item in _iter_org_rows(table, args.org_id):
        if _should_delete(item, keep_audits=args.keep_audits):
            to_delete.append({'pk': item['pk'], 'sk': item['sk']})
            label = _classify(item)
            by_class[label] = by_class.get(label, 0) + 1
        else:
            skipped += 1

    print(f"Found {len(to_delete)} demo rows to delete "
          f"under pk=ORG#{args.org_id} (skipped {skipped} non-demo rows):")
    for label, count in sorted(by_class.items()):
        print(f"  {count:>5}  {label}")

    if args.dry_run:
        print("\n--dry-run set; no rows deleted.")
        return 0

    if not to_delete:
        print("Nothing to delete.")
        return 0

    # batch_writer batches PutItem/DeleteItem in groups of 25 with retries
    # for unprocessed items — does what BatchWriteItem expects manually.
    with table.batch_writer() as batch:
        for key in to_delete:
            batch.delete_item(Key=key)

    print(f"Deleted {len(to_delete)} rows.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
