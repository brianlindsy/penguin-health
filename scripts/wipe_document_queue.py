#!/usr/bin/env python3
"""
Wipe every row from `penguin-health-document-queue`.

Use when the queue's derived state is known to be wrong and you'd
rather rebuild it from scratch via `backfill_document_queue.py`. The
validation-results table is NOT touched — that's the source of truth
for the pointer rollups.

Default is `--dry-run`. Pass `--commit` to actually delete.

Projection is limited to `pk`/`sk` so PHI never lands on stdout.
Deletes go through `batch_writer` (25 per DDB round trip).

Usage:
  python scripts/wipe_document_queue.py             # dry-run: count only
  python scripts/wipe_document_queue.py --commit    # actually delete
  python scripts/wipe_document_queue.py --org supportive-care --commit
"""

import argparse
import sys

import boto3
from botocore.config import Config


DOCUMENT_QUEUE_TABLE = "penguin-health-document-queue"
AWS_REGION = "us-east-1"


def _iter_pk_sk(table, org_id: str | None):
    """Yield {pk, sk} for every row (optionally scoped to one org).

    Queue rows come in two shapes:
      * pointer:  pk=ORG#{org_id}, sk=DOC#{doc_id}
      * version:  pk=ORG#{org_id}#DOC#{doc_id}, sk=VERSION#{iso_ts}

    When `--org` is set, we filter on the pk prefix so unrelated orgs
    stay intact.
    """
    kwargs = {
        "ProjectionExpression": "#pk, #sk",
        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
    }
    if org_id:
        kwargs["FilterExpression"] = (
            "begins_with(#pk, :o) OR begins_with(#pk, :od)"
        )
        kwargs["ExpressionAttributeValues"] = {
            ":o": f"ORG#{org_id}",
            ":od": f"ORG#{org_id}#DOC#",
        }
    last = None
    while True:
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            yield {"pk": item["pk"], "sk": item["sk"]}
        last = resp.get("LastEvaluatedKey")
        if not last:
            return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--org",
        help="Restrict wipe to a single org_id. Default: every row on the "
             "table.",
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--dry-run", action="store_true", default=True,
                     help="Report the count; don't delete. Default.")
    grp.add_argument("--commit", action="store_true",
                     help="Actually delete.")
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", config=Config(region_name=AWS_REGION))
    table = dynamodb.Table(DOCUMENT_QUEUE_TABLE)

    print(f"wipe_document_queue: table={DOCUMENT_QUEUE_TABLE} "
          f"org={args.org or 'ALL'} commit={args.commit}")

    keys = list(_iter_pk_sk(table, args.org))
    print(f"  matched rows: {len(keys)}")

    if not args.commit:
        print("DRY RUN — no writes. Re-run with --commit to delete.")
        return

    if not keys:
        print("Nothing to delete.")
        return

    deleted = 0
    with table.batch_writer() as batch:
        for k in keys:
            batch.delete_item(Key=k)
            deleted += 1
            if deleted % 500 == 0:
                print(f"  deleted {deleted}/{len(keys)}...")
    print(f"Done. Deleted {deleted} rows.")


if __name__ == "__main__":
    main()
