#!/usr/bin/env python3
"""
One-time cleanup: remove float-bug error sentinels from validation-results.

Background: the rules-engine queue-write path put items via boto3's
DynamoDB resource without converting Python floats to Decimal, so every
CR-ingested document raised "Float types are not supported. Use Decimal
types instead." on `upsert_new_or_version`. The outer `except` in
`process_file` swallowed the exception and wrote an ERROR sentinel row
to `penguin-health-validation-results` so the file wouldn't be retried.

The sentinel is what keeps the continuation handler from re-processing
the file on the next run. To let the affected docs land in the queue
after the fix, we delete those sentinel rows so the file looks
"unprocessed" again and the next rules-engine run picks it up.

Scope, narrow by design:
  * Only rows where `document_id` starts with `ERROR#`.
  * AND the `error` attribute contains "Float types are not supported"
    — pinning to the specific bug so unrelated error sentinels for real
    parse failures are left alone.
  * Optional `--org` filter; without it, all orgs.
  * Optional `--run` filter (validation_run_id) using GSI2 for a scoped
    scan; without it, full-table scan of ERROR# rows.

Idempotent: DeleteItem on a missing key is a no-op. Safe to re-run.

Usage:
    # Dry-run against all orgs — default:
    python scripts/cleanup_float_error_sentinels.py

    # Dry-run for one org:
    python scripts/cleanup_float_error_sentinels.py --org supportive-care

    # Commit for one org:
    python scripts/cleanup_float_error_sentinels.py --org supportive-care --commit

    # Target one validation run (uses GSI2, much cheaper than a scan):
    python scripts/cleanup_float_error_sentinels.py --run run-abc --commit
"""

from __future__ import annotations

import argparse
import sys
from typing import Iterator

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


_RESULTS_TABLE = "penguin-health-validation-results"
_REGION = "us-east-1"

# The exact substring the bug raised. Match on this to avoid deleting
# sentinels written for other kinds of failure.
_FLOAT_BUG_MARKER = "Float types are not supported"

_PROGRESS_EVERY = 500


def _iter_error_sentinels_by_run(
    table, validation_run_id: str,
) -> Iterator[dict]:
    """GSI2 query for one run's rows, filtered to ERROR# document_ids.

    GSI2 keys off `RUN#{validation_run_id}`, so this is O(rows in run)
    rather than a full-table scan. Cheaper when a single incident is
    localized to one run.
    """
    kwargs = {
        "IndexName": "gsi2",
        "KeyConditionExpression": Key("gsi2pk").eq(f"RUN#{validation_run_id}"),
        "FilterExpression": (
            "begins_with(document_id, :err) AND contains(#e, :marker)"
        ),
        "ExpressionAttributeNames": {"#e": "error"},
        "ExpressionAttributeValues": {
            ":err": "ERROR#",
            ":marker": _FLOAT_BUG_MARKER,
        },
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield item
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


def _iter_error_sentinels_by_scan(
    table, org_id: str | None,
) -> Iterator[dict]:
    """Full-table scan filtered to ERROR# rows carrying the marker.

    Slower + costlier than the GSI2 path but the only option when the
    operator doesn't know which run id(s) hit the bug (typical: many
    runs affected across days).
    """
    filter_expr = (
        "begins_with(document_id, :err) AND contains(#e, :marker)"
    )
    values = {
        ":err": "ERROR#",
        ":marker": _FLOAT_BUG_MARKER,
    }
    if org_id:
        filter_expr += " AND organization_id = :org"
        values[":org"] = org_id
    kwargs = {
        "FilterExpression": filter_expr,
        "ExpressionAttributeNames": {"#e": "error"},
        "ExpressionAttributeValues": values,
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            yield item
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


def _delete(table, item: dict) -> str:
    """Return 'deleted' | 'errored'."""
    try:
        table.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})
        return "deleted"
    except ClientError as e:
        print(
            f"  ERROR: delete failed for pk={item['pk']} sk={item['sk']}: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return "errored"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--org",
        help="Filter to a single organization_id (default: all orgs)",
    )
    parser.add_argument(
        "--run",
        help=(
            "Filter to a single validation_run_id via GSI2. Much cheaper "
            "than a full scan when the incident is scoped to one run."
        ),
    )
    parser.add_argument(
        "--region", default=_REGION,
        help=f"AWS region (default: {_REGION})",
    )
    parser.add_argument(
        "--profile",
        help="AWS profile to use (defaults to ambient credentials)",
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually delete. Default is dry-run (lists what WOULD be deleted).",
    )
    args = parser.parse_args()

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.Session(**session_kwargs)

    ddb = session.resource("dynamodb")
    table = ddb.Table(_RESULTS_TABLE)

    dry_run = not args.commit

    if args.run:
        source = _iter_error_sentinels_by_run(table, args.run)
        source_desc = f"GSI2 query on RUN#{args.run}"
        if args.org:
            # Post-filter in Python since the query is already scoped
            # by run id.
            base = source
            source = (
                item for item in base
                if item.get("organization_id") == args.org
            )
    else:
        source = _iter_error_sentinels_by_scan(table, args.org)
        source_desc = (
            f"full-table scan (org filter: {args.org or 'ALL'})"
        )

    print(f"scanning: {source_desc}")
    if dry_run:
        print("DRY-RUN — no rows will be deleted. Pass --commit to delete.")
    else:
        print("COMMIT mode.")

    totals = {"found": 0, "deleted": 0, "errored": 0}
    for i, item in enumerate(source, start=1):
        totals["found"] += 1
        s3_key = item.get("s3_key") or "<no s3_key>"
        doc_id = item.get("document_id") or "<no document_id>"
        org = item.get("organization_id") or "<no org>"
        run = item.get("validation_run_id") or "<no run>"
        if dry_run:
            print(f"  WOULD DELETE org={org} run={run} s3_key={s3_key}")
        else:
            outcome = _delete(table, item)
            totals[outcome] += 1
            if outcome == "deleted":
                print(f"  deleted org={org} run={run} s3_key={s3_key}")
        if i % _PROGRESS_EVERY == 0:
            print(f"progress: {i} rows examined")

    print(
        f"\n===== TOTAL =====\n"
        f"found={totals['found']} "
        f"deleted={totals['deleted']} "
        f"errored={totals['errored']}"
    )
    return 0 if totals["errored"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
