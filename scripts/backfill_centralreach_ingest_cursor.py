#!/usr/bin/env python3
"""
One-time backfill for the centralreach ingest-cursor table.

The ingest-cursor dedupe (`CENTRALREACH_INGEST_DEDUPE_ENABLED=true`)
skips CR billing entries the runner has already ingested. On a cold
cursor table, the first post-flag run would re-ingest everything in
the lookback window because no rows exist — defeating the point.

This script seeds the cursor from the per-org S3 `data/` prefix,
which is the source of truth for successful ingests. Every record
JSON that exists at `s3://penguin-health-{org}/data/YYYY-MM-DD/*.json`
gets one cursor row.

Data source: S3 keys ONLY. We deliberately do not open the record
JSON bodies — the cursor's only load-bearing job is presence, and
the S3 key already carries enough for provenance
(`captured_at_compact` becomes `first_ingested_at`, key parses to
`source_record_id`). Not opening the bodies keeps this script off
the PHI-bearing hot path.

Idempotent: PutItem uses `attribute_not_exists(pk)` — a re-run and
a live-ingest write during the run are both silent no-ops.

If a `source_record_id` has multiple JSON files (multiple past
ingests before dedupe existed), the earliest `captured_at_compact`
wins so the cursor's `first_ingested_at` reflects the true first
ingest.

Run BEFORE flipping `CENTRALREACH_INGEST_DEDUPE_ENABLED=true` on the
Fargate task.

Usage:
    # Dry-run against all configured CR orgs — the default:
    python scripts/backfill_centralreach_ingest_cursor.py

    # Commit for one org:
    python scripts/backfill_centralreach_ingest_cursor.py \\
        --org demo --commit

    # Commit for every configured CR org:
    python scripts/backfill_centralreach_ingest_cursor.py --commit
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Iterator

import boto3
from botocore.exceptions import ClientError


_CURSOR_TABLE = "penguin-health-centralreach-ingest-cursor"
_ORG_CONFIG_TABLE = "penguin-health-org-config"
_REGION = "us-east-1"

# `data/{YYYY-MM-DD}/{YYYYMMDDTHHMMSSZ}__{source_record_id}.json`
# Anchored so a stray path with the right suffix doesn't slip through.
_KEY_RE = re.compile(
    r"^data/(?P<date>\d{4}-\d{2}-\d{2})/"
    r"(?P<compact>\d{8}T\d{6}Z)__(?P<id>[^/]+)\.json$"
)

_PROGRESS_EVERY = 1000


def _iso_from_compact(compact: str) -> str:
    """`20260628T220000Z` -> `2026-06-28T22:00:00Z`."""
    dt = datetime.strptime(compact, "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc,
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _list_cr_org_ids(ddb) -> list[str]:
    """Every org with an enabled CENTRALREACH_CONFIG row.

    We scan `penguin-health-org-config` for `sk=CENTRALREACH_CONFIG`
    items and project only the org id + enabled flag — no full-item
    reads, no PHI risk.
    """
    table = ddb.Table(_ORG_CONFIG_TABLE)
    orgs: list[str] = []
    kwargs = {
        "FilterExpression": "sk = :sk AND enabled = :true",
        "ExpressionAttributeValues": {
            ":sk": "CENTRALREACH_CONFIG",
            ":true": True,
        },
        "ProjectionExpression": "pk, organization_id",
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            org_id = item.get("organization_id")
            if org_id:
                orgs.append(org_id)
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token
    return sorted(orgs)


def _iter_data_keys(s3, bucket: str) -> Iterator[str]:
    """Yield every `data/**/*.json` key in the bucket, paginated."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="data/"):
        for obj in page.get("Contents") or []:
            key = obj.get("Key") or ""
            if key.endswith(".json"):
                yield key


def _earliest_by_record_id(
    keys: Iterator[str],
) -> Iterator[tuple[str, str, str]]:
    """Fold the key stream to (source_record_id, earliest_compact, key).

    Records ingested multiple times pre-dedupe collapse to their
    earliest ingest so `first_ingested_at` matches live semantics.
    """
    # source_record_id -> (compact, key)
    earliest: dict[str, tuple[str, str]] = {}
    for key in keys:
        m = _KEY_RE.match(key)
        if not m:
            continue
        src_id = m.group("id")
        compact = m.group("compact")
        cur = earliest.get(src_id)
        if cur is None or compact < cur[0]:
            earliest[src_id] = (compact, key)
    for src_id, (compact, key) in earliest.items():
        yield src_id, compact, key


def _put_cursor(
    cursor_table, *,
    org_id: str, source_record_id: str,
    first_ingested_at: str, record_s3_key: str,
    ingest_run_id: str,
) -> str:
    """Write one cursor row. Returns 'written' | 'duplicate' | 'errored'."""
    item = {
        "pk": f"ORG#{org_id}",
        "sk": f"ENTRY#{source_record_id}",
        "first_ingested_at": first_ingested_at,
        "first_ingest_run_id": ingest_run_id,
        # No PDF key on the backfill — we didn't parse the pdfs/ prefix.
        # The live ingest path populates this on any future write; the
        # cursor itself never reads it.
        "pdf_s3_key": "",
        "record_s3_key": record_s3_key,
    }
    try:
        cursor_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
        return "written"
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return "duplicate"
        print(
            f"  ERROR: put failed for {org_id}/{source_record_id}: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return "errored"


def _backfill_org(
    *,
    org_id: str,
    s3,
    cursor_table,
    ingest_run_id: str,
    dry_run: bool,
) -> dict:
    """Backfill one org. Returns per-org totals."""
    bucket = f"penguin-health-{org_id}"
    totals = {
        "keys_scanned": 0,
        "unmatched_keys": 0,
        "unique_records": 0,
        "written": 0,
        "duplicate": 0,
        "errored": 0,
    }

    # First pass: count + fold, so we can print an accurate "unique
    # records" before writing.
    folded: list[tuple[str, str, str]] = []
    try:
        for i, (src_id, compact, key) in enumerate(
            _earliest_by_record_id(_iter_data_keys(s3, bucket)),
            start=1,
        ):
            folded.append((src_id, compact, key))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchBucket":
            print(
                f"[{org_id}] no bucket ({bucket}); skipping org",
                file=sys.stderr,
            )
            return totals
        raise

    totals["unique_records"] = len(folded)

    for i, (src_id, compact, key) in enumerate(folded, start=1):
        if dry_run:
            totals["written"] += 1  # count what we WOULD write
        else:
            outcome = _put_cursor(
                cursor_table,
                org_id=org_id,
                source_record_id=src_id,
                first_ingested_at=_iso_from_compact(compact),
                record_s3_key=key,
                ingest_run_id=ingest_run_id,
            )
            totals[outcome] += 1
        if i % _PROGRESS_EVERY == 0:
            print(f"[{org_id}] progress: {i}/{len(folded)}")

    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--org",
        help="Backfill a single org id. Default: every enabled CR org.",
    )
    parser.add_argument(
        "--region", default=_REGION,
        help=f"AWS region (default: {_REGION})",
    )
    parser.add_argument(
        "--profile",
        help="AWS profile to use (defaults to the ambient credentials)",
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually write. Default is dry-run.",
    )
    args = parser.parse_args()

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.Session(**session_kwargs)

    ddb = session.resource("dynamodb")
    s3 = session.client("s3")
    cursor_table = ddb.Table(_CURSOR_TABLE)

    ingest_run_id = f"backfill-{uuid.uuid4()}"
    dry_run = not args.commit

    if args.org:
        org_ids = [args.org]
    else:
        org_ids = _list_cr_org_ids(ddb)
        if not org_ids:
            print("no enabled CENTRALREACH_CONFIG orgs found", file=sys.stderr)
            return 1
        print(f"discovered {len(org_ids)} enabled CR orgs: {org_ids}")

    if dry_run:
        print("DRY-RUN — no cursor rows will be written. "
              "Pass --commit to write.")
    else:
        print(f"COMMIT mode. ingest_run_id={ingest_run_id}")

    grand = {
        "unique_records": 0, "written": 0,
        "duplicate": 0, "errored": 0,
    }
    for org_id in org_ids:
        print(f"\n===== {org_id} =====")
        totals = _backfill_org(
            org_id=org_id, s3=s3, cursor_table=cursor_table,
            ingest_run_id=ingest_run_id, dry_run=dry_run,
        )
        print(
            f"[{org_id}] unique_records={totals['unique_records']} "
            f"written={totals['written']} "
            f"duplicate={totals['duplicate']} "
            f"errored={totals['errored']}"
        )
        for k in grand:
            grand[k] += totals.get(k, 0)

    print(
        f"\n===== TOTAL =====\n"
        f"unique_records={grand['unique_records']} "
        f"written={grand['written']} "
        f"duplicate={grand['duplicate']} "
        f"errored={grand['errored']}"
    )
    return 0 if grand["errored"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
