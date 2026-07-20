#!/usr/bin/env python3
"""
Reset an organization's CentralReach-derived state to pre-first-ingest.

DESTRUCTIVE. After a successful reset, the next scheduled or manual
CR ingest for the org behaves as if no CR data had ever been ingested:
every entry the list-query returns is treated as new, PDFs are fetched
fresh, records are written, and the queue is repopulated.

Scope — four DDB targets, each opt-in via a flag:

    --reset-cursor              penguin-health-centralreach-ingest-cursor
                                  pk=ORG#{org}, sk=ENTRY#{source_record_id}
    --reset-validation-results  penguin-health-validation-results
                                  full-table scan filtered by organization_id
    --reset-queue               penguin-health-document-queue
                                  pointer rows (pk=ORG#{org}) + version rows
                                  (pk=ORG#{org}#DOC#*)
    --reset-narrative-hashes    penguin-health-narrative-hashes
                                  pk=ORG#{org}, sk=HASH#*

Explicitly NOT touched (deleting any of these would be a compliance
problem or would disable the org):

  * penguin-health-audit + audit Firehose  — 7yr retention
  * penguin-health-org-config
      - CENTRALREACH_CONFIG    (would disable ingest)
      - RULE#*                 (rule definitions)
      - USER#*                 (reviewer permissions)
  * Secrets Manager credentials
  * EventBridge schedule       (leave enabled or disable in the console
                                yourself if you want to pause the reset)
  * S3 record + PDF objects    (operator will delete these manually)

Idempotent: DeleteItem on a missing key is a no-op, safe to re-run.

Dry-run by default. Every destructive flag requires `--commit` to
actually write.

Usage:
    # Dry-run everything for supportive-care:
    python scripts/reset_centralreach_org.py --org supportive-care \\
        --reset-cursor --reset-validation-results \\
        --reset-queue --reset-narrative-hashes

    # Commit:
    python scripts/reset_centralreach_org.py --org supportive-care \\
        --reset-cursor --reset-validation-results \\
        --reset-queue --reset-narrative-hashes --commit

    # Reset just the queue (one target at a time is fine):
    python scripts/reset_centralreach_org.py --org supportive-care \\
        --reset-queue --commit

The org is a required positional-shaped flag (no default) so you
cannot accidentally reset the wrong org.
"""

from __future__ import annotations

import argparse
import sys
from typing import Iterator

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


_REGION = "us-east-1"
_CURSOR_TABLE = "penguin-health-centralreach-ingest-cursor"
_VALIDATION_RESULTS_TABLE = "penguin-health-validation-results"
_DOCUMENT_QUEUE_TABLE = "penguin-health-document-queue"
_NARRATIVE_HASHES_TABLE = "penguin-health-narrative-hashes"

_PROGRESS_EVERY = 500


# ----- shared helpers ------------------------------------------------------


def _batch_delete(table, keys: Iterator[dict], *, dry_run: bool) -> dict:
    """Delete an iterable of `{pk, sk}` key dicts. Returns counters."""
    totals = {"found": 0, "deleted": 0, "errored": 0}
    if dry_run:
        for k in keys:
            totals["found"] += 1
        return totals

    # batch_writer chunks into 25-row BatchWriteItem calls and retries
    # unprocessed items automatically — the boto3-idiomatic way to bulk
    # delete without hand-rolling backoff.
    try:
        with table.batch_writer() as writer:
            for i, k in enumerate(keys, start=1):
                totals["found"] += 1
                writer.delete_item(Key=k)
                totals["deleted"] += 1
                if i % _PROGRESS_EVERY == 0:
                    print(f"    progress: {i} deletes queued")
    except ClientError as e:
        print(
            f"    ERROR during batch delete: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        totals["errored"] += 1
    return totals


# ----- ingest cursor -------------------------------------------------------


def _iter_cursor_keys(table, org_id: str) -> Iterator[dict]:
    """Yield {pk, sk} for every cursor row belonging to `org_id`.

    Single-partition query — no scan. Projects only pk + sk so we
    don't pull the S3-key attributes into the operator's memory
    (they aren't PHI, but there's no reason to read them here).
    """
    pk = f"ORG#{org_id}"
    kwargs = {
        "KeyConditionExpression": Key("pk").eq(pk),
        "ProjectionExpression": "pk, sk",
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield {"pk": item["pk"], "sk": item["sk"]}
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


# ----- narrative hashes ----------------------------------------------------


def _iter_narrative_hash_keys(table, org_id: str) -> Iterator[dict]:
    """Yield {pk, sk} for every narrative-hash row belonging to `org_id`."""
    pk = f"ORG#{org_id}"
    kwargs = {
        "KeyConditionExpression": Key("pk").eq(pk),
        "ProjectionExpression": "pk, sk",
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield {"pk": item["pk"], "sk": item["sk"]}
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


# ----- document queue ------------------------------------------------------


def _iter_queue_pointer_keys(table, org_id: str) -> Iterator[tuple[str, dict]]:
    """Yield (document_id, {pk, sk}) for every pointer row.

    Pointer pk = ORG#{org}, sk = DOC#{document_id}. We need the doc_id
    to compute the sibling `pk = ORG#{org}#DOC#{document_id}` used by
    the version rows.
    """
    pk = f"ORG#{org_id}"
    kwargs = {
        "KeyConditionExpression": Key("pk").eq(pk),
        "ProjectionExpression": "pk, sk, document_id",
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield (
                item.get("document_id") or "",
                {"pk": item["pk"], "sk": item["sk"]},
            )
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


def _iter_queue_version_keys(
    table, org_id: str, document_id: str,
) -> Iterator[dict]:
    """Yield {pk, sk} for every version row of one document."""
    pk = f"ORG#{org_id}#DOC#{document_id}"
    kwargs = {
        "KeyConditionExpression": Key("pk").eq(pk),
        "ProjectionExpression": "pk, sk",
    }
    while True:
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield {"pk": item["pk"], "sk": item["sk"]}
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


def _reset_queue(table, org_id: str, *, dry_run: bool) -> dict:
    """Delete pointer + version rows for one org.

    Order is significant only for observability: we iterate pointers
    first so the version-row cleanup can address each document. The
    actual deletes can run in any order — DDB has no referential
    integrity to worry about.
    """
    totals = {
        "pointers_found": 0, "pointers_deleted": 0,
        "versions_found": 0, "versions_deleted": 0,
        "errored": 0,
    }
    pointers = list(_iter_queue_pointer_keys(table, org_id))
    totals["pointers_found"] = len(pointers)

    for doc_id, _ in pointers:
        if not doc_id:
            continue
        version_keys = list(_iter_queue_version_keys(table, org_id, doc_id))
        totals["versions_found"] += len(version_keys)
        if version_keys:
            sub = _batch_delete(table, iter(version_keys), dry_run=dry_run)
            totals["versions_deleted"] += sub["deleted"]
            totals["errored"] += sub["errored"]

    pointer_keys = (k for _, k in pointers)
    sub = _batch_delete(table, pointer_keys, dry_run=dry_run)
    totals["pointers_deleted"] = sub["deleted"]
    totals["errored"] += sub["errored"]
    return totals


# ----- validation results --------------------------------------------------


def _iter_validation_results_keys(
    table, org_id: str,
) -> Iterator[dict]:
    """Full-table scan filtered by organization_id.

    validation-results has no org-in-pk; a scan with FilterExpression
    is the only correct option. Projects only pk + sk so we don't
    pull PHI-bearing attributes into the operator's memory.
    """
    kwargs = {
        "FilterExpression": "organization_id = :org",
        "ExpressionAttributeValues": {":org": org_id},
        "ProjectionExpression": "pk, sk",
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            yield {"pk": item["pk"], "sk": item["sk"]}
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token


# ----- driver --------------------------------------------------------------


def _run_target(name: str, work) -> dict:
    """Print a banner, run the callable, print totals, return them."""
    print(f"\n===== {name} =====")
    totals = work()
    print(f"[{name}] totals: {totals}")
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--org", required=True,
        help="organization_id to reset (required; no default)",
    )
    parser.add_argument("--reset-cursor", action="store_true")
    parser.add_argument("--reset-validation-results", action="store_true")
    parser.add_argument("--reset-queue", action="store_true")
    parser.add_argument("--reset-narrative-hashes", action="store_true")
    parser.add_argument("--region", default=_REGION)
    parser.add_argument("--profile")
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually delete. Default is dry-run.",
    )
    args = parser.parse_args()

    if not any([
        args.reset_cursor, args.reset_validation_results,
        args.reset_queue, args.reset_narrative_hashes,
    ]):
        parser.error(
            "at least one of --reset-cursor / --reset-validation-results / "
            "--reset-queue / --reset-narrative-hashes is required",
        )

    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    session = boto3.Session(**session_kwargs)
    ddb = session.resource("dynamodb")

    dry_run = not args.commit
    org_id = args.org

    print(f"org: {org_id}")
    if dry_run:
        print("DRY-RUN — no rows will be deleted. Pass --commit to delete.")
    else:
        print("COMMIT mode. Deletions are irreversible.")

    grand_errored = 0

    if args.reset_cursor:
        table = ddb.Table(_CURSOR_TABLE)
        totals = _run_target(
            "ingest-cursor",
            lambda: _batch_delete(
                table, _iter_cursor_keys(table, org_id), dry_run=dry_run,
            ),
        )
        grand_errored += totals["errored"]

    if args.reset_narrative_hashes:
        table = ddb.Table(_NARRATIVE_HASHES_TABLE)
        totals = _run_target(
            "narrative-hashes",
            lambda: _batch_delete(
                table, _iter_narrative_hash_keys(table, org_id),
                dry_run=dry_run,
            ),
        )
        grand_errored += totals["errored"]

    if args.reset_queue:
        table = ddb.Table(_DOCUMENT_QUEUE_TABLE)
        totals = _run_target(
            "document-queue",
            lambda: _reset_queue(table, org_id, dry_run=dry_run),
        )
        grand_errored += totals["errored"]

    if args.reset_validation_results:
        table = ddb.Table(_VALIDATION_RESULTS_TABLE)
        totals = _run_target(
            "validation-results",
            lambda: _batch_delete(
                table, _iter_validation_results_keys(table, org_id),
                dry_run=dry_run,
            ),
        )
        grand_errored += totals["errored"]

    return 0 if grand_errored == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
