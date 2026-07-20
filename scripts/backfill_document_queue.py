#!/usr/bin/env python3
"""
Backfill the document queue from the existing validation-results table.

The document queue (`penguin-health-document-queue`) is the ongoing
reviewer surface. New documents land in the queue automatically once
`QUEUE_WRITE_ENABLED=true` on the rules-engine Lambda; this script
seeds the queue with the current state of every document already in
`penguin-health-validation-results` so reviewers don't lose in-flight
work at the cutover.

Backfill contract (per the plan):

  * One pointer row per unique (org_id, document_id) — pk=ORG#{org_id},
    sk=DOC#{document_id}.
  * One version row for the LATEST validation result observed —
    pk=ORG#{org_id}#DOC#{document_id}, sk=VERSION#{iso_ts}. We do NOT
    write full history for older rows; those remain queryable in the
    old table.
  * Derived status: `confirmed` if `document_confirmed=True` on the row,
    `resolved` if every FAIL rule has `fixed=True`, else `open`.
  * `first_seen_at` / `first_seen_run_id` come from the EARLIEST row
    observed for this document; `last_updated_at` from the LATEST.
  * Content hash is computed by RE-READING the source S3 object the
    historical row was validated from and running the write-path
    canonicalizer against it. This is the only way to guarantee that a
    subsequent nightly validation of the same unchanged record hashes
    identically to the backfilled seed — otherwise every backfilled
    doc would spuriously re-open on the first live re-run. Documents
    whose s3_key is missing or whose S3 object is gone are SKIPPED
    (they'd otherwise seed a wrong hash and force a re-open); those
    docs will re-appear in the queue naturally the next time the rules
    engine sees their source object.

Idempotency:
  * Pointer put uses ConditionExpression=attribute_not_exists(pk), so
    re-running the script never clobbers a live pointer already
    produced by the rules-engine write path. Version-row writes are
    idempotent by primary key (sk=VERSION#{iso_ts}).

Usage:
  python scripts/backfill_document_queue.py --dry-run                 # default
  python scripts/backfill_document_queue.py --org catholic-charities-multi-org
  python scripts/backfill_document_queue.py --commit
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError


VALIDATION_RESULTS_TABLE = "penguin-health-validation-results"
DOCUMENT_QUEUE_TABLE = "penguin-health-document-queue"
ORG_CONFIG_TABLE = "penguin-health-org-config"
AWS_REGION = "us-east-1"

# Progress log cadence — one status line per N documents processed.
_PROGRESS_EVERY = 500


# Reuse the write-path canonicalizer + hash so a document backfilled here
# hashes the same as one produced fresh by the rules-engine Lambda. If
# canonicalization ever diverges between these two entry points, the
# first nightly run after backfill would spuriously re-version every
# document — hence the shared import.
_MULTI_ORG_DIR = os.path.join(
    os.path.dirname(__file__), "..", "lambda", "multi-org"
)
_RULES_ENGINE_DIR = os.path.join(_MULTI_ORG_DIR, "rules-engine")
# `audit` is bundled as a flat package inside every emitting Lambda; the
# rules-engine module imports `from audit import …` so we need the parent
# `multi-org` dir on the path first, then the rules-engine dir for the
# `queue_handler` import itself. Matches the ordering in
# lambda/tests/conftest.py.
sys.path.insert(0, os.path.abspath(_MULTI_ORG_DIR))
sys.path.insert(0, os.path.abspath(_RULES_ENGINE_DIR))
from queue_handler import (  # noqa: E402
    compute_content_hash,
    _DENORMALIZED_FILTERS,
)


_aws_config = Config(region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", config=_aws_config)
s3 = boto3.client("s3", config=_aws_config)


def _iter_doc_rows(results_table, org_id: str):
    """Yield every per-document row for `org_id` from the validation-results
    table.

    Uses the base table's Scan with a FilterExpression on
    organization_id + pk begins_with DOC#. Not the cheapest read but
    this is a one-shot migration; a GSI just for backfill is not worth
    provisioning.
    """
    last_evaluated = None
    while True:
        kwargs = {
            "FilterExpression": (
                "begins_with(#pk, :doc) AND organization_id = :org"
            ),
            "ExpressionAttributeNames": {"#pk": "pk"},
            "ExpressionAttributeValues": {
                ":doc": "DOC#",
                ":org": org_id,
            },
        }
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = results_table.scan(**kwargs)
        for item in resp.get("Items", []):
            # Skip sentinel rows written by queue_handler.write_sentinel_row
            # (they're only present post-cutover but a re-run of this
            # script after some live traffic could hit them).
            if item.get("duplicate_of_version_sk"):
                continue
            pk = item.get("pk", "")
            if "#SKIPPED#" in pk or "#ERROR#" in pk:
                continue
            yield item
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


def _derive_status(item: dict) -> str:
    """Terminal state derivation mirrors _reconcile_queue_from_result on
    the admin API side. `open` unless every failing rule was fixed
    (resolved) or the document was doc-confirmed (confirmed)."""
    if item.get("document_confirmed"):
        return "confirmed"
    rules = item.get("rules") or []
    failing = [r for r in rules if r.get("status") == "FAIL"]
    if failing and all(r.get("fixed") for r in failing):
        return "resolved"
    return "open"


def _finding_counts(rules) -> dict:
    total = len(rules or [])
    failed = 0
    resolved = 0
    confirmed = 0
    for r in rules or []:
        if r.get("status") != "FAIL":
            continue
        failed += 1
        if r.get("fixed"):
            resolved += 1
        elif r.get("finding_confirmed"):
            confirmed += 1
    open_fails = failed - resolved - confirmed
    return {
        "total_findings": total,
        "failed_findings": failed,
        "resolved_findings": resolved,
        "confirmed_findings": confirmed,
        "open_findings": open_fails,
    }


def _denormalize_filters(field_values):
    out = {}
    for key in _DENORMALIZED_FILTERS:
        value = field_values.get(key)
        if value in (None, ""):
            continue
        out[key] = value
    return out


def _org_bucket(org_id: str):
    """Resolve the org's PHI S3 bucket. Reads only `s3_bucket_name` from
    the ORG#{id}/METADATA config row — nothing PHI-adjacent."""
    tbl = dynamodb.Table(ORG_CONFIG_TABLE)
    try:
        resp = tbl.get_item(
            Key={"pk": f"ORG#{org_id}", "sk": "METADATA"},
            ProjectionExpression="s3_bucket_name",
        )
    except ClientError as e:
        print(f"WARN: could not read bucket for {org_id}: {e}")
        return None
    item = resp.get("Item") or {}
    return item.get("s3_bucket_name") or f"penguin-health-{org_id}"


def _rebuild_data_from_s3(bucket: str, key: str):
    """Reconstruct the `data` dict the rules-engine passed into
    `validate_document` when it originally validated this s3 object.

    Mirrors `rules_engine_rag.process_file`:
      * `.csv` → {'text': <raw utf-8>}
      * anything else → json.loads(<raw utf-8>)

    Returns None if the object no longer exists or can't be decoded —
    caller must handle by skipping the doc, otherwise the fabricated
    hash would spuriously re-open the pointer on the next real run.
    """
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return None
        raise

    body = resp["Body"].read()
    try:
        content = body.decode("utf-8")
    except UnicodeDecodeError:
        return None

    if key.endswith(".csv"):
        return {"text": content}
    try:
        return json.loads(content)
    except (json.JSONDecodeError, ValueError):
        return None


def _plan_backfill_for_org(results_table, org_id: str):
    """Group results by document_id, pick the latest per doc, compute
    pointer + version rows.

    The content hash is computed by re-reading the source S3 object the
    latest row was validated from, so the seeded hash matches whatever
    the rules-engine will compute on the next nightly run of the same
    unchanged record. Docs whose s3_key is missing, whose S3 object no
    longer exists, or whose body can't be decoded are SKIPPED with a
    warning — better to leave them for the rules engine to pick up
    fresh than to seed a wrong hash that spuriously re-opens the entry.

    Returns (pointer_items, version_items, stats_dict)."""
    latest_by_doc = {}
    first_by_doc = {}
    processed = 0

    for item in _iter_doc_rows(results_table, org_id):
        processed += 1
        doc_id = item.get("document_id")
        if not doc_id or doc_id == "UNKNOWN":
            continue
        ts = item.get("validation_timestamp") or ""
        latest = latest_by_doc.get(doc_id)
        if latest is None or ts > (latest.get("validation_timestamp") or ""):
            latest_by_doc[doc_id] = item
        first = first_by_doc.get(doc_id)
        if first is None or ts < (first.get("validation_timestamp") or ""):
            first_by_doc[doc_id] = item

        if processed % _PROGRESS_EVERY == 0:
            print(f"  scanned {processed} rows for {org_id}...")

    bucket = _org_bucket(org_id)
    if not bucket:
        print(f"WARN: no bucket for {org_id}; every doc will be skipped")

    # Cache raw records by s3_key so we don't re-fetch when multiple
    # entries in the same run share a source object (rare but possible).
    raw_by_key: dict[str, dict] = {}

    pointer_items = []
    version_items = []
    skipped_missing_key = 0
    skipped_missing_object = 0

    for doc_id, latest in latest_by_doc.items():
        first = first_by_doc[doc_id]
        field_values = latest.get("field_values") or {}

        s3_key = latest.get("s3_key")
        if not s3_key or not bucket:
            skipped_missing_key += 1
            continue

        cached = raw_by_key.get(s3_key, "unset")
        if cached == "unset":
            cached = _rebuild_data_from_s3(bucket, s3_key)
            raw_by_key[s3_key] = cached
        if cached is None:
            skipped_missing_object += 1
            continue

        # Same input the write path hashes at the top of
        # validate_document, so a subsequent unchanged re-run matches
        # this seed exactly.
        content_hash = compute_content_hash(cached)
        status = _derive_status(latest)
        counts = _finding_counts(latest.get("rules"))
        validation_ts = latest.get("validation_timestamp")
        validation_run_id = latest.get("validation_run_id")

        version_sk = f"VERSION#{validation_ts}"
        version_items.append({
            "pk": f"ORG#{org_id}#DOC#{doc_id}",
            "sk": version_sk,
            "document_id": doc_id,
            "organization_id": org_id,
            "content_hash": content_hash,
            "validation_run_id": validation_run_id,
            "validation_timestamp": validation_ts,
            "field_values_snapshot": field_values,
            "summary": latest.get("summary") or {},
            "validation_result_pk": latest.get("pk"),
            "validation_result_sk": latest.get("sk"),
            "previous_version_sk": None,
        })

        pointer = {
            "pk": f"ORG#{org_id}",
            "sk": f"DOC#{doc_id}",
            "document_id": doc_id,
            "organization_id": org_id,
            "status": status,
            "content_hash": content_hash,
            "latest_version_sk": version_sk,
            "latest_validation_run_id": validation_run_id,
            "latest_validation_timestamp": validation_ts,
            "first_seen_run_id": first.get("validation_run_id"),
            "first_seen_at": first.get("validation_timestamp"),
            "last_updated_at": validation_ts,
            "last_seen_at": validation_ts,
            "seen_count": 1,
            "version_count": 1,
            "field_values_snapshot": field_values,
            "latest_validation_result_pk": latest.get("pk"),
            "latest_validation_result_sk": latest.get("sk"),
            "gsi1pk": f"ORG#{org_id}#STATUS#{status}",
            "gsi1sk": f"LAST_UPDATED#{validation_ts}",
            **counts,
        }
        pointer.update(_denormalize_filters(field_values))
        # Sparse GSI2 — only open rows are visible to the auto-close scan.
        if status == "open":
            pointer["gsi2pk"] = "STATUS#open"
            pointer["gsi2sk"] = f"LAST_UPDATED#{validation_ts}"
        pointer_items.append(pointer)

    stats = {
        "scanned": processed,
        "unique_docs": len(latest_by_doc),
        "skipped_missing_s3_key": skipped_missing_key,
        "skipped_missing_s3_object": skipped_missing_object,
    }
    return pointer_items, version_items, stats


def _commit(queue_table, pointer_items, version_items, dry_run: bool):
    """Write pointer + version rows. Pointer put is conditional so a
    re-run never clobbers a live entry produced by the rules-engine
    write path in the meantime."""
    if dry_run:
        return {"pointers_written": 0, "versions_written": 0, "skipped_existing": 0}

    pointers_written = 0
    skipped_existing = 0
    for item in pointer_items:
        try:
            queue_table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(pk)",
            )
            pointers_written += 1
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                skipped_existing += 1
            else:
                raise

    versions_written = 0
    for item in version_items:
        # Versions are keyed by their timestamp; overwrite-safe since a
        # collision means an identical write.
        queue_table.put_item(Item=item)
        versions_written += 1

    return {
        "pointers_written": pointers_written,
        "versions_written": versions_written,
        "skipped_existing": skipped_existing,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--org", action="append",
                        help="Restrict to specific org_ids (repeatable). "
                             "Default: every org present in the results table.")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--dry-run", action="store_true", default=True,
                     help="Report what would be written; no S3/DDB writes. Default.")
    grp.add_argument("--commit", action="store_true",
                     help="Actually write to DynamoDB.")
    args = parser.parse_args()

    dry_run = not args.commit
    print(f"backfill_document_queue: dry_run={dry_run}")

    results_table = dynamodb.Table(VALIDATION_RESULTS_TABLE)
    queue_table = dynamodb.Table(DOCUMENT_QUEUE_TABLE)

    orgs = args.org
    if not orgs:
        print("Discovering orgs by scanning ORG# summary rows in results table...")
        orgs = _discover_orgs(results_table)
        print(f"Found {len(orgs)} org(s): {sorted(orgs)}")

    started = datetime.now(timezone.utc)
    totals = defaultdict(int)
    for org_id in sorted(orgs):
        print(f"\n=== {org_id} ===")
        pointers, versions, stats = _plan_backfill_for_org(results_table, org_id)
        print(f"  scanned={stats['scanned']} "
              f"unique_docs={stats['unique_docs']} "
              f"planned_pointers={len(pointers)} "
              f"skipped_missing_s3_key={stats['skipped_missing_s3_key']} "
              f"skipped_missing_s3_object={stats['skipped_missing_s3_object']}")
        result = _commit(queue_table, pointers, versions, dry_run)
        print(f"  wrote pointers={result['pointers_written']} "
              f"versions={result['versions_written']} "
              f"skipped_existing={result['skipped_existing']}")
        for k, v in stats.items():
            totals[k] += v
        for k, v in result.items():
            totals[k] += v

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nDone in {elapsed:.1f}s. Totals: {dict(totals)}")


def _discover_orgs(results_table):
    """Return every org_id that has at least one ORG#/RUN# summary row.
    Cheaper than scanning DOC# rows for org_ids and gives us the same set."""
    orgs = set()
    last_evaluated = None
    while True:
        kwargs = {
            "FilterExpression": "begins_with(#pk, :o) AND begins_with(#sk, :r)",
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {":o": "ORG#", ":r": "RUN#"},
            "ProjectionExpression": "#pk",
        }
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = results_table.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            if pk.startswith("ORG#"):
                orgs.add(pk[len("ORG#"):])
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return orgs


if __name__ == "__main__":
    main()
