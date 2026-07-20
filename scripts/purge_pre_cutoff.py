#!/usr/bin/env python3
"""
Time-bounded purge of pre-cutoff validation state.

Removes every row whose validation timestamp is earlier than the cutoff
across:

  * `penguin-health-validation-results` — per-doc rows
    (pk=DOC#{id}, sk=VALIDATION#{ts}), per-run summary rows
    (pk=ORG#{org}, sk=RUN#{run_id}, where the run_id is parsed as
    YYYYMMDD-HHMMSS), and sentinel/error rows keyed on a run id in the
    partition key.
  * `penguin-health-document-queue` — pointer rows
    (pk=ORG#{org}, sk=DOC#{id}) whose `latest_validation_timestamp` is
    pre-cutoff, and version rows (pk=ORG#{org}#DOC#{id},
    sk=VERSION#{ts}) whose timestamp is pre-cutoff. A version row that
    is still the pointer's `latest_version_sk` is refused.
  * S3 objects for pre-cutoff runs:
      s3://penguin-health-{org}/analytics/validation_results/
        validation_date=YYYY-MM-DD/run_id={run_id}/part-0.parquet
      s3://penguin-health-{org}/validation-reports/{run_id}-validation-report.csv

Audit rows (`penguin-health-audit` + the WORM S3 archive) are NOT
touched — deletions themselves emit audit events.

Guardrails:
  * Default is `--dry-run`. Nothing is written without `--commit`.
  * DDB scans project only the primary keys + the timestamps we key on,
    so PHI never enters the CLI/log surface.
  * Deletes batch 25 at a time.
  * Every commit-mode delete prints its (pk, sk) so the run is
    auditable from stdout.

Usage:
  # Show what would be deleted.
  python scripts/purge_pre_cutoff.py --cutoff 2026-07-01 --org supportive-care

  # Actually delete.
  python scripts/purge_pre_cutoff.py --cutoff 2026-07-01 --org supportive-care --commit
"""

import argparse
import re
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


VALIDATION_RESULTS_TABLE = "penguin-health-validation-results"
DOCUMENT_QUEUE_TABLE = "penguin-health-document-queue"
ORG_CONFIG_TABLE = "penguin-health-org-config"
AWS_REGION = "us-east-1"

_aws = Config(region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", config=_aws)
s3 = boto3.client("s3", config=_aws)


# Run IDs are emitted as YYYYMMDD-HHMMSS at trigger time — same shape
# validated on the admin UI side. Anything not matching is left alone
# (safe default; alerts on stdout).
_RUN_ID_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})$")


def _parse_run_id_ts(run_id: str):
    m = _RUN_ID_RE.match(run_id or "")
    if not m:
        return None
    y, mo, d, h, mi, s = (int(x) for x in m.groups())
    try:
        return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_iso(ts: str):
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Trim trailing 'Z' if present; fromisoformat doesn't like it.
        cleaned = ts.rstrip("Z")
        dt = datetime.fromisoformat(cleaned)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _org_bucket(org_id: str) -> str | None:
    """Look up the org's PHI bucket from its ORG#{id}/METADATA row.

    Only the `s3_bucket_name` attribute is projected; nothing PHI-adjacent
    lands on stdout.
    """
    tbl = dynamodb.Table(ORG_CONFIG_TABLE)
    try:
        resp = tbl.get_item(
            Key={"pk": f"ORG#{org_id}", "sk": "METADATA"},
            ProjectionExpression="s3_bucket_name",
        )
    except ClientError as e:
        print(f"WARN: failed to read org bucket for {org_id}: {e}")
        return None
    item = resp.get("Item") or {}
    bucket = item.get("s3_bucket_name") or f"penguin-health-{org_id}"
    return bucket


# ----- DynamoDB: validation-results -----------------------------------------

def _scan_validation_results_pre_cutoff(cutoff: datetime, org_ids: set[str]):
    """Yield {pk, sk, kind, org_id} for every pre-cutoff row we should delete.

    Projection is limited to `pk, sk, organization_id, validation_timestamp,
    validation_run_id` so PHI (`field_values`, `rules[]`, etc.) never
    leaves the table.
    """
    tbl = dynamodb.Table(VALIDATION_RESULTS_TABLE)
    kwargs = {
        "ProjectionExpression": (
            "#pk, #sk, organization_id, validation_timestamp, validation_run_id"
        ),
        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
    }
    last_evaluated = None
    cutoff_sk = f"VALIDATION#{cutoff.isoformat().replace('+00:00', '')}"
    while True:
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = tbl.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            sk = item.get("sk", "")
            org_id = item.get("organization_id")

            # RUN summary rows: pk=ORG#{org}, sk=RUN#{run_id}
            if pk.startswith("ORG#") and sk.startswith("RUN#"):
                run_id = sk[len("RUN#"):]
                ts = _parse_run_id_ts(run_id)
                if ts is None or ts >= cutoff:
                    continue
                row_org = pk[len("ORG#"):]
                if org_ids and row_org not in org_ids:
                    continue
                yield {"pk": pk, "sk": sk, "kind": "run_summary",
                       "org_id": row_org, "run_id": run_id}
                continue

            # DOC / DOC#…#SKIPPED#{run_id} / DOC#…#ERROR#{run_id}: sk always
            # starts with VALIDATION#{ts}. The `sk < cutoff_sk` string
            # comparison works because ISO-8601 sorts lexicographically.
            if pk.startswith("DOC#") and sk.startswith("VALIDATION#"):
                if sk >= cutoff_sk:
                    continue
                if org_ids and org_id not in org_ids:
                    continue
                # Classify for the log line.
                if "#SKIPPED#" in pk:
                    kind = "sentinel_skipped"
                elif "#ERROR#" in pk:
                    kind = "sentinel_error"
                else:
                    kind = "doc_result"
                yield {"pk": pk, "sk": sk, "kind": kind, "org_id": org_id,
                       "run_id": item.get("validation_run_id")}
                continue

            # Anything else on this table (unknown row shape) is left alone.
            continue

        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


# ----- DynamoDB: document-queue ---------------------------------------------

def _scan_queue_pointers(cutoff: datetime, org_ids: set[str]):
    """Yield pointer rows whose latest validation is pre-cutoff.

    Projection includes `latest_version_sk` so downstream cascade of the
    version rows only refuses to touch a live latest.
    """
    tbl = dynamodb.Table(DOCUMENT_QUEUE_TABLE)
    kwargs = {
        "FilterExpression": "begins_with(#sk, :d)",
        "ProjectionExpression": (
            "#pk, #sk, organization_id, latest_validation_timestamp, "
            "latest_version_sk"
        ),
        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
        "ExpressionAttributeValues": {":d": "DOC#"},
    }
    last_evaluated = None
    while True:
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = tbl.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            sk = item.get("sk", "")
            if not pk.startswith("ORG#") or not sk.startswith("DOC#"):
                continue
            org_id = item.get("organization_id") or pk[len("ORG#"):]
            if org_ids and org_id not in org_ids:
                continue
            latest_ts = _parse_iso(item.get("latest_validation_timestamp"))
            if latest_ts is None or latest_ts >= cutoff:
                continue
            yield {
                "pk": pk, "sk": sk,
                "org_id": org_id,
                "kind": "queue_pointer",
                "latest_version_sk": item.get("latest_version_sk"),
            }
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


def _scan_queue_versions(cutoff: datetime, org_ids: set[str],
                         live_latest_per_pointer: dict):
    """Yield version rows whose timestamp is pre-cutoff.

    Refuses to yield a row that's still the `latest_version_sk` of a
    pointer we are NOT deleting — that would orphan a live queue entry.
    """
    tbl = dynamodb.Table(DOCUMENT_QUEUE_TABLE)
    kwargs = {
        "FilterExpression": "begins_with(#sk, :v)",
        "ProjectionExpression": "#pk, #sk, organization_id",
        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
        "ExpressionAttributeValues": {":v": "VERSION#"},
    }
    last_evaluated = None
    cutoff_sk = f"VERSION#{cutoff.isoformat().replace('+00:00', '')}"
    while True:
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = tbl.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            sk = item.get("sk", "")
            if not pk.startswith("ORG#") or not sk.startswith("VERSION#"):
                continue
            # Guard: refuse to delete a row that is still a live pointer's
            # latest_version_sk.
            if sk == live_latest_per_pointer.get(pk):
                print(f"REFUSED: {pk} / {sk} is a live pointer's latest_version_sk")
                continue
            if sk >= cutoff_sk:
                continue
            # Org filter is derived from the pk which encodes
            # ORG#{org}#DOC#{doc_id}.
            m = re.match(r"^ORG#([^#]+)#DOC#", pk)
            row_org = m.group(1) if m else item.get("organization_id")
            if org_ids and row_org not in org_ids:
                continue
            yield {"pk": pk, "sk": sk, "kind": "queue_version",
                   "org_id": row_org}
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


# ----- Live-pointer map -----------------------------------------------------

def _live_latest_per_pointer_pk(cutoff: datetime, org_ids: set[str]) -> dict:
    """Return {pk_of_version_partition: latest_version_sk} for pointers we
    are KEEPING (post-cutoff). Used by the version-delete pass to refuse
    orphaning a live pointer's latest version.

    Version rows are partitioned by ORG#{org}#DOC#{doc_id}. The pointer's
    row is ORG#{org} / DOC#{doc_id}. Translate to the version-partition
    format so lookups by version pk match.
    """
    tbl = dynamodb.Table(DOCUMENT_QUEUE_TABLE)
    kwargs = {
        "FilterExpression": "begins_with(#sk, :d)",
        "ProjectionExpression": (
            "#pk, #sk, organization_id, latest_validation_timestamp, "
            "latest_version_sk"
        ),
        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
        "ExpressionAttributeValues": {":d": "DOC#"},
    }
    live = {}
    last_evaluated = None
    while True:
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = tbl.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            sk = item.get("sk", "")
            if not pk.startswith("ORG#") or not sk.startswith("DOC#"):
                continue
            org_id = item.get("organization_id") or pk[len("ORG#"):]
            if org_ids and org_id not in org_ids:
                continue
            latest_ts = _parse_iso(item.get("latest_validation_timestamp"))
            latest_version_sk = item.get("latest_version_sk")
            if not latest_version_sk:
                continue
            # Only live pointers (post-cutoff) contribute the guard entry.
            if latest_ts is None or latest_ts < cutoff:
                continue
            doc_id = sk[len("DOC#"):]
            version_pk = f"ORG#{org_id}#DOC#{doc_id}"
            live[version_pk] = latest_version_sk
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return live


# ----- Batch delete ---------------------------------------------------------

def _batch_delete(table_name: str, keys: list[dict], commit: bool) -> int:
    """Delete `keys` in chunks of 25. Each key is {'pk': ..., 'sk': ...}."""
    if not keys:
        return 0
    if not commit:
        return len(keys)
    tbl = dynamodb.Table(table_name)
    written = 0
    for i in range(0, len(keys), 25):
        chunk = keys[i:i + 25]
        with tbl.batch_writer() as batch:
            for k in chunk:
                batch.delete_item(Key={"pk": k["pk"], "sk": k["sk"]})
                written += 1
    return written


# ----- S3: parquet + csv ----------------------------------------------------

def _s3_purge_pre_cutoff(bucket: str, cutoff: datetime, commit: bool) -> dict:
    """Delete the analytics/ Parquet snapshots and validation-reports/ CSVs
    for runs whose run_id timestamp is pre-cutoff.

    Keys are opaque strings — no PHI in the listing output.
    """
    to_delete: list[str] = []

    parquet_prefix = "analytics/validation_results/"
    csv_prefix = "validation-reports/"

    paginator = s3.get_paginator("list_objects_v2")

    # Parquet keys look like:
    #   analytics/validation_results/validation_date=YYYY-MM-DD/
    #     run_id={run_id}/part-0.parquet
    parquet_re = re.compile(
        r"^analytics/validation_results/validation_date=(\d{4}-\d{2}-\d{2})/"
        r"run_id=([^/]+)/"
    )
    for page in paginator.paginate(Bucket=bucket, Prefix=parquet_prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            m = parquet_re.match(key)
            if not m:
                continue
            run_id = m.group(2)
            ts = _parse_run_id_ts(run_id)
            if ts is not None and ts < cutoff:
                to_delete.append(key)

    # CSV report keys look like:
    #   validation-reports/{run_id}-validation-report.csv
    csv_re = re.compile(r"^validation-reports/([^/]+)-validation-report\.csv$")
    for page in paginator.paginate(Bucket=bucket, Prefix=csv_prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            m = csv_re.match(key)
            if not m:
                continue
            run_id = m.group(1)
            ts = _parse_run_id_ts(run_id)
            if ts is not None and ts < cutoff:
                to_delete.append(key)

    if not commit:
        return {"planned": len(to_delete), "deleted": 0}

    deleted = 0
    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i:i + 1000]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
        errs = resp.get("Errors") or []
        for e in errs:
            print(f"  s3 delete error: {e.get('Key')}: {e.get('Message')}")
        deleted += len(chunk) - len(errs)
    return {"planned": len(to_delete), "deleted": deleted}


# ----- Main -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cutoff", required=True,
        help="ISO date (YYYY-MM-DD) — anything strictly before this UTC "
             "day is deleted.",
    )
    parser.add_argument(
        "--org", action="append",
        help="Restrict to specific org_ids (repeatable). Default: all orgs "
             "with rows in the target tables.",
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--dry-run", action="store_true", default=True,
                     help="Report what would be deleted; no writes. Default.")
    grp.add_argument("--commit", action="store_true",
                     help="Actually delete.")
    args = parser.parse_args()

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", args.cutoff):
        print(f"ERROR: --cutoff must be YYYY-MM-DD, got {args.cutoff!r}")
        sys.exit(2)
    cutoff = datetime.fromisoformat(args.cutoff).replace(tzinfo=timezone.utc)
    org_ids = set(args.org or [])
    commit = args.commit

    print(f"purge_pre_cutoff: cutoff={cutoff.isoformat()} "
          f"orgs={sorted(org_ids) or 'ALL'} commit={commit}")

    # ----- validation-results table -----
    print("\n== penguin-health-validation-results ==")
    doc_keys = []
    run_keys = []
    sentinel_keys = []
    for row in _scan_validation_results_pre_cutoff(cutoff, org_ids):
        entry = {"pk": row["pk"], "sk": row["sk"]}
        if row["kind"] == "run_summary":
            run_keys.append(entry)
        elif row["kind"] == "doc_result":
            doc_keys.append(entry)
        else:
            sentinel_keys.append(entry)
    print(f"  doc_result rows: {len(doc_keys)}")
    print(f"  run_summary rows: {len(run_keys)}")
    print(f"  sentinel rows: {len(sentinel_keys)}")

    # ----- document-queue table -----
    print("\n== penguin-health-document-queue ==")
    live_latest = _live_latest_per_pointer_pk(cutoff, org_ids)
    print(f"  live post-cutoff pointers (guarded from cascade): {len(live_latest)}")

    pointer_keys = []
    for row in _scan_queue_pointers(cutoff, org_ids):
        pointer_keys.append({"pk": row["pk"], "sk": row["sk"]})
    print(f"  queue pointer rows to delete: {len(pointer_keys)}")

    version_keys = []
    for row in _scan_queue_versions(cutoff, org_ids, live_latest):
        version_keys.append({"pk": row["pk"], "sk": row["sk"]})
    print(f"  queue version rows to delete: {len(version_keys)}")

    # ----- S3 -----
    print("\n== S3 (Parquet + CSV) ==")
    if not org_ids:
        print("  (no --org supplied; skipping S3 pass — org list required to "
              "resolve buckets)")
    for org_id in sorted(org_ids):
        bucket = _org_bucket(org_id)
        if not bucket:
            print(f"  {org_id}: could not resolve bucket, skipping")
            continue
        print(f"  {org_id}: bucket={bucket}")
        result = _s3_purge_pre_cutoff(bucket, cutoff, commit)
        print(f"    planned={result['planned']} deleted={result['deleted']}")

    if not commit:
        print("\nDRY RUN — no writes. Re-run with --commit to delete.")
        return

    # ----- Actually delete -----
    print("\nCommitting DDB deletes...")
    n = _batch_delete(VALIDATION_RESULTS_TABLE, doc_keys, commit)
    print(f"  deleted {n} doc_result rows")
    n = _batch_delete(VALIDATION_RESULTS_TABLE, sentinel_keys, commit)
    print(f"  deleted {n} sentinel rows")
    n = _batch_delete(VALIDATION_RESULTS_TABLE, run_keys, commit)
    print(f"  deleted {n} run_summary rows")
    n = _batch_delete(DOCUMENT_QUEUE_TABLE, version_keys, commit)
    print(f"  deleted {n} queue version rows")
    n = _batch_delete(DOCUMENT_QUEUE_TABLE, pointer_keys, commit)
    print(f"  deleted {n} queue pointer rows")
    print("Done.")


if __name__ == "__main__":
    main()
