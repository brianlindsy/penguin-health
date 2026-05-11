#!/usr/bin/env python3
"""
Backfill validation-result Parquet snapshots for runs already stored in
DynamoDB. New runs land in Parquet automatically (the rules-engine Lambda
writes them at end of run); this script catches up everything from the
cutoff date forward.

Usage:
    python scripts/backfill_validation_parquet.py

Configuration is set as constants at the top of the script.

Cutoff: only runs with timestamp >= BACKFILL_FROM are migrated. Older
runs are deliberately excluded — analytics for earlier data stays in
DynamoDB if needed, or simply isn't available.

The Parquet builder used here is the same one the Lambda uses
(lambda/multi-org/rules-engine/parquet_writer.py), so the schema is
guaranteed to match.

Idempotent: re-running overwrites the same S3 keys.
"""

import os
import sys
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BACKFILL_FROM = "2026-05-04"  # inclusive, ISO date

# org_id -> S3 bucket name. Bucket follows the project convention
# `penguin-health-{org_id}` enforced by extract_org_id_from_bucket(); the
# explicit map keeps this script independent of that helper.
ORGS = {
    "catholic-charities-multi-org": "penguin-health-catholic-charities-multi-org",
    "circles-of-care": "penguin-health-circles-of-care",
    "demo": "penguin-health-demo",
}

VALIDATION_RESULTS_TABLE = "penguin-health-validation-results"
AWS_REGION = "us-east-1"

DRY_RUN = False  # True = report what would be written; no S3 writes


# ---------------------------------------------------------------------------
# Imports from the shared Parquet builder
# ---------------------------------------------------------------------------

# Add the rules-engine module dir to the path so we can import the same
# Parquet writer the Lambda uses. Keeping a single source of truth for the
# schema is the whole reason this is shared, so do not copy/paste it.
_RULES_ENGINE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "lambda", "multi-org", "rules-engine"
)
sys.path.insert(0, os.path.abspath(_RULES_ENGINE_DIR))

from parquet_writer import build_parquet_bytes, parquet_key  # noqa: E402


# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

_aws_config = Config(region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", config=_aws_config)
s3_client = boto3.client("s3", config=_aws_config)
table = dynamodb.Table(VALIDATION_RESULTS_TABLE)


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def list_run_summaries(org_id):
    """Return all run-summary items for an org with timestamp >= BACKFILL_FROM."""
    items = []
    last_evaluated = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("pk").eq(f"ORG#{org_id}")
            & Key("sk").begins_with("RUN#"),
        }
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            break

    in_scope = []
    for item in items:
        ts = item.get("timestamp", "")
        if ts and ts >= BACKFILL_FROM:
            in_scope.append(item)
    return in_scope


def fetch_run_documents(validation_run_id):
    """Query GSI2 for all per-document items belonging to a run."""
    items = []
    last_evaluated = None
    while True:
        kwargs = {
            "IndexName": "gsi2",
            "KeyConditionExpression": Key("gsi2pk").eq(f"RUN#{validation_run_id}"),
        }
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            break
    return items


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def backfill_org(org_id, bucket):
    summaries = list_run_summaries(org_id)
    print(f"[{org_id}] {len(summaries)} run(s) in scope (>= {BACKFILL_FROM})")

    for summary in summaries:
        run_id = summary.get("validation_run_id")
        if not run_id:
            print(f"[{org_id}] skipping summary with no validation_run_id: {summary.get('sk')}")
            continue

        items = fetch_run_documents(run_id)
        if not items:
            print(f"[{org_id}] run {run_id}: 0 document items — skipping")
            continue

        payload, row_count = build_parquet_bytes(items)
        if row_count == 0:
            print(f"[{org_id}] run {run_id}: 0 rule rows — skipping")
            continue

        validation_date = _validation_date_from_items(items)
        key = parquet_key(run_id, validation_date)

        if DRY_RUN:
            print(
                f"[{org_id}] DRY RUN: would write {row_count} rows "
                f"to s3://{bucket}/{key}"
            )
            continue

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/octet-stream",
        )
        print(
            f"[{org_id}] run {run_id}: wrote {row_count} rows "
            f"to s3://{bucket}/{key}"
        )


def _validation_date_from_items(items):
    for item in items:
        ts = item.get("validation_timestamp")
        if ts:
            return str(ts)[:10]
    # Should not happen — every validated document has a timestamp.
    raise RuntimeError("Could not determine validation_date for run")


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main():
    print(
        f"Backfill cutoff: {BACKFILL_FROM} | DRY_RUN={DRY_RUN} | "
        f"orgs={list(ORGS)}"
    )
    for org_id, bucket in ORGS.items():
        backfill_org(org_id, bucket)
    print("Done.")


if __name__ == "__main__":
    main()
