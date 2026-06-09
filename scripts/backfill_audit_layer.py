#!/usr/bin/env python3
"""
One-shot backfill: copy the last 30 days of legacy `AUDIT#` rows from
`penguin-health-stedi` into the new `penguin-health-audit` table.

Why 30 days: the orchestrator's eligibility dedup pre-check looks back 30
minutes (audit.recent_check_summary) and the worklist discrepancy logic
looks back `_DISCOVERY_LOOKBACK_DAYS = 30` days. Once we cut the read
paths over to `penguin-health-audit`, those queries return empty without
the historical data. This script bridges the gap.

The script DOES NOT write to Firehose / S3 — the WORM archive of legacy
rows is the existing `penguin-health-stedi` table (which retains them
for 7 years via expires_at TTL). We are mirroring into the DDB hot
mirror only; new writes start populating Firehose naturally from PR 3
onward.

Idempotent: PutItem uses ConditionExpression='attribute_not_exists(pk)'
so re-runs don't double-write. Already-mirrored rows are skipped with
no DDB delta.

Usage:
    python scripts/backfill_audit_layer.py \\
        [--source-table penguin-health-stedi] \\
        [--dest-table penguin-health-audit] \\
        [--lookback-days 30] \\
        [--region us-east-1] \\
        [--profile my-aws-profile] \\
        [--dry-run]
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


DEFAULT_SOURCE_TABLE = "penguin-health-stedi"
DEFAULT_DEST_TABLE = "penguin-health-audit"
DEFAULT_REGION = "us-east-1"
DEFAULT_LOOKBACK_DAYS = 30
_NINETY_DAYS_SECONDS = 90 * 24 * 60 * 60


def list_orgs(table):
    """Yield each org_id (the suffix of pk=ORG#<org_id>) by scanning the
    GSI1's PATIENT# partition. We'd love to query org_id directly, but
    the pk/sk schema doesn't carry an org-list partition — the cheap way
    to find every org with audit rows is to scan distinct pk values.

    Scan is fine here because this script runs once during migration.
    """
    seen_orgs = set()
    last = None
    while True:
        kwargs = {
            "ProjectionExpression": "pk",
            "FilterExpression": "begins_with(sk, :a)",
            "ExpressionAttributeValues": {":a": "AUDIT#"},
        }
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item["pk"]
            if pk.startswith("ORG#"):
                org = pk[len("ORG#"):]
                if org not in seen_orgs:
                    seen_orgs.add(org)
                    yield org
        last = resp.get("LastEvaluatedKey")
        if not last:
            break


def query_audit_rows(table, org_id, since_iso):
    """Yield every AUDIT# row for one org with sk >= AUDIT#{since_iso}.

    Paginates through query results to handle orgs with many calls.
    """
    last = None
    while True:
        kwargs = {
            "KeyConditionExpression": (
                Key("pk").eq(f"ORG#{org_id}")
                & Key("sk").gte(f"AUDIT#{since_iso}")
            ),
        }
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            yield item
        last = resp.get("LastEvaluatedKey")
        if not last:
            break


def mirror_row(dest_table, source_row, *, dry_run):
    """Translate a legacy AUDIT# row into the new flat schema and write it
    to the dest table. Returns one of: 'written', 'duplicate', 'skipped'."""
    org_id_full = source_row["pk"]  # ORG#<org_id>
    if not org_id_full.startswith("ORG#"):
        return "skipped"
    org_id = org_id_full[len("ORG#"):]

    event_time = source_row.get("requested_at")
    event_id = source_row.get("request_id")
    if not event_time or not event_id:
        return "skipped"

    expires_at = (
        int(datetime.now(timezone.utc).timestamp()) + _NINETY_DAYS_SECONDS
    )

    item = {
        "pk": f"ORG#{org_id}",
        "sk": f"AUDIT#{event_time}#{event_id}",
        "expires_at": expires_at,
        "event_id": event_id,
        "event_time": event_time,
        "org_id": org_id,
        "action": "read",
        "outcome": "success",
        # The legacy row had `user_email`; the new schema uses both
        # `agent_id` and `agent_email`. For backfill purposes the
        # email goes in both — every legacy row was a human-triggered
        # eligibility call.
        "agent_email": source_row.get("user_email"),
        "agent_id": source_row.get("user_email"),
        "resource_type": "Coverage",
        "resource_id": None,
        "call_type": source_row.get("call_type"),
        "patient_hash": source_row.get("patient_hash"),
        "patient_first_initial": source_row.get("patient_first_initial"),
        "patient_last_initial": source_row.get("patient_last_initial"),
        "patient_dob": source_row.get("patient_dob"),
        "member_id_last4": source_row.get("member_id_last4"),
        "payer_id": source_row.get("payer_id"),
        "payer_name": source_row.get("payer_name"),
        # Preserve the full original row under `event` so a future
        # Firehose backfill can replay it into S3 if needed.
        "event": {
            "event_id": event_id,
            "event_time": event_time,
            "schema_version": "1",
            "action": "read",
            "outcome": "success",
            "purpose_of_use": "ELIGIBILITY",
            "org_id": org_id,
            "agent_type": "human",
            "agent_id": source_row.get("user_email"),
            "agent_email": source_row.get("user_email"),
            "client_ip": source_row.get("client_ip"),
            "resource_type": "Coverage",
            "patient_hash": source_row.get("patient_hash"),
            "patient_first_initial": source_row.get("patient_first_initial"),
            "patient_last_initial": source_row.get("patient_last_initial"),
            "patient_dob": source_row.get("patient_dob"),
            "member_id_last4": source_row.get("member_id_last4"),
            "payer_id": source_row.get("payer_id"),
            "payer_name": source_row.get("payer_name"),
            "call_type": source_row.get("call_type"),
            "external_control_number": source_row.get("stedi_control_number"),
            "duration_ms": int(source_row["duration_ms"])
                if source_row.get("duration_ms") is not None else None,
            "result_summary": source_row.get("result_summary"),
            "_backfilled_from": "penguin-health-stedi",
        },
    }
    if source_row.get("patient_hash"):
        item["gsi1pk"] = f"PATIENT#{org_id}#{source_row['patient_hash']}"
        item["gsi1sk"] = event_time

    if dry_run:
        return "written"

    try:
        dest_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
        return "written"
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == \
                "ConditionalCheckFailedException":
            return "duplicate"
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--dest-table", default=DEFAULT_DEST_TABLE)
    parser.add_argument("--lookback-days", type=int,
                        default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    session = (
        boto3.Session(profile_name=args.profile, region_name=args.region)
        if args.profile else boto3.Session(region_name=args.region)
    )
    dynamodb = session.resource("dynamodb")
    source = dynamodb.Table(args.source_table)
    dest = dynamodb.Table(args.dest_table)

    since = datetime.now(timezone.utc) - timedelta(days=args.lookback_days)
    since_iso = since.isoformat()
    print(f"Backfilling AUDIT# rows since {since_iso} "
          f"({args.lookback_days} days)")
    print(f"  source: {args.source_table}")
    print(f"  dest  : {args.dest_table}")
    if args.dry_run:
        print("  DRY-RUN: not writing any items")

    totals = {"written": 0, "duplicate": 0, "skipped": 0, "errored": 0}

    for org_id in list_orgs(source):
        print(f"\norg={org_id}")
        per_org = {"written": 0, "duplicate": 0, "skipped": 0, "errored": 0}
        for row in query_audit_rows(source, org_id, since_iso):
            try:
                result = mirror_row(dest, row, dry_run=args.dry_run)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code") or "ClientError"
                print(f"  ERROR row={row.get('request_id')} code={code}")
                per_org["errored"] += 1
                totals["errored"] += 1
                continue
            per_org[result] += 1
            totals[result] += 1
        print(f"  written={per_org['written']} "
              f"duplicate={per_org['duplicate']} "
              f"skipped={per_org['skipped']} "
              f"errored={per_org['errored']}")

    print(
        f"\nTotal: written={totals['written']} "
        f"duplicate={totals['duplicate']} "
        f"skipped={totals['skipped']} "
        f"errored={totals['errored']}"
    )
    return 0 if totals["errored"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
