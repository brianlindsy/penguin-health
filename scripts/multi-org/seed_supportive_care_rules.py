#!/usr/bin/env python3
"""Seed (or update) the supportive-care org's compliance rules in DynamoDB.

Reads `scripts/multi-org/rule-seeds/supportive-care-aba.json` and writes one
DynamoDB item per rule to `penguin-health-org-config` under
  pk = ORG#<org_id>, sk = RULE#<rule_id>

Idempotent: re-running overwrites existing rule items (boto3 PutItem).

Prereqs:
  - scripts/multi-org/create-organization.sh has already created the org's
    metadata record (ORG#supportive-care / METADATA).
  - The `penguin-health-narrative-hashes` DynamoDB table exists (deployed
    by the AuditEngine CDK construct). Rule 1 uses it for duplicate
    detection and will SKIP if the table is missing.

Usage:
    python scripts/multi-org/seed_supportive_care_rules.py \\
        [--org-id supportive-care] \\
        [--region us-east-1] \\
        [--seed-file scripts/multi-org/rule-seeds/supportive-care-aba.json] \\
        [--dry-run]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = "penguin-health-org-config"
DEFAULT_SEED = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "rule-seeds",
    "supportive-care-aba.json",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rule_item(*, org_id: str, rule: dict, category: str, version: str) -> dict:
    """Shape a seed-file rule into the DynamoDB item format used by admin_api."""
    now = _now_iso()
    rule_id = str(rule["id"])
    return {
        "pk": f"ORG#{org_id}",
        "sk": f"RULE#{rule_id}",
        "gsi1pk": "RULE",
        "gsi1sk": f"ORG#{org_id}#RULE#{rule_id}",
        "rule_id": rule_id,
        "name": rule["name"],
        "category": category,
        "description": rule.get("description", ""),
        "enabled": rule.get("enabled", True),
        "type": rule["type"],
        "version": version,
        "rule_text": rule.get("rule_text", ""),
        "fields_to_extract": rule.get("fields_to_extract", []),
        "notes": rule.get("notes", []),
        "conditions": rule.get("conditions", []),
        "conditionals": rule.get("conditionals", []),
        "logic": rule.get("logic", "all"),
        "created_at": now,
        "updated_at": now,
    }


def parse_args(argv):
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--org-id", default=None,
                   help="Override the org_id from the seed file.")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--seed-file", default=DEFAULT_SEED)
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be written; make no DynamoDB calls.")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv or sys.argv[1:])

    with open(args.seed_file, "r", encoding="utf-8") as f:
        seed = json.load(f)

    org_id = args.org_id or seed["org_id"]
    category = seed["category"]
    version = seed["version"]
    rules = seed["rules"]

    if not rules:
        print(f"No rules in {args.seed_file}; nothing to seed.")
        return 0

    if args.dry_run:
        for rule in rules:
            item = _rule_item(org_id=org_id, rule=rule, category=category, version=version)
            print(f"[dry-run] would put rule {item['rule_id']} ({item['name']}) "
                  f"type={item['type']} enabled={item['enabled']}")
        print(f"[dry-run] {len(rules)} rule(s) would be upserted for {org_id}")
        return 0

    table = boto3.resource("dynamodb", region_name=args.region).Table(TABLE_NAME)
    for rule in rules:
        item = _rule_item(org_id=org_id, rule=rule, category=category, version=version)
        table.put_item(Item=item)
        print(f"Upserted rule {item['rule_id']} ({item['name']}) "
              f"type={item['type']} enabled={item['enabled']}")

    print(f"Upserted {len(rules)} rules for {org_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
