#!/usr/bin/env python3
"""
Import rules from a JSON config file into the penguin-health-org-config DynamoDB table.

Usage:
    python3 scripts/multi-org/import-rules-from-json.py <path-to-rules-json>

Example:
    python3 scripts/multi-org/import-rules-from-json.py config/rules/catholic-charities-multi-org.json
"""

import sys
import json
import boto3
from datetime import datetime

TABLE_NAME = 'penguin-health-org-config'
REGION = 'us-east-1'


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path-to-rules-json>")
        sys.exit(1)

    json_path = sys.argv[1]

    with open(json_path, 'r') as f:
        config = json.load(f)

    org_id = config['organization_id']
    version = config.get('version', '1.0.0')
    field_mappings = config.get('field_mappings', {})
    rules = config.get('rules', [])

    if not rules:
        print(f"No rules found in {json_path}")
        sys.exit(1)

    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)
    now = datetime.utcnow().isoformat() + 'Z'

    print(f"Importing rules for organization: {org_id}")
    print(f"Version: {version}")
    print(f"Rules to import: {len(rules)}")
    print()

    # Write RULES_CONFIG item
    table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': 'RULES_CONFIG',
        'gsi1pk': 'RULES_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'field_mappings': field_mappings,
        'version': version,
        'updated_at': now,
    })
    print(f"  RULES_CONFIG written (field_mappings: {list(field_mappings.keys())})")

    # Write each RULE# item
    for rule in rules:
        rule_id = rule['id']
        item = {
            'pk': f'ORG#{org_id}',
            'sk': f'RULE#{rule_id}',
            'gsi1pk': 'RULE',
            'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
            'gsi2pk': f'ORG#{org_id}#VERSION#{version}',
            'gsi2sk': f'RULE#{rule_id}',
            'rule_id': rule_id,
            'name': rule['name'],
            'category': rule.get('category', ''),
            'description': rule.get('description', ''),
            'enabled': rule.get('enabled', True),
            'type': rule.get('type', 'llm'),
            'version': version,
            'llm_config': rule.get('llm_config', {}),
            'created_at': now,
            'updated_at': now,
        }
        table.put_item(Item=item)
        print(f"  RULE#{rule_id} written: {rule['name']}")

    print()
    print(f"Done. Imported {len(rules)} rules + RULES_CONFIG for {org_id}.")


if __name__ == '__main__':
    main()
