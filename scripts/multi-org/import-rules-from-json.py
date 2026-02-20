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
    org_name = config.get('organization_name', org_id)
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
    print(f"Organization name: {org_name}")
    print(f"Version: {version}")
    print(f"Rules to import: {len(rules)}")
    print()

    # Write METADATA item (organization metadata)
    table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': 'METADATA',
        'gsi1pk': 'ORG_METADATA',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'organization_name': org_name,
        'display_name': org_name,
        's3_bucket_name': f'penguin-health-{org_id}',
        'enabled': True,
        'created_at': config.get('created_at', now),
        'updated_at': now,
    })
    print(f"  METADATA written: {org_name}")

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

    # Write each RULE# item (new flat schema - no llm_config, no GSI2)
    for rule in rules:
        rule_id = rule['id']
        item = {
            'pk': f'ORG#{org_id}',
            'sk': f'RULE#{rule_id}',
            'gsi1pk': 'RULE',
            'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
            'rule_id': rule_id,
            'name': rule['name'],
            'category': rule.get('category', ''),
            'description': rule.get('description', ''),
            'enabled': rule.get('enabled', True),
            'type': rule.get('type', 'llm'),
            'version': version,
            'rule_text': rule.get('rule_text', ''),
            'fields_to_extract': rule.get('fields_to_extract', []),
            'notes': rule.get('notes', []),
            'created_at': now,
            'updated_at': now,
        }
        table.put_item(Item=item)
        print(f"  RULE#{rule_id} written: {rule['name']}")

    print()
    print(f"Done. Imported METADATA + RULES_CONFIG + {len(rules)} rules for {org_id}.")


if __name__ == '__main__':
    main()
