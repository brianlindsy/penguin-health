#!/usr/bin/env python3
"""
Add a validation rule to an organization's configuration in DynamoDB

Usage:
    ./scripts/add-rule.py <org-id> <rule-json-file>

Example:
    ./scripts/add-rule.py community-health rules/example-rule.json
"""

import boto3
import json
import sys
from datetime import datetime
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def add_rule(org_id, rule_data, version='1.0.0'):
    """Add a validation rule to DynamoDB"""

    # Validate required fields
    required_fields = ['id', 'name', 'category', 'type', 'llm_config', 'messages']
    for field in required_fields:
        if field not in rule_data:
            raise ValueError(f"Missing required field: {field}")

    # Use version from rule_data if provided
    if 'version' in rule_data:
        version = rule_data['version']

    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'RULE#{rule_data["id"]}',
        'gsi1pk': 'RULE',
        'gsi1sk': f'ORG#{org_id}#RULE#{rule_data["id"]}',
        'gsi2pk': f'ORG#{org_id}#VERSION#{version}',
        'gsi2sk': f'RULE#{rule_data["id"]}',
        'rule_id': rule_data['id'],
        'name': rule_data['name'],
        'category': rule_data['category'],
        'description': rule_data.get('description', ''),
        'enabled': rule_data.get('enabled', True),
        'type': rule_data.get('type', 'llm'),
        'version': version,
        'llm_config': rule_data['llm_config'],
        'messages': rule_data['messages'],
        'created_at': datetime.utcnow().isoformat() + 'Z',
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    table.put_item(Item=item)

    print(f"✓ Added rule {rule_data['id']}: {rule_data['name']}")
    print(f"  Organization: {org_id}")
    print(f"  Category: {rule_data['category']}")
    print(f"  Type: {rule_data['type']}")
    print(f"  Enabled: {rule_data.get('enabled', True)}")
    print(f"  Version: {version}")

    return item


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: ./add-rule.py <org-id> <rule-json-file>")
        print("")
        print("Example: ./add-rule.py community-health rules/example-rule.json")
        sys.exit(1)

    org_id = sys.argv[1]
    rule_file = sys.argv[2]

    # Check if file exists
    if not Path(rule_file).exists():
        print(f"❌ Error: File not found: {rule_file}")
        sys.exit(1)

    # Load rule data
    try:
        with open(rule_file) as f:
            rule_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {rule_file}")
        print(f"   {str(e)}")
        sys.exit(1)

    # Add rule
    try:
        add_rule(org_id, rule_data)
    except Exception as e:
        print(f"❌ Error adding rule: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
