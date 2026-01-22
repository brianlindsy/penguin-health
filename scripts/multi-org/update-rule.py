#!/usr/bin/env python3
"""
Update an existing validation rule in DynamoDB

Usage:
    ./scripts/update-rule.py <org-id> <rule-id> <rule-json-file>

Example:
    ./scripts/update-rule.py community-health 13 rules/updated-rule-13.json
"""

import boto3
import json
import sys
from datetime import datetime
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def update_rule(org_id, rule_id, rule_data):
    """Update an existing rule in DynamoDB"""

    # First, check if rule exists
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'}
        )

        if 'Item' not in response:
            print(f"❌ Error: Rule {rule_id} not found for organization {org_id}")
            return False

        existing_rule = response['Item']
        print(f"Found existing rule: {existing_rule.get('name')}")

    except Exception as e:
        print(f"❌ Error checking existing rule: {str(e)}")
        return False

    # Get version
    version = rule_data.get('version', existing_rule.get('version', '1.0.0'))

    # Build updated item
    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'RULE#{rule_id}',
        'gsi1pk': 'RULE',
        'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
        'gsi2pk': f'ORG#{org_id}#VERSION#{version}',
        'gsi2sk': f'RULE#{rule_id}',
        'rule_id': rule_id,
        'name': rule_data.get('name', existing_rule.get('name')),
        'category': rule_data.get('category', existing_rule.get('category')),
        'description': rule_data.get('description', existing_rule.get('description', '')),
        'enabled': rule_data.get('enabled', existing_rule.get('enabled', True)),
        'type': rule_data.get('type', existing_rule.get('type', 'llm')),
        'version': version,
        'llm_config': rule_data.get('llm_config', existing_rule.get('llm_config')),
        'messages': rule_data.get('messages', existing_rule.get('messages')),
        'created_at': existing_rule.get('created_at', datetime.utcnow().isoformat() + 'Z'),
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    table.put_item(Item=item)

    print(f"✓ Updated rule {rule_id}: {item['name']}")
    print(f"  Organization: {org_id}")
    print(f"  Category: {item['category']}")
    print(f"  Type: {item['type']}")
    print(f"  Enabled: {item['enabled']}")
    print(f"  Version: {version}")

    return True


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: ./update-rule.py <org-id> <rule-id> <rule-json-file>")
        print("")
        print("Example: ./update-rule.py community-health 13 rules/updated-rule-13.json")
        sys.exit(1)

    org_id = sys.argv[1]
    rule_id = sys.argv[2]
    rule_file = sys.argv[3]

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

    # Update rule
    try:
        success = update_rule(org_id, rule_id, rule_data)
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error updating rule: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
