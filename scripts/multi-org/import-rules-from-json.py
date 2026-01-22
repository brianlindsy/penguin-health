#!/usr/bin/env python3
"""
Import validation rules from a JSON config file to DynamoDB

Usage:
    ./scripts/multi-org/import-rules-from-json.py <org-id> <json-file>

Example:
    ./scripts/multi-org/import-rules-from-json.py community-health config/rules/community-health.json
"""

import boto3
import json
import sys
from datetime import datetime
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def import_rules(org_id, rules_data):
    """Import all rules from JSON file to DynamoDB"""

    rules = rules_data.get('rules', [])

    if not rules:
        print("❌ No rules found in JSON file")
        return False

    print(f"Found {len(rules)} rule(s) to import for {org_id}\n")

    success_count = 0
    error_count = 0

    for rule in rules:
        try:
            rule_id = rule['id']
            version = rules_data.get('version', '1.0.0')

            # Build DynamoDB item
            item = {
                'pk': f'ORG#{org_id}',
                'sk': f'RULE#{rule_id}',
                'gsi1pk': 'RULE',
                'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
                'gsi2pk': f'ORG#{org_id}#VERSION#{version}',
                'gsi2sk': f'RULE#{rule_id}',
                'rule_id': rule_id,
                'name': rule['name'],
                'category': rule['category'],
                'description': rule.get('description', ''),
                'enabled': rule.get('enabled', True),
                'type': rule.get('type', 'llm'),
                'version': version,
                'llm_config': rule['llm_config'],
                'messages': rule['messages'],
                'created_at': rules_data.get('created_at', datetime.utcnow().isoformat() + 'Z'),
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }

            # Write to DynamoDB
            table.put_item(Item=item)

            print(f"✓ Imported rule {rule_id}: {rule['name']}")
            success_count += 1

        except Exception as e:
            print(f"✗ Failed to import rule {rule.get('id', 'unknown')}: {str(e)}")
            error_count += 1

    print(f"\n{'='*80}")
    print(f"Import Summary:")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {error_count}")
    print(f"  Total: {len(rules)}")
    print(f"{'='*80}")

    return error_count == 0


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: ./import-rules-from-json.py <org-id> <json-file>")
        print("")
        print("Example: ./import-rules-from-json.py community-health config/rules/community-health.json")
        sys.exit(1)

    org_id = sys.argv[1]
    json_file = sys.argv[2]

    # Check if file exists
    if not Path(json_file).exists():
        print(f"❌ Error: File not found: {json_file}")
        sys.exit(1)

    # Load JSON data
    try:
        with open(json_file) as f:
            rules_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {json_file}")
        print(f"   {str(e)}")
        sys.exit(1)

    # Verify org_id matches
    json_org_id = rules_data.get('organization_id')
    if json_org_id and json_org_id != org_id:
        print(f"⚠️  Warning: JSON org_id '{json_org_id}' doesn't match provided org_id '{org_id}'")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting.")
            sys.exit(1)

    # Import rules
    try:
        success = import_rules(org_id, rules_data)
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error importing rules: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
