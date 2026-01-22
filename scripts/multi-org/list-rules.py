#!/usr/bin/env python3
"""
List all validation rules for an organization

Usage:
    ./scripts/list-rules.py <org-id>

Example:
    ./scripts/list-rules.py community-health
"""

import boto3
import sys
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def list_rules(org_id):
    """List all rules for an organization"""
    try:
        response = table.query(
            KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RULE#')
        )

        rules = response['Items']

        if not rules:
            print(f"No rules found for organization: {org_id}")
            return []

        print(f"Found {len(rules)} rule(s) for {org_id}:\n")
        print(f"{'ID':<6} {'Name':<50} {'Category':<20} {'Type':<10} {'Enabled':<10} {'RAG'}")
        print("-" * 120)

        for rule in sorted(rules, key=lambda x: x['rule_id']):
            rule_id = rule['rule_id']
            name = rule.get('name', 'N/A')[:48]
            category = rule.get('category', 'N/A')[:18]
            rule_type = rule.get('type', 'llm')
            enabled = '✓ Yes' if rule.get('enabled', True) else '✗ No'
            use_rag = '✓' if rule.get('llm_config', {}).get('use_rag', False) else ''

            print(f"{rule_id:<6} {name:<50} {category:<20} {rule_type:<10} {enabled:<10} {use_rag}")

        return rules

    except Exception as e:
        print(f"❌ Error listing rules: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: ./list-rules.py <org-id>")
        print("")
        print("Example: ./list-rules.py community-health")
        sys.exit(1)

    org_id = sys.argv[1]
    rules = list_rules(org_id)

    if rules:
        print(f"\nTo add a new rule:")
        print(f"  ./scripts/add-rule.py {org_id} rule-config.json")
