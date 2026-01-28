#!/usr/bin/env python3
"""
Migrate existing organization configuration from JSON files to DynamoDB

This script:
1. Reads config/rules/{org_id}.json
2. Reads config/irp/{org_id}.json
3. Writes to penguin-health-org-config DynamoDB table

Usage:
  # Migrate all config for an org (creates new or overwrites)
  ./migrate-config-to-dynamodb.py --org-id example-org --org-name "Example Organization"

  # Update a specific rule by ID
  ./migrate-config-to-dynamodb.py --org-id example-org --update-rule rule-001

  # Update multiple rules by ID
  ./migrate-config-to-dynamodb.py --org-id example-org --update-rule rule-001 --update-rule rule-002

  # List existing rules for an org
  ./migrate-config-to-dynamodb.py --org-id example-org --list-rules

  # Migrate only rules (skip org metadata and IRP)
  ./migrate-config-to-dynamodb.py --org-id example-org --rules-only
"""

import json
import boto3
import sys
import argparse
from datetime import datetime
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def migrate_organization(org_id, org_name, s3_bucket_name=None):
    """Create organization metadata record"""
    if not s3_bucket_name:
        s3_bucket_name = f'penguin-health-{org_id}'

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'METADATA',
        'gsi1pk': 'ORG_METADATA',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'organization_name': org_name,
        'display_name': org_name,
        'enabled': True,
        's3_bucket_name': s3_bucket_name,
        'created_at': datetime.utcnow().isoformat() + 'Z',
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    table.put_item(Item=item)
    print(f"✓ Created org metadata: {org_id}")
    return item


def migrate_rules_config(org_id, rules_file):
    """
    Migrate field_mappings from rules config to DynamoDB as RULES_CONFIG

    This stores the top-level field_mappings (document_id, consumer_name, etc.)
    separately from individual rules.
    """
    if not Path(rules_file).exists():
        print(f"  ⚠️  Rules file not found: {rules_file}")
        return False

    with open(rules_file) as f:
        config = json.load(f)

    field_mappings = config.get('field_mappings', {})
    if not field_mappings:
        print(f"  ⚠️  No field_mappings found in rules config for {org_id}")
        return False

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'RULES_CONFIG',
        'gsi1pk': 'RULES_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'field_mappings': field_mappings,
        'version': config.get('version', '1.0.0'),
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    table.put_item(Item=item)
    print(f"✓ Migrated rules config for {org_id} with field_mappings: {list(field_mappings.keys())}")
    return True


def migrate_rules(org_id, rules_file, rule_ids_to_update=None):
    """
    Migrate rules from JSON file to DynamoDB

    Args:
        org_id: Organization ID
        rules_file: Path to rules JSON file
        rule_ids_to_update: Optional list of specific rule IDs to update.
                           If None, all rules are migrated.
    """
    if not Path(rules_file).exists():
        print(f"  ⚠️  Rules file not found: {rules_file}")
        return 0

    with open(rules_file) as f:
        config = json.load(f)

    version = config.get('version', '1.0.0')
    rules = config.get('rules', [])

    # Filter rules if specific IDs provided
    if rule_ids_to_update:
        rules = [r for r in rules if r['id'] in rule_ids_to_update]
        if not rules:
            print(f"  ⚠️  No matching rules found for IDs: {rule_ids_to_update}")
            return 0

    count = 0
    for rule in rules:
        # Check if rule exists
        existing = get_rule(org_id, rule['id'])

        item = {
            'pk': f'ORG#{org_id}',
            'sk': f'RULE#{rule["id"]}',
            'gsi1pk': 'RULE',
            'gsi1sk': f'ORG#{org_id}#RULE#{rule["id"]}',
            'gsi2pk': f'ORG#{org_id}#VERSION#{version}',
            'gsi2sk': f'RULE#{rule["id"]}',
            'rule_id': rule['id'],
            'name': rule['name'],
            'category': rule['category'],
            'description': rule.get('description', ''),
            'enabled': rule.get('enabled', True),
            'type': rule.get('type', 'llm'),
            'version': version,
            'llm_config': rule['llm_config'],
            'messages': rule['messages'],
            'updated_at': datetime.utcnow().isoformat() + 'Z'
        }

        # Preserve created_at if updating existing rule
        if existing:
            item['created_at'] = existing.get('created_at', datetime.utcnow().isoformat() + 'Z')
            action = "Updated"
        else:
            item['created_at'] = datetime.utcnow().isoformat() + 'Z'
            action = "Created"

        table.put_item(Item=item)
        count += 1
        print(f"  ✓ {action} rule {rule['id']}: {rule['name']}")

    print(f"✓ Processed {count} rules for {org_id}")
    return count


def get_rule(org_id, rule_id):
    """Get a specific rule from DynamoDB"""
    try:
        response = table.get_item(
            Key={
                'pk': f'ORG#{org_id}',
                'sk': f'RULE#{rule_id}'
            }
        )
        return response.get('Item')
    except Exception as e:
        print(f"  Warning: Could not check existing rule: {e}")
        return None


def list_rules(org_id):
    """List all rules for an organization"""
    try:
        response = table.query(
            KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': f'ORG#{org_id}',
                ':sk_prefix': 'RULE#'
            }
        )

        rules = response.get('Items', [])

        if not rules:
            print(f"No rules found for organization: {org_id}")
            return []

        print(f"\nRules for {org_id}:")
        print("-" * 80)
        print(f"{'ID':<20} {'Name':<35} {'Type':<10} {'Enabled':<8}")
        print("-" * 80)

        for rule in sorted(rules, key=lambda x: x.get('rule_id', '')):
            rule_id = rule.get('rule_id', 'N/A')
            name = rule.get('name', 'N/A')[:33]
            rule_type = rule.get('type', 'llm')
            enabled = '✓' if rule.get('enabled', True) else '✗'
            print(f"{rule_id:<20} {name:<35} {rule_type:<10} {enabled:<8}")

        print("-" * 80)
        print(f"Total: {len(rules)} rules")

        return rules

    except Exception as e:
        print(f"Error listing rules: {e}")
        return []


def migrate_irp_config(org_id, irp_file):
    """Migrate IRP config to DynamoDB"""
    if not Path(irp_file).exists():
        print(f"  ⚠️  IRP config file not found: {irp_file}")
        return False

    with open(irp_file) as f:
        config = json.load(f)

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'IRP_CONFIG',
        'gsi1pk': 'IRP_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'field_mappings': config.get('field_mappings', {}),
        'text_patterns': config.get('text_patterns', {}),
        'version': '1.0.0',
        'updated_at': datetime.utcnow().isoformat() + 'Z'
    }

    table.put_item(Item=item)
    print(f"✓ Migrated IRP config for {org_id}")
    return True


def migrate_org(org_id, org_name, base_path='config', rules_only=False):
    """Migrate complete organization configuration"""
    print(f"\n{'='*60}")
    print(f"Migrating organization: {org_name} ({org_id})")
    print(f"{'='*60}")

    if not rules_only:
        # Migrate organization metadata
        migrate_organization(org_id, org_name)

    # Migrate rules and rules config (field_mappings)
    rules_file = f'{base_path}/rules/{org_id}.json'
    migrate_rules_config(org_id, rules_file)
    rules_count = migrate_rules(org_id, rules_file)

    if not rules_only:
        # Migrate IRP config
        irp_file = f'{base_path}/irp/{org_id}.json'
        migrate_irp_config(org_id, irp_file)

    print(f"\n✓ Migration complete for {org_id}")
    if not rules_only:
        print(f"  - Organization metadata: Created")
    print(f"  - Rules config (field_mappings): Migrated")
    print(f"  - Rules: {rules_count} migrated")
    if not rules_only:
        print(f"  - IRP config: Migrated")

    return True


def update_rules(org_id, rule_ids, base_path='config'):
    """Update specific rules by ID"""
    print(f"\n{'='*60}")
    print(f"Updating rules for organization: {org_id}")
    print(f"Rule IDs: {', '.join(rule_ids)}")
    print(f"{'='*60}")

    rules_file = f'{base_path}/rules/{org_id}.json'
    rules_count = migrate_rules(org_id, rules_file, rule_ids_to_update=rule_ids)

    print(f"\n✓ Update complete")
    print(f"  - Rules updated: {rules_count}")

    return rules_count > 0


def main():
    parser = argparse.ArgumentParser(
        description='Migrate organization configuration to DynamoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate all config for an org
  %(prog)s --org-id example-org --org-name "Example Organization"

  # Update a specific rule by ID
  %(prog)s --org-id example-org --update-rule rule-001

  # Update multiple rules
  %(prog)s --org-id example-org --update-rule rule-001 --update-rule rule-002

  # List existing rules for an org
  %(prog)s --org-id example-org --list-rules

  # Migrate only rules (skip org metadata and IRP)
  %(prog)s --org-id example-org --rules-only
        """
    )

    parser.add_argument('--org-id', required=True, help='Organization ID')
    parser.add_argument('--org-name', help='Organization display name (required for full migration)')
    parser.add_argument('--update-rule', action='append', dest='update_rules',
                        help='Update specific rule by ID (can be used multiple times)')
    parser.add_argument('--list-rules', action='store_true', help='List all rules for the org')
    parser.add_argument('--rules-only', action='store_true',
                        help='Migrate only rules (skip org metadata and IRP config)')
    parser.add_argument('--base-path', default='config', help='Base path for config files')

    args = parser.parse_args()

    print("╔════════════════════════════════════════════════════════╗")
    print("║   Migrate Configuration to DynamoDB                    ║")
    print("╚════════════════════════════════════════════════════════╝")
    print("")

    # Check if table exists
    try:
        table.table_status
    except Exception as e:
        print("❌ Error: Table 'penguin-health-org-config' does not exist")
        print("Run: ./scripts/multi-org/create-org-config-table.sh first")
        sys.exit(1)

    try:
        # List rules mode
        if args.list_rules:
            list_rules(args.org_id)
            return

        # Update specific rules mode
        if args.update_rules:
            success = update_rules(args.org_id, args.update_rules, args.base_path)
            if not success:
                sys.exit(1)
            return

        # Full migration mode
        if not args.org_name and not args.rules_only:
            print("❌ Error: --org-name is required for full migration")
            print("Use --rules-only to migrate only rules without org metadata")
            sys.exit(1)

        org_name = args.org_name or args.org_id
        migrate_org(args.org_id, org_name, args.base_path, args.rules_only)

        print("\n" + "="*60)
        print("✅ Migration Complete!")
        print("="*60)
        print("\nNext steps:")
        print("1. Deploy updated rules-engine-rag Lambda")
        print("2. Test with existing organization setup")
        print("3. Create new organizations with: ./scripts/multi-org/create-organization.sh")

    except Exception as e:
        print(f"\n❌ Migration failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
