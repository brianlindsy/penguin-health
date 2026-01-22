#!/usr/bin/env python3
"""
Migrate existing organization configuration from JSON files to DynamoDB

This script:
1. Reads config/rules/{org_id}.json
2. Reads config/irp/{org_id}.json
3. Writes to penguin-health-org-config DynamoDB table
"""

import json
import boto3
import sys
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

def migrate_rules(org_id, rules_file):
    """Migrate rules from JSON file to DynamoDB"""
    if not Path(rules_file).exists():
        print(f"  ⚠️  Rules file not found: {rules_file}")
        return 0

    with open(rules_file) as f:
        config = json.load(f)

    version = config.get('version', '1.0.0')
    rules = config.get('rules', [])

    count = 0
    for rule in rules:
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
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'updated_at': datetime.utcnow().isoformat() + 'Z'
        }

        table.put_item(Item=item)
        count += 1
        print(f"  ✓ Migrated rule {rule['id']}: {rule['name']}")

    print(f"✓ Migrated {count} rules for {org_id}")
    return count

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

def migrate_org(org_id, org_name, base_path='config'):
    """Migrate complete organization configuration"""
    print(f"\n{'='*60}")
    print(f"Migrating organization: {org_name} ({org_id})")
    print(f"{'='*60}")

    # Migrate organization metadata
    migrate_organization(org_id, org_name)

    # Migrate rules
    rules_file = f'{base_path}/rules/{org_id}.json'
    rules_count = migrate_rules(org_id, rules_file)

    # Migrate IRP config
    irp_file = f'{base_path}/irp/{org_id}.json'
    migrate_irp_config(org_id, irp_file)

    print(f"\n✓ Migration complete for {org_id}")
    print(f"  - Organization metadata: Created")
    print(f"  - Rules: {rules_count} migrated")
    print(f"  - IRP config: Migrated")

    return True

if __name__ == '__main__':
    print("╔════════════════════════════════════════════════════════╗")
    print("║   Migrate Configuration to DynamoDB                    ║")
    print("╚════════════════════════════════════════════════════════╝")
    print("")

    # Check if table exists
    try:
        table.table_status
    except Exception as e:
        print("❌ Error: Table 'penguin-health-org-config' does not exist")
        print("Run: ./scripts/create-org-config-table.sh first")
        sys.exit(1)

    # Migrate existing organization (update org-id and name as needed)
    try:
        # Replace 'example-org' and 'Example Organization' with your actual values
        migrate_org('example-org', 'Example Organization')

        print("\n" + "="*60)
        print("✅ Migration Complete!")
        print("="*60)
        print("\nNext steps:")
        print("1. Deploy updated rules-engine-rag Lambda")
        print("2. Test with existing organization setup")
        print("3. Create new organizations with: ./scripts/create-organization.sh")

    except Exception as e:
        print(f"\n❌ Migration failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
