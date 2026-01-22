#!/usr/bin/env python3
"""
Add default CHART_CONFIG to existing organizations

Usage:
    ./scripts/multi-org/add-chart-config.py <org-id>
    ./scripts/multi-org/add-chart-config.py --all

Examples:
    ./scripts/multi-org/add-chart-config.py community-health
    ./scripts/multi-org/add-chart-config.py --all
"""

import boto3
import sys
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def add_chart_config(org_id):
    """Add default chart configuration for an organization"""

    # Check if chart config already exists
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'CHART_CONFIG'}
        )

        if 'Item' in response:
            print(f"⚠️  Chart config already exists for {org_id}")
            return False

    except Exception as e:
        print(f"❌ Error checking existing config: {str(e)}")
        return False

    # Create default chart configuration
    try:
        table.put_item(Item={
            'pk': f'ORG#{org_id}',
            'sk': 'CHART_CONFIG',
            'gsi1pk': 'CHART_CONFIG',
            'gsi1sk': f'ORG#{org_id}',
            'organization_id': org_id,
            'encounter_delimiter': 'Consumer Service ID:',
            'encounter_id_field': 'Consumer Service ID:',
            'irp_folder_pattern': 'irp/',
            'folders': {
                'raw_charts': 'textract-raw/',
                'raw_irp': 'textract-raw/irp/',
                'archive_charts': 'archived/textract/',
                'archive_irp': 'archived/irp/textract/'
            },
            'version': '1.0.0'
        })

        print(f"✓ Added default chart config for {org_id}")
        return True

    except Exception as e:
        print(f"❌ Error adding chart config for {org_id}: {str(e)}")
        return False


def list_all_organizations():
    """List all organizations from DynamoDB"""
    try:
        response = table.query(
            IndexName='GSI1',
            KeyConditionExpression=Key('gsi1pk').eq('ORG_METADATA')
        )

        orgs = response['Items']
        return [org['organization_id'] for org in orgs]

    except Exception as e:
        print(f"❌ Error listing organizations: {str(e)}")
        return []


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: ./add-chart-config.py <org-id>")
        print("       ./add-chart-config.py --all")
        print("")
        print("Examples:")
        print("  ./add-chart-config.py community-health")
        print("  ./add-chart-config.py --all")
        sys.exit(1)

    if sys.argv[1] == '--all':
        # Add chart config to all organizations
        org_ids = list_all_organizations()

        if not org_ids:
            print("No organizations found")
            sys.exit(1)

        print(f"Found {len(org_ids)} organization(s)\n")

        success_count = 0
        skip_count = 0

        for org_id in org_ids:
            result = add_chart_config(org_id)
            if result:
                success_count += 1
            else:
                skip_count += 1

        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Added: {success_count}")
        print(f"  Skipped: {skip_count}")
        print(f"  Total: {len(org_ids)}")
        print(f"{'='*60}")

    else:
        # Add chart config to specific organization
        org_id = sys.argv[1]

        try:
            add_chart_config(org_id)
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
