#!/usr/bin/env python3
"""
Update chart processing configuration for an organization

Usage:
    ./scripts/multi-org/update-chart-config.py <org-id> <config-json-file>

Example:
    ./scripts/multi-org/update-chart-config.py community-health chart-config.json
"""

import boto3
import json
import sys
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def update_chart_config(org_id, config_data):
    """Update chart configuration for an organization"""

    # Check if config exists
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'CHART_CONFIG'}
        )

        if 'Item' not in response:
            print(f"❌ No chart config found for {org_id}")
            print(f"   Run: ./scripts/multi-org/add-chart-config.py {org_id}")
            return False

        existing_config = response['Item']
        print(f"Found existing chart config for {org_id}")

    except Exception as e:
        print(f"❌ Error checking existing config: {str(e)}")
        return False

    # Build updated configuration
    updated_config = {
        'pk': f'ORG#{org_id}',
        'sk': 'CHART_CONFIG',
        'gsi1pk': 'CHART_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'encounter_delimiter': config_data.get('encounter_delimiter', existing_config.get('encounter_delimiter', 'Consumer Service ID:')),
        'encounter_id_field': config_data.get('encounter_id_field', existing_config.get('encounter_id_field', 'Consumer Service ID:')),
        'irp_folder_pattern': config_data.get('irp_folder_pattern', existing_config.get('irp_folder_pattern', 'irp/')),
        'folders': config_data.get('folders', existing_config.get('folders', {
            'raw_charts': 'textract-raw/',
            'raw_irp': 'textract-raw/irp/',
            'archive_charts': 'archived/textract/',
            'archive_irp': 'archived/irp/textract/'
        })),
        'version': config_data.get('version', existing_config.get('version', '1.0.0'))
    }

    # Update in DynamoDB
    try:
        table.put_item(Item=updated_config)

        print(f"\n✓ Updated chart config for {org_id}")
        print(f"  Encounter delimiter: '{updated_config['encounter_delimiter']}'")
        print(f"  Encounter ID field: '{updated_config['encounter_id_field']}'")
        print(f"  IRP folder pattern: '{updated_config['irp_folder_pattern']}'")
        print(f"  Version: {updated_config['version']}")

        return True

    except Exception as e:
        print(f"❌ Error updating chart config: {str(e)}")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: ./update-chart-config.py <org-id> <config-json-file>")
        print("")
        print("Example: ./update-chart-config.py community-health chart-config.json")
        print("")
        print("Example config file:")
        print(json.dumps({
            'encounter_delimiter': 'Client ID:',
            'encounter_id_field': 'Client ID:',
            'version': '1.1.0'
        }, indent=2))
        sys.exit(1)

    org_id = sys.argv[1]
    config_file = sys.argv[2]

    # Check if file exists
    if not Path(config_file).exists():
        print(f"❌ Error: File not found: {config_file}")
        sys.exit(1)

    # Load config data
    try:
        with open(config_file) as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {config_file}")
        print(f"   {str(e)}")
        sys.exit(1)

    # Update configuration
    try:
        success = update_chart_config(org_id, config_data)
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
