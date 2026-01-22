#!/usr/bin/env python3
"""
List all organizations in the system

Usage:
    ./scripts/list-organizations.py
"""

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def list_organizations():
    """List all organizations from DynamoDB"""
    try:
        response = table.query(
            IndexName='GSI1',
            KeyConditionExpression=Key('gsi1pk').eq('ORG_METADATA')
        )

        orgs = response['Items']

        if not orgs:
            print("No organizations found")
            return []

        print(f"Found {len(orgs)} organization(s):\n")
        print(f"{'ID':<30} {'Name':<40} {'Enabled':<10} {'S3 Bucket'}")
        print("-" * 120)

        for org in sorted(orgs, key=lambda x: x['organization_id']):
            org_id = org['organization_id']
            org_name = org.get('organization_name', 'N/A')
            enabled = '✓ Yes' if org.get('enabled', False) else '✗ No'
            bucket = org.get('s3_bucket_name', 'N/A')

            print(f"{org_id:<30} {org_name:<40} {enabled:<10} {bucket}")

        return orgs

    except Exception as e:
        print(f"❌ Error listing organizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


if __name__ == '__main__':
    orgs = list_organizations()

    if orgs:
        print(f"\nTo view rules for an organization:")
        print(f"  ./scripts/list-rules.py <org-id>")
