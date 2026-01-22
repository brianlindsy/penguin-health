"""
Multi-Organization Configuration Utilities

This module provides functions to load organization-specific configuration
from DynamoDB for the penguin-health multi-org system.

Functions:
- get_organization(org_id) - Get org metadata (cached)
- load_org_rules(org_id) - Load validation rules from DynamoDB
- load_irp_config(org_id) - Load IRP configuration
- load_chart_config(org_id) - Load chart processing configuration
- extract_org_id_from_bucket(bucket_name) - Parse org ID from S3 bucket
- build_env_config(org_id) - Build Lambda env_config dict
"""

import boto3
from functools import lru_cache
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


@lru_cache(maxsize=100)
def get_organization(org_id):
    """
    Get organization metadata from DynamoDB (cached)

    Args:
        org_id (str): Organization identifier (e.g., 'community-health')

    Returns:
        dict: Organization metadata including s3_bucket_name, enabled status, etc.

    Raises:
        ValueError: If organization not found or disabled
    """
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'METADATA'}
        )

        if 'Item' not in response:
            raise ValueError(f"Organization '{org_id}' not found in registry")

        org = response['Item']

        if not org.get('enabled', False):
            raise ValueError(f"Organization '{org_id}' is disabled")

        print(f"Loaded organization: {org.get('organization_name')} ({org_id})")
        return org

    except Exception as e:
        print(f"Error loading organization '{org_id}': {str(e)}")
        raise


def load_org_rules(org_id):
    """
    Load all validation rules for an organization from DynamoDB

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: Configuration dict with 'rules' list and 'organization_id'

    Example return:
        {
            'rules': [
                {
                    'rule_id': '13',
                    'name': 'Recipient vs. Contact Method',
                    'enabled': True,
                    'type': 'llm',
                    'llm_config': {...},
                    'messages': {...}
                },
                ...
            ],
            'organization_id': 'example-org'
        }
    """
    try:
        # Query all rules for this organization
        response = table.query(
            KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RULE#')
        )

        rules = response['Items']

        # Filter to only enabled rules
        enabled_rules = [rule for rule in rules if rule.get('enabled', True)]

        print(f"Loaded {len(enabled_rules)} enabled rules for {org_id} (of {len(rules)} total)")

        return {
            'rules': enabled_rules,
            'organization_id': org_id
        }

    except Exception as e:
        print(f"Error loading rules for '{org_id}': {str(e)}")
        raise


def load_irp_config(org_id):
    """
    Load IRP configuration for an organization

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: IRP config with field_mappings and text_patterns, or None if not found

    Example return:
        {
            'organization_id': 'example-org',
            'field_mappings': {
                'consumer_name': 'Consumer Name:',
                'document_id': 'Consumer Service ID:'
            },
            'text_patterns': {
                'consumer_name': 'Consumer Name:\\s*(.+)'
            }
        }
    """
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'IRP_CONFIG'}
        )

        if 'Item' not in response:
            print(f"No IRP config found for {org_id}")
            return None

        irp_config = response['Item']
        print(f"Loaded IRP config for {org_id}")
        return irp_config

    except Exception as e:
        print(f"Error loading IRP config for '{org_id}': {str(e)}")
        return None


def load_chart_config(org_id):
    """
    Load chart processing configuration for an organization

    This configuration controls how textract results are processed,
    including encounter splitting delimiters and folder paths.

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: Chart config with encounter_delimiter, folder paths, etc.

    Example return:
        {
            'organization_id': 'example-org',
            'encounter_delimiter': 'Consumer Service ID:',
            'encounter_id_field': 'Consumer Service ID:',
            'irp_folder_pattern': 'irp/',
            'folders': {
                'raw_charts': 'textract-raw/',
                'raw_irp': 'textract-raw/irp/',
                'archive_charts': 'archived/textract/',
                'archive_irp': 'archived/irp/textract/'
            }
        }
    """
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'CHART_CONFIG'}
        )

        if 'Item' in response:
            chart_config = response['Item']
            print(f"Loaded chart config for {org_id}")
            return chart_config

        # Return default config if not found
        print(f"No chart config found for {org_id}, using defaults")
        return {
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
        }

    except Exception as e:
        print(f"Error loading chart config for '{org_id}': {str(e)}")
        # Return defaults on error to prevent processing failures
        return {
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
        }


def extract_org_id_from_bucket(bucket_name):
    """
    Extract organization ID from S3 bucket name

    Expected bucket naming format: penguin-health-{org-id}

    Args:
        bucket_name (str): S3 bucket name (e.g., 'penguin-health-example-org')

    Returns:
        str: Organization ID (e.g., 'example-org')

    Raises:
        ValueError: If bucket name doesn't match expected format
    """
    prefix = 'penguin-health-'

    if not bucket_name.startswith(prefix):
        raise ValueError(
            f"Invalid bucket name: '{bucket_name}'. "
            f"Expected format: {prefix}{{org-id}}"
        )

    org_id = bucket_name.replace(prefix, '')

    if not org_id:
        raise ValueError(f"Could not extract org_id from bucket: '{bucket_name}'")

    return org_id


def build_env_config(org_id):
    """
    Build Lambda env_config dict from organization metadata

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: Environment configuration for Lambda function

    Example return:
        {
            'ORGANIZATION_ID': 'example-org',
            'BUCKET_NAME': 'penguin-health-example-org',
            'DYNAMODB_TABLE': 'penguin-health-validation-results',
            'DYNAMODB_IRP_TABLE': 'penguin-health-irp',
            'TEXTRACT_PROCESSED': 'textract-processed/'
        }
    """
    org = get_organization(org_id)

    env_config = {
        'ORGANIZATION_ID': org_id,
        'BUCKET_NAME': org['s3_bucket_name'],
        'DYNAMODB_TABLE': 'penguin-health-validation-results',
        'DYNAMODB_IRP_TABLE': 'penguin-health-irp',
        'TEXTRACT_PROCESSED': 'textract-processed/'
    }

    print(f"Built env_config for {org_id}: bucket={env_config['BUCKET_NAME']}")
    return env_config


def list_all_organizations():
    """
    List all organizations in the system

    Returns:
        list: List of organization metadata dicts

    Example return:
        [
            {
                'organization_id': 'example-org',
                'organization_name': 'Example Organization',
                'enabled': True,
                's3_bucket_name': 'penguin-health-example-org'
            },
            ...
        ]
    """
    try:
        response = table.query(
            IndexName='GSI1',
            KeyConditionExpression=Key('gsi1pk').eq('ORG_METADATA')
        )

        orgs = response['Items']
        print(f"Found {len(orgs)} organizations")
        return orgs

    except Exception as e:
        print(f"Error listing organizations: {str(e)}")
        raise
