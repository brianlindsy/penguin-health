"""
Admin API Lambda - CRUD operations for organization configuration

Handles API Gateway HTTP API v2 events with route-based dispatch.
Reuses multi_org_config.py for DynamoDB reads where possible.
"""

import json
import boto3
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')


def lambda_handler(event, context):
    """Route API Gateway HTTP API v2 events to handlers"""
    route_key = event.get('routeKey', '')
    path_params = event.get('pathParameters', {}) or {}
    body = event.get('body')

    if body:
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            return response(400, {'error': 'Invalid JSON body'})

    routes = {
        'GET /api/organizations': list_organizations,
        'GET /api/organizations/{orgId}': get_organization,
        'GET /api/organizations/{orgId}/rules': list_rules,
        'GET /api/organizations/{orgId}/rules/{ruleId}': get_rule,
        'PUT /api/organizations/{orgId}/rules/{ruleId}': update_rule,
        'POST /api/organizations/{orgId}/rules': create_rule,
        'GET /api/organizations/{orgId}/rules-config': get_rules_config,
        'PUT /api/organizations/{orgId}/rules-config': update_rules_config,
    }

    handler = routes.get(route_key)
    if not handler:
        return response(404, {'error': f'Route not found: {route_key}'})

    try:
        return handler(path_params=path_params, body=body)
    except Exception as e:
        print(f"Error handling {route_key}: {str(e)}")
        return response(500, {'error': str(e)})


# ---- Organizations ----

def list_organizations(**kwargs):
    """List all organizations"""
    result = table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq('ORG_METADATA')
    )

    orgs = []
    for item in result.get('Items', []):
        orgs.append({
            'organization_id': item.get('organization_id'),
            'organization_name': item.get('organization_name'),
            'enabled': item.get('enabled', False),
            's3_bucket_name': item.get('s3_bucket_name'),
            'created_at': item.get('created_at'),
            'updated_at': item.get('updated_at'),
        })

    return response(200, {'organizations': orgs})


def get_organization(path_params, **kwargs):
    """Get organization detail"""
    org_id = path_params.get('orgId')

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'METADATA'}
    )

    if 'Item' not in result:
        return response(404, {'error': f'Organization not found: {org_id}'})

    item = result['Item']
    return response(200, {
        'organization_id': item.get('organization_id'),
        'organization_name': item.get('organization_name'),
        'display_name': item.get('display_name'),
        'enabled': item.get('enabled', False),
        's3_bucket_name': item.get('s3_bucket_name'),
        'created_at': item.get('created_at'),
        'updated_at': item.get('updated_at'),
    })


# ---- Rules ----

def list_rules(path_params, **kwargs):
    """List all rules for an organization"""
    org_id = path_params.get('orgId')

    result = table.query(
        KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RULE#')
    )

    rules = []
    for item in result.get('Items', []):
        rules.append(format_rule(item))

    return response(200, {'rules': rules, 'count': len(rules)})


def get_rule(path_params, **kwargs):
    """Get a single rule"""
    org_id = path_params.get('orgId')
    rule_id = path_params.get('ruleId')

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'}
    )

    if 'Item' not in result:
        return response(404, {'error': f'Rule not found: {rule_id}'})

    return response(200, format_rule(result['Item']))


def create_rule(path_params, body, **kwargs):
    """Create a new rule"""
    org_id = path_params.get('orgId')

    if not body:
        return response(400, {'error': 'Request body required'})

    # Validate required fields (new flat schema - no llm_config)
    required = ['id', 'name', 'category', 'rule_text']
    missing = [f for f in required if f not in body]
    if missing:
        return response(400, {'error': f'Missing required fields: {missing}'})

    rule_id = body['id']

    # Check if rule already exists
    existing = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'}
    )
    if 'Item' in existing:
        return response(409, {'error': f'Rule {rule_id} already exists. Use PUT to update.'})

    version = body.get('version', '1.0.0')
    now = datetime.utcnow().isoformat() + 'Z'

    # New flat schema - no llm_config, no GSI2
    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'RULE#{rule_id}',
        'gsi1pk': 'RULE',
        'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
        'rule_id': rule_id,
        'name': body['name'],
        'category': body['category'],
        'description': body.get('description', ''),
        'enabled': body.get('enabled', True),
        'type': body.get('type', 'llm'),
        'version': version,
        'rule_text': body['rule_text'],
        'fields_to_extract': body.get('fields_to_extract', []),
        'notes': body.get('notes', []),
        'created_at': now,
        'updated_at': now,
    }

    table.put_item(Item=item)
    print(f"Created rule {rule_id} for {org_id}")

    return response(201, format_rule(item))


def update_rule(path_params, body, **kwargs):
    """Update an existing rule"""
    org_id = path_params.get('orgId')
    rule_id = path_params.get('ruleId')

    if not body:
        return response(400, {'error': 'Request body required'})

    # Get existing rule
    existing_result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'}
    )

    if 'Item' not in existing_result:
        return response(404, {'error': f'Rule not found: {rule_id}'})

    existing = existing_result['Item']
    version = body.get('version', existing.get('version', '1.0.0'))

    # New flat schema - no llm_config, no GSI2
    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'RULE#{rule_id}',
        'gsi1pk': 'RULE',
        'gsi1sk': f'ORG#{org_id}#RULE#{rule_id}',
        'rule_id': rule_id,
        'name': body.get('name', existing.get('name')),
        'category': body.get('category', existing.get('category')),
        'description': body.get('description', existing.get('description', '')),
        'enabled': body.get('enabled', existing.get('enabled', True)),
        'type': body.get('type', existing.get('type', 'llm')),
        'version': version,
        'rule_text': body.get('rule_text', existing.get('rule_text', '')),
        'fields_to_extract': body.get('fields_to_extract', existing.get('fields_to_extract', [])),
        'notes': body.get('notes', existing.get('notes', [])),
        'created_at': existing.get('created_at', datetime.utcnow().isoformat() + 'Z'),
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }

    table.put_item(Item=item)
    print(f"Updated rule {rule_id} for {org_id}")

    return response(200, format_rule(item))


# ---- Rules Config (field_mappings) ----

def get_rules_config(path_params, **kwargs):
    """Get rules config (field_mappings) for an organization"""
    org_id = path_params.get('orgId')

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'RULES_CONFIG'}
    )

    if 'Item' not in result:
        return response(200, {
            'organization_id': org_id,
            'field_mappings': {},
        })

    item = result['Item']
    return response(200, {
        'organization_id': item.get('organization_id'),
        'field_mappings': item.get('field_mappings', {}),
        'version': item.get('version'),
        'updated_at': item.get('updated_at'),
    })


def update_rules_config(path_params, body, **kwargs):
    """Update rules config (field_mappings) for an organization"""
    org_id = path_params.get('orgId')

    if not body or 'field_mappings' not in body:
        return response(400, {'error': 'Request body must include field_mappings'})

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'RULES_CONFIG',
        'gsi1pk': 'RULES_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'field_mappings': body['field_mappings'],
        'version': body.get('version', '1.0.0'),
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }

    table.put_item(Item=item)
    print(f"Updated rules config for {org_id}")

    return response(200, {
        'organization_id': org_id,
        'field_mappings': body['field_mappings'],
        'version': item['version'],
        'updated_at': item['updated_at'],
    })


# ---- Helpers ----

def format_rule(item):
    """Format a DynamoDB rule item for API response (new flat schema)"""
    return {
        'rule_id': item.get('rule_id'),
        'name': item.get('name'),
        'category': item.get('category'),
        'description': item.get('description', ''),
        'enabled': item.get('enabled', True),
        'type': item.get('type', 'llm'),
        'version': item.get('version'),
        'rule_text': item.get('rule_text', ''),
        'fields_to_extract': convert_decimals(item.get('fields_to_extract', [])),
        'notes': item.get('notes', []),
        'created_at': item.get('created_at'),
        'updated_at': item.get('updated_at'),
    }


def convert_decimals(obj):
    """Convert Decimal types from DynamoDB to int/float for JSON serialization"""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    return obj


def response(status_code, body):
    """Build API Gateway HTTP API response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        'body': json.dumps(body, default=str),
    }
