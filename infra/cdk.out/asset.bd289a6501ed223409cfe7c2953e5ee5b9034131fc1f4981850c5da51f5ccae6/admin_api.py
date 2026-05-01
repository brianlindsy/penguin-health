"""
Admin API Lambda - CRUD operations for organization configuration

Handles API Gateway HTTP API v2 events with route-based dispatch.
Reuses multi_org_config.py for DynamoDB reads where possible.
Includes LLM-enhanced endpoints for rule field extraction and note enhancement.
"""

import json
import re
from typing import Optional
import boto3
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

import permissions as perms_module

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
table = dynamodb.Table('penguin-health-org-config')
validation_results_table = dynamodb.Table('penguin-health-validation-results')

bedrock = boto3.client('bedrock-runtime')

RULES_ENGINE_LAMBDA = 'penguin-health-rules-engine-rag'
MODEL_ID = 'global.anthropic.claude-sonnet-4-5-20250929-v1:0'


# ---- Authorization Helpers ----

def get_user_claims(event):
    """
    Extract user claims from JWT token in request context.

    The JWT authorizer in API Gateway validates the token and populates
    claims in event.requestContext.authorizer.jwt.claims.
    """
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    jwt_claims = authorizer.get('jwt', {}).get('claims', {})

    # cognito:groups comes as a string like "[Admins]" or as a list
    groups = jwt_claims.get('cognito:groups', [])
    if isinstance(groups, str):
        # Parse string format "[Admins, Users]" to list
        groups = [g.strip() for g in groups.strip('[]').split(',') if g.strip()]

    return {
        'email': jwt_claims.get('email'),
        'groups': groups,
        'organization_id': jwt_claims.get('custom:organization_id'),
    }


def is_super_admin(claims):
    """Check if user is in Admins group (super admin)."""
    groups = claims.get('groups', [])
    return 'Admins' in groups


def can_access_org(claims, org_id):
    """Check if user can access the specified organization."""
    if is_super_admin(claims):
        return True
    return claims.get('organization_id') == org_id


def authorize_request(event, org_id=None):
    """
    Authorize request based on JWT claims.

    Args:
        event: API Gateway event with JWT claims in requestContext
        org_id: Optional org ID to check access for

    Returns:
        tuple: (claims_dict, error_response) - error_response is None if authorized
    """
    claims = get_user_claims(event)

    # Check if we have a valid user identity (email or sub)
    # The JWT authorizer already validated the token, so if we got here
    # the request has a valid token. We just need to extract identity.
    request_context = event.get('requestContext', {})
    authorizer = request_context.get('authorizer', {})
    jwt_claims = authorizer.get('jwt', {}).get('claims', {})

    # Use email or sub as identity - sub is always present in valid JWT
    user_identity = claims.get('email') or jwt_claims.get('sub')
    if not user_identity:
        return None, response(401, {'error': 'Unauthorized - no valid user claims'})

    if org_id and not can_access_org(claims, org_id):
        return None, response(403, {'error': 'Access denied to this organization'})

    return claims, None


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
        'POST /api/organizations/{orgId}/rules/enhance-fields': enhance_fields,
        'POST /api/organizations/{orgId}/rules/enhance-note': enhance_note,
        'GET /api/organizations/{orgId}/validation-runs': list_validation_runs,
        'POST /api/organizations/{orgId}/validation-runs': trigger_validation_run,
        'GET /api/organizations/{orgId}/validation-runs/{runId}': get_validation_run,
        'GET /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}': get_validation_result,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/confirm-finding': confirm_finding,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-resolved': mark_resolved,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-incorrect': mark_incorrect,
        'GET /api/me/permissions': get_my_permissions,
        'GET /api/organizations/{orgId}/users': list_org_users,
        'GET /api/organizations/{orgId}/users/{email}': get_org_user,
        'PUT /api/organizations/{orgId}/users/{email}': upsert_org_user,
        'DELETE /api/organizations/{orgId}/users/{email}': delete_org_user,
    }

    handler = routes.get(route_key)
    if not handler:
        return response(404, {'error': f'Route not found: {route_key}'})

    try:
        return handler(event=event, path_params=path_params, body=body)
    except Exception as e:
        print(f"Error handling {route_key}: {str(e)}")
        return response(500, {'error': str(e)})


# ---- Organizations ----

def list_organizations(event, **kwargs):
    """List all organizations (filtered by user's org for non-super-admins)"""
    claims, error = authorize_request(event)
    if error:
        return error

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

    # Filter organizations for non-super-admins
    if not is_super_admin(claims):
        user_org_id = claims.get('organization_id')
        orgs = [o for o in orgs if o.get('organization_id') == user_org_id]

    return response(200, {'organizations': orgs})

def get_organization_by_id(org_id) -> tuple[dict | None, str | None]:
    """Get organization metadata by ID. Returns (org_dict, error_message)."""
    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'METADATA'}
    )

    if 'Item' not in result:
        return None, f'Organization not found: {org_id}'

    item = result['Item']
    return {
        'organization_id': item.get('organization_id'),
        'organization_name': item.get('organization_name'),
        'display_name': item.get('display_name'),
        'enabled': item.get('enabled', False),
        's3_bucket_name': item.get('s3_bucket_name'),
        'created_at': item.get('created_at'),
        'updated_at': item.get('updated_at'),
    }, None


def get_organization(event, path_params, **kwargs):
    """Get organization detail"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

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

def list_rules(event, path_params, **kwargs):
    """List all rules for an organization"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = table.query(
        KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RULE#')
    )

    allowed = perms_module.viewable_categories(claims, org_id)
    rules = []
    for item in result.get('Items', []):
        if item.get('category') not in allowed:
            continue
        rules.append(format_rule(item))

    return response(200, {'rules': rules, 'count': len(rules)})


def get_rule(event, path_params, **kwargs):
    """Get a single rule"""
    org_id = path_params.get('orgId')
    rule_id = path_params.get('ruleId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'}
    )

    if 'Item' not in result:
        return response(404, {'error': f'Rule not found: {rule_id}'})

    item = result['Item']
    if not perms_module.can_view_category(claims, org_id, item.get('category')):
        return response(403, {'error': 'Access denied to this rule'})

    return response(200, format_rule(item))


def create_rule(event, path_params, body, **kwargs):
    """Create a new rule"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not perms_module.is_org_admin(claims, org_id):
        return response(403, {'error': 'Org admin required to manage rules'})

    if not body:
        return response(400, {'error': 'Request body required'})

    # Validate required fields (new flat schema - no llm_config)
    # For deterministic rules, rule_text is optional
    rule_type = body.get('type', 'llm')
    if rule_type == 'deterministic':
        required = ['id', 'name', 'category']
    else:
        required = ['id', 'name', 'category', 'rule_text']
    missing = [f for f in required if f not in body]
    if missing:
        return response(400, {'error': f'Missing required fields: {missing}'})

    if body['category'] not in perms_module.CATEGORIES:
        return response(400, {
            'error': f'Invalid category. Must be one of: {perms_module.CATEGORIES}'
        })

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
        'type': rule_type,
        'version': version,
        'rule_text': body.get('rule_text', ''),
        'fields_to_extract': body.get('fields_to_extract', []),
        'notes': body.get('notes', []),
        # Deterministic rule fields
        'conditions': body.get('conditions', []),
        'conditionals': body.get('conditionals', []),
        'logic': body.get('logic', 'all'),
        'created_at': now,
        'updated_at': now,
    }

    table.put_item(Item=item)
    print(f"Created rule {rule_id} for {org_id}")

    return response(201, format_rule(item))


def update_rule(event, path_params, body, **kwargs):
    """Update an existing rule"""
    org_id = path_params.get('orgId')
    rule_id = path_params.get('ruleId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not perms_module.is_org_admin(claims, org_id):
        return response(403, {'error': 'Org admin required to manage rules'})

    if not body:
        return response(400, {'error': 'Request body required'})

    if 'category' in body and body['category'] not in perms_module.CATEGORIES:
        return response(400, {
            'error': f'Invalid category. Must be one of: {perms_module.CATEGORIES}'
        })

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
        # Deterministic rule fields
        'conditions': body.get('conditions', existing.get('conditions', [])),
        'conditionals': body.get('conditionals', existing.get('conditionals', [])),
        'logic': body.get('logic', existing.get('logic', 'all')),
        'fail_message': body.get('fail_message', existing.get('fail_message', '')),
        'created_at': existing.get('created_at', datetime.utcnow().isoformat() + 'Z'),
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }

    table.put_item(Item=item)
    print(f"Updated rule {rule_id} for {org_id}")

    return response(200, format_rule(item))


# ---- Rules Config (field_mappings) ----

def get_rules_config(event, path_params, **kwargs):
    """Get rules config (field_mappings) for an organization"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'RULES_CONFIG'}
    )

    if 'Item' not in result:
        return response(200, {
            'organization_id': org_id,
            'field_mappings': {},
            'csv_column_mappings': {},
        })

    item = result['Item']
    return response(200, {
        'organization_id': item.get('organization_id'),
        'field_mappings': item.get('field_mappings', {}),
        'csv_column_mappings': item.get('csv_column_mappings', {}),
        'version': item.get('version'),
        'updated_at': item.get('updated_at'),
    })


def update_rules_config(event, path_params, body, **kwargs):
    """Update rules config (field_mappings and csv_column_mappings) for an organization"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body:
        return response(400, {'error': 'Request body required'})

    # At least one of field_mappings or csv_column_mappings should be present
    if 'field_mappings' not in body and 'csv_column_mappings' not in body:
        return response(400, {'error': 'Request body must include field_mappings or csv_column_mappings'})

    # Get existing config to preserve fields not being updated
    existing_result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'RULES_CONFIG'}
    )
    existing = existing_result.get('Item', {})

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'RULES_CONFIG',
        'gsi1pk': 'RULES_CONFIG',
        'gsi1sk': f'ORG#{org_id}',
        'organization_id': org_id,
        'field_mappings': body.get('field_mappings', existing.get('field_mappings', {})),
        'csv_column_mappings': body.get('csv_column_mappings', existing.get('csv_column_mappings', {})),
        'version': body.get('version', '1.0.0'),
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }

    table.put_item(Item=item)
    print(f"Updated rules config for {org_id}")

    return response(200, {
        'organization_id': org_id,
        'field_mappings': item['field_mappings'],
        'csv_column_mappings': item['csv_column_mappings'],
        'version': item['version'],
        'updated_at': item['updated_at'],
    })


# ---- Validation Results ----

def list_validation_runs(event, path_params, **kwargs):
    """
    List validation runs for an organization.

    Queries validation run summaries stored with pk=ORG#{org_id}, sk=RUN#{run_id}.
    Returns runs sorted by timestamp descending (most recent first).
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = validation_results_table.query(
        KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RUN#'),
        ScanIndexForward=False,  # Descending order (newest first)
    )

    allowed = perms_module.viewable_categories(claims, org_id)
    runs = []
    for item in result.get('Items', []):
        run_categories = item.get('categories') or []
        # Legacy runs without a categories field are visible to org-admins only.
        if run_categories:
            if not (set(run_categories) & allowed):
                continue
        else:
            if not perms_module.is_org_admin(claims, org_id):
                continue
        runs.append({
            'validation_run_id': item.get('validation_run_id'),
            'timestamp': item.get('timestamp'),
            'total_documents': convert_decimals(item.get('total_documents', 0)),
            'passed': convert_decimals(item.get('passed', 0)),
            'failed': convert_decimals(item.get('failed', 0)),
            'skipped': convert_decimals(item.get('skipped', 0)),
            'status': item.get('status', 'completed'),
            'categories': run_categories,
        })

    return response(200, {'runs': runs, 'count': len(runs)})


def get_validation_run(event, path_params, **kwargs):
    """
    Get all validation results for a specific run.

    Queries GSI2 with gsi2pk=RUN#{run_id} to get all documents validated in the run.
    """
    org_id = path_params.get('orgId')
    run_id = path_params.get('runId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = validation_results_table.query(
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}'),
    )

    allowed = perms_module.viewable_categories(claims, org_id)
    documents = []
    for item in result.get('Items', []):
        # Filter by organization_id for RBAC (non-super-admins)
        if item.get('organization_id') != org_id and not is_super_admin(claims):
            continue

        rules = [r for r in (item.get('rules') or []) if r.get('category') in allowed]
        if not rules:
            continue
        documents.append({
            'document_id': item.get('document_id'),
            'validation_timestamp': item.get('validation_timestamp'),
            'filename': item.get('filename'),
            'summary': convert_decimals(item.get('summary', {})),
            'rules': convert_decimals(rules),
            'field_values': convert_decimals(item.get('field_values', {})),
        })

    return response(200, {
        'validation_run_id': run_id,
        'organization_id': org_id,
        'documents': documents,
        'total_count': len(documents),
    })


def get_validation_result(event, path_params, **kwargs):
    """
    Get detailed validation result for a single document.

    Queries GSI2 with both partition and sort key to get a specific document.
    """
    org_id = path_params.get('orgId')
    run_id = path_params.get('runId')
    doc_id = path_params.get('docId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = validation_results_table.query(
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}') & Key('gsi2sk').eq(f'DOC#{doc_id}'),
    )

    if not result.get('Items'):
        return response(404, {'error': f'Validation result not found for document {doc_id} in run {run_id}'})

    item = result['Items'][0]

    # RBAC check
    if item.get('organization_id') != org_id and not is_super_admin(claims):
        return response(403, {'error': 'Access denied to this validation result'})

    allowed = perms_module.viewable_categories(claims, org_id)
    rules = [r for r in (item.get('rules') or []) if r.get('category') in allowed]
    if not rules:
        return response(403, {'error': 'No viewable rules for this document'})

    return response(200, {
        'document_id': item.get('document_id'),
        'validation_run_id': item.get('validation_run_id'),
        'organization_id': item.get('organization_id'),
        'validation_timestamp': item.get('validation_timestamp'),
        'filename': item.get('filename'),
        'summary': convert_decimals(item.get('summary', {})),
        'rules': convert_decimals(rules),
        'field_values': convert_decimals(item.get('field_values', {})),
    })


def confirm_finding(event, path_params, body, **kwargs):
    """
    Confirm a finding for a specific rule on a document validation.

    Expects body with rule_id to identify which rule's finding is being confirmed.
    Updates the specific rule within the document's rules array to set finding_confirmed=true.
    """
    org_id = path_params.get('orgId')
    run_id = path_params.get('runId')
    doc_id = path_params.get('docId')

    if not body or 'rule_id' not in body:
        return response(400, {'error': 'Request body must include rule_id'})

    rule_id = body['rule_id']

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    # First, query using GSI2 to get the item's primary keys
    result = validation_results_table.query(
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}') & Key('gsi2sk').eq(f'DOC#{doc_id}'),
    )

    if not result.get('Items'):
        return response(404, {'error': f'Validation result not found for document {doc_id} in run {run_id}'})

    item = result['Items'][0]

    # RBAC check
    if item.get('organization_id') != org_id and not is_super_admin(claims):
        return response(403, {'error': 'Access denied to this validation result'})

    # Get the primary key and sort key from the item
    pk = item.get('pk')
    sk = item.get('sk')

    if not pk or not sk:
        return response(500, {'error': 'Item missing primary key attributes'})

    # Find the rule index in the rules array
    rules = item.get('rules', [])
    rule_index = None
    rule_category = None
    for idx, rule in enumerate(rules):
        if rule.get('rule_id') == rule_id:
            rule_index = idx
            rule_category = rule.get('category')
            break

    if rule_index is None:
        return response(404, {'error': f'Rule {rule_id} not found on document {doc_id}'})

    if not perms_module.can_view_category(claims, org_id, rule_category):
        return response(403, {'error': 'Access denied to this rule category'})

    # Update the specific rule in the rules array to set finding_confirmed=true
    try:
        timestamp = datetime.utcnow().isoformat() + 'Z'
        user = claims.get('email') or 'unknown'

        validation_results_table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression=f'SET #rules[{rule_index}].finding_confirmed = :val, #rules[{rule_index}].finding_confirmed_at = :ts, #rules[{rule_index}].finding_confirmed_by = :user',
            ExpressionAttributeNames={
                '#rules': 'rules',
            },
            ExpressionAttributeValues={
                ':val': True,
                ':ts': timestamp,
                ':user': user,
            },
        )
        print(f"Confirmed finding for rule {rule_id} on document {doc_id} in run {run_id}")

        return response(200, {
            'message': 'Finding confirmed successfully',
            'document_id': doc_id,
            'validation_run_id': run_id,
            'rule_id': rule_id,
            'finding_confirmed': True,
            'finding_confirmed_at': timestamp,
            'finding_confirmed_by': user,
        })

    except Exception as e:
        print(f"Error confirming finding: {e}")
        return response(500, {'error': str(e)})


def mark_resolved(event, path_params, body, **kwargs):
    """
    Mark a rule finding as resolved/fixed.

    Expects body with rule_id to identify which rule is being resolved.
    Sets fixed=true, fixed_at, fixed_by and removes finding_confirmed attributes.
    """
    org_id = path_params.get('orgId')
    run_id = path_params.get('runId')
    doc_id = path_params.get('docId')

    if not body or 'rule_id' not in body:
        return response(400, {'error': 'Request body must include rule_id'})

    rule_id = body['rule_id']

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    # First, query using GSI2 to get the item's primary keys
    result = validation_results_table.query(
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}') & Key('gsi2sk').eq(f'DOC#{doc_id}'),
    )

    if not result.get('Items'):
        return response(404, {'error': f'Validation result not found for document {doc_id} in run {run_id}'})

    item = result['Items'][0]

    # RBAC check
    if item.get('organization_id') != org_id and not is_super_admin(claims):
        return response(403, {'error': 'Access denied to this validation result'})

    # Get the primary key and sort key from the item
    pk = item.get('pk')
    sk = item.get('sk')

    if not pk or not sk:
        return response(500, {'error': 'Item missing primary key attributes'})

    # Find the rule index in the rules array
    rules = item.get('rules', [])
    rule_index = None
    rule_category = None
    for idx, rule in enumerate(rules):
        if rule.get('rule_id') == rule_id:
            rule_index = idx
            rule_category = rule.get('category')
            break

    if rule_index is None:
        return response(404, {'error': f'Rule {rule_id} not found on document {doc_id}'})

    if not perms_module.can_view_category(claims, org_id, rule_category):
        return response(403, {'error': 'Access denied to this rule category'})

    # Update the specific rule: set fixed=true and remove finding_confirmed
    try:
        timestamp = datetime.utcnow().isoformat() + 'Z'
        user = claims.get('email') or 'unknown'

        validation_results_table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression=f'SET #rules[{rule_index}].#fixed = :val, #rules[{rule_index}].fixed_at = :ts, #rules[{rule_index}].fixed_by = :user REMOVE #rules[{rule_index}].finding_confirmed, #rules[{rule_index}].finding_confirmed_at, #rules[{rule_index}].finding_confirmed_by',
            ExpressionAttributeNames={
                '#rules': 'rules',
                '#fixed': 'fixed',
            },
            ExpressionAttributeValues={
                ':val': True,
                ':ts': timestamp,
                ':user': user,
            },
        )
        print(f"Marked rule {rule_id} as resolved on document {doc_id} in run {run_id}")

        return response(200, {
            'message': 'Rule marked as resolved successfully',
            'document_id': doc_id,
            'validation_run_id': run_id,
            'rule_id': rule_id,
            'fixed': True,
            'fixed_at': timestamp,
            'fixed_by': user,
        })

    except Exception as e:
        print(f"Error marking rule as resolved: {e}")
        return response(500, {'error': str(e)})


def mark_incorrect(event, path_params, body, **kwargs):
    """
    Mark a rule finding as incorrect (false positive).

    Sets feedback_given=true and changes status to PASS.
    Expects body with rule_id.
    """
    org_id = path_params.get('orgId')
    run_id = path_params.get('runId')
    doc_id = path_params.get('docId')

    if not body or 'rule_id' not in body:
        return response(400, {'error': 'Request body must include rule_id'})

    rule_id = body['rule_id']

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    # Query using GSI2 to get the item's primary keys
    result = validation_results_table.query(
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}') & Key('gsi2sk').eq(f'DOC#{doc_id}'),
    )

    if not result.get('Items'):
        return response(404, {'error': f'Validation result not found for document {doc_id} in run {run_id}'})

    item = result['Items'][0]

    # RBAC check
    if item.get('organization_id') != org_id and not is_super_admin(claims):
        return response(403, {'error': 'Access denied to this validation result'})

    pk = item.get('pk')
    sk = item.get('sk')

    if not pk or not sk:
        return response(500, {'error': 'Item missing primary key attributes'})

    # Find the rule index
    rules = item.get('rules', [])
    rule_index = None
    rule_category = None
    for idx, rule in enumerate(rules):
        if rule.get('rule_id') == rule_id:
            rule_index = idx
            rule_category = rule.get('category')
            break

    if rule_index is None:
        return response(404, {'error': f'Rule {rule_id} not found on document {doc_id}'})

    if not perms_module.can_view_category(claims, org_id, rule_category):
        return response(403, {'error': 'Access denied to this rule category'})

    # Update: set feedback_given=true, status=PASS
    try:
        timestamp = datetime.utcnow().isoformat() + 'Z'
        user = claims.get('email') or 'unknown'

        validation_results_table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression=f'SET #rules[{rule_index}].feedback_given = :fg, #rules[{rule_index}].feedback_given_at = :ts, #rules[{rule_index}].feedback_given_by = :user, #rules[{rule_index}].#status = :pass',
            ExpressionAttributeNames={
                '#rules': 'rules',
                '#status': 'status',
            },
            ExpressionAttributeValues={
                ':fg': True,
                ':ts': timestamp,
                ':user': user,
                ':pass': 'PASS',
            },
        )
        print(f"Marked rule {rule_id} as incorrect on document {doc_id} in run {run_id}")

        return response(200, {
            'message': 'Rule marked as incorrect successfully',
            'document_id': doc_id,
            'validation_run_id': run_id,
            'rule_id': rule_id,
            'feedback_given': True,
            'feedback_given_at': timestamp,
            'status': 'PASS',
        })

    except Exception as e:
        print(f"Error marking rule as incorrect: {e}")
        return response(500, {'error': str(e)})


def trigger_validation_run(event, path_params, body, **kwargs):
    """
    Trigger a validation run for an organization.

    Invokes the rules-engine-rag Lambda asynchronously to process all pending
    files in textract-processed/ and csv-staging/ folders.

    Body may include `categories: [...]` to limit the run to specific rule
    categories. If absent, defaults to every category the caller can run.
    Caller must have `run` permission on every requested category.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    runnable = perms_module.runnable_categories(claims, org_id)
    if not runnable:
        return response(403, {'error': 'No runnable rule categories for this user'})

    requested = (body or {}).get('categories')
    if requested is None:
        run_categories = sorted(runnable)
    else:
        if not isinstance(requested, list) or not requested:
            return response(400, {'error': 'categories must be a non-empty list'})
        unknown = [c for c in requested if c not in perms_module.CATEGORIES]
        if unknown:
            return response(400, {'error': f'Unknown categories: {unknown}'})
        denied = [c for c in requested if c not in runnable]
        if denied:
            return response(403, {'error': f'Cannot run categories: {denied}'})
        run_categories = list(requested)

    # Verify organization exists
    org, err = get_organization_by_id(org_id)
    if err:
        return response(404, {'error': err})

    try:
        # Generate validation_run_id upfront so retries use the same ID
        from datetime import datetime, timezone
        validation_run_id = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')

        # Invoke rules engine Lambda asynchronously
        lambda_response = lambda_client.invoke(
            FunctionName=RULES_ENGINE_LAMBDA,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps({
                'organization_id': org_id,
                'validation_run_id': validation_run_id,
                'categories': run_categories,
            }),
        )

        status_code = lambda_response.get('StatusCode', 0)

        if status_code == 202:  # Accepted for async invocation
            print(f"Triggered validation run {validation_run_id} for {org_id} categories={run_categories}")
            return response(202, {
                'message': 'Validation run triggered successfully',
                'organization_id': org_id,
                'validation_run_id': validation_run_id,
                'categories': run_categories,
                'status': 'processing',
            })
        else:
            print(f"Unexpected status code from Lambda: {status_code}")
            return response(500, {'error': f'Lambda invocation returned status {status_code}'})

    except lambda_client.exceptions.ResourceNotFoundException:
        return response(500, {'error': f'Rules engine Lambda not found: {RULES_ENGINE_LAMBDA}'})
    except Exception as e:
        print(f"Error triggering validation: {e}")
        return response(500, {'error': str(e)})


# ---- LLM Enhancement ----

def _extract_complete_json(text: str) -> Optional[str]:
    """
    Extract a complete JSON object by properly matching braces.
    Handles nested objects and strings containing braces.
    """
    start = text.find('{')
    if start == -1:
        return None

    brace_count = 0
    in_string = False
    escape_next = False

    for i in range(start, len(text)):
        char = text[i]

        if escape_next:
            escape_next = False
            continue

        if char == '\\':
            escape_next = True
            continue

        if char == '"':
            in_string = not in_string
            continue

        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    return text[start:i+1]

    return None


def extract_json_from_claude_response(response_body: dict) -> Optional[dict]:
    """Extract JSON from a Bedrock Claude model response."""
    content_list = response_body.get("content", [])
    if not content_list:
        print("No 'content' field found or it's empty in the model response.")
        return None

    all_text = " ".join(
        block.get("text", "") for block in content_list if block.get("type") == "text"
    )

    # Try ```json``` code block first
    match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", all_text)
    if not match:
        print("No JSON code block found. Trying raw extraction...")
        json_str = _extract_complete_json(all_text)
        if not json_str:
            print("No valid JSON object found.")
            return None

        try:
            parsed = json.loads(json_str)
            print(f"Extracted JSON data: {json.dumps(parsed, indent=2)}")
            return parsed
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"Raw JSON string: {json_str}")
        return None

    json_str = match.group(1)
    try:
        parsed = json.loads(json_str)
        print(f"Extracted JSON data: {json.dumps(parsed, indent=2)}")
        return parsed
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Raw JSON string: {json_str}")
        return None


def invoke_claude_model(
    inference_profile_id: str,
    body: dict,
    return_json_only: bool,
    bedrock_client=None,
    retries: int = 1,
    raise_on_error: bool = True,
    region_name: str = 'us-east-1',
):
    """Invoke Claude model via Bedrock with optional JSON extraction and retry logic."""
    if bedrock_client is None:
        bedrock_client = boto3.client('bedrock-runtime', region_name=region_name)

    model_response = bedrock_client.invoke_model(
        modelId=inference_profile_id,
        body=json.dumps(body),
        contentType='application/json',
        accept='application/json',
    )

    response_body = json.loads(model_response['body'].read())

    if not return_json_only:
        return response_body

    extracted_json = extract_json_from_claude_response(response_body)

    if extracted_json is None:
        if retries > 0:
            return invoke_claude_model(
                inference_profile_id=inference_profile_id,
                body=body,
                return_json_only=return_json_only,
                bedrock_client=bedrock_client,
                retries=retries - 1,
                raise_on_error=raise_on_error,
                region_name=region_name,
            )
        else:
            if raise_on_error:
                raise ValueError("No JSON found in Claude response")
            return None

    return extracted_json


def enhance_fields(event, path_params, body, **kwargs):
    """Use LLM to extract fields_to_extract from rule_text"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or 'rule_text' not in body:
        return response(400, {'error': 'Request body must include rule_text'})

    rule_text = body['rule_text']

    system_prompt = """You are an expert at analyzing medical chart validation rules.
    Given a rule description, identify the key fields that need to be extracted from chart documents to evaluate this rule.

    Return a JSON array of field objects. Each object must have:
    - "name": A snake_case identifier for the field (e.g., "recipient", "service_location")
    - "type": The data type - one of "string", "number", "boolean", "datetime"
    - "description": A brief description of what this field represents in the chart

    Only include fields that are explicitly or implicitly referenced in the rule.
    Return ONLY the JSON array, no other text.

    Here are examples:

    Example 1:
    Rule text: Determine if the 'Recipient' field is consistent with the 'Service Location' and the actual method of contact documented in the text.

    Output:
    [
    {
        "name": "recipient",
        "type": "string",
        "description": "The individual or entity receiving the service or communication"
    },
    {
        "name": "service_location",
        "type": "string",
        "description": "The documented location where the service took place"
    },
    {
        "name": "method_of_contact",
        "type": "string",
        "description": "The communication method used such as in-person, phone, or video"
    }
    ]

    Example 2:
    Rule text: Identify the modality (Physical In-Person, Audio/Phone, or Video/Telehealth).

    Output:
    [
    {
        "name": "modality",
        "type": "string",
        "description": "The communication modality: In-Person, Phone, or Video"
    },
    {
        "name": "platform_or_phone",
        "type": "string",
        "description": "The video platform name or phone number, depending on modality"
    }
    ]
    """

    user_prompt = f"""Analyze this validation rule and identify the fields to extract:

Rule text: {rule_text}

Return a JSON array of fields to extract."""

    try:
        resp = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps({
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens': 2048,
                'system': system_prompt,
                'messages': [{'role': 'user', 'content': user_prompt}],
            }),
        )
        result = json.loads(resp['body'].read())
        content = result['content'][0]['text']

        # Parse the JSON array from the response
        # Handle potential markdown code blocks
        if '```json' in content:
            content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content:
            content = content.split('```')[1].split('```')[0].strip()

        fields = json.loads(content)
        return response(200, {'fields_to_extract': fields})

    except json.JSONDecodeError as e:
        print(f"Failed to parse LLM response as JSON: {e}")
        return response(500, {'error': 'Failed to parse LLM response'})
    except Exception as e:
        print(f"Error in enhance_fields: {e}")
        return response(500, {'error': str(e)})


def enhance_note(event, path_params, body, **kwargs):
    """Use LLM to enhance a note for better validation based on feedback"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or 'note' not in body:
        return response(400, {'error': 'Request body must include note'})

    note = body['note']
    rule_text = body.get('rule_text', '')
    rule_id = body.get('rule_id')
    document_id = body.get('document_id')
    validation_run_id = body.get('validation_run_id')
    notes = body.get('notes') or []

    system_prompt = """
    You are an expert medical chart validation assistant responsible for improving rule clarification notes used by an automated medical chart validation system.

    Your role is to convert human reviewer feedback into reusable clarification guidance that will help the system make more accurate validation decisions in the future.

    SYSTEM CONTEXT
    The validation system operates as follows:
    1. A medical chart is evaluated against a compliance rule.
    2. The system produces a validation result (pass/fail or similar).
    3. A human reviewer evaluates the result.
    4. If the system made an incorrect or unclear decision, the human provides feedback.
    5. Clarification notes are added to help the system interpret the rule more accurately in future validations.

    You will receive the following inputs:
    - The rule text
    - The medical chart that was evaluated (optional)
    - The system's previous validation result
    - Human feedback explaining the mistake or clarification
    - Existing clarification notes already associated with the rule

    YOUR OBJECTIVE
    Determine whether the human feedback reveals a reusable insight that should become a new clarification note for future validations.

    If it does, write a concise clarification note that improves the system's ability to correctly interpret the rule.

    If the feedback is already covered by existing notes, or does not add reusable guidance, do NOT create a new note.

    HOW TO THINK ABOUT THE TASK
    Internally reason through the following steps before producing your answer:

    1. Understand the rule's intent.
    2. Analyze how the chart relates to the rule.
    3. Identify what the system likely misunderstood.
    4. Interpret the human feedback to determine the correction.
    5. Check whether existing notes already capture this guidance.
    6. Decide whether a new clarification note would meaningfully improve future validation accuracy.

    IMPORTANT: Perform this reasoning internally. Do NOT output your reasoning.

    HOW TO WRITE A CLARIFICATION NOTE
    If a new note is needed, it must follow these rules:

    - Be concise but precise.
    - Use a factual, instructional tone.
    - Focus on how the rule should be interpreted when validating charts.
    - Generalize the insight so it applies to future charts.
    - Do NOT reference specific patients, documents, or this specific validation run.
    - Avoid vague statements; provide clear guidance.
    - Prefer concrete validation instructions or interpretation rules.
    - Clarify edge cases or evidence requirements when relevant.

    DUPLICATION RULE
    You MUST check existing notes carefully.

    If the same guidance already exists:
    Return null.

    If the feedback partially overlaps with an existing note:
    Only create a new note if it adds meaningful clarification or resolves ambiguity.

    OUTPUT FORMAT
    Return ONLY valid JSON.

    If a new clarification note should be added:

    {
    "new_clarification_note": "<clarification note text>"
    }

    If no new note is necessary:

    {
    "new_clarification_note": null
    }

    Do not include explanations, reasoning, markdown, or additional fields.
    Return JSON only.
    """

    print(f"Rule ID: {rule_id}, Document ID: {document_id}, Validation Run ID: {validation_run_id}")
    print(f"Previous notes: {notes}")
    print(f"Organization ID: {org_id}")

    # Fetch organization to get S3 bucket
    org, err = get_organization_by_id(org_id)
    if err:
        return response(500, {'error': err})

    print(f"Organization: {org}")

    s3_bucket_name = org.get('s3_bucket_name')
    if s3_bucket_name is None:
        return response(500, {'error': 'Organization S3 bucket not found'})

    # Query validation results table for the validation result via GSI
    validation_result = None
    if validation_run_id:
        validation_query = validation_results_table.query(
            IndexName='gsi2',
            KeyConditionExpression='gsi2pk = :pk',
            ExpressionAttributeValues={':pk': f'RUN#{validation_run_id}'},
            Limit=1,
        )
        if validation_query.get('Items'):
            validation_result = validation_query['Items'][0]
            print(f"Validation result: {validation_result}")

    user_prompt = f"""
    Rule text: {rule_text}
    Validation result: {validation_result}
    Feedback on validation result: {note}
    Existing clarification notes: {notes}
    """

    body_payload = {
        'system': system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [
            {
                'role': 'user',
                'content': user_prompt
            }
        ]
    }

    try:
        response_json = invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body=body_payload,
            return_json_only=True,
            raise_on_error=True,
            retries=1
        )
        print(f"Response JSON: {response_json}")

        enhanced_note = response_json.get('new_clarification_note')

        # If a new note was generated and rule_id is provided, save it to the rule
        if enhanced_note and rule_id:
            try:
                # Append the new note to the rule's notes array
                table.update_item(
                    Key={'pk': f'ORG#{org_id}', 'sk': f'RULE#{rule_id}'},
                    UpdateExpression='SET notes = list_append(if_not_exists(notes, :empty_list), :new_note), updated_at = :ts',
                    ExpressionAttributeValues={
                        ':new_note': [enhanced_note],
                        ':empty_list': [],
                        ':ts': datetime.utcnow().isoformat() + 'Z',
                    },
                )
                print(f"Saved enhanced note to rule {rule_id}")
            except Exception as save_err:
                print(f"Error saving note to rule {rule_id}: {save_err}")
                # Don't fail the request if saving fails, just log it

        return response(200, {'enhanced_note': enhanced_note})
    except Exception as e:
        print(f"Error in enhance_note invoking Claude model: {e}")
        return response(500, {'error': str(e)})


# ---- User Permissions (RBAC) ----

def get_my_permissions(event, **kwargs):
    """Return the calling user's permission view (used by the frontend)."""
    claims, error = authorize_request(event)
    if error:
        return error
    return response(200, perms_module.serialize_for_me_endpoint(claims))


def list_org_users(event, path_params, **kwargs):
    """List all users with permissions in the given org. Super admin only."""
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    result = table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq('USER_PERM') &
                               Key('gsi1sk').begins_with(f'ORG#{org_id}#'),
    )

    users = [_format_user_perm(item) for item in result.get('Items', [])]
    return response(200, {'users': users, 'count': len(users)})


def get_org_user(event, path_params, **kwargs):
    """Get a single user's permissions in an org. Super admin only."""
    org_id = path_params.get('orgId')
    email = path_params.get('email')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    result = table.get_item(Key={'pk': f'USER#{email}', 'sk': f'ORG#{org_id}'})
    if 'Item' not in result:
        return response(404, {'error': f'No permissions found for {email} in {org_id}'})

    return response(200, _format_user_perm(result['Item']))


def upsert_org_user(event, path_params, body, **kwargs):
    """Create or replace a user's permissions in an org. Super admin only."""
    org_id = path_params.get('orgId')
    email = path_params.get('email')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    if not body:
        return response(400, {'error': 'Request body required'})

    existing_result = table.get_item(Key={'pk': f'USER#{email}', 'sk': f'ORG#{org_id}'})
    existing = existing_result.get('Item')

    try:
        item = perms_module.build_user_perm_item(email, org_id, body, existing=existing)
    except ValueError as e:
        return response(400, {'error': str(e)})

    table.put_item(Item=item)
    perms_module.invalidate_cache(email=email, org_id=org_id)
    print(f"Upserted permissions for {email} in {org_id} (role={item['role']})")

    return response(200 if existing else 201, _format_user_perm(item))


def delete_org_user(event, path_params, **kwargs):
    """Revoke a user's permissions in an org. Super admin only."""
    org_id = path_params.get('orgId')
    email = path_params.get('email')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    table.delete_item(Key={'pk': f'USER#{email}', 'sk': f'ORG#{org_id}'})
    perms_module.invalidate_cache(email=email, org_id=org_id)
    print(f"Deleted permissions for {email} in {org_id}")

    return response(204, {})


def _format_user_perm(item):
    return {
        'email': item.get('email'),
        'organization_id': item.get('organization_id'),
        'role': item.get('role', 'member'),
        'report_permissions': item.get('report_permissions', {}),
        'analytics_permissions': item.get('analytics_permissions', []),
        'created_at': item.get('created_at'),
        'updated_at': item.get('updated_at'),
    }


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
        # Deterministic rule fields
        'conditions': convert_decimals(item.get('conditions', [])),
        'conditionals': convert_decimals(item.get('conditionals', [])),
        'logic': item.get('logic', 'all'),
        'fail_message': item.get('fail_message', ''),
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
