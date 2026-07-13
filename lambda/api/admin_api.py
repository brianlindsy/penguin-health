"""
Admin API Lambda - CRUD operations for organization configuration

Handles API Gateway HTTP API v2 events with route-based dispatch.
Reuses multi_org_config.py for DynamoDB reads where possible.
Includes LLM-enhanced endpoints for rule field extraction and note enhancement.
"""

import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import boto3
from botocore.config import Config as BotoConfig
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key

import permissions as perms_module
import analytics_helpers
import nl_agent
import nl_agent_tools
import eligibility_api
import eligibility_worklist_api
import centralreach_api
from audit import SystemPrincipal, audited, emit as audit_emit

_NL_AGENT_PRINCIPAL = SystemPrincipal(
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'admin-api-nl-agent')
)
from bedrock_client import (
    invoke_claude_model,
    extract_json_from_claude_response,
    MODEL_ID,
)

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
table = dynamodb.Table('penguin-health-org-config')
validation_results_table = dynamodb.Table('penguin-health-validation-results')
analytics_reports_table = dynamodb.Table(
    os.environ.get('ANALYTICS_REPORTS_TABLE', 'penguin-health-analytics-reports')
)
deep_jobs_table = dynamodb.Table(
    os.environ.get('DEEP_JOBS_TABLE', 'penguin-health-analytics-deep-jobs')
)

# Module-level Bedrock client shared across agent steps in one worker
# invocation so HTTP connection pooling kicks in for multi-turn loops.
# Config is owned by bedrock_client._BEDROCK_BOTO_CONFIG (read_timeout=300
# for tool-use turns, botocore retries disabled — we drive retries
# explicitly). Importing the private constant by name keeps the tuning
# in one place; the alternative was duplicating BotoConfig() here.
from bedrock_client import _BEDROCK_BOTO_CONFIG
bedrock = boto3.client('bedrock-runtime', config=_BEDROCK_BOTO_CONFIG)

RULES_ENGINE_LAMBDA = 'penguin-health-rules-engine-rag'
DEEP_WORKER_LAMBDA = os.environ.get('DEEP_WORKER_LAMBDA', 'penguin-health-deep-analytics-worker')

# Deep-job item retention (24h after creation). DynamoDB TTL is best-effort
# and may run hours late, but is fine here — jobs are display-only.
DEEP_JOB_TTL_SECONDS = 24 * 60 * 60


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
        'GET /api/organizations/{orgId}/ui-display-fields': get_ui_display_fields,
        'PUT /api/organizations/{orgId}/ui-display-fields': update_ui_display_fields,
        'GET /api/organizations/{orgId}/programs': get_org_programs,
        'PUT /api/organizations/{orgId}/programs': update_org_programs,
        'POST /api/organizations/{orgId}/rules/enhance-fields': enhance_fields,
        'POST /api/organizations/{orgId}/rules/enhance-note': enhance_note,
        'POST /api/organizations/{orgId}/analytics/nl-query': nl_query,
        'POST /api/organizations/{orgId}/analytics/nl-query/deep': nl_query_deep,
        'GET /api/organizations/{orgId}/analytics/nl-query/deep/{jobId}': get_deep_job,
        'POST /api/organizations/{orgId}/analytics/reports': save_report,
        'GET /api/organizations/{orgId}/analytics/reports': list_reports,
        'GET /api/organizations/{orgId}/analytics/reports/{reportId}': get_report,
        'DELETE /api/organizations/{orgId}/analytics/reports/{reportId}': delete_report,
        'GET /api/organizations/{orgId}/validation-runs': list_validation_runs,
        'POST /api/organizations/{orgId}/validation-runs': trigger_validation_run,
        'GET /api/organizations/{orgId}/validation-runs/{runId}': get_validation_run,
        'GET /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}': get_validation_result,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/confirm-finding': confirm_finding,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-resolved': mark_resolved,
        'PUT /api/organizations/{orgId}/validation-runs/{runId}/documents/{docId}/mark-incorrect': mark_incorrect,
        'GET /api/me/permissions': get_my_permissions,
        'GET /api/organizations/{orgId}/subscriptions': list_org_subscriptions,
        'PUT /api/organizations/{orgId}/subscriptions/{email}': upsert_org_user_subscription,
        'GET /api/organizations/{orgId}/users': list_org_users,
        'GET /api/organizations/{orgId}/users/{email}': get_org_user,
        'PUT /api/organizations/{orgId}/users/{email}': upsert_org_user,
        'DELETE /api/organizations/{orgId}/users/{email}': delete_org_user,
        'POST /api/organizations/{orgId}/eligibility/verify':
            lambda event, path_params, body, **kw: eligibility_api.verify(
                event=event, path_params=path_params, body=body,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/eligibility/history':
            lambda event, path_params, body, **kw: eligibility_api.history(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/eligibility/config':
            lambda event, path_params, body, **kw: eligibility_api.get_config(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
        'PUT /api/organizations/{orgId}/eligibility/config':
            lambda event, path_params, body, **kw: eligibility_api.update_config(
                event=event, path_params=path_params, body=body,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/eligibility/encounters':
            lambda event, path_params, body, **kw: eligibility_worklist_api.list_encounters(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
        'PUT /api/organizations/{orgId}/eligibility/encounters/{encounterId}/resolve':
            lambda event, path_params, body, **kw: eligibility_worklist_api.resolve_encounter(
                event=event, path_params=path_params, body=body,
                authorize_fn=authorize_request),
        'POST /api/organizations/{orgId}/eligibility/encounters/{encounterId}/rerun':
            lambda event, path_params, body, **kw: eligibility_worklist_api.rerun_encounter(
                event=event, path_params=path_params, body=body,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/centralreach/config':
            lambda event, path_params, body, **kw: centralreach_api.get_config(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
        'POST /api/organizations/{orgId}/centralreach/run':
            lambda event, path_params, body, **kw: centralreach_api.trigger_run(
                event=event, path_params=path_params, body=body,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/centralreach/runs':
            lambda event, path_params, body, **kw: centralreach_api.list_runs(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
        'GET /api/organizations/{orgId}/centralreach/runs/{runId}':
            lambda event, path_params, body, **kw: centralreach_api.get_run(
                event=event, path_params=path_params,
                authorize_fn=authorize_request),
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


# ---- UI Display Fields ----

def get_ui_display_fields(event, path_params, **kwargs):
    """Get the org's UI_DISPLAY_FIELDS mapping (canonical → source key).

    Returns an empty `mappings` dict when unset — that's the fallback
    signal the rules-engine and UI both read as "no projection". The UI
    editor treats it as "start from scratch."
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'UI_DISPLAY_FIELDS'}
    )

    if 'Item' not in result:
        return response(200, {
            'organization_id': org_id,
            'mappings': {},
        })

    item = result['Item']
    return response(200, {
        'organization_id': item.get('organization_id'),
        'mappings': item.get('mappings', {}),
        'updated_at': item.get('updated_at'),
    })


def update_ui_display_fields(event, path_params, body, **kwargs):
    """Overwrite the org's UI_DISPLAY_FIELDS mapping.

    Body: `{"mappings": {"employee_name": "provider_display", ...}}`.
    An empty `mappings` dict is a valid write — it turns projection off
    without deleting the item, so a future re-enable is one PUT away.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or 'mappings' not in body:
        return response(400, {'error': 'Request body must include mappings'})

    mappings = body['mappings']
    if not isinstance(mappings, dict):
        return response(400, {'error': 'mappings must be an object'})

    for k, v in mappings.items():
        if not isinstance(k, str) or not k or not isinstance(v, str) or not v:
            return response(
                400,
                {'error': 'mappings must map non-empty strings to non-empty strings'},
            )

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'UI_DISPLAY_FIELDS',
        'organization_id': org_id,
        'mappings': mappings,
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }
    table.put_item(Item=item)
    print(f"Updated UI_DISPLAY_FIELDS for {org_id} ({len(mappings)} entries)")

    return response(200, {
        'organization_id': org_id,
        'mappings': mappings,
        'updated_at': item['updated_at'],
    })


# ---- Org Programs ----

def get_org_programs(event, path_params, **kwargs):
    """Return the org's canonical list of program names.

    Programs are the org-scoped labels that appear on each document validation
    at `field_values.program`. The list is used both by the UI (as the source of
    truth for the per-user program_permissions checkboxes) and by the API
    validation in `build_user_perm_item` to reject unknown programs.
    Returns an empty list when unset.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = table.get_item(Key={'pk': f'ORG#{org_id}', 'sk': 'PROGRAMS'})
    item = result.get('Item') or {}
    return response(200, {
        'organization_id': org_id,
        'programs': list(item.get('programs') or []),
        'updated_at': item.get('updated_at'),
    })


def update_org_programs(event, path_params, body, **kwargs):
    """Overwrite the org's PROGRAMS list. Super admin only.

    Body: `{"programs": ["Program A", "Program B", ...]}`. Duplicates are
    dropped; entries are stored sorted so the DDB item is stable across writes.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    if not body or 'programs' not in body:
        return response(400, {'error': 'Request body must include programs'})

    raw = body['programs']
    if not isinstance(raw, list):
        return response(400, {'error': 'programs must be a list'})

    programs = []
    seen = set()
    for p in raw:
        if not isinstance(p, str) or not p.strip():
            return response(400, {'error': 'programs must be non-empty strings'})
        clean = p.strip()
        if clean in seen:
            continue
        seen.add(clean)
        programs.append(clean)
    programs.sort()

    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'PROGRAMS',
        'organization_id': org_id,
        'programs': programs,
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }
    table.put_item(Item=item)
    perms_module.invalidate_org_programs_cache(org_id)
    print(f"Updated PROGRAMS for {org_id} ({len(programs)} entries)")

    return response(200, {
        'organization_id': org_id,
        'programs': programs,
        'updated_at': item['updated_at'],
    })


# ---- Validation Results ----

DETAILS_DEFAULT_LIMIT = 50
DETAILS_MAX_LIMIT = 200
DETAILS_FETCH_WORKERS = 10

# Fields the analytics pages actually read off each rule and field_values blob.
# Anything else (LLM reasoning, evidence, full clinical fields) is dropped from
# the bulk response to keep us under API Gateway's 6 MB ceiling. The single-run
# endpoint still returns the full payload for the drill-down detail page.
SLIM_RULE_FIELDS = ('rule_id', 'rule_name', 'category', 'status')
SLIM_FIELD_VALUE_KEYS = ('date', 'employee_name', 'program')


def merge_display_field_values(item):
    """Overlay `ui_display_fields` onto raw `field_values`.

    The rules-engine writes `ui_display_fields` (canonical-name projection)
    only when the org has a `UI_DISPLAY_FIELDS` config item. Rows without
    it — legacy runs and un-configured orgs — fall through untouched, which
    is the fallback path the UI relies on.
    """
    fv = item.get('field_values') or {}
    udf = item.get('ui_display_fields') or {}
    return {**fv, **udf} if udf else fv


def _query_all(table, **kwargs):
    """Query a DynamoDB table, walking LastEvaluatedKey until exhausted.

    A single Query() caps at 1 MB per page; on real-sized runs (thousands of
    document rows, tens of KB each) that silently drops the tail. Callers
    that need the complete result set must use this helper instead of a raw
    table.query().
    """
    items = []
    last_evaluated = None
    while True:
        if last_evaluated:
            kwargs['ExclusiveStartKey'] = last_evaluated
        resp = table.query(**kwargs)
        items.extend(resp.get('Items', []))
        last_evaluated = resp.get('LastEvaluatedKey')
        if not last_evaluated:
            break
    return items


def _document_program(item):
    """Read the program label off a validation-result row.

    Prefers the canonical UI projection (`ui_display_fields.program`) so a
    per-org rename via UI_DISPLAY_FIELDS doesn't silently break the filter,
    and falls back to the raw `field_values.program`.
    """
    udf = item.get('ui_display_fields') or {}
    if 'program' in udf:
        return udf.get('program')
    fv = item.get('field_values') or {}
    return fv.get('program')


def _program_allowed(item, allowed_programs):
    """True when the doc's program is in the caller's allowed set.

    `allowed_programs is None` means "unrestricted" — every doc passes.
    When restricted, a doc with no program label is denied; leaking rows
    the caller can't classify would defeat the point of the filter.
    """
    if allowed_programs is None:
        return True
    program = _document_program(item)
    return program in allowed_programs


def _fetch_run_documents(run_id, org_id, claims, allowed, allowed_programs, slim=False):
    """Fetch + RBAC-filter the documents for a single validation run.

    Mirrors the per-document filtering in get_validation_run so the bulk
    response stays consistent with the single-run endpoint. When slim=True,
    projects to the subset of fields used by analytics pages.
    """
    items = _query_all(
        validation_results_table,
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}'),
    )
    documents = []
    for item in items:
        if item.get('organization_id') != org_id and not is_super_admin(claims):
            continue
        if not _program_allowed(item, allowed_programs):
            continue
        rules = [r for r in (item.get('rules') or []) if r.get('category') in allowed]
        if not rules:
            continue
        merged_fv = merge_display_field_values(item)
        if slim:
            slim_rules = [{k: r.get(k) for k in SLIM_RULE_FIELDS} for r in rules]
            slim_fv = {k: merged_fv.get(k) for k in SLIM_FIELD_VALUE_KEYS if k in merged_fv}
            documents.append({
                'document_id': item.get('document_id'),
                'validation_timestamp': item.get('validation_timestamp'),
                'summary': convert_decimals(item.get('summary', {})),
                'rules': convert_decimals(slim_rules),
                'field_values': convert_decimals(slim_fv),
            })
        else:
            documents.append({
                'document_id': item.get('document_id'),
                'validation_timestamp': item.get('validation_timestamp'),
                'filename': item.get('filename'),
                'summary': convert_decimals(item.get('summary', {})),
                'rules': convert_decimals(rules),
                'field_values': convert_decimals(merged_fv),
            })
    return documents


def list_validation_runs(event, path_params, **kwargs):
    """
    List validation runs for an organization.

    Queries validation run summaries stored with pk=ORG#{org_id}, sk=RUN#{run_id}.
    Returns runs sorted by timestamp descending (most recent first).

    Optional query params:
      - since, until: ISO-8601 timestamps; filter runs by their timestamp field.
        ISO-8601 sorts lexicographically so string comparison is correct.
      - include=details: embed each run's documents inline (replaces N+1
        get_validation_run calls from the staff-performance page).
      - slim=true: with include=details, project documents/rules/field_values
        down to the fields the analytics pages actually consume — required to
        stay under API Gateway's 6 MB response ceiling on real-sized runs.
      - limit: cap the number of runs returned (default 50 when include=details,
        unbounded otherwise; hard max 200 with details).
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    qs = event.get('queryStringParameters') or {}
    since = qs.get('since')
    until = qs.get('until')
    include_details = qs.get('include') == 'details'
    slim = qs.get('slim', '').lower() == 'true'
    try:
        limit = int(qs['limit']) if 'limit' in qs else None
    except (TypeError, ValueError):
        return response(400, {'error': 'limit must be an integer'})

    items = _query_all(
        validation_results_table,
        KeyConditionExpression=Key('pk').eq(f'ORG#{org_id}') & Key('sk').begins_with('RUN#'),
        ScanIndexForward=False,  # Descending order (newest first)
    )

    allowed = perms_module.viewable_categories(claims, org_id)
    allowed_programs = perms_module.viewable_programs(claims, org_id)
    runs = []
    for item in items:
        ts = item.get('timestamp')
        if since and (not ts or ts < since):
            continue
        if until and (not ts or ts > until):
            continue
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
            'timestamp': ts,
            'total_documents': convert_decimals(item.get('total_documents', 0)),
            'passed': convert_decimals(item.get('passed', 0)),
            'failed': convert_decimals(item.get('failed', 0)),
            'skipped': convert_decimals(item.get('skipped', 0)),
            'status': item.get('status', 'completed'),
            'categories': run_categories,
            'dates': item.get('dates') or [],
        })

    truncated = False
    if include_details:
        effective_limit = min(limit or DETAILS_DEFAULT_LIMIT, DETAILS_MAX_LIMIT)
        if len(runs) > effective_limit:
            runs = runs[:effective_limit]
            truncated = True
        # Fan out the per-run GSI2 queries in parallel — sequential 50× would
        # blow past API Gateway's 30s timeout on a cold Lambda.
        with ThreadPoolExecutor(max_workers=DETAILS_FETCH_WORKERS) as pool:
            doc_lists = list(pool.map(
                lambda r: _fetch_run_documents(
                    r['validation_run_id'], org_id, claims, allowed, allowed_programs, slim=slim),
                runs,
            ))
        for run, docs in zip(runs, doc_lists):
            run['documents'] = docs
            run['total_count'] = len(docs)
    elif limit is not None and len(runs) > limit:
        runs = runs[:limit]
        truncated = True

    return response(200, {'runs': runs, 'count': len(runs), 'truncated': truncated})


@audited(action='read', resource_type='ValidationRun',
         resource_from_path='runId', purpose_of_use='OPERATIONS')
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

    items = _query_all(
        validation_results_table,
        IndexName='gsi2',
        KeyConditionExpression=Key('gsi2pk').eq(f'RUN#{run_id}'),
    )

    allowed = perms_module.viewable_categories(claims, org_id)
    allowed_programs = perms_module.viewable_programs(claims, org_id)
    documents = []
    for item in items:
        # Filter by organization_id for RBAC (non-super-admins)
        if item.get('organization_id') != org_id and not is_super_admin(claims):
            continue

        if not _program_allowed(item, allowed_programs):
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
            'field_values': convert_decimals(merge_display_field_values(item)),
        })

    return response(200, {
        'validation_run_id': run_id,
        'organization_id': org_id,
        'documents': documents,
        'total_count': len(documents),
    })


@audited(action='read', resource_type='ValidationResult',
         resource_from_path='docId', purpose_of_use='OPERATIONS')
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

    allowed_programs = perms_module.viewable_programs(claims, org_id)
    if not _program_allowed(item, allowed_programs):
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
        'field_values': convert_decimals(merge_display_field_values(item)),
    })


@audited(action='write', resource_type='ValidationFinding',
         resource_from_path='docId', purpose_of_use='OPERATIONS',
         call_type='ddb_write')
def confirm_finding(event, path_params, body, **kwargs):
    """
    Confirm a finding for a specific rule on a document validation.

    Expects body with rule_id to identify which rule's finding is being confirmed.
    Updates the specific rule within the document's rules array to set finding_confirmed=true.

    The in-place row mutation (finding_confirmed_by, finding_confirmed_at) is
    intentionally kept as a UI summary field — the immutable record of who
    confirmed this lives in the audit event emitted by the decorator above.
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

    if not _program_allowed(item, perms_module.viewable_programs(claims, org_id)):
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


@audited(action='write', resource_type='ValidationFinding',
         resource_from_path='docId', purpose_of_use='OPERATIONS',
         call_type='ddb_write')
def mark_resolved(event, path_params, body, **kwargs):
    """
    Mark a rule finding as resolved/fixed.

    Expects body with rule_id to identify which rule is being resolved.
    Sets fixed=true, fixed_at, fixed_by and removes finding_confirmed attributes.

    NOTE: this handler intentionally REMOVEs `finding_confirmed_*` from the
    DDB item — the prior confirmation evidence is destroyed on the row. The
    immutable record of the prior confirmation (and this resolution) lives
    in the audit event emitted by the decorator above and in the WORM S3
    archive. This is the right trade-off: the row stays small and the UI
    has a single "currently resolved" summary, while history is preserved
    in the audit log.
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

    if not _program_allowed(item, perms_module.viewable_programs(claims, org_id)):
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


@audited(action='write', resource_type='ValidationFinding',
         resource_from_path='docId', purpose_of_use='OPERATIONS',
         call_type='ddb_write')
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

    if not _program_allowed(item, perms_module.viewable_programs(claims, org_id)):
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


CUTOVER_DATE = '2026-05-01'
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _parse_iso_date(s):
    """Parse YYYY-MM-DD; return a date or None if malformed."""
    if not isinstance(s, str) or not DATE_RE.match(s):
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def _validate_dates(requested):
    """
    Normalize and validate the `dates` body field.

    Returns (dates_list, error_response). On success, error_response is None.
    """
    today = datetime.utcnow().date()
    cutover = datetime.strptime(CUTOVER_DATE, '%Y-%m-%d').date()

    if requested is None:
        # Default to today (UTC) for callers that don't specify.
        return [today.isoformat()], None

    if not isinstance(requested, list) or not requested:
        return None, response(400, {'error': 'dates must be a non-empty list'})

    parsed = []
    for s in requested:
        d = _parse_iso_date(s)
        if d is None:
            return None, response(400, {'error': f'Malformed date: {s!r} (expected YYYY-MM-DD)'})
        if d < cutover:
            return None, response(400, {'error': f'Date {s} is before cutover {CUTOVER_DATE}'})
        if d > today:
            return None, response(400, {'error': f'Date {s} is in the future'})
        parsed.append(d.isoformat())

    # Deduplicate while preserving order (Python dict preserves insertion order).
    return list(dict.fromkeys(parsed)), None


def trigger_validation_run(event, path_params, body, **kwargs):
    """
    Trigger a validation run for an organization.

    Body may include:
      - `categories: [...]` to limit the run to specific rule categories.
         Caller must have `run` permission on every requested category.
         Defaults to every category the caller can run.
      - `dates: ["YYYY-MM-DD", ...]` to limit the run to specific ingest
         dates. Each date must be in [2026-05-01, today_utc].
         Defaults to [today_utc].
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

    dates, dates_err = _validate_dates((body or {}).get('dates'))
    if dates_err:
        return dates_err

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
                'dates': dates,
            }),
        )

        status_code = lambda_response.get('StatusCode', 0)

        if status_code == 202:  # Accepted for async invocation
            print(f"Triggered validation run {validation_run_id} for {org_id} "
                  f"categories={run_categories} dates={dates}")
            return response(202, {
                'message': 'Validation run triggered successfully',
                'organization_id': org_id,
                'validation_run_id': validation_run_id,
                'categories': run_categories,
                'dates': dates,
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
        # Returns a JSON ARRAY (not object) so we can't use
        # return_json_only=True — that path only extracts objects.
        # Route through the wrapper anyway so the call shows up in
        # PenguinHealth/LLMCost dimensioned by this org.
        result = invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body={
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens': 2048,
                'system': system_prompt,
                'messages': [{'role': 'user', 'content': user_prompt}],
            },
            return_json_only=False,
            bedrock_client=bedrock,
            org_id=org_id,
            user_email=claims.get('email'),
            call_type='rule_fields_enhance',
        )
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
    print(f"Previous notes count: {len(notes) if notes else 0}")
    print(f"Organization ID: {org_id}")

    # Fetch organization to get S3 bucket
    org, err = get_organization_by_id(org_id)
    if err:
        return response(500, {'error': err})

    print(f"Organization ID: {org.get('organization_id')}, Bucket: {org.get('s3_bucket_name')}")

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
            print(f"Validation result found for run {validation_run_id}: pk={validation_result.get('pk')}, sk={validation_result.get('sk')}")

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
            retries=1,
            org_id=org_id,
            user_email=claims.get('email'),
            call_type='rule_note_enhance',
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


_SUBSCRIPTION_EVENT_TYPES = ("validation_run_complete", "eligibility_issue")


def list_org_subscriptions(event, path_params, **kwargs):
    """List every user in the org with their subscription state per event.

    Super-admin only. The user roster comes from the existing USER_PERM
    rows (same source as listOrgUsers); subscription rows are merged in
    so missing rows render as enabled=false in the UI without a second
    round-trip.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    perm_rows = table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq('USER_PERM')
                               & Key('gsi1sk').begins_with(f'ORG#{org_id}#'),
    ).get('Items', [])
    emails = sorted({row.get('email') for row in perm_rows if row.get('email')})

    sub_rows = table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq('SUBSCRIPTION')
                               & Key('gsi1sk').begins_with(f'ORG#{org_id}#'),
    ).get('Items', [])
    # subs[email][event_type] = {enabled, updated_at}
    subs: dict[str, dict[str, dict]] = {}
    for row in sub_rows:
        email = row.get('email')
        event_type = row.get('event_type')
        if not email or not event_type:
            continue
        subs.setdefault(email, {})[event_type] = {
            'enabled': bool(row.get('enabled')),
            'updated_at': row.get('updated_at'),
        }

    users = []
    for email in emails:
        user_subs = subs.get(email, {})
        users.append({
            'email': email,
            'subscriptions': [
                {
                    'event_type': event_type,
                    'enabled': bool(user_subs.get(event_type, {}).get('enabled')),
                    'updated_at': user_subs.get(event_type, {}).get('updated_at'),
                }
                for event_type in _SUBSCRIPTION_EVENT_TYPES
            ],
        })
    return response(200, {
        'users': users,
        'event_types': list(_SUBSCRIPTION_EVENT_TYPES),
    })


def upsert_org_user_subscription(event, path_params, body, **kwargs):
    """Super-admin upsert of one subscription row for a specific user.

    Path: PUT /api/organizations/{orgId}/subscriptions/{email}
    Body: { event_type, enabled }
    """
    org_id = path_params.get('orgId')
    email = path_params.get('email')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error
    if not is_super_admin(claims):
        return response(403, {'error': 'Super admin required'})

    if not body:
        return response(400, {'error': 'Request body required'})
    event_type = body.get('event_type')
    enabled = body.get('enabled')
    if not event_type:
        return response(400, {'error': 'event_type is required'})
    if event_type not in _SUBSCRIPTION_EVENT_TYPES:
        return response(400, {'error': f'Unknown event_type: {event_type}'})
    if not isinstance(enabled, bool):
        return response(400, {'error': 'enabled must be a boolean'})

    from notifications import set_subscription
    item = set_subscription(
        email=email, org_id=org_id, event_type=event_type, enabled=enabled,
    )
    return response(200, {
        'email': item['email'],
        'event_type': item['event_type'],
        'organization_id': item['organization_id'],
        'enabled': bool(item['enabled']),
        'updated_at': item['updated_at'],
    })


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
        'program_permissions': item.get('program_permissions', []),
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


# ---- Analytics: Natural-Language Query ----

def _build_nl_system_prompt(org_id: str) -> str:
    schema = analytics_helpers.ORG_SCHEMAS[org_id]
    suffix = analytics_helpers.table_suffix(org_id)

    def fmt_col(c):
        flag = " [narrative]" if c.get("narrative") else ""
        notes = c.get("notes")
        line = f"  - {c['name']} ({c['type']}){flag}"
        if notes:
            # Indent multi-line notes under the column for readability.
            note_text = notes.replace("\n", " ")
            line += f"\n      note: {note_text}"
        return line

    chart_lines = "\n".join(fmt_col(c) for c in schema["chart_columns"])
    validation_lines = "\n".join(fmt_col(c) for c in schema["validation_columns"])
    chart_partition_note = schema.get("chart_partition_notes", "")
    validation_partition_note = schema.get("validation_partition_notes", "")

    return f"""You are an analytics SQL assistant for the Penguin Health admin dashboard.
You convert natural-language questions into safe AWS Athena SQL SELECT
queries, OR you flag the question as one that requires deep narrative
analysis when SQL alone cannot reliably answer it.

Org: {org_id}
Database: penguin_health_analytics
Available tables (ONLY these — do not reference any others):

1. charts_{suffix} — clinical chart rows.
   All columns are stored as string regardless of semantic type. Cast
   before arithmetic/comparison (use TRY_CAST for fields that may be
   empty or non-numeric).
   Columns:
{chart_lines}
   Columns marked [narrative] contain free-form prose. You MAY use them
   in WHERE/LIKE for keyword filtering, but counts will be approximate
   because phrasing varies.
   Partition key: ingest_date (string, yyyy-MM-dd). {chart_partition_note}

2. validation_results_{suffix} — rule validation outputs (one row per
   rule per document).
   Columns:
{validation_lines}
   Partition key: validation_date (string, yyyy-MM-dd). {validation_partition_note}

Critical rules from auditing the live data:
- Column values are case-sensitive. Use the EXACT casing shown in the
  notes above (e.g. status='FAIL', not 'failed' or 'Failed').
- Date/timestamp strings have varied formats — read the per-column
  notes before using date_parse(); a wrong format produces silent
  nulls and zero-row results, not an error.
- When you don't know the value set of a free-form column (e.g.
  category, field_program), prefer GROUP BY over guessing literal
  values in WHERE clauses.
- For numeric aggregation over chart string columns or field_rate, use
  TRY_CAST and IS NOT NULL filters so non-numeric rows don't poison
  the result.

Decision rule:
- If the question can be answered with structured columns OR with a
  reasonable LIKE filter on a narrative column where false positives
  are acceptable, respond with mode "sql".
- If the question requires extracting a specific concept buried in
  narrative prose (e.g. "where did the referral come from", "what was
  the presenting concern", "what medication was prescribed"), respond
  with mode "needs_deep_analysis" and provide a scoping query that
  narrows to candidate rows (must include LIMIT <= 200).

Constraints (both modes):
- SELECT only. No DDL/DML. No multiple statements.
- Always include LIMIT. Max 1000 for sql mode, max 200 for scoping.
- Athena/Presto dialect.
- Prefer partition filters (ingest_date / validation_date) when the
  question implies a time range.

Respond with ONLY one of these JSON shapes — no prose, no markdown:

{{ "mode": "sql",
   "sql": "...",
   "viz_type": "bar" | "line" | "pie" | "table",
   "explanation": "one sentence; mention if results are approximate" }}

{{ "mode": "needs_deep_analysis",
   "reason": "one sentence explaining why SQL alone can't answer this",
   "scope_sql": "SELECT ... LIMIT 200" }}

viz_type guidance:
- "line" for time series with multiple date points
- "bar" for categorical comparisons
- "pie" for parts-of-whole with <= 8 slices
- "table" for everything else"""


_VALID_VIZ_TYPES = {"bar", "line", "pie", "table"}


@audited(action='read', resource_type='AnalyticsResult',
         purpose_of_use='ANALYTICS', call_type='nl_query')
def nl_query(event, path_params, body, **kwargs):
    """
    Kick off an agent job to answer a natural-language question.

    Returns 202 with a job_id; the client polls GET .../nl-query/deep/{jobId}
    for live progress (`current_step_label`, `step_count`, `trace`) and
    the final {columns, rows, viz_type} when the agent calls `finalize`.

    All questions go through the agent — there's no longer a synchronous
    SQL fast path. Pure-SQL questions typically resolve in 2 turns
    (`run_sql` + `finalize`).
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or not isinstance(body, dict) or not body.get('question'):
        return response(400, {'error': 'Request body must include question'})

    question = body['question']
    if not isinstance(question, str) or len(question) > 1000:
        return response(400, {'error': 'question must be a string <= 1000 chars'})

    if org_id not in analytics_helpers.ORG_SCHEMAS:
        return response(400, {
            'error': f"Analytics is not provisioned for org '{org_id}'.",
            'code': 'ORG_NOT_PROVISIONED',
        })

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    ttl = int(now.timestamp()) + DEEP_JOB_TTL_SECONDS

    item = {
        'pk': _deep_job_pk(org_id),
        'sk': _deep_job_sk(job_id),
        'job_id': job_id,
        'organization_id': org_id,
        'status': 'running',
        'agent_mode': True,
        'question': question.strip(),
        'step_count': 0,
        'current_step_label': 'Starting agent…',
        'trace': [],
        'agent_columns': [],
        'agent_rows': [],
        'created_at': created_at,
        'updated_at': created_at,
        'created_by': claims.get('email') or 'unknown',
        'ttl': ttl,
    }
    deep_jobs_table.put_item(Item=item)

    try:
        lambda_client.invoke(
            FunctionName=DEEP_WORKER_LAMBDA,
            InvocationType='Event',
            Payload=json.dumps({'org_id': org_id, 'job_id': job_id}).encode('utf-8'),
        )
    except Exception as e:
        print(f"nl_query: failed to invoke worker for job {job_id}: {e}")
        deep_jobs_table.update_item(
            Key={'pk': item['pk'], 'sk': item['sk']},
            UpdateExpression='SET #s = :s, #err = :err, updated_at = :u',
            ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
            ExpressionAttributeValues={
                ':s': 'failed',
                ':err': f'Failed to start worker: {e}',
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
        return response(502, {
            'error': 'Failed to start agent worker.',
            'code': 'WORKER_START_FAILED',
        })

    return response(202, {
        'job_id': job_id,
        'status': 'running',
        'agent_mode': True,
    })


def _deep_extract_for_row(question: str, row_payload: dict,
                          *, org_id: str = None, user_email: str = None,
                          parent_request_id: str = None) -> str:
    """
    Call Claude to extract a single answer string from one row's narrative
    fields. Returns 'unknown' on any error so a single bad row doesn't kill
    the whole batch.

    Cost-attribution kwargs are keyword-only so existing positional callers
    keep working; pass them from the worker handler so each row's Claude
    spend lands in CloudWatch under this org's job.
    """
    system = (
        "You extract one short answer from a chart record's narrative fields. "
        "Respond with ONLY a JSON object: {\"value\": \"<short answer or 'unknown'>\"}."
    )
    user = (
        f"Original question: {question}\n\n"
        f"Row data:\n{json.dumps(row_payload, default=str)}\n\n"
        f"Extract the answer as a short string. If the answer cannot be "
        f"determined from this row, respond with \"unknown\"."
    )
    # Audit the Bedrock invocation. Every deep-extract row contains
    # patient-level data; without this emit, the analytics path would
    # be the largest unaudited PHI-to-Bedrock surface in the app.
    audit_emit(
        action='execute',
        resource={'type': 'BedrockPrompt', 'id': MODEL_ID,
                  'org': org_id or 'unknown'},
        actor=_NL_AGENT_PRINCIPAL.as_actor(),
        org_id=org_id or 'unknown',
        purpose_of_use='ANALYTICS',
        call_type='bedrock_invoke',
        external_control_number=parent_request_id,
    )
    try:
        resp = invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body={
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens': 200,
                'system': system,
                'messages': [{'role': 'user', 'content': user}],
            },
            return_json_only=True,
            retries=0,
            raise_on_error=False,
            bedrock_client=bedrock,
            org_id=org_id,
            user_email=user_email,
            call_type='deep_extract_row',
            parent_request_id=parent_request_id,
        )
        if isinstance(resp, dict) and isinstance(resp.get('value'), str):
            return resp['value'].strip() or 'unknown'
    except Exception as e:
        print(f"deep_extract row error: {type(e).__name__}")
    return 'unknown'


def _deep_job_pk(org_id: str) -> str:
    return f'ORG#{org_id}'


def _deep_job_sk(job_id: str) -> str:
    return f'JOB#{job_id}'


@audited(action='read', resource_type='AnalyticsResult',
         purpose_of_use='ANALYTICS', call_type='nl_query_deep')
def nl_query_deep(event, path_params, body, **kwargs):
    """
    Kick off an async deep-analysis job: validate the scope SQL, run the
    fast scope Athena query inline (well under the 30s API GW ceiling),
    persist the job, and async-invoke the worker Lambda to do the per-row
    Claude extraction. Returns 202 with a job_id the client can poll.
    """
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or not isinstance(body, dict):
        return response(400, {'error': 'Request body must include question and scope_sql'})
    question = body.get('question')
    scope_sql_raw = body.get('scope_sql')
    if not isinstance(question, str) or not question.strip():
        return response(400, {'error': 'question is required'})
    if not isinstance(scope_sql_raw, str) or not scope_sql_raw.strip():
        return response(400, {'error': 'scope_sql is required'})

    if org_id not in analytics_helpers.ORG_SCHEMAS:
        return response(400, {
            'error': f"Analytics is not provisioned for org '{org_id}'.",
            'code': 'ORG_NOT_PROVISIONED',
        })

    try:
        scope_sql = analytics_helpers.validate_athena_sql(
            scope_sql_raw,
            org_id,
            max_limit=analytics_helpers.MAX_DEEP_SCOPE_LIMIT,
        )
    except analytics_helpers.SqlValidationError as e:
        return response(400, {
            'error': e.message,
            'code': e.code,
            'sql': scope_sql_raw,
        })

    try:
        scope_result = analytics_helpers.run_athena_query(scope_sql, org_id)
    except analytics_helpers.AthenaQueryError as e:
        return response(400, {
            'error': e.message,
            'code': 'ATHENA_ERROR',
            'sql': scope_sql,
        })

    columns = scope_result['columns']
    rows = scope_result['rows']
    if len(rows) > analytics_helpers.MAX_DEEP_SCOPE_LIMIT:
        return response(400, {
            'error': (
                f"Scope query returned {len(rows)} rows; deep analysis is "
                f"capped at {analytics_helpers.MAX_DEEP_SCOPE_LIMIT}. "
                f"Tighten the scope filters."
            ),
            'code': 'SCOPE_TOO_LARGE',
            'sql': scope_sql,
        })

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    created_at = now.isoformat()
    ttl = int(now.timestamp()) + DEEP_JOB_TTL_SECONDS

    item = {
        'pk': _deep_job_pk(org_id),
        'sk': _deep_job_sk(job_id),
        'job_id': job_id,
        'organization_id': org_id,
        'status': 'running',
        'question': question.strip(),
        'scope_sql': scope_sql,
        'columns': columns,
        'rows': rows,
        'total_rows': len(rows),
        'done_rows': 0,
        'extracted': [None] * len(rows),
        'created_at': created_at,
        'updated_at': created_at,
        'created_by': claims.get('email') or 'unknown',
        'ttl': ttl,
    }
    deep_jobs_table.put_item(Item=item)

    try:
        lambda_client.invoke(
            FunctionName=DEEP_WORKER_LAMBDA,
            InvocationType='Event',
            Payload=json.dumps({'org_id': org_id, 'job_id': job_id}).encode('utf-8'),
        )
    except Exception as e:
        print(f"nl_query_deep: failed to invoke worker for job {job_id}: {e}")
        deep_jobs_table.update_item(
            Key={'pk': item['pk'], 'sk': item['sk']},
            UpdateExpression='SET #s = :s, #err = :err, updated_at = :u',
            ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
            ExpressionAttributeValues={
                ':s': 'failed',
                ':err': f'Failed to start worker: {e}',
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
        return response(502, {
            'error': 'Failed to start deep analysis worker.',
            'code': 'WORKER_START_FAILED',
        })

    return response(202, {
        'job_id': job_id,
        'status': 'running',
        'total_rows': len(rows),
        'done_rows': 0,
        'sql': scope_sql,
    })


@audited(action='read', resource_type='AnalyticsResult',
         resource_from_path='jobId',
         purpose_of_use='ANALYTICS', call_type='nl_query_deep_poll')
def get_deep_job(event, path_params, **kwargs):
    """
    Return current state of an async deep-analysis job. While running,
    returns progress + the partial extracted column so the UI can render
    live updates. On completion, returns the same shape the previous
    synchronous endpoint returned.
    """
    org_id = path_params.get('orgId')
    job_id = path_params.get('jobId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = deep_jobs_table.get_item(Key={
        'pk': _deep_job_pk(org_id),
        'sk': _deep_job_sk(job_id),
    })
    item = result.get('Item')
    if item is None:
        return response(404, {'error': f'Deep analysis job not found: {job_id}'})

    item = convert_decimals(item)
    status = item.get('status')

    if item.get('agent_mode'):
        # Agent jobs don't have a per-row `extracted` list — the agent
        # shapes the final result itself via the finalize tool. Return
        # the final {columns, rows} when succeeded; while running, return
        # only the current step label so the UI can render progress.
        agent_columns = item.get('agent_columns') or []
        agent_rows = item.get('agent_rows') or []
        payload = {
            'job_id': job_id,
            'status': status,
            'question': item.get('question'),
            'columns': agent_columns,
            'rows': agent_rows,
            'row_count': len(agent_rows),
            'agent_mode': True,
            'step_count': item.get('step_count', 0),
            'current_step_label': item.get('current_step_label', ''),
            'trace': item.get('trace') or [],
        }
        if status == 'succeeded':
            payload['mode'] = 'agent'
            payload['viz_type'] = item.get('viz_type', 'table')
            payload['explanation'] = item.get('explanation', '')
        elif status == 'failed':
            payload['error'] = item.get('error') or 'Agent run failed.'
            payload['code'] = item.get('error_code') or 'WORKER_FAILED'
        return response(200, payload)

    columns = item.get('columns') or []
    rows = item.get('rows') or []
    extracted = item.get('extracted') or []
    scope_sql = item.get('scope_sql')
    total = item.get('total_rows', len(rows))
    done = item.get('done_rows', 0)

    # Build a "view" of the result that's valid at any progress level: the
    # extracted column has either the value or null for not-yet-done rows.
    out_columns = list(columns) + [{'name': 'extracted_value', 'type': 'string'}]
    out_rows = [r + [extracted[i] if i < len(extracted) else None]
                for i, r in enumerate(rows)]

    payload = {
        'job_id': job_id,
        'status': status,
        'sql': scope_sql,
        'question': item.get('question'),
        'columns': out_columns,
        'rows': out_rows,
        'row_count': len(out_rows),
        'total_rows': total,
        'done_rows': done,
    }

    if status == 'succeeded':
        payload['mode'] = 'deep'
        payload['viz_type'] = item.get('viz_type', 'bar')
        payload['explanation'] = item.get('explanation', '')
    elif status == 'failed':
        payload['error'] = item.get('error') or 'Deep analysis failed.'
        payload['code'] = item.get('error_code') or 'WORKER_FAILED'

    return response(200, payload)


def _deep_job_progress_update(org_id: str, job_id: str, extracted: list, done: int):
    """Persist incremental progress so the polling client can render live updates."""
    deep_jobs_table.update_item(
        Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
        UpdateExpression='SET extracted = :e, done_rows = :d, updated_at = :u',
        ExpressionAttributeValues={
            ':e': extracted,
            ':d': done,
            ':u': datetime.now(timezone.utc).isoformat(),
        },
    )


# ---- Analytics: agent loop execution path ----

AGENT_MAX_STEPS = int(os.environ.get('AGENT_MAX_STEPS', '20'))


def _build_agent_system_prompt(org_id: str) -> str:
    """System prompt for the agent loop. Lists the tools, the org's tables,
    and the contract for finishing (must call `finalize`). Schema details
    are also retrievable on demand via the `inspect_schema` tool so the
    agent can refresh them mid-run.
    """
    schema = analytics_helpers.ORG_SCHEMAS[org_id]
    suffix = analytics_helpers.table_suffix(org_id)
    narrative_names = sorted(analytics_helpers.narrative_columns_for_org(org_id))

    return f"""You are an analytics agent for the Penguin Health admin dashboard.
You answer the user's question by orchestrating tool calls — you do not
respond with prose; instead, you call tools and ultimately call `finalize`
with the final answer columns/rows.

Org: {org_id}
Database: penguin_health_analytics
Tables: charts_{suffix}, validation_results_{suffix}
Narrative columns (free-text prose, read these via extract_from_rows when
the answer is buried in narrative): {", ".join(narrative_names) or "(none)"}

Tools available:
- inspect_schema(): full schema + per-column notes
- run_sql(sql): execute a SELECT (auto-LIMIT applied). Returns
  {{run_id, columns, preview_rows, row_count, sql}}. The FULL rows live
  server-side under run_id; you only see a small preview. Pass
  `from_run_id` to downstream tools — do NOT copy rows into their inputs.
- extract_from_rows(from_run_id, question): per-row Claude extraction
  of a short string from narrative fields. Cap 500 rows per call.
  Returns {{run_id, row_count, preview_results}}. The output rows
  PRESERVE every source-row field (e.g. clientvisit_id, answer) AND
  add `extracted_value`. You do NOT need to merge the extraction back
  to the source — it's already merged.
- aggregate(from_run_id, group_by, agg, sum_field?, order_by?):
  in-memory GROUP BY on the rows behind a run_id. Returns
  {{run_id, columns, rows, row_count}} in positional shape.
- filter_rows(from_run_id, field, op, value? | values?): drop rows from
  a cached run by a single-column predicate. Ops: '==', '!=', 'is_null',
  'is_not_null', 'in', 'not_in'. Use this AFTER extract_from_rows to keep
  only the matching rows (e.g. filter_rows(field='extracted_value',
  op='==', value='YES')). Columns are preserved; row count drops.
- select_columns(from_run_id, columns, computed?): pick a subset of
  columns from a run_id and optionally add a conditional column via
  computed[col] = {{case_when: {{field, op, value, then, else}}}}.
  `then`/`else` may be literals OR source-column names (auto-resolved).
  Use this to shape the final output before finalize, especially for
  conditional columns like "show answer only when extracted_value
  == 'UNKNOWN'".
- concat_runs(from_run_ids): UNION ALL rows from multiple run_ids
  (which must share the same column names) into a new run_id. Use
  this ONLY when a single extract_from_rows call would exceed the
  500-row cap and you've batched via multiple run_sql + extract_from_rows
  pairs. Returns {{run_id, columns, row_count}}.
- finalize(from_run_id?, columns?, rows?, viz_type?, explanation?):
  emit final answer and end the loop. PREFERRED: pass `from_run_id` with
  the run_id of the result you want rendered — the server reads the
  cached columns + rows directly. Copying large row payloads into a
  literal `rows` array is unreliable: Bedrock truncates the tool input
  silently, producing an empty final answer for any result >~50 rows.
  Literal `columns` + `rows` remain available for tiny ad-hoc data.
  You MUST call finalize exactly once when done.

Canonical patterns:

A) "Count <thing> extracted from narrative":
  1. run_sql → run_id "sql-001".
  2. extract_from_rows(from_run_id="sql-001", question=<what to pull out>)
     → run_id "extract-002".
  3. aggregate(from_run_id="extract-002", group_by=["extracted_value"],
     agg="count") → run_id "agg-003".
  4. finalize(from_run_id="agg-003", viz_type="bar").

B) "Per-row extracted value + identifier column" (e.g. clientvisit_id +
   referral agency, with answer shown when agency is UNKNOWN):
  1. run_sql SELECT clientvisit_id, answer → run_id "sql-001".
  2. extract_from_rows(from_run_id="sql-001", question="extract agency")
     → run_id "extract-002" whose rows are
     {{row_index, clientvisit_id, answer, extracted_value}}.
  3. select_columns(
       from_run_id="extract-002",
       columns=["clientvisit_id", "extracted_value", "answer_if_unknown"],
       computed={{
         "answer_if_unknown": {{"case_when": {{
           "field": "extracted_value", "op": "==", "value": "UNKNOWN",
           "then": "answer", "else": ""
         }}}}
       }}
     ) → run_id "select-003".
  4. finalize(from_run_id="select-003", viz_type="table").

C) "Keep only rows whose extracted_value matches a condition":
  1. run_sql SELECT identifier, narrative → run_id "sql-001".
  2. extract_from_rows(from_run_id="sql-001",
       question="Does X apply? Answer YES or NO.") → "extract-002".
  3. filter_rows(from_run_id="extract-002", field="extracted_value",
       op="==", value="YES") → "filter-003".
  4. finalize / select_columns / aggregate from filter-003. Do NOT try
     to re-run SQL against an extract run_id — extract results are not
     in Athena.

D) "Batched extraction" when the scope exceeds the 500-row extract cap:
  1. Probe the total count with run_sql SELECT COUNT(*) FROM ... → check.
  2. If count > 500, batch with OFFSET/LIMIT (or by partition):
     - run_sql ... ORDER BY clientvisit_id LIMIT 500 OFFSET 0 → "sql-001"
     - extract_from_rows(from_run_id="sql-001", question=...) → "extract-002"
     - run_sql ... ORDER BY clientvisit_id LIMIT 500 OFFSET 500 → "sql-003"
     - extract_from_rows(from_run_id="sql-003", question=...) → "extract-004"
     - ... continue until exhausted.
  3. concat_runs(from_run_ids=["extract-002", "extract-004", ...]) → "concat-N".
  4. Then aggregate / select_columns / finalize as usual from concat-N.

Critical rules:
- CTEs (WITH ...) and subqueries (FROM (SELECT ...) alias) ARE allowed in
  run_sql, as long as every base table inside is one of the allowed tables.
  Prefer them when a question genuinely needs intermediate aggregation; do
  NOT invent multi-tool workarounds for what one CTE can express.
- Always include partition filters (ingest_date / validation_date) when
  the question implies a time range.
- Column values are case-sensitive. Use exact casing from inspect_schema notes.
- ALWAYS use `from_run_id` to pass rows between tools. Do not try to
  copy rows from one tool's output into another tool's input — pass the
  run_id and the server will load the rows.
- NEVER try to encode a join, merge, or conditional column inside an
  extract_from_rows prompt. extract_from_rows already preserves source
  columns; for conditional output use select_columns.computed.
- The final result you call finalize on should be the FINAL shape shown
  to the user. Do not include intermediate columns the user didn't ask
  for — if you need a narrower shape, use select_columns first, then
  finalize(from_run_id=<that select run_id>).
- Always prefer finalize(from_run_id=...) over copying literal rows.
  Copying more than ~50 rows inline frequently produces an empty answer.
- If a tool returns an error, READ THE ERROR MESSAGE CAREFULLY before
  retrying. Do not repeat the same call with the same shape.
- Once you have the data you need, call finalize IMMEDIATELY.
- Stop after at most {AGENT_MAX_STEPS} tool-use turns; running over is
  an error.
"""


def _agent_worker_run(org_id: str, job_id: str, item: dict) -> dict:
    """Execute the agent loop for a job. Persists progress + final result
    on the job item. Returns {ok: bool} for the Lambda invoke contract.

    Intermediate payloads spill into the org's own bucket under
    `agent-io/{job_id}/...` — same compliance boundary as Athena's
    `athena-results/` output, never a shared cross-org bucket.
    """
    question = item.get('question') or ''
    user_email = item.get('created_by')
    bucket = nl_agent_tools.org_data_bucket(org_id)
    spill = nl_agent_tools.make_s3_spill(s3_client, bucket, job_id)
    job_started_at = time.monotonic()
    print(json.dumps({
        'agent_event': 'job_start',
        'job_id': job_id,
        'org_id': org_id,
        'question_chars': len(question),
    }))

    def extractor(q: str, row_payload: dict) -> str:
        return _deep_extract_for_row(
            q, row_payload,
            org_id=org_id, user_email=user_email, parent_request_id=job_id,
        )

    # Shared scratch cache for tool-to-tool row handoffs. Lives only for
    # this one worker invocation; freed when the function returns.
    run_cache = nl_agent_tools.RunCache()
    raw_handlers = nl_agent_tools.make_tool_handlers(
        org_id=org_id,
        extractor=extractor,
        cache=run_cache,
        spill=spill,
    )

    # Wrap each tool handler with a timing + structured-log shim so every
    # call shows up in CloudWatch as one JSON line. Filterable by job_id,
    # tool, or status. The `finalize` tool isn't in raw_handlers (the
    # loop captures it directly), so it's logged via on_step instead.
    def _wrap_handler(tool_name, fn):
        def wrapped(tu_input):
            t0 = time.monotonic()
            err = None
            output = None
            try:
                output = fn(tu_input)
                return output
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                raise
            finally:
                ms = int((time.monotonic() - t0) * 1000)
                status = 'error' if err else 'ok'
                # If the handler returned a dict with an error code (e.g.
                # SQL validation rejection), surface that as the status so
                # log filters catch it without crashing the loop.
                if err is None and isinstance(output, dict) and output.get('code'):
                    status = f"error:{output['code']}"
                print(json.dumps({
                    'agent_event': 'tool_call',
                    'job_id': job_id,
                    'org_id': org_id,
                    'tool': tool_name,
                    'status': status,
                    'ms': ms,
                    'error': err,
                }, default=str))
        return wrapped

    handlers = {name: _wrap_handler(name, fn) for name, fn in raw_handlers.items()}

    def invoke(messages, tools, system):
        # Single-turn Bedrock call; the agent loop manages multi-turn state.
        # Pass the module-level `bedrock` client so we reuse its tuned
        # read_timeout (300s) and HTTP connection pool across all turns
        # in one worker invocation. parent_request_id=job_id so every
        # step of this agent loop rolls up to one queryable parent in
        # PenguinHealth/LLMCost.
        return invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body={
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens': 4096,
                'system': system,
                'tools': tools,
                'messages': messages,
            },
            org_id=org_id,
            user_email=user_email,
            call_type='nl_agent_step',
            parent_request_id=job_id,
            return_json_only=False,
            retries=0,
            raise_on_error=True,
            bedrock_client=bedrock,
        )

    def on_step(step_info):
        # Persist a lightweight progress marker + the trace-so-far. Fired
        # twice per agent step: once before the Bedrock turn (pre-step,
        # "thinking" label) and once after the turn's tool calls resolve
        # (post-step, includes the new trace entries). Writing the trace
        # on every fire means the polling UI sees the agent's history
        # live, and a worker that dies mid-run still leaves a partial
        # record in DynamoDB.
        try:
            deep_jobs_table.update_item(
                Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
                UpdateExpression=(
                    'SET step_count = :n, current_step_label = :l, '
                    'trace = :t, updated_at = :u'
                ),
                ExpressionAttributeValues={
                    ':n': step_info['step'],
                    ':l': step_info.get('label', ''),
                    ':t': list(step_info.get('trace') or []),
                    ':u': datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception as e:
            # Don't crash the agent loop on a transient DynamoDB hiccup —
            # just log. The next flush will include this step too since
            # we send the full trace each time.
            print(f"agent_worker: progress update failed: {e}")

    system_prompt = _build_agent_system_prompt(org_id)

    try:
        result = nl_agent.run_agent_loop(
            system=system_prompt,
            initial_user=question,
            tools=nl_agent_tools.ALL_TOOL_SCHEMAS,
            tool_handlers=handlers,
            invoke_fn=invoke,
            max_steps=AGENT_MAX_STEPS,
            on_step=on_step,
            run_cache=run_cache,
        )
    except nl_agent.AgentError as e:
        print(json.dumps({
            'agent_event': 'job_fail',
            'job_id': job_id,
            'org_id': org_id,
            'code': e.code,
            'message': e.message,
            'ms': int((time.monotonic() - job_started_at) * 1000),
            'steps': len(e.trace),
        }))
        deep_jobs_table.update_item(
            Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
            UpdateExpression=(
                'SET #s = :s, #err = :err, error_code = :ec, '
                'trace = :t, updated_at = :u'
            ),
            ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
            ExpressionAttributeValues={
                ':s': 'failed',
                ':err': e.message,
                ':ec': e.code,
                ':t': e.trace,
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
        return {'ok': False}
    except Exception as e:
        print(json.dumps({
            'agent_event': 'job_fail',
            'job_id': job_id,
            'org_id': org_id,
            'code': 'UNEXPECTED',
            'message': f'{type(e).__name__}: {e}',
            'ms': int((time.monotonic() - job_started_at) * 1000),
        }))
        deep_jobs_table.update_item(
            Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
            UpdateExpression='SET #s = :s, #err = :err, updated_at = :u',
            ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
            ExpressionAttributeValues={
                ':s': 'failed',
                ':err': f'{type(e).__name__}: {e}',
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
        return {'ok': False}

    final = result['final']
    final_columns = final.get('columns') or []
    final_rows = final.get('rows') or []
    viz_type = final.get('viz_type') or 'table'
    if viz_type not in _VALID_VIZ_TYPES:
        viz_type = 'table'
    explanation = final.get('explanation') or ''

    deep_jobs_table.update_item(
        Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
        UpdateExpression=(
            'SET #s = :s, agent_columns = :c, agent_rows = :r, '
            'viz_type = :v, explanation = :x, trace = :t, '
            'step_count = :n, current_step_label = :l, updated_at = :u'
        ),
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={
            ':s': 'succeeded',
            ':c': final_columns,
            ':r': final_rows,
            ':v': viz_type,
            ':x': explanation,
            ':t': result['trace'],
            ':n': result['steps'],
            ':l': 'done',
            ':u': datetime.now(timezone.utc).isoformat(),
        },
    )
    print(json.dumps({
        'agent_event': 'job_done',
        'job_id': job_id,
        'org_id': org_id,
        'steps': result['steps'],
        'final_rows': len(final_rows),
        'ms': int((time.monotonic() - job_started_at) * 1000),
    }))
    return {'ok': True}


def deep_worker_handler(event, context):
    """
    Worker Lambda entry point. Two execution paths:

    - Legacy deep-extraction (item.agent_mode falsy): loads the scoped
      rows from the job item and fans out per-row Claude extraction with
      bounded concurrency. Kept for in-flight jobs created before the
      agentic path landed; new traffic shouldn't hit this branch.

    - Agent loop (item.agent_mode truthy): runs nl_agent.run_agent_loop
      with the registered tools so Claude orchestrates schema probes,
      SQL queries, narrative extraction, aggregation, and the final
      result shape itself. Output stamped via the `finalize` tool.
    """
    org_id = event.get('org_id')
    job_id = event.get('job_id')
    if not org_id or not job_id:
        print(f"deep_worker: missing org_id/job_id in event: {event}")
        return {'ok': False}

    result = deep_jobs_table.get_item(Key={
        'pk': _deep_job_pk(org_id),
        'sk': _deep_job_sk(job_id),
    })
    item = result.get('Item')
    if item is None:
        print(f"deep_worker: job not found {org_id}/{job_id}")
        return {'ok': False}

    item = convert_decimals(item)

    if item.get('agent_mode'):
        return _agent_worker_run(org_id, job_id, item)

    question = item['question']
    user_email = item.get('created_by')
    columns = item.get('columns') or []
    rows = item.get('rows') or []
    col_names = [c['name'] for c in columns]
    narrative_cols = analytics_helpers.narrative_columns_for_org(org_id)

    def row_to_payload(row):
        payload = {}
        for name, value in zip(col_names, row):
            if name in narrative_cols or value is not None:
                payload[name] = value
        return payload

    extracted = list(item.get('extracted') or [None] * len(rows))
    PROGRESS_BATCH = 10

    try:
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {
                pool.submit(
                    _deep_extract_for_row, question, row_to_payload(r),
                    org_id=org_id, user_email=user_email, parent_request_id=job_id,
                ): i
                for i, r in enumerate(rows)
                if extracted[i] is None
            }
            since_flush = 0
            for fut in futures:
                idx = futures[fut]
                try:
                    extracted[idx] = fut.result()
                except Exception as e:
                    print(f"deep_worker: row {idx} failed: {e}")
                    extracted[idx] = 'unknown'
                since_flush += 1
                if since_flush >= PROGRESS_BATCH:
                    done = sum(1 for v in extracted if v is not None)
                    _deep_job_progress_update(org_id, job_id, extracted, done)
                    since_flush = 0

        distinct_values = {v for v in extracted if v and v != 'unknown'}
        viz_type = 'pie' if 0 < len(distinct_values) <= 8 else 'bar'
        explanation = (
            f'Deep analysis: extracted answers from {len(rows)} chart rows '
            f'using AI narrative analysis.'
        )
        deep_jobs_table.update_item(
            Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
            UpdateExpression=(
                'SET extracted = :e, done_rows = :d, #s = :s, '
                'viz_type = :v, explanation = :x, updated_at = :u'
            ),
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':e': extracted,
                ':d': len(rows),
                ':s': 'succeeded',
                ':v': viz_type,
                ':x': explanation,
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as e:
        print(f"deep_worker: job {job_id} failed: {e}")
        deep_jobs_table.update_item(
            Key={'pk': _deep_job_pk(org_id), 'sk': _deep_job_sk(job_id)},
            UpdateExpression='SET #s = :s, #err = :err, updated_at = :u',
            ExpressionAttributeNames={'#s': 'status', '#err': 'error'},
            ExpressionAttributeValues={
                ':s': 'failed',
                ':err': str(e),
                ':u': datetime.now(timezone.utc).isoformat(),
            },
        )
        return {'ok': False}

    return {'ok': True}


# ---- Analytics: Saved Reports ----

_REPORT_MAX_PAYLOAD_BYTES = 380_000  # DynamoDB item limit is 400KB; leave headroom.


def _report_pk(org_id: str) -> str:
    return f'ORG#{org_id}'


def _report_sk(created_at: str, report_id: str) -> str:
    return f'REPORT#{created_at}#{report_id}'


def _strip_report_keys(item: dict) -> dict:
    """Drop DynamoDB-specific keys before returning to client."""
    out = {k: v for k, v in item.items() if k not in ('pk', 'sk')}
    return convert_decimals(out)


@audited(action='write', resource_type='AnalyticsReport',
         purpose_of_use='ANALYTICS', call_type='ddb_write')
def save_report(event, path_params, body, **kwargs):
    """Persist a snapshot of a successful NL-analytics result."""
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not body or not isinstance(body, dict):
        return response(400, {'error': 'Request body must include report fields'})

    name = body.get('name')
    if not isinstance(name, str) or not name.strip():
        return response(400, {'error': 'name is required'})

    required = ('question', 'sql', 'viz_type', 'mode', 'columns', 'rows')
    missing = [f for f in required if f not in body]
    if missing:
        return response(400, {'error': f'Missing required fields: {missing}'})

    report_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    created_by = claims.get('email') or 'unknown'

    item = {
        'pk': _report_pk(org_id),
        'sk': _report_sk(created_at, report_id),
        'report_id': report_id,
        'organization_id': org_id,
        'name': name.strip(),
        'question': body['question'],
        'sql': body['sql'],
        'viz_type': body['viz_type'],
        'mode': body['mode'],
        'explanation': body.get('explanation', ''),
        'columns': body['columns'],
        'rows': body['rows'],
        'row_count': body.get('row_count', len(body['rows']) if isinstance(body['rows'], list) else 0),
        'created_at': created_at,
        'created_by': created_by,
    }

    # Reject oversized payloads up front rather than letting DynamoDB do it.
    payload_size = len(json.dumps(item, default=str).encode('utf-8'))
    if payload_size > _REPORT_MAX_PAYLOAD_BYTES:
        return response(400, {
            'error': (
                f'Report payload is {payload_size} bytes, over the '
                f'{_REPORT_MAX_PAYLOAD_BYTES}-byte cap. Tighten the query '
                f'(fewer rows or columns) and try again.'
            ),
            'code': 'REPORT_TOO_LARGE',
        })

    analytics_reports_table.put_item(Item=item)
    return response(201, _strip_report_keys(item))


def list_reports(event, path_params, **kwargs):
    """List report metadata for the org (newest first)."""
    org_id = path_params.get('orgId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    result = analytics_reports_table.query(
        KeyConditionExpression=(
            Key('pk').eq(_report_pk(org_id)) & Key('sk').begins_with('REPORT#')
        ),
        ScanIndexForward=False,
    )

    reports = []
    for item in result.get('Items', []):
        reports.append(convert_decimals({
            'report_id': item.get('report_id'),
            'name': item.get('name'),
            'created_at': item.get('created_at'),
            'created_by': item.get('created_by'),
            'mode': item.get('mode'),
            'viz_type': item.get('viz_type'),
            'row_count': item.get('row_count'),
        }))

    return response(200, {'reports': reports, 'count': len(reports)})


def _find_report_item(org_id: str, report_id: str) -> Optional[dict]:
    """
    Look up a single report by report_id. sk includes a timestamp, so we
    query the org's partition and filter on report_id rather than doing a
    full Scan. Reports per org are bounded — this is fine in practice.
    """
    result = analytics_reports_table.query(
        KeyConditionExpression=(
            Key('pk').eq(_report_pk(org_id)) & Key('sk').begins_with('REPORT#')
        ),
    )
    for item in result.get('Items', []):
        if item.get('report_id') == report_id:
            return item
    return None


@audited(action='read', resource_type='AnalyticsReport',
         resource_from_path='reportId', purpose_of_use='ANALYTICS')
def get_report(event, path_params, **kwargs):
    """Return a single saved report including columns/rows."""
    org_id = path_params.get('orgId')
    report_id = path_params.get('reportId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    item = _find_report_item(org_id, report_id)
    if item is None:
        return response(404, {'error': f'Report not found: {report_id}'})

    payload = _strip_report_keys(item)
    if not is_super_admin(claims):
        payload.pop('sql', None)
        payload['redacted'] = True
    return response(200, payload)


def delete_report(event, path_params, **kwargs):
    """Delete a saved report."""
    org_id = path_params.get('orgId')
    report_id = path_params.get('reportId')

    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

    if not is_super_admin(claims):
        return response(403, {'error': 'Only super admins can delete saved reports'})

    item = _find_report_item(org_id, report_id)
    if item is None:
        return response(404, {'error': f'Report not found: {report_id}'})

    analytics_reports_table.delete_item(Key={'pk': item['pk'], 'sk': item['sk']})
    return response(200, {'deleted': report_id})


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
