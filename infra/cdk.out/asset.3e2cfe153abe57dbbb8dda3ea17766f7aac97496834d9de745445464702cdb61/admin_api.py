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

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
table = dynamodb.Table('penguin-health-org-config')
validation_results_table = dynamodb.Table('penguin-health-validation-results')

bedrock = boto3.client('bedrock-runtime')
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

    rules = []
    for item in result.get('Items', []):
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

    return response(200, format_rule(result['Item']))


def create_rule(event, path_params, body, **kwargs):
    """Create a new rule"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

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


def update_rule(event, path_params, body, **kwargs):
    """Update an existing rule"""
    org_id = path_params.get('orgId')
    rule_id = path_params.get('ruleId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

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
        })

    item = result['Item']
    return response(200, {
        'organization_id': item.get('organization_id'),
        'field_mappings': item.get('field_mappings', {}),
        'version': item.get('version'),
        'updated_at': item.get('updated_at'),
    })


def update_rules_config(event, path_params, body, **kwargs):
    """Update rules config (field_mappings) for an organization"""
    org_id = path_params.get('orgId')

    # Check authorization for this org
    claims, error = authorize_request(event, org_id=org_id)
    if error:
        return error

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
        return response(200, {'enhanced_note': response_json.get('new_clarification_note')})
    except Exception as e:
        print(f"Error in enhance_note invoking Claude model: {e}")
        return response(500, {'error': str(e)})


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
