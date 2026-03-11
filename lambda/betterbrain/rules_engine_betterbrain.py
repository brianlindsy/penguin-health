"""
Rules Engine RAG Lambda - Validates documents against configurable LLM rules.

Uses Claude Sonnet 4.5 via AWS Bedrock for structured JSON rule evaluation.
Loads organization configuration and rules from DynamoDB.
"""

from functools import lru_cache
import json
import re
import csv
import io
from typing import Optional
import boto3
from datetime import datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from boto3.dynamodb.conditions import Key

s3_client = boto3.client('s3')
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

        return {
 "pk": "ORG#catholic-charities-betterbrain",
 "sk": "METADATA",
 "created_at": "2026-01-08T16:00:00Z",
 "display_name": "Catholic Charities BetterBrain",
 "enabled": True,
 "gsi1pk": "ORG_METADATA",
 "gsi1sk": "ORG#catholic-charities-betterbrain",
 "organization_id": "catholic-charities-betterbrain",
 "organization_name": "Catholic Charities BetterBrain",
 "s3_bucket_name": "penguin-health-catholic-charities-betterbrain",
 "updated_at": "2026-02-17T13:20:21.091648Z"
}

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


def load_org_rules(org_id):
    """
    Load all validation rules for an organization from DynamoDB
    Also loads field_mappings from RULES_CONFIG for field extraction

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: Configuration dict with 'rules' list, 'organization_id', and 'field_mappings'

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
            'organization_id': 'example-org',
            'field_mappings': {
                'document_id': 'Consumer Service ID:',
                'consumer_name': 'Consumer Name:'
            }
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

        # Load field_mappings from RULES_CONFIG
        rules_config = load_rules_config(org_id)
        field_mappings = rules_config.get('field_mappings', {}) if rules_config else {}

        if field_mappings:
            print(f"Loaded field_mappings with {len(field_mappings)} fields: {list(field_mappings.keys())}")
        else:
            print(f"Warning: No field_mappings found for {org_id} - document_id will be N/A")

        return {
            'rules': enabled_rules,
            'organization_id': org_id,
            'field_mappings': field_mappings
        }

    except Exception as e:
        print(f"Error loading rules for '{org_id}': {str(e)}")
        raise

def load_rules_config(org_id):
    """
    Load rules configuration (field_mappings) for an organization

    Args:
        org_id (str): Organization identifier

    Returns:
        dict: Rules config with field_mappings, or None if not found
    """
    try:
        response = table.get_item(
            Key={'pk': f'ORG#{org_id}', 'sk': 'RULES_CONFIG'}
        )

        if 'Item' not in response:
            print(f"No RULES_CONFIG found for {org_id}")
            return None

        return response['Item']

    except Exception as e:
        print(f"Error loading rules config for '{org_id}': {str(e)}")
        return None

def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON documents against configurable rules.

    Expects event with:
    - organization_id: Organization ID (looks up bucket and rules from DynamoDB)
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise ValueError("organization_id is required in event")

    print(f"Loading configuration for organization: {org_id}")
    env_config = build_env_config(org_id)
    config = load_org_rules(org_id)

    validation_run_id = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    print(f"Starting validation run: {validation_run_id}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=env_config['BUCKET_NAME'],
            Prefix=env_config['TEXTRACT_PROCESSED']
        )

        if 'Contents' not in response:
            return {
                'statusCode': 200,
                'body': json.dumps('No files to validate')
            }

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.json') and '/raw/' not in key:
                process_file(env_config['BUCKET_NAME'], key, config, org_id, env_config, validation_run_id)

        print(f"Generating CSV report for run: {validation_run_id}")
        csv_report = generate_csv_from_dynamodb(validation_run_id, env_config)
        save_csv_to_s3(csv_report, validation_run_id, env_config)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Validation completed successfully',
                'validation_run_id': validation_run_id
            })
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise e


def process_file(bucket, key, config, org_id, env_config, validation_run_id):
    """Process a single JSON file from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        results = validate_document(data, key, config, org_id, validation_run_id)
        store_results(results, env_config)

        print(f"Validated {key}: {results['summary']}")

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e


def validate_document(data, filename, config, org_id, validation_run_id):
    """Run all validation rules against a document."""
    text = data.get('text', '')
    field_mappings = config.get('field_mappings', {})
    fields = extract_fields_from_text(text, field_mappings)

    enabled_rules = [rule for rule in config.get('rules', []) if rule.get('enabled', True)]

    if not enabled_rules:
        rule_results = []
    else:
        print(f"Evaluating {len(enabled_rules)} rules in parallel...")
        max_workers = min(3, len(enabled_rules))

        rule_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_rule = {
                executor.submit(evaluate_rule, rule_config, fields, data): rule_config
                for rule_config in enabled_rules
            }

            for future in as_completed(future_to_rule):
                rule_config = future_to_rule[future]
                try:
                    result = future.result()
                    rule_results.append(result)
                except Exception as e:
                    print(f"Error evaluating rule {rule_config.get('id')}: {str(e)}")
                    rule_results.append({
                        'rule_id': rule_config.get('rule_id'),
                        'rule_name': rule_config.get('name'),
                        'category': rule_config.get('category'),
                        'status': 'ERROR',
                        'message': f'Exception during parallel execution: {str(e)}'
                    })

    passed = sum(1 for r in rule_results if r['status'] == 'PASS')
    failed = sum(1 for r in rule_results if r['status'] == 'FAIL')
    skipped = sum(1 for r in rule_results if r['status'] == 'SKIP')

    return {
        'validation_run_id': validation_run_id,
        'organization_id': org_id,
        'document_id': fields.get('document_id', 'UNKNOWN'),
        'filename': filename,
        'validation_timestamp': datetime.utcnow().isoformat(),
        'config_version': config.get('version', 'unknown'),
        'summary': {
            'total_rules': len(rule_results),
            'passed': passed,
            'failed': failed,
            'skipped': skipped
        },
        'rules': rule_results,
        'field_values': fields
    }


def extract_fields_from_text(text, field_mappings):
    """
    Extract field values from text using simple pattern matching.

    For each field mapping (e.g., "document_id": "Consumer Service ID:"):
    - Searches for lines containing the key
    - Extracts the value after the key on the same line
    """
    fields = {}

    if not text or not field_mappings:
        return fields

    lines = text.split('\n')

    for field_name, key_pattern in field_mappings.items():
        value = None

        for line in lines:
            if key_pattern in line:
                parts = line.split(key_pattern, 1)
                if len(parts) > 1:
                    value = parts[1].strip()
                    print(f"Extracted {field_name}: '{value}' from key '{key_pattern}'")
                    break

        fields[field_name] = value if value else None

        if value is None:
            print(f"Field '{field_name}' not found (looking for key: '{key_pattern}')")

    return fields


def evaluate_rule(rule_config, fields, data=None):
    """Evaluate a single rule against the document fields."""
    rule_type = rule_config.get('type', 'llm')

    result = {
        'rule_id': rule_config.get('id'),
        'rule_name': rule_config.get('name'),
        'category': rule_config.get('category')
    }

    try:
        if rule_type == 'llm':
            status, message, _ = evaluate_llm_rule(rule_config, fields, data)
        else:
            status = 'SKIP'
            message = f'Unsupported rule type: {rule_type}. Only "llm" is supported.'

        result['status'] = status
        result['message'] = message

    except Exception as e:
        result['status'] = 'ERROR'
        result['message'] = f'Error evaluating rule: {str(e)}'

    return result


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


def evaluate_llm_rule(rule_config, fields, data=None):
    """
    Evaluate a rule using AWS Bedrock Claude with structured JSON output.
    Uses the new flat schema with rule_text, fields_to_extract, and notes.

    Two-step approach:
    1. Extract fields from chart text (if fields_to_extract is defined)
    2. Validate the rule using extracted fields
    """
    model_id = 'global.anthropic.claude-sonnet-4-5-20250929-v1:0'

    # New flat schema fields
    rule_text = rule_config.get('rule_text', '')
    fields_to_extract = rule_config.get('fields_to_extract', [])
    notes = rule_config.get('notes', [])

    print(f"Evaluating rule {rule_config.get('id')} - {rule_config.get('name')}")

    # Get the full text from the document
    chart_text = ''
    if data:
        chart_text = data.get('text', '')
        print(f"Chart text length: {len(chart_text)} characters")

    # If no text available, fall back to fields
    if not chart_text:
        chart_text = json.dumps(fields, indent=2)
        print(f"No text found, using fields JSON: {len(chart_text)} characters")

    try:
        extracted_fields = None

        # Step 1: Extract fields if fields_to_extract is defined
        if fields_to_extract:
            extracted_fields = _extract_rule_fields(
                model_id, rule_text, notes, fields_to_extract, chart_text
            )
            if extracted_fields is None:
                return 'ERROR', 'No JSON found in Claude response (field extraction)', ''

        # Step 2: Validate the rule
        return _validate_rule(
            model_id, rule_text, notes, chart_text, extracted_fields
        )

    except Exception as e:
        print(f"LLM ERROR: {str(e)}")
        import traceback
        print(f"LLM TRACEBACK: {traceback.format_exc()}")
        error_msg = f'LLM evaluation error: {str(e)}'
        return 'ERROR', error_msg, error_msg


def _extract_rule_fields(model_id, rule_text, notes, fields_to_extract, chart_text):
    """
    Step 1: Extract fields from chart text to help validate the rule.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Text, and a list of fields to extract from the Chart Text. Your only purpose is to extract the fields, and return them in a JSON object.
Please respond with JSON, with the key: 'fields'. The value should be an object with the field names as keys."""

    # Build JSON schema for field extraction
    properties = {
        f['name']: {
            'type': f.get('type', 'string'),
            'description': f.get('description', '')
        } for f in fields_to_extract
    }
    field_names = [f['name'] for f in fields_to_extract]

    json_schema = {
        "type": "object",
        "properties": {
            "fields": {
                "type": "object",
                "properties": properties,
                "required": field_names
            }
        },
        "required": ["fields"]
    }

    # Format notes as string
    notes_text = '\n'.join(f"- {note}" for note in notes) if notes else 'None'

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
                    {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
                    {"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"},
                ]
            }
        ]
    }

    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1
    )

    if response_json is None:
        return None

    extracted = response_json.get('fields', {})
    print(f"Fields extracted: {extracted}")
    return extracted


def _validate_rule(model_id, rule_text, notes, chart_text, extracted_fields=None):
    """
    Step 2: Validate the rule using extracted fields (if any).
    Returns (status, message, reasoning) tuple.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Text, and optionally some pre-extracted fields. Validate whether the rule passes or fails.
Please respond with JSON, with the keys: 'status' and 'reasoning'. The status should be one of: 'PASS', 'FAIL', 'SKIP'. The reasoning should be a short explanation of the reason for the status."""

    json_schema = {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "description": "The status of the rule. One of: 'PASS', 'FAIL', 'SKIP'.",
                "enum": ["PASS", "FAIL", "SKIP"]
            },
            "reasoning": {
                "type": "string",
                "description": "The reasoning for the status.",
            },
        },
        "required": ["status", "reasoning"]
    }

    # Format notes as string
    notes_text = '\n'.join(f"- {note}" for note in notes) if notes else 'None'

    # Build message content
    content = [
        {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
        {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
    ]

    if extracted_fields:
        content.append({"type": "text", "text": f"Extracted fields:\n\n{json.dumps(extracted_fields)}"})

    content.append({"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"})

    print(f'System prompt: {system_prompt}')
    print(f'User message: {content}')

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [{"role": "user", "content": content}]
    }

    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1
    )

    if response_json is None:
        return 'ERROR', 'No JSON found in Claude response', ''

    status = response_json['status']
    reasoning = response_json['reasoning']

    return status, f"{status} - {reasoning}", reasoning


def store_results(results, env_config):
    """Store validation results in DynamoDB."""
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        item = json.loads(json.dumps(results), parse_float=Decimal)

        item['pk'] = f"DOC#{results['document_id']}"
        item['sk'] = f"VALIDATION#{results['validation_timestamp']}"
        item['gsi1pk'] = f"DATE#{results['validation_timestamp'][:10]}"
        item['gsi1sk'] = f"DOC#{results['document_id']}"
        item['gsi2pk'] = f"RUN#{results['validation_run_id']}"
        item['gsi2sk'] = f"DOC#{results['document_id']}"
        item['organization_id'] = results.get('organization_id', 'unknown')

        table.put_item(Item=item)

        print(f"Stored results for document {results['document_id']} in DynamoDB (run: {results['validation_run_id']})")

    except Exception as e:
        print(f"Error storing results in DynamoDB: {str(e)}")


def generate_csv_from_dynamodb(validation_run_id, env_config):
    """
    Query all validation results for this run from DynamoDB and generate CSV.

    CSV format: One row per service_id with separate columns for each rule's status.
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        response = table.query(
            IndexName='gsi2',
            KeyConditionExpression='gsi2pk = :run_key',
            ExpressionAttributeValues={
                ':run_key': f"RUN#{validation_run_id}"
            }
        )

        items = response.get('Items', [])
        print(f"Found {len(items)} documents for validation run {validation_run_id}")

        all_rule_names = set()
        for item in items:
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                all_rule_names.add(rule_name)

        sorted_rule_names = sorted(all_rule_names)

        output = io.StringIO()
        writer = csv.writer(output)
        header = ['Service ID', 'Consumer Name'] + sorted_rule_names
        writer.writerow(header)

        for item in items:
            field_values = item.get('field_values', {})
            service_id = field_values.get('document_id', 'N/A') if field_values else 'N/A'
            consumer_name = field_values.get('consumer_name', 'N/A') if field_values else 'N/A'

            rule_statuses = {}
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                status = rule.get('status', 'N/A')
                message = rule.get('message', '')

                if status == 'PASS':
                    rule_statuses[rule_name] = 'PASS'
                elif message and message != status:
                    if message.upper().startswith(status.upper()):
                        rule_statuses[rule_name] = message
                    else:
                        rule_statuses[rule_name] = f"{status}: {message}"
                else:
                    rule_statuses[rule_name] = status

            row = [service_id, consumer_name]
            for rule_name in sorted_rule_names:
                row.append(rule_statuses.get(rule_name, 'N/A'))

            writer.writerow(row)

        csv_content = output.getvalue()
        print(f"Generated CSV with {len(items)} rows (one per service_id) and {len(sorted_rule_names)} rule columns")
        return csv_content

    except Exception as e:
        print(f"Error generating CSV from DynamoDB: {str(e)}")
        raise e


def save_csv_to_s3(csv_content, validation_run_id, env_config):
    """Save CSV report to S3."""
    try:
        csv_key = f"validation-reports/{validation_run_id}-validation-report.csv"

        s3_client.put_object(
            Bucket=env_config['BUCKET_NAME'],
            Key=csv_key,
            Body=csv_content,
            ContentType='text/csv'
        )

        print(f"Saved CSV report to s3://{env_config['BUCKET_NAME']}/{csv_key}")

    except Exception as e:
        print(f"Error saving CSV to S3: {str(e)}")
        raise e
