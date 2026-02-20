import json
import re
import boto3
from datetime import datetime
from decimal import Decimal
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from multi_org_config import (
    build_env_config,
    load_org_rules
)

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Multi-organization Lambda function to validate processed JSON documents

    Event structure:
    {
        "organization_id": "community-health",  # Required
        "config": {  # Optional overrides
            "textract_processed": "textract-processed/"
        }
    }

    Returns:
        dict: Status with validation_run_id and processing results
    """
    # Validate required parameter
    if 'organization_id' not in event:
        raise ValueError("Missing required parameter: organization_id")

    org_id = event['organization_id']
    print(f"Processing validation for organization: {org_id}")

    # Load organization-specific configuration from DynamoDB
    env_config = build_env_config(org_id)

    # Override with event-level config if provided
    if 'config' in event:
        env_config.update(event['config'])

    # Generate unique validation run ID
    validation_run_id = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    print(f"Starting validation run: {validation_run_id} for org: {org_id}")

    try:
        # Load rules from DynamoDB
        config = load_org_rules(org_id)

        # Process all files in processed folder
        response = s3_client.list_objects_v2(
            Bucket=env_config['BUCKET_NAME'],
            Prefix=env_config['TEXTRACT_PROCESSED']
        )

        if 'Contents' not in response:
            return {
                'statusCode': 200,
                'organization_id': org_id,
                'message': 'No files to validate',
                'validation_run_id': validation_run_id,
                'files_processed': 0
            }

        files_processed = 0
        for obj in response['Contents']:
            key = obj['Key']

            # Skip manifest files, folders, and raw files
            if key.endswith('/') or key.endswith('-batch-complete.json') or '/raw/' in key:
                continue

            if key.endswith('.json'):
                process_file(env_config['BUCKET_NAME'], key, config, org_id, env_config, validation_run_id)
                files_processed += 1

        # Generate CSV report from DynamoDB results
        print(f"Generating CSV report for run: {validation_run_id}")
        csv_report = generate_csv_from_dynamodb(validation_run_id, env_config)
        save_csv_to_s3(csv_report, validation_run_id, env_config)

        return {
            'statusCode': 200,
            'organization_id': org_id,
            'message': 'Validation completed successfully',
            'validation_run_id': validation_run_id,
            'files_processed': files_processed
        }

    except Exception as e:
        print(f"Error validating for organization {org_id}: {str(e)}")
        raise e


# Removed handle_batch_manifest and handle_legacy_mode functions
# Lambda now only accepts organization_id in event for simplified invocation


def process_file(bucket, key, config, org_id, env_config, validation_run_id):
    """
    Process a single JSON file from S3
    """
    try:
        # Get the JSON file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        # Validate the document
        results = validate_document(data, key, config, org_id, env_config, validation_run_id)

        # Store results in DynamoDB
        store_results(results, env_config)

        print(f"Validated {key}: {results['summary']}")

        # Move the processed file to archive to prevent reprocessing
        archive_key = key.replace(env_config['TEXTRACT_PROCESSED'], 'archived/validation/')
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=archive_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"Moved {key} to {archive_key}")

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e


def validate_document(data, filename, config, org_id, env_config, validation_run_id):
    """
    Run all validation rules against a document

    Extracts field values from text using field_mappings.
    The text field contains all extracted content from the encounter.
    """
    # Extract fields from text using field_mappings
    text = data.get('text', '')
    field_mappings = config.get('field_mappings', {})
    fields = extract_fields_from_text(text, field_mappings)

    # Filter enabled rules
    enabled_rules = [rule for rule in config.get('rules', []) if rule.get('enabled', True)]

    if not enabled_rules:
        rule_results = []
    else:
        # Run validation rules in parallel using ThreadPoolExecutor
        print(f"Evaluating {len(enabled_rules)} rules in parallel...")
        max_workers = min(20, len(enabled_rules))  # Cap at 20 concurrent threads

        rule_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all rule evaluations
            future_to_rule = {
                executor.submit(
                    evaluate_rule,
                    rule_config,
                    fields,
                    data,
                    env_config
                ): rule_config
                for rule_config in enabled_rules
            }

            # Collect results as they complete
            for future in as_completed(future_to_rule):
                rule_config = future_to_rule[future]
                try:
                    result = future.result()
                    rule_results.append(result)
                except Exception as e:
                    # Handle any exceptions from rule evaluation
                    print(f"Error evaluating rule {rule_config.get('id')}: {str(e)}")
                    rule_results.append({
                        'rule_id': rule_config.get('id'),
                        'rule_name': rule_config.get('name'),
                        'category': rule_config.get('category'),
                        'status': 'ERROR',
                        'message': f'Exception during parallel execution: {str(e)}'
                    })

    # Calculate summary
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
    Extract field values from text using simple pattern matching

    For each field mapping (e.g., "document_id": "Consumer Service ID:"):
    - Searches for lines containing the key
    - Extracts the value after the key on the same line
    """
    fields = {}

    if not text or not field_mappings:
        return fields

    # Split text into lines
    lines = text.split('\n')

    # For each field mapping, search for the key in the text
    for field_name, key_pattern in field_mappings.items():
        value = None

        # Search through lines for the key pattern
        for line in lines:
            if key_pattern in line:
                # Extract the value after the key pattern
                parts = line.split(key_pattern, 1)
                if len(parts) > 1:
                    value = parts[1].strip()
                    print(f"Extracted {field_name}: '{value}' from key '{key_pattern}'")
                    break

        # Store the value (or None if not found)
        fields[field_name] = value if value else None

        if value is None:
            print(f"Field '{field_name}' not found (looking for key: '{key_pattern}')")

    return fields



def evaluate_rule(rule_config, fields, data=None, env_config=None):
    """
    Evaluate a single rule against the document fields
    Supports LLM-based rule type: 'llm'
    """
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
    """
    Extract JSON from a Bedrock Claude model response.
    Tries ```json``` code blocks first, then raw brace-matching extraction.
    """
    content_list = response_body.get("content", [])
    if not content_list:
        print("No 'content' field found or it's empty in the model response.")
        return None

    all_text = " ".join(
        block.get("text", "") for block in content_list if block.get("type") == "text"
    )

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
    """
    Invoke a Claude model via Bedrock with optional JSON extraction and retry logic.
    """
    if bedrock_client is None:
        bedrock_client = boto3.client('bedrock-runtime', region_name=region_name)

    response = bedrock_client.invoke_model(
        modelId=inference_profile_id,
        body=json.dumps(body),
        contentType='application/json',
        accept='application/json',
    )

    response_body = json.loads(response['body'].read())

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
    Uses the Anthropic Messages API format with JSON schema enforcement.
    """
    llm_config = rule_config.get('llm_config', {})
    model_id = 'global.anthropic.claude-opus-4-5-20251101-v1:0'
    system_prompt = llm_config.get('system_prompt', '')
    question = llm_config.get('question', '')

    system_prompt += "\nPlease respond with JSON, with the keys: 'status' and 'reasoning'. The status should be one of: 'PASS', 'FAIL', 'SKIP'. The reasoning should be a short explanation of the reason for the status."

    print(f"DEBUG LLM: Evaluating rule {rule_config.get('id')} - {rule_config.get('name')}")

    # Get the full text from the document
    chart_text = ''
    if data:
        chart_text = data.get('text', '')
        print(f"DEBUG LLM: Chart text length: {len(chart_text)} characters")

    # If no text available, fall back to fields
    if not chart_text:
        chart_text = json.dumps(fields, indent=2)
        print(f"DEBUG LLM: No text found, using fields JSON: {len(chart_text)} characters")

    # Construct the user message
    user_message = f"{question}\n\nChart Data:\n{chart_text}"

    try:
        json_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "description": "Schema for rule evaluation result with status and reasoning.",
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

        body = {
            "system": system_prompt,
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "temperature": 0.01,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": user_message
                        },
                        {
                            "type": "text",
                            "text": f"JSON schema:\n\n{json.dumps(json_schema)}"
                        },
                    ]
                }
            ]
        }

        print(f"DEBUG LLM: Calling Bedrock with model: {model_id}")

        response_json = invoke_claude_model(
            inference_profile_id=model_id,
            body=body,
            return_json_only=True,
            raise_on_error=True,
            retries=1
        )

        print(f"DEBUG LLM: Response: {response_json}")

        if response_json is None:
            return 'ERROR', 'No JSON found in Claude response', 'No JSON found in Claude response'

        status = response_json['status']
        reasoning = response_json['reasoning']

        return status, f"{status} - {reasoning}", reasoning

    except Exception as e:
        print(f"DEBUG LLM ERROR: {str(e)}")
        import traceback
        print(f"DEBUG LLM TRACEBACK: {traceback.format_exc()}")
        error_msg = f'LLM evaluation error: {str(e)}'
        return 'ERROR', error_msg, error_msg



def store_results(results, env_config):
    """
    Store validation results in DynamoDB
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        # Convert floats to Decimal for DynamoDB
        item = json.loads(json.dumps(results), parse_float=Decimal)

        # Add partition key
        item['pk'] = f"DOC#{results['document_id']}"
        item['sk'] = f"VALIDATION#{results['validation_timestamp']}"

        # Add GSI keys for querying
        item['gsi1pk'] = f"DATE#{results['validation_timestamp'][:10]}"
        item['gsi1sk'] = f"DOC#{results['document_id']}"

        # Add GSI2 keys for querying by validation run
        item['gsi2pk'] = f"RUN#{results['validation_run_id']}"
        item['gsi2sk'] = f"DOC#{results['document_id']}"

        # Add organization key for filtering
        item['organization_id'] = results.get('organization_id', 'unknown')

        table.put_item(Item=item)

        print(f"Stored results for document {results['document_id']} in DynamoDB (run: {results['validation_run_id']})")

    except Exception as e:
        print(f"Error storing results in DynamoDB: {str(e)}")
        # Don't raise - continue even if DynamoDB fails


def generate_csv_from_dynamodb(validation_run_id, env_config):
    """
    Query all validation results for this run from DynamoDB and generate CSV

    CSV format: One row per service_id with separate columns for each rule's status
    """
    import csv
    import io

    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        # Query using GSI2 to get all results for this validation run
        response = table.query(
            IndexName='gsi2',
            KeyConditionExpression='gsi2pk = :run_key',
            ExpressionAttributeValues={
                ':run_key': f"RUN#{validation_run_id}"
            }
        )

        items = response.get('Items', [])
        print(f"Found {len(items)} documents for validation run {validation_run_id}")

        # First pass: collect all unique rule names to create column headers
        all_rule_names = set()
        for item in items:
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                all_rule_names.add(rule_name)

        # Sort rule names for consistent column ordering
        sorted_rule_names = sorted(all_rule_names)

        # Generate CSV header: Service ID, Consumer Name, then one column per rule
        output = io.StringIO()
        writer = csv.writer(output)
        header = ['Service ID'] + sorted_rule_names
        writer.writerow(header)

        # Second pass: write one row per service_id
        for item in items:
            # Extract field values
            field_values = item.get('field_values', {})
            service_id = field_values.get('document_id', 'N/A') if field_values else 'N/A'

            # Build a map of rule_name -> status for this document
            rule_statuses = {}
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                status = rule.get('status', 'N/A')
                message = rule.get('message', '')

                # For PASS status, only show "PASS" without reasoning
                if status == 'PASS':
                    rule_statuses[rule_name] = 'PASS'
                # For FAIL/SKIP/ERROR with message
                elif message and message != status:
                    # If message already starts with status, don't duplicate it
                    if message.upper().startswith(status.upper()):
                        rule_statuses[rule_name] = message
                    else:
                        rule_statuses[rule_name] = f"{status}: {message}"
                else:
                    rule_statuses[rule_name] = status

            # Build row with status for each rule (in same order as header)
            row = [service_id]
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
    """
    Save CSV report to S3
    """
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