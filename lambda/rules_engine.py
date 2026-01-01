import json
import boto3
import os
from datetime import datetime
from decimal import Decimal

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON documents against configurable rules

    Configuration can be passed via environment variables or event parameters.
    Event parameters take precedence over environment variables.
    """
    # Load configuration from environment variables
    env_config = {
        'BUCKET_NAME': os.environ.get('BUCKET_NAME'),
        'DYNAMODB_TABLE': os.environ.get('DYNAMODB_TABLE'),
        'DYNAMODB_IRP_TABLE': os.environ.get('DYNAMODB_IRP_TABLE'),
        'ORGANIZATION_ID': os.environ.get('ORGANIZATION_ID'),
        'TEXTRACT_PROCESSED': os.environ.get('TEXTRACT_PROCESSED')
    }

    # Override with event-level config if provided
    if 'config' in event:
        env_config.update(event['config'])

    try:
        config = load_configuration(env_config['ORGANIZATION_ID'], env_config)
        # Process all files in processed folder
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
                process_file(env_config['BUCKET_NAME'], key, config, env_config['ORGANIZATION_ID'], env_config)

        return {
            'statusCode': 200,
            'body': json.dumps('Validation completed successfully')
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise e


def load_configuration(org_id, env_config):
    """
    Load organization-specific rule configuration from S3
    """
    try:
        # Load from S3
        config_key = f"validation-rules/{org_id}.json"
        response = s3_client.get_object(Bucket=env_config['BUCKET_NAME'], Key=config_key)
        config = json.loads(response['Body'].read().decode('utf-8'))

        print(f"Loaded configuration for {org_id}: {len(config.get('rules', []))} rules")
        return config

    except s3_client.exceptions.NoSuchKey:
        print(f"No configuration found for {org_id}, using default")
        # Return empty config - will skip all rules
        return {
            'organization_id': org_id,
            'field_mappings': {},
            'value_lists': {},
            'rules': []
        }
    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        raise e


def process_file(bucket, key, config, org_id, env_config):
    """
    Process a single JSON file from S3
    """
    try:
        # Get the JSON file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        # Validate the document
        results = validate_document(data, key, config, org_id, env_config)

        # Store results in DynamoDB
        store_results(results, env_config)

        # Also save results to S3 for reference
        results_key = key.replace(env_config['TEXTRACT_PROCESSED'], 'validation-results/').replace('.json', '-validation.json')
        s3_client.put_object(
            Bucket=bucket,
            Key=results_key,
            Body=json.dumps(results, indent=2, default=str),
            ContentType='application/json'
        )

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


def validate_document(data, filename, config, org_id, env_config):
    """
    Run all validation rules against a document

    Extracts field values from text using field_mappings.
    The text field contains all extracted content from the encounter.
    """
    # Extract fields from text using field_mappings
    text = data.get('text', '')
    field_mappings = config.get('field_mappings', {})
    fields = extract_fields_from_text(text, field_mappings)

    # Run validation rules
    rule_results = []

    for rule_config in config.get('rules', []):
        if not rule_config.get('enabled', True):
            continue

        result = evaluate_rule(rule_config, fields, data, env_config)
        rule_results.append(result)

    # Calculate summary
    passed = sum(1 for r in rule_results if r['status'] == 'PASS')
    failed = sum(1 for r in rule_results if r['status'] == 'FAIL')
    skipped = sum(1 for r in rule_results if r['status'] == 'SKIP')

    return {
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


def extract_fields(forms, field_mappings):
    """
    Extract field values from forms dictionary using field mappings
    Handles fields with # suffix (e.g., "ID:#1") by checking original_key
    """
    fields = {}

    # Debug: Log available form keys
    if forms:
        print(f"DEBUG: Available form keys: {list(forms.keys())[:10]}")  # Show first 10 keys
        print(f"DEBUG: Total forms extracted: {len(forms)}")
    else:
        print("DEBUG: No forms found in data")

    for field_name, form_key in field_mappings.items():
        value = None
        confidence = 0

        # Try to get from forms directly
        if forms and form_key in forms:
            value = forms[form_key].get('value', '')
            confidence = forms[form_key].get('confidence', 0)
            print(f"DEBUG: Found {field_name} via direct match: {form_key} = {value}")
        else:
            # Check if any form has this as its original_key (handles # suffix)
            if forms:
                for actual_key, form_data in forms.items():
                    original_key = form_data.get('original_key', actual_key)
                    if original_key == form_key:
                        value = form_data.get('value', '')
                        confidence = form_data.get('confidence', 0)
                        print(f"DEBUG: Found {field_name} via original_key match: {form_key} = {value}")
                        break

        if value is None:
            print(f"DEBUG: Field '{field_name}' not found (looking for key: '{form_key}')")

        fields[field_name] = value
        fields[f"{field_name}_confidence"] = confidence

    return fields


def add_computed_fields(fields, forms, computed_fields):
    """
    Add computed fields based on organization-specific logic defined in config
    Supports: priority_fields, template, concat operations
    """
    for computed_field_name, config in computed_fields.items():
        computed_value = None

        # Priority fields: use first non-null value
        if 'priority_fields' in config:
            priority_fields = config.get('priority_fields', [])
            for form_key in priority_fields:
                # Try direct match first
                if form_key in forms:
                    value = forms[form_key].get('value', '')
                    if value:
                        computed_value = value.strip()
                        break
                else:
                    # Check if any form has this as its original_key
                    for actual_key, form_data in forms.items():
                        original_key = form_data.get('original_key', actual_key)
                        if original_key == form_key:
                            value = form_data.get('value', '')
                            if value:
                                computed_value = value.strip()
                                break
                    if computed_value:
                        break

        # Template: allows string formatting with field references
        if 'template' in config:
            template = config['template']

            # If no computed_value from priority_fields, start with empty
            if not computed_value:
                computed_value = ''

            # Check if value already looks like a full datetime (has date separators)
            has_date_separator = '/' in computed_value or '-' in computed_value

            if not has_date_separator:
                # Replace {field_name} with actual field values (including other computed fields)
                for field_name, field_value in fields.items():
                    if field_value:
                        template = template.replace(f'{{{field_name}}}', str(field_value))

                # Replace {value} placeholder if it exists
                template = template.replace('{value}', computed_value)

                # If template still has placeholders, keep original value
                if '{' not in template:
                    computed_value = template
            else:
                # Already has a date, don't apply template
                pass

        fields[computed_field_name] = computed_value

    return fields


def evaluate_rule(rule_config, fields, data=None, env_config=None):
    """
    Evaluate a single rule against the document fields
    Supports only LLM-based rule types: 'llm' and 'llm_irp'
    """
    rule_type = rule_config.get('type', 'llm')

    result = {
        'rule_id': rule_config.get('id'),
        'rule_name': rule_config.get('name'),
        'category': rule_config.get('category')
    }

    try:
        if rule_type == 'llm':
            status, message = evaluate_llm_rule(rule_config, fields, data)
        elif rule_type == 'llm_irp':
            status, message = evaluate_llm_irp_rule(rule_config, fields, data, env_config['ORGANIZATION_ID'], env_config)
        else:
            status = 'SKIP'
            message = f'Unsupported rule type: {rule_type}. Only "llm" and "llm_irp" are supported.'

        result['status'] = status
        result['message'] = format_message(message, fields, rule_config)

    except Exception as e:
        result['status'] = 'ERROR'
        result['message'] = f'Error evaluating rule: {str(e)}'

    return result


def evaluate_llm_rule(rule_config, fields, data=None):
    """
    Evaluate a rule using AWS Bedrock LLM
    """
    llm_config = rule_config.get('llm_config', {})
    messages_config = rule_config.get('messages', {})

    model_id = llm_config.get('model_id', 'openai.gpt-oss-120b-1:0')
    system_prompt = llm_config.get('system_prompt', '')
    question = llm_config.get('question', '')

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
    print(f"DEBUG LLM: User message length: {len(user_message)} characters")

    try:
        # Call Bedrock with OpenAI model
        request_body = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": user_message
                }
            ],
            "max_tokens": 500,
            "temperature": 0
        }

        print(f"DEBUG LLM: Calling Bedrock with model: {model_id}")
        print(f"DEBUG LLM: System prompt length: {len(system_prompt)} characters")

        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )

        response_body = json.loads(response['body'].read())
        llm_response = response_body['choices'][0]['message']['content']

        print(f"DEBUG LLM: Raw response from Bedrock: {llm_response[:200]}...")  # First 200 chars

        # Remove <reasoning> tags if present
        if '<reasoning>' in llm_response:
            # Extract content after </reasoning> tag
            llm_response = llm_response.split('</reasoning>')[-1].strip()
            print(f"DEBUG LLM: After removing reasoning tags: {llm_response[:200]}...")

        # Clean up Unicode characters
        # Replace smart quotes and other Unicode punctuation with ASCII equivalents
        unicode_replacements = {
            '\u2019': "'",  # Right single quotation mark
            '\u2018': "'",  # Left single quotation mark
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
            '\u2011': '-',  # Non-breaking hyphen
            '\u2010': '-',  # Hyphen
        }

        for unicode_char, ascii_char in unicode_replacements.items():
            llm_response = llm_response.replace(unicode_char, ascii_char)

        # Parse LLM response to extract status and reasoning
        # Expected format: "Pass - explanation" or "Fail - explanation" or "Skip - explanation"
        llm_response_lower = llm_response.lower().strip()

        if llm_response_lower.startswith('pass'):
            status = 'PASS'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        elif llm_response_lower.startswith('fail'):
            status = 'FAIL'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        elif llm_response_lower.startswith('skip'):
            status = 'SKIP'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        else:
            # If format doesn't match, treat as reasoning for manual review
            status = 'SKIP'
            reasoning = f'LLM response format unclear: {llm_response}'

        print(f"DEBUG LLM: Parsed status: {status}")
        print(f"DEBUG LLM: Parsed reasoning: {reasoning[:100]}...")

        # Store reasoning in fields for message formatting
        fields['llm_reasoning'] = reasoning

        final_message = messages_config.get(status.lower(), reasoning)
        print(f"DEBUG LLM: Final message: {final_message}")

        return status, final_message

    except Exception as e:
        print(f"DEBUG LLM ERROR: {str(e)}")
        import traceback
        print(f"DEBUG LLM TRACEBACK: {traceback.format_exc()}")
        return 'ERROR', f'LLM evaluation error: {str(e)}'


def evaluate_llm_irp_rule(rule_config, fields, data, org_id, env_config):
    """
    Evaluate a rule that compares chart against consumer's IRP using AWS Bedrock LLM
    Only runs when chart consumer name matches an IRP consumer name and organization matches
    """
    llm_config = rule_config.get('llm_config', {})
    messages_config = rule_config.get('messages', {})

    # Check if service_type should be excluded from LLM IRP rule
    service_type = fields.get('service_type', '')
    excluded_service_types = [
        'No-Show / Cancel',
        'Suicide Screening',
        'General Note',
        'DLA-20',
        'IRP Prep & FPSA'
    ]

    if service_type in excluded_service_types:
        return 'SKIP', messages_config.get('skip', f'Service type "{service_type}" is excluded from IRP validation')

    # Check if consumer_name field exists in the chart
    consumer_name = fields.get('consumer_name')
    if not consumer_name:
        return 'SKIP', messages_config.get('skip', 'Consumer name not found in chart')

    # Query DynamoDB IRP table for matching consumer and organization
    try:
        irp_data = get_irp_for_consumer(consumer_name, org_id, env_config)

        if not irp_data:
            return 'SKIP', messages_config.get('skip', f'No IRP found for consumer: {consumer_name}')

        # Get plan of care text from IRP
        plan_of_care_text = irp_data.get('plan_of_care_text', '')

        if not plan_of_care_text:
            return 'SKIP', messages_config.get('skip', 'IRP found but plan of care text is empty')

    except Exception as e:
        print(f"Error retrieving IRP: {str(e)}")
        return 'SKIP', f'Error retrieving IRP: {str(e)}'

    # Get the chart text
    chart_text = ''
    if data:
        chart_text = data.get('text', '')

    if not chart_text:
        chart_text = json.dumps(fields, indent=2)

    # Prepare LLM request
    model_id = llm_config.get('model_id', 'openai.gpt-oss-120b-1:0')
    system_prompt = llm_config.get('system_prompt', '')
    question = llm_config.get('question', '')

    # Construct the user message with both IRP and chart
    user_message = f"""{question}

IRP Plan of Care:
{plan_of_care_text}

Chart Data:
{chart_text}"""

    try:
        # Call Bedrock with OpenAI model
        request_body = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": user_message
                }
            ],
            "max_tokens": 500,
            "temperature": 0
        }

        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )

        response_body = json.loads(response['body'].read())
        llm_response = response_body['choices'][0]['message']['content']

        # Remove <reasoning> tags if present
        if '<reasoning>' in llm_response:
            llm_response = llm_response.split('</reasoning>')[-1].strip()

        # Clean up Unicode characters
        unicode_replacements = {
            '\u2019': "'",  # Right single quotation mark
            '\u2018': "'",  # Left single quotation mark
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
            '\u2011': '-',  # Non-breaking hyphen
            '\u2010': '-',  # Hyphen
        }

        for unicode_char, ascii_char in unicode_replacements.items():
            llm_response = llm_response.replace(unicode_char, ascii_char)

        # Parse LLM response to extract status and reasoning
        llm_response_lower = llm_response.lower().strip()

        if llm_response_lower.startswith('pass'):
            status = 'PASS'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        elif llm_response_lower.startswith('fail'):
            status = 'FAIL'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        elif llm_response_lower.startswith('skip'):
            status = 'SKIP'
            reasoning = llm_response.split('-', 1)[1].strip() if '-' in llm_response else llm_response
        else:
            status = 'SKIP'
            reasoning = f'LLM response format unclear: {llm_response}'

        # Store reasoning in fields for message formatting
        fields['llm_reasoning'] = reasoning

        return status, messages_config.get(status.lower(), reasoning)

    except Exception as e:
        print(f"Error calling LLM for IRP rule: {str(e)}")
        return 'ERROR', f'LLM IRP evaluation error: {str(e)}'


def get_irp_for_consumer(consumer_name, org_id, env_config):
    """
    Query DynamoDB IRP table to find IRP for a specific consumer
    Returns the most recent IRP if multiple exist
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_IRP_TABLE'])

        consumer_key = f'CONSUMER#{consumer_name}'
        print(f"Querying IRP table for consumer: {consumer_name} (key: {consumer_key})")

        # Query using GSI1 (consumer name index)
        response = table.query(
            IndexName='gsi1',
            KeyConditionExpression='gsi1pk = :consumer_key',
            ExpressionAttributeValues={
                ':consumer_key': consumer_key,
                ':org_id': org_id
            },
            FilterExpression='organization_id = :org_id',
            ScanIndexForward=False,  # Sort descending to get most recent first
            Limit=1
        )

        items = response.get('Items', [])
        print(f"Found {len(items)} IRP(s) for consumer: {consumer_name}")

        if items:
            irp = items[0]
            print(f"Using IRP with start date: {irp.get('irp_start_date')}, end date: {irp.get('irp_end_date')}")
            return irp

        return None

    except Exception as e:
        print(f"Error querying IRP table: {str(e)}")
        raise e


def format_message(message, fields, rule_config):
    """
    Format message template with field values
    """
    if not message:
        return message

    # Replace field placeholders
    for key, value in fields.items():
        placeholder = '{' + key + '}'
        if placeholder in message:
            message = message.replace(placeholder, str(value) if value else 'N/A')

    # Replace special placeholders
    if '{actual_value}' in message:
        # Try to get the actual value from the then condition
        condition = rule_config.get('condition', {})
        then_field = condition.get('then', {}).get('field')
        if then_field:
            actual = fields.get(then_field, 'N/A')
            message = message.replace('{actual_value}', str(actual))

    return message


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

        # Add organization key for filtering
        item['organization_id'] = results.get('organization_id', 'unknown')

        table.put_item(Item=item)

        print(f"Stored results for document {results['document_id']} in DynamoDB")

    except Exception as e:
        print(f"Error storing results in DynamoDB: {str(e)}")
        # Don't raise - continue even if DynamoDB fails