import json
import boto3
import os
import re
from datetime import datetime
from decimal import Decimal

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def load_irp_configuration(org_id, env_config):
    """
    Load organization-specific IRP configuration from S3
    """
    try:
        # Load from S3
        config_key = f"irp-config/{org_id}.json"
        response = s3_client.get_object(Bucket=env_config['BUCKET_NAME'], Key=config_key)
        config = json.loads(response['Body'].read().decode('utf-8'))

        print(f"Loaded IRP configuration for {org_id}: {config.get('organization_name', org_id)}")
        return config

    except s3_client.exceptions.NoSuchKey:
        print(f"No IRP configuration found for {org_id}, using default field mappings")
        # Return default config
        return {
            'organization_id': org_id,
            'field_mappings': {
                'consumer_name': 'Consumer Name:',
                'irp_start_date': 'IRP Start Date:',
                'irp_end_date': 'IRP End Date:'
            }
        }
    except Exception as e:
        print(f"Error loading IRP configuration: {str(e)}")
        raise e


def lambda_handler(event, context):
    """
    Lambda function to process IRP (Individual Recovery Plan) documents
    Extracts key information and stores in DynamoDB irp table

    Configuration can be passed via environment variables or event parameters.
    Event parameters take precedence over environment variables.
    """
    # Load configuration from environment variables
    env_config = {
        'BUCKET_NAME': os.environ.get('BUCKET_NAME'),
        'DYNAMODB_TABLE': os.environ.get('DYNAMODB_IRP_TABLE'),
        'ORGANIZATION_ID': os.environ.get('ORGANIZATION_ID'),
        'TEXTRACT_PROCESSED': os.environ.get('TEXTRACT_PROCESSED_IRP')
    }

    # Override with event-level config if provided
    if 'config' in event:
        env_config.update(event['config'])

    try:
        # Load organization-specific configuration
        config = load_irp_configuration(env_config['ORGANIZATION_ID'], env_config)

        # Process all files in IRP processed folder
        response = s3_client.list_objects_v2(
            Bucket=env_config['BUCKET_NAME'],
            Prefix=env_config['TEXTRACT_PROCESSED']
        )

        if 'Contents' not in response:
            return {
                'statusCode': 200,
                'body': json.dumps('No IRP files to process')
            }

        files_to_process = [obj['Key'] for obj in response['Contents']
                           if obj['Key'].endswith('.json') and obj['Key'] != env_config['TEXTRACT_PROCESSED']]

        print(f"Found {len(files_to_process)} IRP files to process: {files_to_process}")

        for key in files_to_process:
            print(f"Processing file: {key}")
            process_irp_file(env_config['BUCKET_NAME'], key, config, env_config)

        return {
            'statusCode': 200,
            'body': json.dumps(f'IRP processing completed successfully. Processed {len(files_to_process)} files.')
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise e


def process_irp_file(bucket, key, config, env_config):
    """
    Process a single IRP JSON file from S3
    """
    try:
        # Get the JSON file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))

        # Extract IRP fields using configuration
        irp_data = extract_irp_data(data, key, config, env_config)

        # Store in DynamoDB
        store_irp(irp_data, env_config)

        # Move the processed file to archive
        archive_key = key.replace(env_config['TEXTRACT_PROCESSED'], 'archived/irp/')
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=archive_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)

        print(f"Successfully processed IRP: {irp_data['consumer_name']}")

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e


def extract_field_with_fallback(forms, text, field_name, field_mapping, text_patterns):
    """
    Extract a field value, falling back to text pattern matching if not found in forms
    """
    # Try to get from forms first
    value = forms.get(field_mapping, {}).get('value', '')

    # If not found and text patterns are configured, try pattern matching
    if not value and field_name in text_patterns:
        patterns = text_patterns[field_name]
        if not isinstance(patterns, list):
            patterns = [patterns]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                # Return the first capturing group if it exists, otherwise the whole match
                value = match.group(1) if match.groups() else match.group(0)
                value = value.strip()
                print(f"Extracted '{field_name}' using text pattern: {value}")
                break

    return value


def extract_irp_data(data, filename, config, env_config):
    """
    Extract relevant fields from IRP document using organization-specific config
    Supports fallback to text pattern matching when key-value pairs are not found
    """
    forms = data.get('forms', {})
    text = data.get('text', '')
    field_mappings = config.get('field_mappings', {})
    text_patterns = config.get('text_patterns', {})

    # Extract fields using configuration mappings with text pattern fallback
    consumer_name = extract_field_with_fallback(
        forms, text, 'consumer_name',
        field_mappings.get('consumer_name', 'Consumer Name:'),
        text_patterns
    )
    irp_start_date = extract_field_with_fallback(
        forms, text, 'irp_start_date',
        field_mappings.get('irp_start_date', 'Start Date:'),
        text_patterns
    )
    irp_end_date = extract_field_with_fallback(
        forms, text, 'irp_end_date',
        field_mappings.get('irp_end_date', 'End Date:'),
        text_patterns
    )

    # Extract additional optional fields from config with text pattern fallback
    document_id = ''
    case_manager = ''
    diagnosis = ''
    goals = ''

    if 'document_id' in field_mappings:
        document_id = extract_field_with_fallback(
            forms, text, 'document_id',
            field_mappings.get('document_id', ''),
            text_patterns
        )
    if 'case_manager' in field_mappings:
        case_manager = extract_field_with_fallback(
            forms, text, 'case_manager',
            field_mappings.get('case_manager', ''),
            text_patterns
        )
    if 'diagnosis' in field_mappings:
        diagnosis = extract_field_with_fallback(
            forms, text, 'diagnosis',
            field_mappings.get('diagnosis', ''),
            text_patterns
        )
    if 'goals' in field_mappings:
        goals = extract_field_with_fallback(
            forms, text, 'goals',
            field_mappings.get('goals', ''),
            text_patterns
        )

    # Get the full text of the document as plan of care text
    plan_of_care_text = data.get('text', '')

    # Generate a unique IRP ID based on consumer name, start date, and timestamp
    # This ensures uniqueness even for same consumer with same start date processed quickly
    timestamp = datetime.utcnow().isoformat()
    consumer_slug = consumer_name.replace(' ', '-').lower() if consumer_name else 'unknown'
    start_date_slug = irp_start_date.replace('/', '-') if irp_start_date else 'no-date'
    irp_id = f"{consumer_slug}-{start_date_slug}-{timestamp}"

    irp_data = {
        'irp_id': irp_id,
        'organization_id': env_config['ORGANIZATION_ID'],
        'consumer_name': consumer_name,
        'irp_start_date': irp_start_date,
        'irp_end_date': irp_end_date,
        'plan_of_care_text': plan_of_care_text,
        'filename': filename,
        'processed_timestamp': timestamp,
        'forms': forms  # Store all form fields for reference
    }

    # Add optional fields if they exist
    if document_id:
        irp_data['document_id'] = document_id
    if case_manager:
        irp_data['case_manager'] = case_manager
    if diagnosis:
        irp_data['diagnosis'] = diagnosis
    if goals:
        irp_data['goals'] = goals

    return irp_data


def store_irp(irp_data, env_config):
    """
    Store IRP data in DynamoDB
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        # Convert to DynamoDB format
        item = json.loads(json.dumps(irp_data), parse_float=Decimal)

        # Add partition key and sort key
        # Use consumer name in pk to ensure uniqueness if table doesn't have a sort key
        item['pk'] = f"ORG#{irp_data['organization_id']}#CONSUMER#{irp_data['consumer_name']}"
        item['sk'] = f"IRP#{irp_data['irp_id']}"

        # Add GSI keys for querying by consumer
        item['gsi1pk'] = f"CONSUMER#{irp_data['consumer_name']}"
        item['gsi1sk'] = f"DATE#{irp_data['irp_start_date']}"

        # Add GSI keys for querying by date range
        item['gsi2pk'] = f"ORG#{irp_data['organization_id']}"
        item['gsi2sk'] = f"STARTDATE#{irp_data['irp_start_date']}"

        print(f"Storing IRP with keys - pk: {item['pk']}, sk: {item['sk']}, gsi1pk: {item['gsi1pk']}, gsi1sk: {item['gsi1sk']}")

        table.put_item(Item=item)

        print(f"Successfully stored IRP for {irp_data['consumer_name']} (IRP ID: {irp_data['irp_id']}) in DynamoDB")

    except Exception as e:
        print(f"Error storing IRP in DynamoDB: {str(e)}")
        raise e
