"""
Rules Engine RAG Lambda - Validates documents against configurable LLM rules.

Uses Claude Sonnet 4.5 via AWS Bedrock for structured JSON rule evaluation.
Loads organization configuration and rules from DynamoDB.

This module is the Lambda entry point. Core functionality is split into:
- rate_limiter.py: Rate limiting for Bedrock API calls
- bedrock_client.py: Claude model invocation with throttle handling
- document_validator.py: Batched LLM validation logic
- results_handler.py: DynamoDB storage and CSV reporting
- field_extractor.py: Text field extraction
"""

import json
import os
from datetime import datetime

import boto3

from multi_org_config import load_org_rules, build_env_config
from document_validator import validate_document
from results_handler import store_results, generate_csv_from_dynamodb, save_csv_to_s3

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


def invoke_continuation(org_id, validation_run_id, processed_files, env_config):
    """Invoke another Lambda to continue processing remaining files."""
    function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

    payload = {
        'organization_id': org_id,
        'validation_run_id': validation_run_id,
        'processed_files': list(processed_files),
        '_env_config': env_config,  # Pass config to avoid re-fetching
    }

    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Async invocation
        Payload=json.dumps(payload),
    )
    print(f"Invoked continuation Lambda with {len(processed_files)} already processed files")


def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON documents against configurable rules.

    Expects event with:
    - organization_id: Organization ID (looks up bucket and rules from DynamoDB)
    - validation_run_id: (optional) Continue existing run
    - processed_files: (optional) List of already processed file keys
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise ValueError("organization_id is required in event")

    # Support continuation from previous invocation
    validation_run_id = event.get('validation_run_id') or datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    processed_files = set(event.get('processed_files', []))
    is_continuation = len(processed_files) > 0

    if is_continuation:
        print(f"Continuing validation run: {validation_run_id} ({len(processed_files)} files already processed)")
    else:
        print(f"Starting validation run: {validation_run_id}")

    print(f"Loading configuration for organization: {org_id}")
    env_config = event.get('_env_config') or build_env_config(org_id)
    config = load_org_rules(org_id)

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

        # Get all JSON files to process
        all_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.json') and '/raw/' not in obj['Key']
        ]
        remaining_files = [f for f in all_files if f not in processed_files]

        print(f"Total files: {len(all_files)}, Already processed: {len(processed_files)}, Remaining: {len(remaining_files)}")

        # Process files until near timeout
        for key in remaining_files:
            # Check remaining time (leave 2 min buffer for cleanup/continuation)
            remaining_ms = context.get_remaining_time_in_millis()
            if remaining_ms < 120_000:  # Less than 2 minutes left
                print(f"Approaching timeout ({remaining_ms}ms remaining). Invoking continuation...")
                invoke_continuation(org_id, validation_run_id, processed_files, env_config)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'continuing',
                        'validation_run_id': validation_run_id,
                        'processed': len(processed_files),
                        'remaining': len(remaining_files) - len([f for f in remaining_files if f in processed_files])
                    })
                }

            process_file(env_config['BUCKET_NAME'], key, config, org_id, env_config, validation_run_id)
            processed_files.add(key)

        # All files processed - generate report
        print(f"All {len(processed_files)} files processed. Generating CSV report...")
        csv_report = generate_csv_from_dynamodb(validation_run_id, env_config)
        save_csv_to_s3(csv_report, validation_run_id, env_config)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'complete',
                'message': 'Validation completed successfully',
                'validation_run_id': validation_run_id,
                'total_processed': len(processed_files)
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
