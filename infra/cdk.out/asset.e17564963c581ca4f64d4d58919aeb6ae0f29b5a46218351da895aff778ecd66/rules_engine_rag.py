"""
Rules Engine RAG Lambda - Validates documents against configurable LLM rules.

Uses Claude Sonnet 4.5 via AWS Bedrock for structured JSON rule evaluation.
Loads organization configuration and rules from DynamoDB.

This module is the Lambda entry point. Core functionality is split into:
- bedrock_client.py: Claude model invocation with JSON extraction
- document_validator.py: Per-rule validation with multi-threading
- results_handler.py: DynamoDB storage and CSV reporting
- field_extractor.py: Text field extraction
"""

import json
import os
from datetime import datetime

import boto3

from multi_org_config import load_org_rules, build_env_config
from document_validator import validate_document
from results_handler import (
    store_results,
    aggregate_run_summary,
    store_run_summary,
    generate_csv_from_dynamodb,
    save_csv_to_s3,
)

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


def invoke_continuation(org_id, validation_run_id):
    """Invoke self to continue processing remaining files."""
    function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

    payload = {
        'organization_id': org_id,
        'validation_run_id': validation_run_id,
        'is_continuation': True,
    }

    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Async - don't wait
        Payload=json.dumps(payload),
    )
    print(f"Invoked continuation Lambda for run {validation_run_id}")


def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON/CSV documents against configurable rules.

    Expects event with:
    - organization_id: Organization ID (looks up bucket and rules from DynamoDB)
    - validation_run_id: (optional) ID for this validation run. If not provided, generates one.
                         Passing this ensures retries use the same run ID.
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise ValueError("organization_id is required in event")

    print(f"Loading configuration for organization: {org_id}")
    env_config = build_env_config(org_id)
    config = load_org_rules(org_id)

    # Use provided validation_run_id or generate a new one
    # This ensures retries use the same run ID for consistent reporting
    validation_run_id = event.get('validation_run_id') or datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    is_continuation = event.get('is_continuation', False)

    if is_continuation:
        print(f"Continuing validation run: {validation_run_id}")
    else:
        print(f"Starting validation run: {validation_run_id}")

    try:
        # Process files from multiple source folders
        folders_to_process = [
            env_config['TEXTRACT_PROCESSED'],  # Existing: textract-processed/
            'csv-staging/',                     # NEW: staged CSV charts from SFTP
        ]

        files_found = False
        for folder in folders_to_process:
            print(f"DEBUG: Listing objects in bucket={env_config['BUCKET_NAME']}, prefix={folder}")

            response = s3_client.list_objects_v2(
                Bucket=env_config['BUCKET_NAME'],
                Prefix=folder
            )

            print(f"DEBUG: list_objects_v2 response - KeyCount={response.get('KeyCount', 'N/A')}, IsTruncated={response.get('IsTruncated', 'N/A')}, NextContinuationToken={response.get('NextContinuationToken', 'None')}")

            if 'Contents' not in response:
                print(f"No files found in {folder}")
                continue

            print(f"DEBUG: Contents has {len(response['Contents'])} objects")

            eligible_count = 0
            skipped_count = 0
            for obj in response['Contents']:
                key = obj['Key']
                # Support both JSON and CSV files, skip raw files
                is_csv = key.endswith('.csv')
                is_json = key.endswith('.json')
                has_raw = '/raw/' in key
                is_eligible = (is_csv or is_json) and not has_raw

                if not is_eligible:
                    skipped_count += 1
                    print(f"DEBUG: SKIPPED key (is_csv={is_csv}, is_json={is_json}, has_raw={has_raw}): {key[-20:]}")  # Last 20 chars only

                if is_eligible:
                    # Check remaining time before processing (leave 2 min buffer)
                    remaining_ms = context.get_remaining_time_in_millis()
                    if remaining_ms < 120_000:
                        print(f"Timeout approaching ({remaining_ms}ms remaining). Invoking continuation...")
                        invoke_continuation(org_id, validation_run_id)
                        return {
                            'statusCode': 200,
                            'body': json.dumps({
                                'status': 'continuing',
                                'validation_run_id': validation_run_id
                            })
                        }

                    eligible_count += 1
                    files_found = True
                    process_file(env_config['BUCKET_NAME'], key, config, org_id, env_config, validation_run_id)

            print(f"DEBUG: Skipped {skipped_count} keys total")

            print(f"DEBUG: Processed {eligible_count} eligible files from {folder}")

        if not files_found:
            return {
                'statusCode': 200,
                'body': json.dumps('No files to validate')
            }

        # Store run summary for efficient UI querying
        print(f"Aggregating run summary for: {validation_run_id}")
        summary = aggregate_run_summary(validation_run_id, env_config)
        store_run_summary(validation_run_id, org_id, summary, env_config)

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
    """Process a single JSON or CSV file from S3."""
    try:
        # Move file to processing folder FIRST to prevent duplicate processing
        # if a continuation Lambda is invoked while we're still validating
        # Archive to a folder named with the validation_run_id for organization
        if key.startswith('csv-staging/'):
            filename = key.replace('csv-staging/', '')
            processing_key = f'processing/csv/{filename}'
            archive_key = f'archived/csv/{validation_run_id}/{filename}'
        else:
            filename = key.replace(env_config['TEXTRACT_PROCESSED'], '')
            processing_key = f'processing/validation/{filename}'
            archive_key = f'archived/validation/{validation_run_id}/{filename}'

        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=processing_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"Moved {key} to processing: {processing_key}")

        # Now read from processing location
        response = s3_client.get_object(Bucket=bucket, Key=processing_key)
        content = response['Body'].read().decode('utf-8')

        if key.endswith('.csv'):
            # For CSV files, use the raw content as text
            data = {'text': content}
        else:
            # For JSON files, parse as before
            data = json.loads(content)

        results = validate_document(data, key, config, org_id, validation_run_id)
        store_results(results, env_config)

        # Log document_id extraction for debugging
        doc_id = results.get('document_id', 'UNKNOWN')
        if doc_id == 'UNKNOWN' or doc_id is None:
            print(f"WARNING: No document_id extracted for {key}")

        print(f"Validated {key} (document_id={doc_id}): {results['summary']}")

        # Move from processing to final archive
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': processing_key},
            Key=archive_key
        )
        s3_client.delete_object(Bucket=bucket, Key=processing_key)
        print(f"Archived to {archive_key}")

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e
