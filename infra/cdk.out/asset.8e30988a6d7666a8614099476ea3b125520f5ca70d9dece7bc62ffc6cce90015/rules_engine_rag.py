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


def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON/CSV documents against configurable rules.

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
        # Process files from multiple source folders
        folders_to_process = [
            env_config['TEXTRACT_PROCESSED'],  # Existing: textract-processed/
            'csv-staging/',                     # NEW: staged CSV charts from SFTP
        ]

        files_found = False
        for folder in folders_to_process:
            response = s3_client.list_objects_v2(
                Bucket=env_config['BUCKET_NAME'],
                Prefix=folder
            )

            if 'Contents' not in response:
                print(f"No files found in {folder}")
                continue

            for obj in response['Contents']:
                key = obj['Key']
                # Support both JSON and CSV files, skip raw files
                if (key.endswith('.json') or key.endswith('.csv')) and '/raw/' not in key:
                    files_found = True
                    process_file(env_config['BUCKET_NAME'], key, config, org_id, env_config, validation_run_id)

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
        response = s3_client.get_object(Bucket=bucket, Key=key)
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

        # Archive the processed file to prevent reprocessing
        # Handle files from different source folders
        if key.startswith('csv-staging/'):
            archive_key = key.replace('csv-staging/', 'archived/csv/')
        else:
            archive_key = key.replace(env_config['TEXTRACT_PROCESSED'], 'archived/validation/')

        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=archive_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"Archived {key} to {archive_key}")

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        raise e
