import json
import boto3
import os
from datetime import datetime
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')


def extract_org_id_from_bucket(bucket_name):
    """
    Extract organization ID from bucket name
    Expected format: penguin-health-{org-id}
    """
    if bucket_name.startswith('penguin-health-'):
        org_id = bucket_name.replace('penguin-health-', '', 1)
        print(f"Extracted organization ID: {org_id}")
        return org_id
    else:
        raise ValueError(f"Bucket name does not match expected pattern: {bucket_name}")


def lambda_handler(event, context):
    """
    Lambda function to process PDF files from to-be-processed folder
    using Textract async analysis and store results in processed folder
    Handles both regular documents and IRP documents

    This multi-org version extracts organization from S3 bucket name
    and propagates it through the processing pipeline.

    Supports two invocation modes:
    1. S3 Event Notification (recommended): Triggered when file uploaded to textract-to-be-processed/
    2. Scheduled/Manual: Scans folders for files to process
    """

    # Check if invoked by S3 event
    if 'Records' in event and event['Records']:
        # S3 Event mode - process single file
        return handle_s3_event(event, context)
    else:
        # Scheduled/Manual mode - scan folders
        return handle_batch_processing(event, context)


def handle_s3_event(event, context):
    """
    Handle S3 event notification for single file upload
    """
    try:
        # Extract S3 event details
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        file_key = unquote_plus(record['s3']['object']['key'])

        print(f"S3 Event received: bucket={bucket_name}, key={file_key}")

        # Extract organization ID from bucket name
        org_id = extract_org_id_from_bucket(bucket_name)

        # Build configuration from bucket
        config = {
            'BUCKET_NAME': bucket_name,
            'SNS_TOPIC_ARN': os.environ.get('SNS_TOPIC_ARN'),
            'SNS_ROLE_ARN': os.environ.get('SNS_ROLE_ARN'),
            'ORGANIZATION_ID': org_id
        }

        # Only process PDF files
        if not file_key.lower().endswith('.pdf'):
            print(f"Skipping non-PDF file: {file_key}")
            return {
                'statusCode': 200,
                'body': json.dumps('Skipped non-PDF file')
            }

        # Start Textract analysis
        job_id = start_textract_analysis(file_key, config)

        if job_id:
            # Store job metadata with organization ID
            store_job_metadata(file_key, job_id, config)

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Started processing file for organization {org_id}',
                    'organization_id': org_id,
                    'file_key': file_key,
                    'job_id': job_id
                })
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to start Textract analysis')
            }

    except Exception as e:
        print(f"Error in handle_s3_event: {str(e)}")
        raise e


def handle_batch_processing(event, context):
    """
    Handle scheduled/manual invocation - scan folders for files to process
    """
    # Load configuration from environment variables
    config = {
        'BUCKET_NAME': os.environ.get('BUCKET_NAME'),
        'SNS_TOPIC_ARN': os.environ.get('SNS_TOPIC_ARN'),
        'SNS_ROLE_ARN': os.environ.get('SNS_ROLE_ARN'),
        'TEXTRACT_TO_BE_PROCESSED': os.environ.get('TEXTRACT_TO_BE_PROCESSED', 'textract-to-be-processed/'),
        'TEXTRACT_IRP_FOLDER': os.environ.get('TEXTRACT_IRP_FOLDER', 'textract-to-be-processed/irp/')
    }

    # Override with event-level config if provided
    if 'config' in event:
        config.update(event['config'])

    # Extract organization ID from bucket name
    org_id = extract_org_id_from_bucket(config['BUCKET_NAME'])
    config['ORGANIZATION_ID'] = org_id

    try:
        # Process regular documents
        regular_response = s3_client.list_objects_v2(
            Bucket=config['BUCKET_NAME'],
            Prefix=config['TEXTRACT_TO_BE_PROCESSED'],
            Delimiter='/'  # Don't recurse into subfolders
        )

        # Process IRP documents
        irp_response = s3_client.list_objects_v2(
            Bucket=config['BUCKET_NAME'],
            Prefix=config['TEXTRACT_IRP_FOLDER']
        )

        # Combine both responses
        all_objects = []
        if 'Contents' in regular_response:
            all_objects.extend(regular_response['Contents'])
        if 'Contents' in irp_response:
            all_objects.extend(irp_response['Contents'])

        response = {'Contents': all_objects} if all_objects else {}

        if 'Contents' not in response:
            print("No files found in to-be-processed folder")
            return {
                'statusCode': 200,
                'body': json.dumps('No files to process')
            }

        processed_count = 0
        job_ids = []

        # Process each file
        for obj in response['Contents']:
            file_key = obj['Key']

            # Skip the folder itself
            if file_key == config['TEXTRACT_TO_BE_PROCESSED']:
                continue

            # Only process PDF files
            if not file_key.lower().endswith('.pdf'):
                print(f"Skipping non-PDF file: {file_key}")
                continue

            print(f"Starting async Textract analysis for: {file_key}")

            # Start async document analysis with Textract
            job_id = start_textract_analysis(file_key, config)

            if job_id:
                job_ids.append({
                    'job_id': job_id,
                    'file_key': file_key
                })
                processed_count += 1

                # Store job metadata for tracking (includes organization_id)
                store_job_metadata(file_key, job_id, config)

        print(f"Started {processed_count} async Textract jobs for organization {org_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Started processing {processed_count} files',
                'organization_id': org_id,
                'job_ids': job_ids
            })
        }

    except Exception as e:
        print(f"Error in handle_batch_processing: {str(e)}")
        raise e


def start_textract_analysis(file_key, config):
    """
    Start asynchronous Textract document analysis with FORMS feature
    FORMS feature maintains proper key-value relationships and reading order
    """
    try:
        response = textract_client.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': config['BUCKET_NAME'],
                    'Name': file_key
                }
            },
            FeatureTypes=['FORMS'],
            NotificationChannel={
                'SNSTopicArn': config['SNS_TOPIC_ARN'],
                'RoleArn': config['SNS_ROLE_ARN']
            }
        )

        job_id = response['JobId']
        org_id = config.get('ORGANIZATION_ID', 'unknown')
        print(f"Started Textract analysis job {job_id} for {file_key} (org: {org_id})")
        return job_id

    except Exception as e:
        print(f"Error starting Textract for {file_key}: {str(e)}")
        return None


def store_job_metadata(file_key, job_id, config):
    """
    Store job metadata in S3 for tracking
    Includes organization_id for multi-org support
    """
    try:
        filename = file_key.split('/')[-1].replace('.pdf', '')
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        org_id = config.get('ORGANIZATION_ID', 'unknown')

        metadata = {
            'job_id': job_id,
            'source_file': file_key,
            'start_time': datetime.utcnow().isoformat(),
            'status': 'IN_PROGRESS',
            'organization_id': org_id,  # Include org_id for downstream processing
            'config': {
                'BUCKET_NAME': config['BUCKET_NAME'],
                'ORGANIZATION_ID': org_id,
                'TEXTRACT_PROCESSED': config.get('TEXTRACT_PROCESSED', 'textract-processed/'),
                'TEXTRACT_IRP_PROCESSED': config.get('TEXTRACT_IRP_PROCESSED', 'textract-processed/irp/')
            }
        }

        metadata_key = f"textract-processing/{filename}-{timestamp}-metadata.json"

        s3_client.put_object(
            Bucket=config['BUCKET_NAME'],
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )

        print(f"Stored metadata at: {metadata_key} (org: {org_id})")

    except Exception as e:
        print(f"Error storing metadata: {str(e)}")
