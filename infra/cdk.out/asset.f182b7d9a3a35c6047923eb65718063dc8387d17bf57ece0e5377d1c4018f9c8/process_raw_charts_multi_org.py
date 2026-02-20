import json
import boto3
import os
from datetime import datetime

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')


def lambda_handler(event, context):
    """
    Lambda function to process PDF files from to-be-processed folder
    using Textract async analysis and store results in processed folder
    Handles both regular documents and IRP documents

    Event structure:
    {
        "organization_id": "community-health",  # Required
        "config": {  # Optional overrides
            "textract_folder": "textract-to-be-processed/",
            "irp_folder": "textract-to-be-processed/irp/"
        }
    }

    Returns:
        dict: Status with processed file count and job IDs
    """

    # Validate required parameter
    if 'organization_id' not in event:
        raise ValueError("Missing required parameter: organization_id")

    org_id = event['organization_id']
    print(f"Processing PDFs for organization: {org_id}")

    # Build bucket name from organization ID
    bucket_name = f"penguin-health-{org_id}"

    # Build configuration
    config = {
        'BUCKET_NAME': bucket_name,
        'ORGANIZATION_ID': org_id,
        'SNS_TOPIC_ARN': os.environ.get('SNS_TOPIC_ARN'),
        'SNS_ROLE_ARN': os.environ.get('SNS_ROLE_ARN'),
        'TEXTRACT_TO_BE_PROCESSED': 'textract-to-be-processed/',
        'TEXTRACT_IRP_FOLDER': 'textract-to-be-processed/irp/'
    }

    # Override with event-level config if provided
    if 'config' in event:
        if 'textract_folder' in event['config']:
            config['TEXTRACT_TO_BE_PROCESSED'] = event['config']['textract_folder']
        if 'irp_folder' in event['config']:
            config['TEXTRACT_IRP_FOLDER'] = event['config']['irp_folder']

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
                'organization_id': org_id,
                'message': 'No files to process',
                'processed_count': 0,
                'job_ids': []
            }

        processed_count = 0
        job_ids = []

        # Process each file
        for obj in response['Contents']:
            file_key = obj['Key']

            # Skip the folder itself
            if file_key.endswith('/'):
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
            'organization_id': org_id,
            'message': f'Started processing {processed_count} files',
            'processed_count': processed_count,
            'job_ids': job_ids
        }

    except Exception as e:
        print(f"Error processing organization {org_id}: {str(e)}")
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
