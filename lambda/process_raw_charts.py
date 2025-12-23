import json
import boto3
import os
from datetime import datetime
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def lambda_handler(event, context):
    """
    Lambda function to process PDF files from to-be-processed folder
    using Textract async analysis and store results in processed folder
    Handles both regular documents and IRP documents

    Configuration can be passed via environment variables or event parameters.
    Event parameters take precedence over environment variables.
    """
    # Load configuration from environment variables
    config = {
        'BUCKET_NAME': os.environ.get('BUCKET_NAME'),
        'SNS_TOPIC_ARN': os.environ.get('SNS_TOPIC_ARN'),
        'SNS_ROLE_ARN': os.environ.get('SNS_ROLE_ARN'),
        'TEXTRACT_TO_BE_PROCESSED': os.environ.get('TEXTRACT_TO_BE_PROCESSED'),
        'TEXTRACT_IRP_FOLDER': os.environ.get('TEXTRACT_IRP_FOLDER')
    }

    # Override with event-level config if provided
    if 'config' in event:
        config.update(event['config'])

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

                # Store job metadata for tracking
                store_job_metadata(file_key, job_id, config)

        print(f"Started {processed_count} async Textract jobs")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Started processing {processed_count} files',
                'job_ids': job_ids
            })
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise e


def start_textract_analysis(file_key, config):
    """
    Start asynchronous Textract document analysis with forms only
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
        print(f"Started Textract job {job_id} for {file_key}")
        return job_id

    except Exception as e:
        print(f"Error starting Textract for {file_key}: {str(e)}")
        return None


def store_job_metadata(file_key, job_id, config):
    """
    Store job metadata in S3 for tracking
    """
    try:
        filename = file_key.split('/')[-1].replace('.pdf', '')
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

        metadata = {
            'job_id': job_id,
            'source_file': file_key,
            'start_time': datetime.utcnow().isoformat(),
            'status': 'IN_PROGRESS',
            'config': {
                'BUCKET_NAME': config['BUCKET_NAME'],
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

        print(f"Stored metadata at: {metadata_key}")

    except Exception as e:
        print(f"Error storing metadata: {str(e)}")