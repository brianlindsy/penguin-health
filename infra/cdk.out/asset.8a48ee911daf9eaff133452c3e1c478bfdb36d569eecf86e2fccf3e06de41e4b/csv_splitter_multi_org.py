"""
CSV Splitter Lambda - Splits bulk CSV files into individual chart files.

Trigger: S3 ObjectCreated event on uploaded-data-sftp/*.csv
Output: Individual CSV files in csv-staging/

This Lambda is triggered when bulk CSV files are uploaded via SFTP.
It uses org-specific splitter scripts to parse the CSV and split it
into individual chart files that are staged for validation.
"""

import boto3

from multi_org_config import extract_org_id_from_bucket

# Import all splitters
from splitters.catholic_charities import CatholicCharitiesSplitter
from splitters.circles_of_care import CirclesOfCareSplitter

s3_client = boto3.client('s3')

# Registry of org-specific splitters
SPLITTER_REGISTRY = {}


def register_splitters():
    """Register all org-specific splitters."""
    splitter_classes = [
        CatholicCharitiesSplitter,
        CirclesOfCareSplitter,
    ]

    for cls in splitter_classes:
        try:
            instance = cls()
            SPLITTER_REGISTRY[instance.org_id] = instance
            print(f"Registered splitter for: {instance.org_id}")
        except Exception as e:
            print(f"Error registering splitter {cls.__name__}: {e}")


# Register splitters at module load time
register_splitters()


def lambda_handler(event, context):
    """
    Process CSV files uploaded to uploaded-data-sftp/ folder.

    For each CSV file:
    1. Load the org-specific splitter based on bucket name
    2. Split the bulk CSV into individual charts
    3. Save each chart to csv-staging/ folder
    4. Archive the original CSV to archived/sftp/
    """
    processed_count = 0
    chart_count = 0

    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Only process CSV files in the SFTP upload folder
        if not key.startswith('uploaded-data-sftp/') or not key.endswith('.csv'):
            print(f"Skipping non-CSV or non-SFTP file: {key}")
            continue

        # Extract org ID from bucket name
        try:
            org_id = extract_org_id_from_bucket(bucket)
        except ValueError as e:
            print(f"ERROR: {e}")
            continue

        print(f"Processing CSV: {key} for org: {org_id}")

        # Get splitter for this org
        splitter = SPLITTER_REGISTRY.get(org_id)
        if not splitter:
            print(f"ERROR: No splitter registered for org: {org_id}")
            print(f"Available splitters: {list(SPLITTER_REGISTRY.keys())}")
            continue

        # Read CSV content from S3
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_bytes = response['Body'].read()

            # Try to detect encoding
            encoding = splitter.detect_encoding(csv_bytes)
            csv_content = csv_bytes.decode(encoding)
        except Exception as e:
            print(f"ERROR reading CSV from S3: {e}")
            continue

        # Split into individual charts
        try:
            charts = splitter.split(csv_content, key)
            print(f"Split {key} into {len(charts)} charts")
        except Exception as e:
            print(f"ERROR splitting CSV: {e}")
            continue

        # Save each chart to STAGING AREA (csv-staging/)
        for chart_id, chart_csv in charts:
            output_key = f"csv-staging/{chart_id}.csv"
            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body=chart_csv.encode('utf-8'),
                    ContentType='text/csv'
                )
                chart_count += 1
                print(f"Staged: {output_key}")
            except Exception as e:
                print(f"ERROR saving chart {chart_id}: {e}")

        # Archive original bulk CSV
        archive_key = key.replace('uploaded-data-sftp/', 'archived/sftp/')
        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=archive_key
            )
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Archived original: {archive_key}")
        except Exception as e:
            print(f"ERROR archiving original CSV: {e}")

        processed_count += 1

    return {
        'statusCode': 200,
        'body': {
            'message': 'CSV splitting complete',
            'files_processed': processed_count,
            'charts_staged': chart_count,
        }
    }
