"""
CSV Splitter Lambda - Splits bulk CSV files into individual chart files.

Trigger: S3 ObjectCreated event on uploaded-data-sftp/*.csv
Output: Individual CSV files in data/{ingest_date_utc}/{ingest_ts_utc}__{chart_id}.csv

The date-partitioned layout lets validation runs target specific ingest
dates and re-run them at will. Older versions of this Lambda wrote to
csv-staging/, which is no longer read by anything.
"""

from datetime import datetime, timezone

import boto3
from urllib.parse import unquote_plus

from multi_org_config import extract_org_id_from_bucket

# Import all splitters
from splitters.catholic_charities import CatholicCharitiesSplitter
from splitters.circles_of_care import CirclesOfCareSplitter
from splitters.demo import DemoSplitter

s3_client = boto3.client('s3')

# Registry of org-specific splitters
SPLITTER_REGISTRY = {}


def register_splitters():
    """Register all org-specific splitters."""
    splitter_classes = [
        CatholicCharitiesSplitter,
        CirclesOfCareSplitter,
        DemoSplitter,
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
        # S3 event keys are URL-encoded, decode them
        key = unquote_plus(record['s3']['object']['key'])

        # Only process CSV files in the SFTP upload folder
        # Also accept .filepart files (Circles of Care sends these as fully processed CSVs)
        if not key.startswith('uploaded-data-sftp/'):
            print(f"Skipping non-SFTP file: {key}")
            continue
        if not (key.endswith('.csv') or key.endswith('.filepart')):
            print(f"Skipping non-CSV file: {key}")
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

        # Compute one ingest timestamp per splitter invocation. All charts
        # split from the same bulk CSV share the same date folder and the
        # same ts prefix, so they're trivially correlated.
        now_utc = datetime.now(timezone.utc)
        ingest_date = now_utc.strftime('%Y-%m-%d')
        ingest_ts = now_utc.strftime('%Y%m%dT%H%M%SZ')

        for chart_id, chart_csv in charts:
            output_key = f"data/{ingest_date}/{ingest_ts}__{chart_id}.csv"
            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=output_key,
                    Body=chart_csv.encode('utf-8'),
                    ContentType='text/csv'
                )
                chart_count += 1
                print(f"Wrote: {output_key}")
            except Exception as e:
                print(f"ERROR saving chart {chart_id}: {e}")

        # Archive original bulk CSV to timestamp-based folder
        filename = key.replace('uploaded-data-sftp/', '')
        upload_timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        archive_key = f'archived/sftp/{upload_timestamp}/{filename}'
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
