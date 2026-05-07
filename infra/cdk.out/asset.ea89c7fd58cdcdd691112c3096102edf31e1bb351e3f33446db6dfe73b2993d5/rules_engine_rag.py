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
import re
from datetime import datetime, timedelta, timezone

import boto3

from multi_org_config import load_org_rules, build_env_config
from document_validator import validate_document
from results_handler import (
    store_results,
    aggregate_run_summary,
    store_run_summary,
    generate_csv_from_dynamodb,
    save_csv_to_s3,
    get_processed_s3_keys,
)

# Earliest date the new layout supports. Validation runs targeting earlier
# dates make no sense — there is no data/{date}/ folder for them.
CUTOVER_DATE = '2026-05-01'

DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


def invoke_continuation(org_id, validation_run_id, *,
                        categories=None, dates=None):
    """Invoke self to continue processing remaining files.

    Continuation legs always carry concrete `dates`, never the relative
    `date_window` form — the first leg has already resolved that.
    """
    function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

    payload = {
        'organization_id': org_id,
        'validation_run_id': validation_run_id,
        'is_continuation': True,
    }
    if categories is not None:
        payload['categories'] = categories
    if dates is not None:
        payload['dates'] = dates

    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Async - don't wait
        Payload=json.dumps(payload),
    )
    print(f"Invoked continuation Lambda for run {validation_run_id}")


def filter_rules_by_categories(rules, categories):
    """
    Restrict a list of rule items to those whose category is in `categories`.

    `categories` is a list of canonical category names from the API caller.
    None or an empty list means "no filter" (run all enabled rules).
    """
    if not categories:
        return rules
    allowed = set(categories)
    return [r for r in rules if r.get('category') in allowed]


def today_utc():
    """Today's date in UTC, as a date object. Wrapped for testability."""
    return datetime.now(timezone.utc).date()


def compute_dates_from_window(window, today):
    """
    Resolve a relative `date_window` payload into concrete YYYY-MM-DD strings.

    Currently supports `{"days_back_from_today": [int, ...]}`. The values are
    used as-is (no business-day skipping) so the cron rule controls "what is
    a business day", not this code.

    Examples:
        {"days_back_from_today": [1]}      -> [yesterday]
        {"days_back_from_today": [3, 2, 1]} -> [3-days-ago, 2-days-ago, yesterday]
    """
    days = window.get('days_back_from_today') or []
    out = []
    for n in days:
        d = today - timedelta(days=int(n))
        out.append(d.isoformat())
    return out


def list_data_folder_keys(bucket, date_str):
    """Yield every S3 key under data/{date_str}/. Paginates through ListObjectsV2."""
    prefix = f'data/{date_str}/'
    continuation_token = None
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or key.endswith('.json'):
                yield key
        if not resp.get('IsTruncated'):
            break
        continuation_token = resp.get('NextContinuationToken')


def date_for_key(key):
    """Pull the YYYY-MM-DD segment out of a data/{date}/... S3 key."""
    parts = key.split('/')
    if len(parts) >= 2 and parts[0] == 'data':
        return parts[1]
    return None


def resolve_dates(event):
    """
    Decide which YYYY-MM-DD ingest dates this run should validate.

    Priority order:
      1. `dates: [...]` from the event (API caller path; also used by every
         continuation leg).
      2. `date_window: {...}` from the event (EventBridge schedule path).
      3. Fallback: today (UTC).
    """
    if event.get('dates'):
        return list(event['dates'])
    if event.get('date_window'):
        return compute_dates_from_window(event['date_window'], today_utc())
    return [today_utc().isoformat()]


def lambda_handler(event, context):
    """
    Lambda function to validate processed JSON/CSV documents against configurable rules.

    Expects event with:
    - organization_id: required.
    - validation_run_id: optional. If absent, a new ID is generated. Passing
      it ensures retries / continuations land in the same run.
    - dates: optional list of YYYY-MM-DD strings to validate.
    - date_window: optional relative date instruction from EventBridge,
      e.g. {"days_back_from_today": [1]} (Tue-Fri) or [3, 2, 1] (Monday).
    - categories: optional list of rule categories to filter by.
    - is_continuation: True when self-invoked after a timeout split.
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise ValueError("organization_id is required in event")

    print(f"Loading configuration for organization: {org_id}")
    env_config = build_env_config(org_id)
    config = load_org_rules(org_id)

    # Optional category filter passed in by the API caller.
    # If absent, run every enabled rule (legacy behavior).
    categories = event.get('categories') or []
    if categories:
        original_count = len(config['rules'])
        config['rules'] = filter_rules_by_categories(config['rules'], categories)
        print(f"Filtered rules to categories {categories}: "
              f"{len(config['rules'])} of {original_count} match")
        if not config['rules']:
            print("No rules match requested categories; nothing to validate")

    dates = resolve_dates(event)
    print(f"Run targets dates: {dates}")

    validation_run_id = event.get('validation_run_id') or datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    is_continuation = event.get('is_continuation', False)

    if is_continuation:
        print(f"Continuing validation run: {validation_run_id}")
    else:
        print(f"Starting validation run: {validation_run_id}")

    try:
        bucket = env_config['BUCKET_NAME']

        # Files already processed by a prior leg of this run. Empty for the
        # first leg; non-empty only on continuations.
        already_processed = get_processed_s3_keys(validation_run_id, env_config)
        print(f"Already processed in this run: {len(already_processed)} files")

        files_found = False
        eligible_count = 0
        for date_str in dates:
            print(f"Listing data/{date_str}/ in bucket {bucket}")
            for key in list_data_folder_keys(bucket, date_str):
                if key in already_processed:
                    continue

                # Check remaining time before each file (leave 2 min buffer).
                remaining_ms = context.get_remaining_time_in_millis()
                if remaining_ms < 120_000:
                    print(f"Timeout approaching ({remaining_ms}ms remaining). Invoking continuation...")
                    invoke_continuation(
                        org_id, validation_run_id,
                        categories=categories or None,
                        dates=dates,
                    )
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'status': 'continuing',
                            'validation_run_id': validation_run_id,
                        })
                    }

                eligible_count += 1
                files_found = True
                process_file(bucket, key, config, org_id, env_config, validation_run_id)

        print(f"Processed {eligible_count} eligible files across {len(dates)} date(s)")

        if not files_found and not already_processed:
            return {
                'statusCode': 200,
                'body': json.dumps('No files to validate')
            }

        # Store run summary for efficient UI querying
        print(f"Aggregating run summary for: {validation_run_id}")
        summary = aggregate_run_summary(validation_run_id, env_config)
        run_categories = sorted({r.get('category') for r in config['rules']
                                 if r.get('category')})
        store_run_summary(validation_run_id, org_id, summary, env_config,
                          categories=run_categories,
                          dates=dates)

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
    """
    Validate a single JSON or CSV file from data/{date}/.

    Files stay where they are — no move-to-processing, no archive — so the
    same file can be re-validated by a future run. To avoid an infinite
    retry loop on a file that *can't* be parsed, we always write at least
    a sentinel ERROR row to DynamoDB before returning. The continuation
    handler then sees the file as "already processed" and moves on.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        if key.endswith('.csv'):
            data = {'text': content}
        else:
            data = json.loads(content)

        results = validate_document(data, key, config, org_id, validation_run_id)
        results['s3_key'] = key
        store_results(results, env_config)

        doc_id = results.get('document_id', 'UNKNOWN')
        if doc_id == 'UNKNOWN' or doc_id is None:
            print(f"WARNING: No document_id extracted for {key}")
        print(f"Validated {key} (document_id={doc_id}): {results['summary']}")

    except Exception as e:
        # Write a sentinel row so the continuation handler doesn't retry this
        # file on every invocation. Keyed by s3_key so it counts toward
        # "already processed" without colliding with a real result.
        print(f"Error processing {key}: {str(e)}")
        store_results({
            'validation_run_id': validation_run_id,
            'organization_id': org_id,
            'document_id': f"ERROR#{key}",
            'filename': key,
            'validation_timestamp': datetime.utcnow().isoformat(),
            'config_version': config.get('version', 'unknown'),
            'summary': {'total_rules': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
            'rules': [],
            'field_values': {},
            's3_key': key,
            'error': str(e),
        }, env_config)
