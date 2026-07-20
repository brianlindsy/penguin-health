"""
Results Handler for validation results storage and reporting.

Handles:
- Storing validation results in DynamoDB
- Storing validation run summaries for efficient querying
- Generating CSV reports from DynamoDB
- Saving CSV reports to S3
"""

import json
import os
import csv
import io
from datetime import datetime
from decimal import Decimal

import boto3

from audit import SystemPrincipal, emit as audit_emit

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

_AUDIT_PRINCIPAL = SystemPrincipal(
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'rules-engine-rag')
)


def store_results(results, env_config):
    """
    Store validation results in DynamoDB.

    Args:
        results: Validation results dict from validate_document. May include
                 an `s3_key` field; if present, it's stored on the row so
                 continuation legs can detect which files in a run are
                 already done.
        env_config: Environment config with DYNAMODB_TABLE
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        item = json.loads(json.dumps(results), parse_float=Decimal)

        item['pk'] = f"DOC#{results['document_id']}"
        item['sk'] = f"VALIDATION#{results['validation_timestamp']}"
        item['gsi1pk'] = f"DATE#{results['validation_timestamp'][:10]}"
        item['gsi1sk'] = f"DOC#{results['document_id']}"
        item['gsi2pk'] = f"RUN#{results['validation_run_id']}"
        item['gsi2sk'] = f"DOC#{results['document_id']}"
        item['organization_id'] = results.get('organization_id', 'unknown')

        table.put_item(Item=item)

        print(f"Stored results for document {results['document_id']} in DynamoDB (run: {results['validation_run_id']})")

        # One audit event per persisted validation result. The DDB row
        # carries the full extracted chart text + per-rule findings — i.e.
        # PHI — so the audit must record every write.
        audit_emit(
            action='write',
            resource={'type': 'ValidationResult',
                      'id': results['document_id'],
                      'org': results.get('organization_id', 'unknown')},
            actor=_AUDIT_PRINCIPAL.as_actor(),
            org_id=results.get('organization_id', 'unknown'),
            purpose_of_use='DOC_PROCESSING',
            call_type='ddb_write',
            external_control_number=results.get('validation_run_id'),
        )

    except Exception as e:
        print(f"Error storing results in DynamoDB: {type(e).__name__}")
        audit_emit(
            action='write',
            resource={'type': 'ValidationResult',
                      'id': results.get('document_id'),
                      'org': results.get('organization_id', 'unknown')},
            actor=_AUDIT_PRINCIPAL.as_actor(),
            org_id=results.get('organization_id', 'unknown'),
            outcome='major-failure',
            purpose_of_use='DOC_PROCESSING',
            call_type='ddb_write',
            error_class=type(e).__name__,
            external_control_number=results.get('validation_run_id'),
        )


def get_processed_s3_keys(validation_run_id, env_config):
    """
    Return the set of S3 keys that already have a per-document validation
    result row for this run. Used by continuation legs to skip files the
    prior leg already processed.

    A late-arriving file is naturally picked up: if it landed in the date
    folder after the first leg's S3 listing, the second leg's listing
    will include it and `get_processed_s3_keys` won't.

    Pagination matters: a busy run can have thousands of rows. Walk
    LastEvaluatedKey until exhausted.
    """
    table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

    keys = set()
    last_evaluated = None
    while True:
        kwargs = {
            'IndexName': 'gsi2',
            'KeyConditionExpression': 'gsi2pk = :run_key',
            'ExpressionAttributeValues': {':run_key': f"RUN#{validation_run_id}"},
            'ProjectionExpression': 's3_key',
        }
        if last_evaluated:
            kwargs['ExclusiveStartKey'] = last_evaluated
        resp = table.query(**kwargs)
        for item in resp.get('Items', []):
            s3_key = item.get('s3_key')
            if s3_key:
                keys.add(s3_key)
        last_evaluated = resp.get('LastEvaluatedKey')
        if not last_evaluated:
            break
    return keys


def aggregate_run_summary(validation_run_id, env_config):
    """
    Aggregate summary statistics for a validation run by querying all documents.

    Sentinel rows written by ``queue_handler.write_sentinel_row`` (a file
    was recognized as a byte-identical resend and skipped) are excluded
    from the rollup — they carry no real rule outcomes and would otherwise
    inflate the passed count.

    Args:
        validation_run_id: ID of the validation run
        env_config: Environment config with DYNAMODB_TABLE

    Returns:
        dict: Summary with total, passed, failed, skipped counts
    """
    table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

    items = []
    last_evaluated = None
    while True:
        kwargs = {
            'IndexName': 'gsi2',
            'KeyConditionExpression': 'gsi2pk = :run_key',
            'ExpressionAttributeValues': {
                ':run_key': f"RUN#{validation_run_id}"
            },
        }
        if last_evaluated:
            kwargs['ExclusiveStartKey'] = last_evaluated
        response = table.query(**kwargs)
        items.extend(response.get('Items', []))
        last_evaluated = response.get('LastEvaluatedKey')
        if not last_evaluated:
            break

    # Drop queue-duplicate-skip sentinel rows so run totals reflect real
    # validation work only.
    items = [i for i in items if not _is_sentinel(i)]

    total_docs = len(items)
    docs_passed = 0
    docs_failed = 0
    docs_skipped = 0

    for item in items:
        summary = item.get('summary', {})
        # A document is considered failed if any rule failed
        if summary.get('failed', 0) > 0:
            docs_failed += 1
        elif summary.get('skipped', 0) > 0 and summary.get('passed', 0) == 0:
            docs_skipped += 1
        else:
            docs_passed += 1

    return {
        'total': total_docs,
        'passed': docs_passed,
        'failed': docs_failed,
        'skipped': docs_skipped,
    }


def _is_sentinel(item):
    """A row written by ``queue_handler.write_sentinel_row``.

    Duplicate marker attribute is the primary signal; the pk shape is a
    belt-and-suspenders backup for pre-migration rows should the schema
    drift.
    """
    if item.get('duplicate_of_version_sk'):
        return True
    pk = item.get('pk')
    return isinstance(pk, str) and '#SKIPPED#' in pk


def store_run_summary(validation_run_id, org_id, summary, env_config,
                      categories=None, dates=None, queue_counters=None):
    """
    Store validation run summary for efficient querying by organization.

    Creates an item with pk=ORG#{org_id}, sk=RUN#{run_id} to enable
    efficient listing of runs by organization.

    Args:
        validation_run_id: ID of the validation run
        org_id: Organization ID
        summary: Dict with total, passed, failed, skipped counts
        env_config: Environment config with DYNAMODB_TABLE
        categories: Optional list of rule categories included in this run.
                    Stored on the run record so the API can filter by RBAC.
        dates: Optional list of YYYY-MM-DD ingest dates this run covered.
               Surfaced to the UI so the runs list shows which day's data
               each run examined.
        queue_counters: Optional dict with `new_documents`, `new_versions`,
                    `duplicate_skips` — the queue-side write path's own
                    counters for this leg. On continuation runs only the
                    final leg's counters are persisted (naked put_item is
                    last-writer-wins); a cross-leg accumulator would need
                    an UpdateItem with ADD semantics but is deferred
                    until reviewers actually miss the delta.
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        timestamp = datetime.utcnow().isoformat()
        date_str = timestamp[:10]

        counters = queue_counters or {}

        item = {
            'pk': f"ORG#{org_id}",
            'sk': f"RUN#{validation_run_id}",
            'gsi1pk': f"DATE#{date_str}",
            'gsi1sk': f"ORG#{org_id}#RUN#{validation_run_id}",
            'validation_run_id': validation_run_id,
            'organization_id': org_id,
            'timestamp': timestamp,
            'total_documents': summary['total'],
            'passed': summary['passed'],
            'failed': summary['failed'],
            'skipped': summary['skipped'],
            'queue_new_documents': int(counters.get('new_documents', 0) or 0),
            'queue_new_versions': int(counters.get('new_versions', 0) or 0),
            'queue_duplicate_skips': int(counters.get('duplicate_skips', 0) or 0),
            'status': 'completed',
            'categories': list(categories) if categories else [],
            'dates': list(dates) if dates else [],
        }

        table.put_item(Item=item)
        print(f"Stored run summary for {validation_run_id}: {summary} queue={counters}")

    except Exception as e:
        print(f"Error storing run summary in DynamoDB: {str(e)}")
        raise e


def generate_csv_from_dynamodb(validation_run_id, env_config):
    """
    Query all validation results for this run from DynamoDB and generate CSV.

    CSV format: One row per service_id with separate columns for each rule's status.

    Args:
        validation_run_id: ID of the validation run to query
        env_config: Environment config with DYNAMODB_TABLE

    Returns:
        (str, list[dict]): CSV content and the raw DynamoDB items used to
        build it. The caller can hand the items to save_parquet_to_s3 to
        avoid a second GSI2 query.
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        items = []
        last_evaluated = None
        while True:
            kwargs = {
                'IndexName': 'gsi2',
                'KeyConditionExpression': 'gsi2pk = :run_key',
                'ExpressionAttributeValues': {
                    ':run_key': f"RUN#{validation_run_id}"
                },
            }
            if last_evaluated:
                kwargs['ExclusiveStartKey'] = last_evaluated
            response = table.query(**kwargs)
            items.extend(response.get('Items', []))
            last_evaluated = response.get('LastEvaluatedKey')
            if not last_evaluated:
                break

        print(f"Found {len(items)} documents for validation run {validation_run_id}")

        all_rule_names = set()
        for item in items:
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                all_rule_names.add(rule_name)

        sorted_rule_names = sorted(all_rule_names)

        output = io.StringIO()
        writer = csv.writer(output)
        header = ['Service ID'] + sorted_rule_names
        writer.writerow(header)

        for item in items:
            # Use top-level document_id (extracted from CSV filename or document fields)
            service_id = item.get('document_id', 'N/A')

            rule_statuses = {}
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                status = rule.get('status', 'N/A')
                message = rule.get('message', '')

                # Use the full message which includes reasoning (e.g., "PASS - reasoning")
                # If message is just the status repeated or empty, use status alone
                if message and message != status:
                    # Message already contains status prefix (e.g., "PASS - reasoning")
                    # so use it directly
                    rule_statuses[rule_name] = message
                else:
                    rule_statuses[rule_name] = status

            row = [service_id]
            for rule_name in sorted_rule_names:
                row.append(rule_statuses.get(rule_name, 'N/A'))

            writer.writerow(row)

        csv_content = output.getvalue()
        print(f"Generated CSV with {len(items)} rows (one per service_id) and {len(sorted_rule_names)} rule columns")
        return csv_content, items

    except Exception as e:
        print(f"Error generating CSV from DynamoDB: {str(e)}")
        raise e


def save_csv_to_s3(csv_content, validation_run_id, env_config):
    """
    Save CSV report to S3.

    Args:
        csv_content: CSV content as string
        validation_run_id: ID of the validation run
        env_config: Environment config with BUCKET_NAME
    """
    try:
        csv_key = f"validation-reports/{validation_run_id}-validation-report.csv"

        s3_client.put_object(
            Bucket=env_config['BUCKET_NAME'],
            Key=csv_key,
            Body=csv_content,
            ContentType='text/csv'
        )

        print(f"Saved CSV report to s3://{env_config['BUCKET_NAME']}/{csv_key}")

    except Exception as e:
        print(f"Error saving CSV to S3: {str(e)}")
        raise e
