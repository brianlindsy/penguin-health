"""
Parquet writer for validation results.

Builds a flat, one-row-per-(document, rule) Arrow table from the validation
result items as they live in DynamoDB, and writes it to S3 under the
analytics/ prefix on the org bucket. Layout:

    s3://{bucket}/analytics/validation_results/
        validation_date={YYYY-MM-DD}/run_id={run_id}/part-0.parquet

The same builder is used by the rules-engine Lambda at end of run and by
the one-off backfill script (scripts/backfill_validation_parquet.py), so the
schema lives in exactly one place.

Mutable feedback fields (finding_confirmed, fixed, feedback_given_*) are
intentionally excluded from the Parquet schema. Those values change after
run completion when reviewers act on findings; including them here would
create stale snapshots that disagree with the DynamoDB source of truth.
"""

import io
import json
from decimal import Decimal

import boto3
import pandas as pd
from fastparquet import write as fp_write

s3_client = boto3.client('s3')


REVIEW_FIELDS = ('date', 'employee_name', 'program', 'cpt_code', 'rate')

# All columns are typed as string. fastparquet honors pandas dtypes; using
# 'string' (not 'object') keeps NULLs as <NA> and round-trips cleanly through
# Athena's Parquet reader.
PARQUET_COLUMNS = [
    'organization_id',
    'validation_run_id',
    'validation_timestamp',
    'document_id',
    'filename',
    's3_key',
    'rule_id',
    'rule_name',
    'category',
    'rule_type',
    'status',
    'message',
    'field_date',
    'field_employee_name',
    'field_program',
    'field_cpt_code',
    'field_rate',
    'field_values_json',
]


def _to_str(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return value
    return str(value)


def build_rule_rows(items):
    """
    Flatten DynamoDB validation result items into one row per rule per document.

    Args:
        items: Iterable of DynamoDB items as returned by a GSI2 query
               (gsi2pk=RUN#{run_id}). Each item has top-level fields plus a
               nested `rules` array and a `field_values` dict.

    Returns:
        list[dict]: rows ready to feed into pyarrow.Table.from_pylist.
    """
    rows = []
    for item in items:
        org_id = _to_str(item.get('organization_id'))
        run_id = _to_str(item.get('validation_run_id'))
        timestamp = _to_str(item.get('validation_timestamp'))
        document_id = _to_str(item.get('document_id'))
        filename = _to_str(item.get('filename'))
        s3_key = _to_str(item.get('s3_key'))

        field_values = item.get('field_values') or {}
        promoted = {f: _to_str(field_values.get(f)) for f in REVIEW_FIELDS}
        # Anything in field_values that isn't a promoted top-level column lands
        # in field_values_json so future analytics can still reach it without a
        # DDL change.
        leftover = {
            k: _to_str(v)
            for k, v in field_values.items()
            if k not in REVIEW_FIELDS
        }
        leftover_json = json.dumps(leftover, default=str) if leftover else None

        for rule in item.get('rules', []) or []:
            rows.append({
                'organization_id': org_id,
                'validation_run_id': run_id,
                'validation_timestamp': timestamp,
                'document_id': document_id,
                'filename': filename,
                's3_key': s3_key,
                'rule_id': _to_str(rule.get('rule_id')),
                'rule_name': _to_str(rule.get('rule_name')),
                'category': _to_str(rule.get('category')),
                'rule_type': _to_str(rule.get('rule_type')),
                'status': _to_str(rule.get('status')),
                'message': _to_str(rule.get('message')),
                'field_date': promoted['date'],
                'field_employee_name': promoted['employee_name'],
                'field_program': promoted['program'],
                'field_cpt_code': promoted['cpt_code'],
                'field_rate': promoted['rate'],
                'field_values_json': leftover_json,
            })
    return rows


def build_parquet_bytes(items):
    """
    Build an in-memory Parquet payload from validation result items.

    Returns:
        (bytes, int): the parquet payload and the row count it contains.
    """
    rows = build_rule_rows(items)
    if not rows:
        return b'', 0
    df = pd.DataFrame(rows, columns=PARQUET_COLUMNS).astype('string')
    buf = io.BytesIO()
    fp_write(buf, df, compression='SNAPPY', write_index=False)
    return buf.getvalue(), len(rows)


def parquet_key(validation_run_id, validation_date):
    """S3 key for a run's Parquet file under the analytics/ prefix."""
    return (
        f"analytics/validation_results/"
        f"validation_date={validation_date}/"
        f"run_id={validation_run_id}/part-0.parquet"
    )


def save_parquet_to_s3(items, validation_run_id, env_config):
    """
    Build and upload the Parquet snapshot for a completed run.

    Args:
        items: DynamoDB items for this run (same shape as
               generate_csv_from_dynamodb consumes).
        validation_run_id: The run's ID.
        env_config: Environment config with BUCKET_NAME.

    A run with zero rule rows is skipped (no empty file written).
    """
    payload, row_count = build_parquet_bytes(items)
    if row_count == 0:
        print(f"No rule rows for run {validation_run_id}; skipping Parquet write")
        return None

    validation_date = _validation_date_from_items(items)
    key = parquet_key(validation_run_id, validation_date)

    s3_client.put_object(
        Bucket=env_config['BUCKET_NAME'],
        Key=key,
        Body=payload,
        ContentType='application/octet-stream',
    )
    print(
        f"Saved Parquet ({row_count} rule rows) to "
        f"s3://{env_config['BUCKET_NAME']}/{key}"
    )
    return key


def _validation_date_from_items(items):
    # The validation_timestamp is identical across all items in a run, so any
    # item works. Fall back to today's UTC date if (somehow) absent.
    for item in items:
        ts = item.get('validation_timestamp')
        if ts:
            return str(ts)[:10]
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')
