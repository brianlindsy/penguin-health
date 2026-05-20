import io
import json

import boto3
import pandas as pd
from fastparquet import write as fp_write


s3 = boto3.client('s3')


ENCOUNTER_PARQUET_COLUMNS = [
    'encounter_id',
    'status',
    'class_code',
    'class_system',
    'period_start',
    'period_end',
    'subject_reference',
    'service_provider_reference',
    'reason_codes_json',
    'type_codes_json',
    'participant_refs_json',
    'ndjson_s3_key',
    'ndjson_line_no',
    'fetched_at',
    'fhir_lookup_status',
]


def ndjson_key(ingest_date, run_id, leg=0):
    yyyy, mm, dd = ingest_date.split('-')
    return f"data/fhir/encounter/{yyyy}/{mm}/{dd}/{run_id}.part-{leg:04d}.ndjson"


def parquet_key(ingest_date, run_id, leg=0):
    return (
        f"analytics/fhir/encounter/ingest_date={ingest_date}/"
        f"{run_id}.part-{leg:04d}.parquet"
    )


def write_ndjson(bucket, key, resources):
    """resources: iterable of (line_no, resource_dict). Writes them all in one PutObject."""
    buf = io.BytesIO()
    for _, resource in resources:
        buf.write(json.dumps(resource, separators=(',', ':')).encode('utf-8'))
        buf.write(b'\n')
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType='application/x-ndjson',
    )
    return f"s3://{bucket}/{key}"


def write_parquet(bucket, key, rows):
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=ENCOUNTER_PARQUET_COLUMNS)
    df = df.astype({c: 'string' for c in ENCOUNTER_PARQUET_COLUMNS if c != 'ndjson_line_no'})
    df['ndjson_line_no'] = df['ndjson_line_no'].astype('Int64')
    buf = io.BytesIO()
    fp_write(buf, df, compression='SNAPPY', write_index=False)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType='application/octet-stream',
    )
    return f"s3://{bucket}/{key}"
