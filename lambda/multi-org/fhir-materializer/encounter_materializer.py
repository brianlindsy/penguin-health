import json
import os
import sys
import uuid

import boto3

# The `fhir/` package ships flat at the Lambda asset root alongside this
# file's directory contents at deploy time. For local imports (tests),
# tests put lambda/multi-org on sys.path.
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_here))

import fhir
from fhir import (
    FhirAuthError,
    FhirNotFound,
    FhirOrgNotConfigured,
    FhirRateLimited,
    FhirUpstreamError,
    empty_encounter_row,
    has_encounter_mapping,
    load_fhir_config,
    project_encounter,
)

import metrics
from athena import (
    AthenaQueryError,
    run_query,
    table_exists,
    table_suffix,
)
from storage import (
    ndjson_key,
    parquet_key,
    write_ndjson,
    write_parquet,
)


# Continuation safety: stop ingesting new resources when this many seconds
# remain in the Lambda invocation. Then self-invoke with the leftovers.
CONTINUATION_BUFFER_SEC = 60


# Lambda invokes itself for continuation
_lambda_client = boto3.client('lambda')


def lambda_handler(event, context):
    """
    Event shapes:

      from CSV-splitter EventBridge:
        { "organization_id": "...", "ingest_date": "YYYY-MM-DD" }
      EventBridge-wrapped:
        { "detail": { "organization_id": "...", "ingest_date": "..." }, ... }
      continuation (self-invoke):
        { "organization_id": "...", "ingest_date": "...",
          "remaining_ids": [...], "run_id": "...", "ndjson_line_offset": N,
          "is_continuation": true }
    """
    detail = event.get('detail') if isinstance(event.get('detail'), dict) else event
    org_id = detail.get('organization_id')
    ingest_date = detail.get('ingest_date')
    if not org_id or not ingest_date:
        raise ValueError(f"event missing organization_id/ingest_date: {event!r}")

    # ----- Config gate (silent skip vs loud fail) -----
    try:
        fhir_config = load_fhir_config(org_id)
    except FhirOrgNotConfigured as e:
        reason = 'disabled' if 'disabled' in str(e).lower() else 'no_config'
        print(f"skip org={org_id} reason={reason}: {e}")
        metrics.emit('FhirMaterializerSkipped', org_id, reason=reason)
        return _result(org_id, ingest_date, action='skipped', reason=reason)

    if not has_encounter_mapping(fhir_config):
        print(f"skip org={org_id} reason=no_encounter_mapping")
        metrics.emit('FhirMaterializerSkipped', org_id, reason='no_encounter_mapping')
        return _result(org_id, ingest_date, action='skipped', reason='no_encounter_mapping')

    encounter_map = fhir_config['fhir_mappings']['encounter']
    source_column = encounter_map['source_column']

    # ----- Continuation path: skip Athena diff, work the remaining_ids -----
    is_continuation = bool(detail.get('is_continuation'))
    if is_continuation:
        ids_to_fetch = list(detail.get('remaining_ids') or [])
        run_id = detail['run_id']
        leg = int(detail.get('leg', 1))
    else:
        # ----- Athena diff -----
        try:
            ids_to_fetch = _diff_ids(org_id, ingest_date, source_column)
        except AthenaQueryError as e:
            print(f"athena failure org={org_id}: {e}")
            metrics.emit('FhirMaterializerFailed', org_id, reason='athena_query_failed')
            raise
        run_id = uuid.uuid4().hex
        leg = 0

    if not ids_to_fetch:
        print(f"no encounters to fetch for org={org_id} date={ingest_date}")
        return _result(org_id, ingest_date, action='noop', fetched=0)

    # ----- Fetch + project (with continuation guard) -----
    bucket = f"penguin-health-{org_id}"
    n_key = ndjson_key(ingest_date, run_id, leg=leg)
    ndjson_s3_key = f"s3://{bucket}/{n_key}"
    p_key = parquet_key(ingest_date, run_id, leg=leg)

    successes = []   # list of (line_no, resource)
    rows = []        # projected Parquet rows
    not_found_count = 0
    remaining = list(ids_to_fetch)

    while remaining:
        if context is not None and _approaching_timeout(context):
            _flush(bucket, n_key, p_key, successes, rows)
            return _self_invoke_continuation(
                context.function_name, org_id, ingest_date,
                remaining, run_id, leg + 1,
            )

        encounter_id = remaining.pop(0)
        line_no = len(successes)  # per-leg line numbering
        try:
            resource = fhir.get_resource(org_id, 'Encounter', encounter_id)
        except FhirNotFound:
            not_found_count += 1
            rows.append(empty_encounter_row(encounter_id, status='not_found'))
            continue
        except FhirRateLimited:
            metrics.emit('FhirMaterializerFailed', org_id, reason='upstream_unavailable')
            remaining.insert(0, encounter_id)
            _flush(bucket, n_key, p_key, successes, rows)
            raise
        except (FhirUpstreamError, FhirAuthError):
            metrics.emit(
                'FhirMaterializerFailed', org_id,
                reason='upstream_unavailable',
            )
            remaining.insert(0, encounter_id)
            _flush(bucket, n_key, p_key, successes, rows)
            raise

        successes.append((line_no, resource))
        rows.append(project_encounter(
            resource,
            ndjson_s3_key=ndjson_s3_key,
            ndjson_line_no=line_no,
            status='ok',
        ))

    _flush(bucket, n_key, p_key, successes, rows)

    metrics.emit('FhirEncountersFetched', org_id, value=len(successes))
    if not_found_count:
        metrics.emit('FhirEncountersNotFound', org_id, value=not_found_count)

    return _result(
        org_id, ingest_date,
        action='ok',
        fetched=len(successes),
        not_found=not_found_count,
        leg=leg,
        ndjson_s3_key=ndjson_s3_key if successes else None,
    )


def _diff_ids(org_id, ingest_date, source_column):
    suffix = table_suffix(org_id)
    charts_table = f"charts_{suffix}"
    fhir_table = f"fhir_encounters_{suffix}"

    charts_sql = (
        f"SELECT DISTINCT \"{source_column}\" "
        f"FROM {charts_table} "
        f"WHERE ingest_date = '{ingest_date}' "
        f"AND \"{source_column}\" IS NOT NULL "
        f"AND \"{source_column}\" <> ''"
    )
    needed = set(run_query(charts_sql, org_id))

    if not table_exists(org_id, fhir_table):
        return sorted(needed)

    have_sql = (
        f"SELECT DISTINCT encounter_id "
        f"FROM {fhir_table} "
        f"WHERE ingest_date <= '{ingest_date}'"
    )
    have = set(run_query(have_sql, org_id))
    return sorted(needed - have)


def _flush(bucket, n_key, p_key, successes, rows):
    if successes:
        write_ndjson(bucket, n_key, successes)
    if rows:
        write_parquet(bucket, p_key, rows)


def _approaching_timeout(context):
    try:
        remaining_ms = context.get_remaining_time_in_millis()
    except Exception:
        return False
    return remaining_ms <= CONTINUATION_BUFFER_SEC * 1000


def _self_invoke_continuation(
    function_name, org_id, ingest_date, remaining, run_id, next_leg
):
    payload = {
        'organization_id': org_id,
        'ingest_date': ingest_date,
        'is_continuation': True,
        'remaining_ids': remaining,
        'run_id': run_id,
        'leg': next_leg,
    }
    _lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=json.dumps(payload).encode('utf-8'),
    )
    return _result(
        org_id, ingest_date,
        action='continuation',
        remaining=len(remaining),
        run_id=run_id,
        next_leg=next_leg,
    )


def _result(org_id, ingest_date, **kwargs):
    return {
        'organization_id': org_id,
        'ingest_date': ingest_date,
        **kwargs,
    }
