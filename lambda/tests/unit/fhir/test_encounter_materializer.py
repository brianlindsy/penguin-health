import io
import json
import os
import sys
from unittest.mock import patch, MagicMock

import boto3
import pytest


_LAMBDA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, os.path.join(_LAMBDA_DIR, 'multi-org'))
sys.path.insert(0, os.path.join(_LAMBDA_DIR, 'multi-org', 'fhir-materializer'))


BUCKET = 'penguin-health-demo'
INGEST_DATE = '2026-05-19'


@pytest.fixture
def seeded(mock_dynamodb, mock_s3):
    # Build the demo org bucket — the conftest mock_s3 fixture only made a
    # 'penguin-health-test-org' bucket; we need the per-org bucket the
    # materializer writes into.
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=BUCKET)

    # These materializer tests patch `fhir.get_resource` directly, so the
    # KMS/JWKS auth path is never exercised. We still write the new
    # client_id + kms_alias fields so the config matches what the
    # production loader expects to see, but no KMS key is created.
    table = mock_dynamodb.Table('penguin-health-org-config')
    table.put_item(Item={
        'pk': 'ORG#demo',
        'sk': 'FHIR_CONFIG',
        'gsi1pk': 'FHIR_CONFIG',
        'gsi1sk': 'ORG#demo',
        'organization_id': 'demo',
        'vendor': 'credible',
        'base_url': 'https://fhir.example.test',
        'token_url': 'https://sts.example.test/connect/token',
        'auth_type': 'oauth2_client_credentials',
        'scopes': [],
        'client_id': 'cid',
        'kms_alias': 'alias/penguin-health-fhir-demo',
        'page_size': 100,
        'concurrency': 4,
        'enabled': True,
        'fhir_mappings': {
            'encounter': {
                'source_table': 'charts_demo',
                'source_column': 'service_id_1',
                'fhir_lookup': 'by_id',
            }
        },
    })

    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()
    yield s3
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()


def _ctx(remaining_ms=900_000):
    ctx = MagicMock()
    ctx.function_name = 'penguin-health-fhir-encounter-materializer'
    ctx.get_remaining_time_in_millis.return_value = remaining_ms
    return ctx


def _encounter(eid):
    return {
        'resourceType': 'Encounter',
        'id': eid,
        'status': 'finished',
        'class': {'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'code': 'AMB'},
        'period': {'start': '2024-01-02T10:00:00Z', 'end': '2024-01-02T10:30:00Z'},
        'subject': {'reference': f'Patient/p-{eid}'},
    }


# -----------------------------------------------------------------------------
# Skip paths (no FHIR/Athena/S3 work expected)
# -----------------------------------------------------------------------------

def test_skip_no_config(mock_dynamodb, mock_s3):
    """An org with no FHIR_CONFIG record should skip silently with a metric."""
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()

    import encounter_materializer
    import metrics as metrics_mod
    with patch.object(metrics_mod, 'emit') as m_emit, \
         patch.object(encounter_materializer, 'fhir') as m_fhir:
        # if the gate fails, fhir.get_resource shouldn't be called
        m_fhir.get_resource.side_effect = AssertionError('must not call FHIR')
        m_fhir.load_fhir_config.side_effect = fhir_config_mod.load_fhir_config
        m_fhir.has_encounter_mapping = fhir_config_mod.has_encounter_mapping
        # Real exception types must still be raisable
        from fhir.exceptions import FhirOrgNotConfigured as _Excp
        m_fhir.FhirOrgNotConfigured = _Excp

        result = encounter_materializer.lambda_handler(
            {'organization_id': 'not-configured', 'ingest_date': INGEST_DATE}, _ctx()
        )
    assert result['action'] == 'skipped'
    assert result['reason'] == 'no_config'
    m_emit.assert_any_call('FhirMaterializerSkipped', 'not-configured', reason='no_config')


def test_skip_disabled_org(mock_dynamodb, mock_s3):
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    table = mock_dynamodb.Table('penguin-health-org-config')
    table.put_item(Item={
        'pk': 'ORG#off',
        'sk': 'FHIR_CONFIG',
        'organization_id': 'off',
        'vendor': 'credible',
        'base_url': 'https://fhir.example.test',
        'token_url': 'https://sts.example.test/connect/token',
        'scopes': [],
        'client_id': 'c',
        'kms_alias': 'alias/penguin-health-fhir-off',
        'enabled': False,
    })
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()

    import encounter_materializer
    import metrics as metrics_mod
    with patch.object(metrics_mod, 'emit') as m_emit:
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'off', 'ingest_date': INGEST_DATE}, _ctx()
        )
    assert result['action'] == 'skipped'
    assert result['reason'] == 'disabled'
    m_emit.assert_any_call('FhirMaterializerSkipped', 'off', reason='disabled')


def test_skip_no_encounter_mapping(mock_dynamodb, mock_s3):
    from fhir import config as fhir_config_mod
    from fhir import fhir_query as fhir_query_mod
    table = mock_dynamodb.Table('penguin-health-org-config')
    table.put_item(Item={
        'pk': 'ORG#nomap',
        'sk': 'FHIR_CONFIG',
        'organization_id': 'nomap',
        'vendor': 'credible',
        'base_url': 'https://fhir.example.test',
        'token_url': 'https://sts.example.test/connect/token',
        'scopes': [],
        'client_id': 'c',
        'kms_alias': 'alias/penguin-health-fhir-nomap',
        'enabled': True,
        # NO fhir_mappings
    })
    fhir_config_mod.load_fhir_config.cache_clear()
    fhir_query_mod.reset_clients_for_tests()

    import encounter_materializer
    import metrics as metrics_mod
    with patch.object(metrics_mod, 'emit') as m_emit:
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'nomap', 'ingest_date': INGEST_DATE}, _ctx()
        )
    assert result['action'] == 'skipped'
    assert result['reason'] == 'no_encounter_mapping'
    m_emit.assert_any_call('FhirMaterializerSkipped', 'nomap', reason='no_encounter_mapping')


# -----------------------------------------------------------------------------
# Happy paths
# -----------------------------------------------------------------------------

def test_empty_diff_noop(seeded):
    """Athena diff returns no IDs — exit clean with action=noop, no S3 writes."""
    import encounter_materializer

    with patch.object(encounter_materializer, '_diff_ids', return_value=[]), \
         patch.object(encounter_materializer, 'fhir') as m_fhir:
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, _ctx()
        )
    assert result['action'] == 'noop'
    assert result['fetched'] == 0
    m_fhir.get_resource.assert_not_called()

    # No files written
    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    assert 'Contents' not in listed or len(listed['Contents']) == 0


def test_happy_path_writes_ndjson_and_parquet(seeded):
    import encounter_materializer
    import metrics as metrics_mod

    ids = ['enc-1', 'enc-2', 'enc-3']
    encounters = {eid: _encounter(eid) for eid in ids}

    with patch.object(encounter_materializer, '_diff_ids', return_value=ids), \
         patch.object(encounter_materializer.fhir, 'get_resource',
                      side_effect=lambda org, rtype, rid: encounters[rid]), \
         patch.object(metrics_mod, 'emit') as m_emit:
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, _ctx()
        )

    assert result['action'] == 'ok'
    assert result['fetched'] == 3
    assert result['not_found'] == 0

    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    keys = [obj['Key'] for obj in listed['Contents']]
    ndjson_keys = [k for k in keys if k.endswith('.ndjson')]
    parquet_keys = [k for k in keys if k.endswith('.parquet')]
    assert len(ndjson_keys) == 1
    assert len(parquet_keys) == 1
    assert ndjson_keys[0].startswith('data/fhir/encounter/2026/05/19/')
    assert parquet_keys[0].startswith('analytics/fhir/encounter/ingest_date=2026-05-19/')

    body = s3.get_object(Bucket=BUCKET, Key=ndjson_keys[0])['Body'].read().decode('utf-8')
    lines = [l for l in body.split('\n') if l]
    assert len(lines) == 3
    parsed_ids = [json.loads(l)['id'] for l in lines]
    assert parsed_ids == ids

    m_emit.assert_any_call('FhirEncountersFetched', 'demo', value=3)


def test_not_found_records_row_but_no_ndjson_line(seeded):
    """A FhirNotFound mid-batch: that encounter gets a row with status=not_found
    and null pointer; others succeed; metric incremented."""
    from fhir.exceptions import FhirNotFound
    import encounter_materializer
    import metrics as metrics_mod
    import storage

    ids = ['enc-1', 'missing', 'enc-3']

    def fake_get_resource(org, rtype, rid):
        if rid == 'missing':
            raise FhirNotFound('not found')
        return _encounter(rid)

    captured_rows = {}
    real_write_parquet = storage.write_parquet

    def trap_parquet(bucket, key, rows):
        captured_rows['rows'] = rows
        return real_write_parquet(bucket, key, rows)

    with patch.object(encounter_materializer, '_diff_ids', return_value=ids), \
         patch.object(encounter_materializer.fhir, 'get_resource', side_effect=fake_get_resource), \
         patch.object(encounter_materializer, 'write_parquet', side_effect=trap_parquet), \
         patch.object(metrics_mod, 'emit') as m_emit:
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, _ctx()
        )

    assert result['fetched'] == 2
    assert result['not_found'] == 1

    rows = captured_rows['rows']
    assert len(rows) == 3
    statuses_by_id = {r['encounter_id']: r['fhir_lookup_status'] for r in rows}
    assert statuses_by_id == {'enc-1': 'ok', 'missing': 'not_found', 'enc-3': 'ok'}
    missing_row = next(r for r in rows if r['encounter_id'] == 'missing')
    assert missing_row['ndjson_s3_key'] is None

    # NDJSON has 2 lines (only successful fetches)
    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    ndjson_keys = [obj['Key'] for obj in listed['Contents'] if obj['Key'].endswith('.ndjson')]
    body = s3.get_object(Bucket=BUCKET, Key=ndjson_keys[0])['Body'].read().decode('utf-8')
    assert len([l for l in body.split('\n') if l]) == 2

    m_emit.assert_any_call('FhirEncountersNotFound', 'demo', value=1)


def test_athena_failure_emits_metric_and_raises(seeded):
    from athena import AthenaQueryError
    import encounter_materializer
    import metrics as metrics_mod

    with patch.object(encounter_materializer, '_diff_ids',
                      side_effect=AthenaQueryError('column missing')), \
         patch.object(metrics_mod, 'emit') as m_emit:
        with pytest.raises(AthenaQueryError):
            encounter_materializer.lambda_handler(
                {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, _ctx()
            )
    m_emit.assert_any_call('FhirMaterializerFailed', 'demo', reason='athena_query_failed')


def test_rate_limit_flushes_partial_and_raises(seeded):
    """If FHIR rate-limits mid-batch, partial results land in S3 + a failure
    metric is emitted + the exception propagates."""
    from fhir.exceptions import FhirRateLimited
    import encounter_materializer
    import metrics as metrics_mod

    ids = ['enc-1', 'enc-2', 'enc-3']
    encounters_seen = []

    def fake_get_resource(org, rtype, rid):
        encounters_seen.append(rid)
        if rid == 'enc-3':
            raise FhirRateLimited('429')
        return _encounter(rid)

    with patch.object(encounter_materializer, '_diff_ids', return_value=ids), \
         patch.object(encounter_materializer.fhir, 'get_resource', side_effect=fake_get_resource), \
         patch.object(metrics_mod, 'emit') as m_emit:
        with pytest.raises(FhirRateLimited):
            encounter_materializer.lambda_handler(
                {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, _ctx()
            )

    m_emit.assert_any_call('FhirMaterializerFailed', 'demo', reason='upstream_unavailable')

    # Partial writes happened: 2 rows in NDJSON, 2 in Parquet.
    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    ndjson_keys = [obj['Key'] for obj in listed['Contents'] if obj['Key'].endswith('.ndjson')]
    body = s3.get_object(Bucket=BUCKET, Key=ndjson_keys[0])['Body'].read().decode('utf-8')
    assert len([l for l in body.split('\n') if l]) == 2


def test_continuation_self_invokes_with_remaining(seeded):
    """When near timeout, the Lambda self-invokes for the next leg."""
    import encounter_materializer

    ids = ['enc-1', 'enc-2', 'enc-3', 'enc-4']

    invoked = {}

    def fake_invoke(**kwargs):
        invoked['payload'] = json.loads(kwargs['Payload'])
        invoked['function_name'] = kwargs['FunctionName']
        invoked['type'] = kwargs['InvocationType']
        return {}

    # First check passes (ample time), second check trips the continuation.
    # The loop only consults get_remaining_time_in_millis once per iteration.
    remaining_times = iter([900_000, 100, 100, 100, 100])
    ctx = MagicMock()
    ctx.function_name = 'penguin-health-fhir-encounter-materializer'
    ctx.get_remaining_time_in_millis.side_effect = lambda: next(remaining_times)

    with patch.object(encounter_materializer, '_diff_ids', return_value=ids), \
         patch.object(encounter_materializer.fhir, 'get_resource',
                      side_effect=lambda o, t, r: _encounter(r)), \
         patch.object(encounter_materializer._lambda_client, 'invoke', side_effect=fake_invoke):
        result = encounter_materializer.lambda_handler(
            {'organization_id': 'demo', 'ingest_date': INGEST_DATE}, ctx
        )

    assert result['action'] == 'continuation'
    assert result['remaining'] == 3  # 3 of 4 still pending
    assert invoked['type'] == 'Event'
    assert invoked['payload']['is_continuation'] is True
    assert invoked['payload']['remaining_ids'] == ['enc-2', 'enc-3', 'enc-4']
    assert invoked['payload']['leg'] == 1
    assert invoked['payload']['run_id'] == result['run_id']

    # First leg's 1 success was flushed to S3
    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    ndjson_keys = [obj['Key'] for obj in listed['Contents'] if obj['Key'].endswith('.ndjson')]
    assert len(ndjson_keys) == 1
    assert '.part-0000.ndjson' in ndjson_keys[0]


def test_continuation_leg_uses_distinct_part_keys(seeded):
    """A continuation invocation (is_continuation=True) writes to a different
    part key than the original leg, so legs don't overwrite each other."""
    import encounter_materializer

    with patch.object(encounter_materializer.fhir, 'get_resource',
                      side_effect=lambda o, t, r: _encounter(r)):
        result = encounter_materializer.lambda_handler(
            {
                'organization_id': 'demo',
                'ingest_date': INGEST_DATE,
                'is_continuation': True,
                'remaining_ids': ['enc-9'],
                'run_id': 'run-x',
                'leg': 2,
            },
            _ctx(),
        )

    assert result['action'] == 'ok'
    assert result['leg'] == 2

    s3 = boto3.client('s3', region_name='us-east-1')
    listed = s3.list_objects_v2(Bucket=BUCKET)
    keys = [obj['Key'] for obj in listed['Contents']]
    assert any('run-x.part-0002.ndjson' in k for k in keys)
    assert any('run-x.part-0002.parquet' in k for k in keys)


# -----------------------------------------------------------------------------
# EventBridge envelope handling
# -----------------------------------------------------------------------------

def test_accepts_eventbridge_detail_envelope(seeded):
    """EventBridge wraps the payload in a 'detail' object."""
    import encounter_materializer

    with patch.object(encounter_materializer, '_diff_ids', return_value=[]):
        result = encounter_materializer.lambda_handler(
            {
                'source': 'penguin-health.csv-splitter',
                'detail-type': 'SftpIngestComplete',
                'detail': {'organization_id': 'demo', 'ingest_date': INGEST_DATE},
            },
            _ctx(),
        )
    assert result['action'] == 'noop'


def test_missing_fields_raises_value_error(mock_dynamodb, mock_s3):
    import encounter_materializer
    with pytest.raises(ValueError):
        encounter_materializer.lambda_handler({}, _ctx())
