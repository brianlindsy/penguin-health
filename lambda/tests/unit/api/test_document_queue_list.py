"""
Regressions for ``list_queue_entries`` — specifically the two-pass
batch-load path added to keep the reviewer's card grid snappy.

Serial GetItem-per-pointer was ~5–15 ms per entry × 200 entries per
page = seconds of wall-clock latency. The list handler now:

  1. Collects pointers with a bounded GSI1 query.
  2. Batches the backing results-row fetches into one ``BatchGetItem``
     call (up to 100 keys per DDB request).

This test pins that behavior by counting DDB calls under moto: for N
pointers with ``include=rules``, the handler must issue exactly 1 base
Query plus ``ceil(N/100)`` BatchGetItem calls — no per-pointer
``GetItem`` sneaking back in.
"""

import json
import os
import sys
from unittest.mock import patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def _seed_queue_page(mock_dynamodb, *, org_id='test-org', count=5,
                     ruleset=None):
    """Seed `count` pointer rows on the queue table + the matching
    results rows. Pointers are in ``STATUS#open``. Ruleset defaults to
    a single Compliance Audit PASS."""
    ruleset = ruleset or [
        {'rule_id': 'r1', 'category': 'Compliance Audit', 'status': 'PASS'},
    ]

    queue = mock_dynamodb.Table('penguin-health-document-queue')
    results = mock_dynamodb.Table('penguin-health-validation-results')

    for i in range(count):
        doc_id = f'doc-{i:03d}'
        ts = f'2026-07-{15 + (i % 10):02d}T10:00:00'
        # Backing results row.
        results.put_item(Item={
            'pk': f'DOC#{doc_id}', 'sk': f'VALIDATION#{ts}',
            'gsi2pk': f'RUN#run-{i}', 'gsi2sk': f'DOC#{doc_id}',
            'document_id': doc_id,
            'organization_id': org_id,
            'validation_run_id': f'run-{i}',
            'validation_timestamp': ts,
            'rules': ruleset,
            'summary': {
                'total_rules': len(ruleset),
                'passed': sum(1 for r in ruleset if r['status'] == 'PASS'),
                'failed': sum(1 for r in ruleset if r['status'] == 'FAIL'),
                'skipped': 0,
            },
            'field_values': {'service_id': doc_id},
        })
        # Pointer row on the queue.
        queue.put_item(Item={
            'pk': f'ORG#{org_id}', 'sk': f'DOC#{doc_id}',
            'gsi1pk': f'ORG#{org_id}#STATUS#open',
            # ScanIndexForward=False + descending timestamp gives us
            # deterministic order in the response.
            'gsi1sk': f'LAST_UPDATED#{ts}',
            'gsi2pk': 'STATUS#open',
            'gsi2sk': f'LAST_UPDATED#{ts}',
            'document_id': doc_id,
            'organization_id': org_id,
            'status': 'open',
            'content_hash': 'seed',
            'latest_version_sk': f'VERSION#{ts}',
            'latest_validation_run_id': f'run-{i}',
            'latest_validation_timestamp': ts,
            'latest_validation_result_pk': f'DOC#{doc_id}',
            'latest_validation_result_sk': f'VALIDATION#{ts}',
            'first_seen_run_id': f'run-{i}',
            'first_seen_at': ts,
            'last_updated_at': ts,
            'last_seen_at': ts,
            'version_count': 1,
            'seen_count': 1,
            'field_values_snapshot': {'service_id': doc_id},
        })


class TestListQueueEntriesBatchLoad:
    def test_include_rules_batches_result_row_fetches(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        """With N pointers and ``include=rules``, the handler must
        BatchGetItem the results rows instead of issuing N GetItems.
        Counts the calls via `boto3.dynamodb.resource.meta.client` wrap."""
        from api.admin_api import list_queue_entries
        import api.admin_api as admin_api

        _seed_queue_page(mock_dynamodb, count=5)

        get_calls = []
        batch_calls = []

        # Wrap the module's `dynamodb` resource so we see batch_get_item
        # invocations, and wrap the results table's low-level client to
        # catch any GetItem sneaking through. Table.get_item flows through
        # the underlying client's `get_item`, so patching that catches
        # the anti-pattern regardless of whether the caller used the
        # resource or raw client.
        real_batch = admin_api.dynamodb.batch_get_item

        def spy_batch(**kw):
            batch_calls.append(kw)
            return real_batch(**kw)

        real_get = admin_api.validation_results_table.meta.client.get_item

        def spy_get(**kw):
            get_calls.append(kw)
            return real_get(**kw)

        with patch.object(admin_api.dynamodb, 'batch_get_item', spy_batch), \
             patch.object(admin_api.validation_results_table.meta.client,
                          'get_item', spy_get):
            event = {
                **super_admin_event,
                'queryStringParameters': {
                    'status': 'open',
                    'include': 'rules',
                    'limit': '10',
                },
            }
            resp = list_queue_entries(
                event=event,
                path_params={'orgId': 'test-org'},
                body=None,
            )

        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert len(body['entries']) == 5
        # Every entry has its rules[] hydrated from the batch response.
        for e in body['entries']:
            assert e['rules'], f"rules missing on {e['document_id']}"
            assert e['summary']

        # Exactly one BatchGetItem (5 keys fits in a single 100-key
        # chunk). No per-pointer GetItem calls against the results
        # table.
        assert len(batch_calls) == 1, batch_calls
        assert get_calls == [], get_calls

    def test_include_rules_paginates_batches_over_100(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        """With more than 100 pointers, batch_get_item must be called
        multiple times (100-key chunks). Limit is capped at 200 by the
        handler."""
        from api.admin_api import list_queue_entries
        import api.admin_api as admin_api

        _seed_queue_page(mock_dynamodb, count=150)

        batch_calls = []
        real_batch = admin_api.dynamodb.batch_get_item

        def spy_batch(**kw):
            batch_calls.append(kw)
            return real_batch(**kw)

        with patch.object(admin_api.dynamodb, 'batch_get_item', spy_batch):
            event = {
                **super_admin_event,
                'queryStringParameters': {
                    'status': 'open',
                    'include': 'rules',
                    'limit': '200',
                },
            }
            resp = list_queue_entries(
                event=event,
                path_params={'orgId': 'test-org'},
                body=None,
            )

        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert len(body['entries']) == 150
        # 150 keys / 100 per batch = 2 calls.
        assert len(batch_calls) == 2, batch_calls

    def test_include_rules_off_makes_zero_result_row_calls(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        """Without ``include=rules`` the handler must not touch the
        results table at all — pointer rollups + field_values_snapshot
        cover the list view."""
        from api.admin_api import list_queue_entries
        import api.admin_api as admin_api

        _seed_queue_page(mock_dynamodb, count=3)

        get_calls = []
        batch_calls = []

        real_batch = admin_api.dynamodb.batch_get_item
        real_get = admin_api.validation_results_table.meta.client.get_item

        with patch.object(
            admin_api.dynamodb, 'batch_get_item',
            lambda **kw: batch_calls.append(kw) or real_batch(**kw),
        ), patch.object(
            admin_api.validation_results_table.meta.client, 'get_item',
            lambda **kw: get_calls.append(kw) or real_get(**kw),
        ):
            event = {
                **super_admin_event,
                'queryStringParameters': {
                    'status': 'open',
                    'limit': '10',
                },
            }
            resp = list_queue_entries(
                event=event,
                path_params={'orgId': 'test-org'},
                body=None,
            )

        assert resp['statusCode'] == 200
        assert batch_calls == []
        assert get_calls == []
