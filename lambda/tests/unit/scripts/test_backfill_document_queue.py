"""
Smoke test for `scripts/backfill_document_queue.py`.

Seeds a handful of validation-results rows for two orgs — including one
with a resolved status and one with a doc-confirmed status — plus the
matching S3 objects so the backfill can re-read the raw records and
compute the same content hash the rules-engine write path will compute
on the next unchanged re-run. Asserts:

  * One pointer row per unique document_id, status derived correctly.
  * The backfilled ``content_hash`` equals
    ``queue_handler.compute_content_hash(<raw record>)`` — the whole
    point of Option B, and the regression that avoids spurious
    re-opens on the first live run.
  * Docs whose s3_key is missing or whose S3 object is gone are
    skipped rather than seeded with a bogus hash.
  * Re-running is a no-op on the pointer (idempotency).
"""

import json
import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine',
    ),
)
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'scripts'),
)


def _seed_doc(results_table, *, org_id, doc_id, ts,
              rules, field_values=None, document_confirmed=False,
              run_id='run-1', s3_key=None):
    item = {
        'pk': f'DOC#{doc_id}',
        'sk': f'VALIDATION#{ts}',
        'gsi2pk': f'RUN#{run_id}',
        'gsi2sk': f'DOC#{doc_id}',
        'document_id': doc_id,
        'validation_run_id': run_id,
        'validation_timestamp': ts,
        'organization_id': org_id,
        'rules': rules,
        'field_values': field_values or {},
        'summary': {
            'total_rules': len(rules),
            'passed': sum(1 for r in rules if r.get('status') == 'PASS'),
            'failed': sum(1 for r in rules if r.get('status') == 'FAIL'),
            'skipped': sum(1 for r in rules if r.get('status') == 'SKIP'),
        },
    }
    if document_confirmed:
        item['document_confirmed'] = True
    if s3_key:
        item['s3_key'] = s3_key
    results_table.put_item(Item=item)


def _seed_org_config(mock_dynamodb, org_id, bucket):
    """Seed the METADATA row the backfill uses to resolve the bucket."""
    mock_dynamodb.Table('penguin-health-org-config').put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': 'METADATA',
        'organization_id': org_id,
        's3_bucket_name': bucket,
    })


def _put_raw_json(mock_s3, bucket, key, record):
    mock_s3.put_object(Bucket=bucket, Key=key,
                       Body=json.dumps(record).encode('utf-8'))


def _put_raw_csv(mock_s3, bucket, key, text):
    mock_s3.put_object(Bucket=bucket, Key=key, Body=text.encode('utf-8'))


class TestBackfillDocumentQueue:
    def test_seeds_pointers_and_matches_write_path_hash(
        self, mock_dynamodb, mock_s3, monkeypatch,
    ):
        import backfill_document_queue as script
        import queue_handler

        monkeypatch.setattr(script, 'dynamodb', mock_dynamodb)
        monkeypatch.setattr(script, 's3', mock_s3)

        # Buckets exist per-org.
        mock_s3.create_bucket(Bucket='penguin-health-org-a')
        mock_s3.create_bucket(Bucket='penguin-health-org-b')
        _seed_org_config(mock_dynamodb, 'org-a', 'penguin-health-org-a')
        _seed_org_config(mock_dynamodb, 'org-b', 'penguin-health-org-b')

        results = mock_dynamodb.Table('penguin-health-validation-results')
        queue = mock_dynamodb.Table('penguin-health-document-queue')

        # Doc A (org-a): JSON record + one failing rule → status=open.
        doc_a_record = {'source_record_id': 'doc-a', 'text': 'session narrative'}
        _put_raw_json(mock_s3, 'penguin-health-org-a',
                      'data/2024-01-15/doc-a.json', doc_a_record)
        _seed_doc(
            results, org_id='org-a', doc_id='doc-a',
            ts='2024-01-15T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL'}],
            field_values={'program': 'Alpha', 'service_type': 'Individual'},
            s3_key='data/2024-01-15/doc-a.json',
        )

        # Doc B (org-a): CSV record + every failing rule fixed → resolved.
        doc_b_csv = 'header1,header2\nvalue1,value2\n'
        _put_raw_csv(mock_s3, 'penguin-health-org-a',
                     'data/2024-01-16/doc-b.csv', doc_b_csv)
        _seed_doc(
            results, org_id='org-a', doc_id='doc-b',
            ts='2024-01-16T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL',
                    'fixed': True}],
            s3_key='data/2024-01-16/doc-b.csv',
        )

        # Doc C (org-b): document_confirmed=True → confirmed.
        doc_c_record = {'source_record_id': 'doc-c', 'text': 'confirmed'}
        _put_raw_json(mock_s3, 'penguin-health-org-b',
                      'data/2024-01-17/doc-c.json', doc_c_record)
        _seed_doc(
            results, org_id='org-b', doc_id='doc-c',
            ts='2024-01-17T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'PASS'}],
            document_confirmed=True,
            s3_key='data/2024-01-17/doc-c.json',
        )
        # Doc B (org-a) also has an earlier row.
        _seed_doc(
            results, org_id='org-a', doc_id='doc-b',
            ts='2024-01-10T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL'}],
            run_id='run-0',
            s3_key='data/2024-01-16/doc-b.csv',
        )

        for org_id in ['org-a', 'org-b']:
            pointers, versions, stats = script._plan_backfill_for_org(
                results, org_id,
            )
            result = script._commit(queue, pointers, versions, dry_run=False)
            assert result['pointers_written'] == len(pointers)
            assert result['skipped_existing'] == 0
            assert stats['skipped_missing_s3_key'] == 0
            assert stats['skipped_missing_s3_object'] == 0

        # Status derivation still works.
        p_a = queue.get_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#doc-a'}
        )['Item']
        assert p_a['status'] == 'open'
        assert p_a['gsi2pk'] == 'STATUS#open'
        assert p_a['program'] == 'Alpha'

        p_b = queue.get_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#doc-b'}
        )['Item']
        assert p_b['status'] == 'resolved'
        assert 'gsi2pk' not in p_b
        assert p_b['first_seen_at'] == '2024-01-10T10:00:00'
        assert p_b['latest_validation_timestamp'] == '2024-01-16T10:00:00'

        p_c = queue.get_item(
            Key={'pk': 'ORG#org-b', 'sk': 'DOC#doc-c'}
        )['Item']
        assert p_c['status'] == 'confirmed'

        # Load-bearing: the seeded content_hash matches what the write
        # path will compute on the same raw record the next time the
        # rules engine sees it. Without this, every unchanged re-run
        # spuriously flips backfilled entries to open.
        assert p_a['content_hash'] == queue_handler.compute_content_hash(doc_a_record)
        assert p_b['content_hash'] == queue_handler.compute_content_hash(
            {'text': doc_b_csv}
        )
        assert p_c['content_hash'] == queue_handler.compute_content_hash(doc_c_record)

        # Idempotency: re-running is a no-op.
        rerun_pointers, _, _ = script._plan_backfill_for_org(results, 'org-a')
        rerun = script._commit(queue, rerun_pointers, [], dry_run=False)
        assert rerun['pointers_written'] == 0
        assert rerun['skipped_existing'] == len(rerun_pointers)

    def test_skips_docs_missing_s3_key_or_object(
        self, mock_dynamodb, mock_s3, monkeypatch,
    ):
        """Docs without an s3_key on the results row, or whose S3
        object no longer exists, must be SKIPPED — seeding them with a
        fabricated hash would defeat the purpose of Option B."""
        import backfill_document_queue as script

        monkeypatch.setattr(script, 'dynamodb', mock_dynamodb)
        monkeypatch.setattr(script, 's3', mock_s3)
        mock_s3.create_bucket(Bucket='penguin-health-org-a')
        _seed_org_config(mock_dynamodb, 'org-a', 'penguin-health-org-a')

        results = mock_dynamodb.Table('penguin-health-validation-results')

        # doc-missing-key: results row has no s3_key.
        _seed_doc(
            results, org_id='org-a', doc_id='doc-missing-key',
            ts='2024-01-15T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL'}],
        )
        # doc-missing-object: results row has s3_key but no object.
        _seed_doc(
            results, org_id='org-a', doc_id='doc-missing-object',
            ts='2024-01-15T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL'}],
            s3_key='data/2024-01-15/gone.json',
        )
        # doc-ok: has both.
        _put_raw_json(mock_s3, 'penguin-health-org-a',
                      'data/2024-01-15/doc-ok.json',
                      {'source_record_id': 'doc-ok', 'text': 'x'})
        _seed_doc(
            results, org_id='org-a', doc_id='doc-ok',
            ts='2024-01-15T10:00:00',
            rules=[{'rule_id': 'r1', 'category': 'X', 'status': 'FAIL'}],
            s3_key='data/2024-01-15/doc-ok.json',
        )

        pointers, versions, stats = script._plan_backfill_for_org(
            results, 'org-a',
        )
        assert stats['scanned'] == 3
        assert stats['unique_docs'] == 3
        assert stats['skipped_missing_s3_key'] == 1
        assert stats['skipped_missing_s3_object'] == 1
        # Only doc-ok gets a pointer + version.
        assert [p['document_id'] for p in pointers] == ['doc-ok']
        assert len(versions) == 1
