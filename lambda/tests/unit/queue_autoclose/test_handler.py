"""
Tests for the queue auto-close Lambda.

Guardrails covered:
- Only entries idle longer than the org's threshold get closed.
- Per-org override on `org_config` (sk=QUEUE_CONFIG) is honored; missing
  overrides fall through to the default 90 days.
- A reviewer race (status flipped between our query and our update) does
  NOT clobber the reviewer state — the ConditionExpression bounces us.
- Terminal state drops the row from GSI2 so subsequent scans skip it.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'queue-autoclose',
    ),
)


@pytest.fixture(autouse=True)
def _reset_handler_ddb():
    """The autoclose handler caches a boto3 resource. Reset between
    tests so each `mock_aws` context gets its own handle."""
    try:
        import handler
        handler._reset_for_tests()
    except ImportError:
        pass
    yield
    try:
        import handler
        handler._reset_for_tests()
    except ImportError:
        pass


def _now():
    return datetime.now(timezone.utc)


def _seed_pointer(table, *, org_id, doc_id, last_updated, status='open'):
    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'DOC#{doc_id}',
        'gsi1pk': f'ORG#{org_id}#STATUS#{status}',
        'gsi1sk': f'LAST_UPDATED#{last_updated}',
        'document_id': doc_id,
        'organization_id': org_id,
        'status': status,
        'last_updated_at': last_updated,
        'content_hash': 'seed',
        'field_values_snapshot': {},
    }
    if status == 'open':
        item['gsi2pk'] = 'STATUS#open'
        item['gsi2sk'] = f'LAST_UPDATED#{last_updated}'
    table.put_item(Item=item)


class TestQueueAutoclose:
    def test_closes_stale_open_entries_leaves_fresh_alone(
        self, mock_dynamodb, monkeypatch,
    ):
        monkeypatch.setenv('DOCUMENT_QUEUE_TABLE', 'penguin-health-document-queue')
        monkeypatch.setenv('ORG_CONFIG_TABLE_NAME', 'penguin-health-org-config')
        monkeypatch.setenv('DEFAULT_AUTOCLOSE_DAYS', '90')

        import handler

        queue = mock_dynamodb.Table('penguin-health-document-queue')
        # 3 stale (100 days ago) + 2 fresh (10 days ago).
        stale_ts = (_now() - timedelta(days=100)).isoformat()
        fresh_ts = (_now() - timedelta(days=10)).isoformat()
        for i in range(3):
            _seed_pointer(queue, org_id='org-a', doc_id=f'stale-{i}',
                          last_updated=stale_ts)
        for i in range(2):
            _seed_pointer(queue, org_id='org-a', doc_id=f'fresh-{i}',
                          last_updated=fresh_ts)

        result = handler.lambda_handler({}, context=None)

        assert result['closed'] == 3
        assert result['races'] == 0

        for i in range(3):
            item = queue.get_item(
                Key={'pk': 'ORG#org-a', 'sk': f'DOC#stale-{i}'}
            )['Item']
            assert item['status'] == 'auto-closed'
            assert item['auto_closed_reason'] == 'idle_over_threshold'
            # Sparse GSI2 attributes must be removed so subsequent scans
            # don't return this row.
            assert 'gsi2pk' not in item
            assert 'gsi2sk' not in item

        for i in range(2):
            item = queue.get_item(
                Key={'pk': 'ORG#org-a', 'sk': f'DOC#fresh-{i}'}
            )['Item']
            assert item['status'] == 'open'
            assert 'gsi2pk' in item

    def test_reviewer_race_does_not_clobber(
        self, mock_dynamodb, monkeypatch,
    ):
        """If a reviewer resolves the entry between our GSI2 read and
        our UpdateItem, the ConditionExpression must fail — we count it
        as a race and leave the reviewer's state alone."""
        monkeypatch.setenv('DOCUMENT_QUEUE_TABLE', 'penguin-health-document-queue')
        monkeypatch.setenv('ORG_CONFIG_TABLE_NAME', 'penguin-health-org-config')
        monkeypatch.setenv('DEFAULT_AUTOCLOSE_DAYS', '90')

        import handler

        queue = mock_dynamodb.Table('penguin-health-document-queue')
        stale_ts = (_now() - timedelta(days=100)).isoformat()
        _seed_pointer(queue, org_id='org-a', doc_id='raced',
                      last_updated=stale_ts)

        # Simulate a reviewer beating us to the punch after our GSI2
        # read: the base row has already flipped to `resolved`.
        queue.update_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#raced'},
            UpdateExpression=(
                'SET #status = :s, gsi1pk = :g REMOVE gsi2pk, gsi2sk'
            ),
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':s': 'resolved',
                ':g': 'ORG#org-a#STATUS#resolved',
            },
        )
        # But keep it visible on GSI2 to simulate the race: re-seed the
        # GSI2 keys so our Query returns it, then confirm our
        # ConditionExpression protects us.
        queue.update_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#raced'},
            UpdateExpression='SET gsi2pk = :p, gsi2sk = :s',
            ExpressionAttributeValues={
                ':p': 'STATUS#open',
                ':s': f'LAST_UPDATED#{stale_ts}',
            },
        )

        result = handler.lambda_handler({}, context=None)

        # We scanned the row but the conditional update failed and left
        # the reviewer's `resolved` intact.
        assert result['closed'] == 0
        assert result['races'] == 1

        item = queue.get_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#raced'}
        )['Item']
        assert item['status'] == 'resolved'

    def test_per_org_threshold_override_honored(
        self, mock_dynamodb, monkeypatch,
    ):
        """org-config sk=QUEUE_CONFIG rows override the default. Below
        the org's threshold => stays open even if past the default."""
        monkeypatch.setenv('DOCUMENT_QUEUE_TABLE', 'penguin-health-document-queue')
        monkeypatch.setenv('ORG_CONFIG_TABLE_NAME', 'penguin-health-org-config')
        monkeypatch.setenv('DEFAULT_AUTOCLOSE_DAYS', '90')

        import handler

        # Org A: 30-day threshold, so a 100-day-idle row closes.
        # Org B: 365-day threshold, so a 100-day-idle row stays open.
        cfg = mock_dynamodb.Table('penguin-health-org-config')
        cfg.put_item(Item={
            'pk': 'ORG#org-a', 'sk': 'QUEUE_CONFIG', 'autoclose_days': 30,
        })
        cfg.put_item(Item={
            'pk': 'ORG#org-b', 'sk': 'QUEUE_CONFIG', 'autoclose_days': 365,
        })

        queue = mock_dynamodb.Table('penguin-health-document-queue')
        stale_ts = (_now() - timedelta(days=100)).isoformat()
        _seed_pointer(queue, org_id='org-a', doc_id='doc-a',
                      last_updated=stale_ts)
        _seed_pointer(queue, org_id='org-b', doc_id='doc-b',
                      last_updated=stale_ts)

        result = handler.lambda_handler({}, context=None)

        # Both entries showed up in the loosest-cutoff GSI2 query (365 days),
        # but per-org filtering kept doc-b open.
        assert result['closed'] == 1

        assert queue.get_item(
            Key={'pk': 'ORG#org-a', 'sk': 'DOC#doc-a'}
        )['Item']['status'] == 'auto-closed'
        assert queue.get_item(
            Key={'pk': 'ORG#org-b', 'sk': 'DOC#doc-b'}
        )['Item']['status'] == 'open'
