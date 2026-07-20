"""Guardrails for ``scripts/purge_pre_cutoff.py``.

The purge is destructive, so the guardrails matter:

  * Rows with a validation timestamp >= cutoff are NEVER selected for
    deletion (pointer, doc result, run summary, or S3 object).
  * A version row that is still the ``latest_version_sk`` of a live
    (post-cutoff) pointer is refused — deleting it would orphan the
    queue entry.
  * Dry-run reports counts but writes nothing.
"""

import os
import sys
from datetime import datetime, timezone


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'scripts'),
)


def _cutoff():
    return datetime(2026, 7, 1, tzinfo=timezone.utc)


def _seed_validation_results(mock_dynamodb):
    """Seed the validation-results table with a mix of pre- and post-cutoff
    rows across two orgs.

    Rows keep only the columns the purge scan projects; nothing PHI-shaped
    is fabricated because we don't want tests reasoning about
    field_values / rules that aren't part of the purge contract."""
    tbl = mock_dynamodb.Table('penguin-health-validation-results')

    tbl.put_item(Item={
        'pk': 'DOC#doc-old', 'sk': 'VALIDATION#2026-06-15T10:00:00',
        'organization_id': 'org-a', 'validation_run_id': '20260615-100000',
    })
    tbl.put_item(Item={
        'pk': 'DOC#doc-fresh', 'sk': 'VALIDATION#2026-07-05T10:00:00',
        'organization_id': 'org-a', 'validation_run_id': '20260705-100000',
    })
    tbl.put_item(Item={
        'pk': 'DOC#doc-old#SKIPPED#20260615-100000',
        'sk': 'VALIDATION#2026-06-15T10:05:00',
        'organization_id': 'org-a',
        'validation_run_id': '20260615-100000',
        'duplicate_of_version_sk': 'VERSION#2026-06-14T10:00:00',
    })
    tbl.put_item(Item={
        'pk': 'ORG#org-a', 'sk': 'RUN#20260615-100000',
        'organization_id': 'org-a',
    })
    tbl.put_item(Item={
        'pk': 'ORG#org-a', 'sk': 'RUN#20260705-100000',
        'organization_id': 'org-a',
    })
    tbl.put_item(Item={
        'pk': 'DOC#doc-org-b-old', 'sk': 'VALIDATION#2026-05-15T10:00:00',
        'organization_id': 'org-b',
    })


def _seed_queue(mock_dynamodb):
    tbl = mock_dynamodb.Table('penguin-health-document-queue')

    # Pointer for doc-old (org-a): pre-cutoff -> must be deleted.
    tbl.put_item(Item={
        'pk': 'ORG#org-a', 'sk': 'DOC#doc-old',
        'organization_id': 'org-a',
        'latest_validation_timestamp': '2026-06-15T10:00:00',
        'latest_version_sk': 'VERSION#2026-06-15T10:00:00',
    })
    # Pointer for doc-fresh (org-a): post-cutoff -> KEEP, and its
    # latest_version_sk is a guarded key even though the version is old.
    tbl.put_item(Item={
        'pk': 'ORG#org-a', 'sk': 'DOC#doc-fresh',
        'organization_id': 'org-a',
        'latest_validation_timestamp': '2026-07-05T10:00:00',
        # Contrived but critical: even if the last version stored on the
        # pointer is dated pre-cutoff, we must NOT delete it — the pointer
        # points at it right now.
        'latest_version_sk': 'VERSION#2026-06-20T09:00:00',
    })

    # Version rows.
    tbl.put_item(Item={
        'pk': 'ORG#org-a#DOC#doc-old',
        'sk': 'VERSION#2026-06-15T10:00:00',
        'organization_id': 'org-a',
    })
    tbl.put_item(Item={
        'pk': 'ORG#org-a#DOC#doc-fresh',
        'sk': 'VERSION#2026-06-20T09:00:00',  # live pointer's latest
        'organization_id': 'org-a',
    })
    tbl.put_item(Item={
        'pk': 'ORG#org-a#DOC#doc-fresh',
        'sk': 'VERSION#2026-07-05T10:00:00',  # post-cutoff, keep
        'organization_id': 'org-a',
    })
    tbl.put_item(Item={
        'pk': 'ORG#org-b#DOC#doc-org-b-old',
        'sk': 'VERSION#2026-05-15T10:00:00',
        'organization_id': 'org-b',
    })


class TestPurgePlan:
    def test_dry_run_selects_only_pre_cutoff_rows(
        self, mock_dynamodb, monkeypatch,
    ):
        """The scan pass must return only pre-cutoff rows and must NOT
        include a version row that's still a live pointer's
        latest_version_sk."""
        import purge_pre_cutoff as p
        monkeypatch.setattr(p, 'dynamodb', mock_dynamodb)

        _seed_validation_results(mock_dynamodb)
        _seed_queue(mock_dynamodb)

        cutoff = _cutoff()

        # 1. validation-results pass.
        rows = list(p._scan_validation_results_pre_cutoff(cutoff, set()))
        by_kind = {}
        for r in rows:
            by_kind.setdefault(r['kind'], []).append(r)
        # doc_result: doc-old and doc-org-b-old, not doc-fresh
        doc_sks = sorted(r['sk'] for r in by_kind.get('doc_result', []))
        assert doc_sks == [
            'VALIDATION#2026-05-15T10:00:00',
            'VALIDATION#2026-06-15T10:00:00',
        ]
        # run_summary: only the pre-cutoff run
        run_sks = [r['sk'] for r in by_kind.get('run_summary', [])]
        assert run_sks == ['RUN#20260615-100000']
        # sentinel captured
        assert len(by_kind.get('sentinel_skipped', [])) == 1

        # 2. queue live-latest map.
        live = p._live_latest_per_pointer_pk(cutoff, set())
        # Only the post-cutoff pointer contributes a guard entry.
        assert live == {
            'ORG#org-a#DOC#doc-fresh': 'VERSION#2026-06-20T09:00:00',
        }

        # 3. pointer rows to delete.
        pointer_rows = list(p._scan_queue_pointers(cutoff, set()))
        pointer_sks = sorted(r['sk'] for r in pointer_rows)
        assert pointer_sks == ['DOC#doc-old']  # doc-fresh preserved

        # 4. version rows to delete — the live pointer's version is
        # refused even though it's pre-cutoff.
        version_rows = list(p._scan_queue_versions(cutoff, set(), live))
        version_pks_sks = sorted((r['pk'], r['sk']) for r in version_rows)
        assert version_pks_sks == [
            ('ORG#org-a#DOC#doc-old', 'VERSION#2026-06-15T10:00:00'),
            ('ORG#org-b#DOC#doc-org-b-old', 'VERSION#2026-05-15T10:00:00'),
        ]

    def test_org_filter_scopes_selection(self, mock_dynamodb, monkeypatch):
        import purge_pre_cutoff as p
        monkeypatch.setattr(p, 'dynamodb', mock_dynamodb)

        _seed_validation_results(mock_dynamodb)
        _seed_queue(mock_dynamodb)
        cutoff = _cutoff()

        rows = list(p._scan_validation_results_pre_cutoff(cutoff, {'org-b'}))
        # Only org-b's doc_result should show up.
        assert [r['pk'] for r in rows] == ['DOC#doc-org-b-old']

        version_rows = list(p._scan_queue_versions(cutoff, {'org-b'}, {}))
        assert [r['pk'] for r in version_rows] == [
            'ORG#org-b#DOC#doc-org-b-old',
        ]

    def test_batch_delete_dry_run_writes_nothing(
        self, mock_dynamodb, monkeypatch,
    ):
        import purge_pre_cutoff as p
        monkeypatch.setattr(p, 'dynamodb', mock_dynamodb)

        _seed_validation_results(mock_dynamodb)
        tbl = mock_dynamodb.Table('penguin-health-validation-results')
        before = tbl.item_count if hasattr(tbl, 'item_count') else None

        keys = [{'pk': 'DOC#doc-old', 'sk': 'VALIDATION#2026-06-15T10:00:00'}]
        planned = p._batch_delete(
            'penguin-health-validation-results', keys, commit=False,
        )
        # Dry-run reports the planned count but leaves the row in place.
        assert planned == 1
        remaining = tbl.get_item(Key=keys[0])
        assert 'Item' in remaining

    def test_batch_delete_commit_removes_rows(
        self, mock_dynamodb, monkeypatch,
    ):
        import purge_pre_cutoff as p
        monkeypatch.setattr(p, 'dynamodb', mock_dynamodb)

        _seed_validation_results(mock_dynamodb)
        tbl = mock_dynamodb.Table('penguin-health-validation-results')

        keys = [{'pk': 'DOC#doc-old', 'sk': 'VALIDATION#2026-06-15T10:00:00'}]
        n = p._batch_delete(
            'penguin-health-validation-results', keys, commit=True,
        )
        assert n == 1
        remaining = tbl.get_item(Key=keys[0])
        assert 'Item' not in remaining
