"""
Tests for the date-scoped validation flow:
- compute_dates_from_window pure function
- resolve_dates event-resolution priority
- get_processed_s3_keys ledger query
- store_run_summary persists dates
"""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'))


# ---------- compute_dates_from_window ----------

class TestComputeDatesFromWindow:
    def test_single_day_back(self):
        from rules_engine_rag import compute_dates_from_window
        out = compute_dates_from_window(
            {'days_back_from_today': [1]},
            date(2026, 5, 5),  # Tuesday
        )
        assert out == ['2026-05-04']

    def test_three_days_back_for_monday(self):
        from rules_engine_rag import compute_dates_from_window
        # Monday 2026-05-04 wants Fri 5/1, Sat 5/2, Sun 5/3
        out = compute_dates_from_window(
            {'days_back_from_today': [3, 2, 1]},
            date(2026, 5, 4),
        )
        assert out == ['2026-05-01', '2026-05-02', '2026-05-03']

    def test_empty_list_returns_empty(self):
        from rules_engine_rag import compute_dates_from_window
        assert compute_dates_from_window({'days_back_from_today': []}, date.today()) == []

    def test_missing_key_returns_empty(self):
        from rules_engine_rag import compute_dates_from_window
        assert compute_dates_from_window({}, date.today()) == []


# ---------- resolve_dates ----------

class TestResolveDates:
    def test_dates_in_event_used_verbatim(self):
        from rules_engine_rag import resolve_dates
        assert resolve_dates({'dates': ['2026-05-01', '2026-05-02']}) == \
               ['2026-05-01', '2026-05-02']

    def test_date_window_resolved(self, monkeypatch):
        from rules_engine_rag import resolve_dates
        import rules_engine_rag
        monkeypatch.setattr(rules_engine_rag, 'today_utc', lambda: date(2026, 5, 5))
        assert resolve_dates({'date_window': {'days_back_from_today': [1]}}) == \
               ['2026-05-04']

    def test_neither_falls_back_to_today(self, monkeypatch):
        from rules_engine_rag import resolve_dates
        import rules_engine_rag
        monkeypatch.setattr(rules_engine_rag, 'today_utc', lambda: date(2026, 5, 7))
        assert resolve_dates({}) == ['2026-05-07']

    def test_dates_takes_precedence_over_window(self):
        """Continuation legs always carry concrete `dates` — they win."""
        from rules_engine_rag import resolve_dates
        out = resolve_dates({
            'dates': ['2026-05-09'],
            'date_window': {'days_back_from_today': [99]},
        })
        assert out == ['2026-05-09']


# ---------- get_processed_s3_keys ----------

class TestGetProcessedS3Keys:
    def test_returns_empty_for_unknown_run(self, mock_dynamodb):
        from results_handler import get_processed_s3_keys
        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}
        assert get_processed_s3_keys('nonexistent-run', env_config) == set()

    def test_returns_keys_from_run_results(self, mock_dynamodb):
        from results_handler import store_results, get_processed_s3_keys
        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        for i in range(3):
            store_results({
                'validation_run_id': 'run-1',
                'organization_id': 'test-org',
                'document_id': f'doc-{i}',
                'filename': f'data/2026-05-01/file{i}.csv',
                'validation_timestamp': f'2026-05-01T00:00:0{i}',
                'summary': {'total_rules': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
                'rules': [],
                's3_key': f'data/2026-05-01/file{i}.csv',
            }, env_config)

        keys = get_processed_s3_keys('run-1', env_config)
        assert keys == {
            'data/2026-05-01/file0.csv',
            'data/2026-05-01/file1.csv',
            'data/2026-05-01/file2.csv',
        }

    def test_isolates_runs(self, mock_dynamodb):
        """A second run's keys must not bleed into the first run's set."""
        from results_handler import store_results, get_processed_s3_keys
        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        store_results({
            'validation_run_id': 'run-A',
            'organization_id': 'test-org',
            'document_id': 'doc-1',
            'filename': 'a.csv',
            'validation_timestamp': '2026-05-01T00:00:00',
            'summary': {'total_rules': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
            'rules': [],
            's3_key': 'data/2026-05-01/a.csv',
        }, env_config)
        store_results({
            'validation_run_id': 'run-B',
            'organization_id': 'test-org',
            'document_id': 'doc-1',
            'filename': 'a.csv',
            'validation_timestamp': '2026-05-01T00:00:01',
            'summary': {'total_rules': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
            'rules': [],
            's3_key': 'data/2026-05-01/a.csv',
        }, env_config)

        # Each run sees its own row only
        assert get_processed_s3_keys('run-A', env_config) == {'data/2026-05-01/a.csv'}
        assert get_processed_s3_keys('run-B', env_config) == {'data/2026-05-01/a.csv'}


# ---------- store_run_summary persists dates ----------

class TestStoreRunSummaryDates:
    def test_dates_written_to_record(self, mock_dynamodb):
        from results_handler import store_run_summary
        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        store_run_summary(
            'run-99', 'test-org',
            {'total': 1, 'passed': 1, 'failed': 0, 'skipped': 0},
            env_config,
            categories=['Billing'],
            dates=['2026-05-01', '2026-05-02'],
        )

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(Key={'pk': 'ORG#test-org', 'sk': 'RUN#run-99'})['Item']
        assert item['dates'] == ['2026-05-01', '2026-05-02']
        assert item['categories'] == ['Billing']

    def test_default_dates_is_empty_list(self, mock_dynamodb):
        from results_handler import store_run_summary
        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        store_run_summary(
            'run-100', 'test-org',
            {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
            env_config,
        )
        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(Key={'pk': 'ORG#test-org', 'sk': 'RUN#run-100'})['Item']
        assert item['dates'] == []
