"""Regression tests for `results_handler.aggregate_run_summary`.

DynamoDB caps a single Query response at 1 MB. Runs with more validated
documents than fit in one page must paginate on `LastEvaluatedKey` or
the run summary undercounts what was actually validated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine',
    ),
)


class TestAggregateRunSummaryPagination:
    def test_walks_all_pages(self, mock_dynamodb, monkeypatch):
        """Simulate a multi-page GSI2 response. The function must call
        Query with ExclusiveStartKey until LastEvaluatedKey is absent,
        and its `total` must reflect every page."""
        import results_handler
        from results_handler import aggregate_run_summary

        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        page_one = [
            {'summary': {'passed': 1, 'failed': 0, 'skipped': 0}}
            for _ in range(130)
        ]
        page_two = [
            {'summary': {'passed': 0, 'failed': 1, 'skipped': 0}}
            for _ in range(90)
        ]
        pages = [
            {'Items': page_one, 'LastEvaluatedKey': {'gsi2pk': 'x', 'gsi2sk': 'y'}},
            {'Items': page_two},
        ]

        call_log = []

        class FakeTable:
            def query(self, **kwargs):
                call_log.append(kwargs)
                return pages[len(call_log) - 1]

        class FakeResource:
            def Table(self, name):
                assert name == env_config['DYNAMODB_TABLE']
                return FakeTable()

        monkeypatch.setattr(results_handler, 'dynamodb', FakeResource())

        summary = aggregate_run_summary('run-multi-page', env_config)

        assert len(call_log) == 2
        assert 'ExclusiveStartKey' not in call_log[0]
        assert call_log[1]['ExclusiveStartKey'] == {'gsi2pk': 'x', 'gsi2sk': 'y'}
        assert summary == {
            'total': 220,
            'passed': 130,
            'failed': 90,
            'skipped': 0,
        }

    def test_single_page_no_extra_query(self, mock_dynamodb, monkeypatch):
        """When the response has no LastEvaluatedKey, the loop exits
        after one query — no infinite pagination."""
        import results_handler
        from results_handler import aggregate_run_summary

        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}

        call_log = []

        class FakeTable:
            def query(self, **kwargs):
                call_log.append(kwargs)
                return {'Items': [
                    {'summary': {'passed': 1, 'failed': 0, 'skipped': 0}},
                ]}

        class FakeResource:
            def Table(self, name):
                return FakeTable()

        monkeypatch.setattr(results_handler, 'dynamodb', FakeResource())

        summary = aggregate_run_summary('run-single-page', env_config)

        assert len(call_log) == 1
        assert summary == {'total': 1, 'passed': 1, 'failed': 0, 'skipped': 0}
