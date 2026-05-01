"""
Tests for category-based filtering and persistence in rules-engine.

Covers:
- filter_rules_by_categories: pure function used in lambda_handler
- store_run_summary: persists `categories` field on the run record
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'))


# ---------- filter_rules_by_categories ----------

class TestFilterRulesByCategories:
    def test_empty_filter_returns_all(self):
        from rules_engine_rag import filter_rules_by_categories
        rules = [
            {'rule_id': '1', 'category': 'Billing'},
            {'rule_id': '2', 'category': 'Intake'},
        ]
        assert filter_rules_by_categories(rules, []) == rules
        assert filter_rules_by_categories(rules, None) == rules

    def test_filter_keeps_only_matching(self):
        from rules_engine_rag import filter_rules_by_categories
        rules = [
            {'rule_id': '1', 'category': 'Billing'},
            {'rule_id': '2', 'category': 'Intake'},
            {'rule_id': '3', 'category': 'Quality Assurance'},
        ]
        out = filter_rules_by_categories(rules, ['Billing', 'Quality Assurance'])
        assert [r['rule_id'] for r in out] == ['1', '3']

    def test_filter_excludes_rules_without_category(self):
        from rules_engine_rag import filter_rules_by_categories
        rules = [
            {'rule_id': '1', 'category': 'Billing'},
            {'rule_id': '2'},  # no category
        ]
        assert filter_rules_by_categories(rules, ['Billing']) == [rules[0]]


# ---------- store_run_summary persists categories ----------

class TestStoreRunSummaryCategories:
    def test_categories_written_to_run_record(self, mock_dynamodb):
        from results_handler import store_run_summary

        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}
        summary = {'total': 5, 'passed': 4, 'failed': 1, 'skipped': 0}
        store_run_summary(
            'run-123', 'test-org', summary, env_config,
            categories=['Billing', 'Intake'],
        )

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(
            Key={'pk': 'ORG#test-org', 'sk': 'RUN#run-123'}
        )['Item']
        assert sorted(item['categories']) == ['Billing', 'Intake']
        assert item['total_documents'] == 5

    def test_legacy_calls_default_to_empty_list(self, mock_dynamodb):
        from results_handler import store_run_summary

        env_config = {'DYNAMODB_TABLE': 'penguin-health-validation-results'}
        summary = {'total': 1, 'passed': 1, 'failed': 0, 'skipped': 0}
        store_run_summary('run-456', 'test-org', summary, env_config)

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(
            Key={'pk': 'ORG#test-org', 'sk': 'RUN#run-456'}
        )['Item']
        assert item['categories'] == []
