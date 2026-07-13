"""Tests for finding/document-level reviewer workflows.

Covers:
- `confirm_document`: new endpoint that stamps `document_confirmed=true` on
  documents whose rules are all PASS/SKIP.
- `mark_incorrect`: outcome parameter (PASS or FAIL) so SKIP rules can be
  recategorized into either bucket while capturing feedback.
- Rule-level mutations (`confirm_finding`, `mark_resolved`, `mark_incorrect`)
  are locked once `document_confirmed=true`.

See docs/validation-workflow-states.md for the full state machine.
"""

import json
import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def _seed_doc_with_rules(mock_dynamodb, rules, *, document_confirmed=False):
    """Overwrite the sample validation result with the given rules payload."""
    table = mock_dynamodb.Table('penguin-health-validation-results')
    item = {
        'pk': 'DOC#12345',
        'sk': 'VALIDATION#2024-01-15T10:00:00',
        'gsi1pk': 'ORG#test-org',
        'gsi1sk': 'DOC#12345',
        'gsi2pk': 'RUN#20240115-100000',
        'gsi2sk': 'DOC#12345',
        'document_id': '12345',
        'validation_run_id': '20240115-100000',
        'organization_id': 'test-org',
        'rules': rules,
        'extracted_fields': {'service_id': '12345'},
    }
    if document_confirmed:
        item['document_confirmed'] = True
        item['document_confirmed_at'] = '2024-02-01T00:00:00Z'
        item['document_confirmed_by'] = 'reviewer@example.com'
    table.put_item(Item=item)
    return table


class TestConfirmDocument:
    def test_super_admin_can_confirm_all_pass_or_skip(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
            {'rule_id': 'rule-002', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ])

        resp = confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body['document_confirmed'] is True
        assert body['document_confirmed_by'] == 'admin@example.com'

        # Verify persistence
        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'})['Item']
        assert item['document_confirmed'] is True

    def test_rejects_when_any_rule_is_fail(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
            {'rule_id': 'rule-002', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409
        assert 'rule-002' in json.loads(resp['body'])['rule_ids']

    def test_rejects_when_any_rule_errored(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'ERROR'},
        ])

        resp = confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_double_confirm_returns_conflict(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ], document_confirmed=True)

        resp = confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_member_without_category_view_blocked(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ])
        seed_user_perms('member@example.com', 'test-org')  # no view perms

        resp = confirm_document(
            event=member_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 403

    def test_member_with_view_can_confirm(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms,
    ):
        from api.admin_api import confirm_document
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ])
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Compliance Audit': ['view']},
        )

        resp = confirm_document(
            event=member_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 200


class TestMarkIncorrectOutcome:
    def test_default_outcome_is_pass_for_fail_flow(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import mark_incorrect
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001'},  # no outcome — defaults to PASS
        )
        assert resp['statusCode'] == 200
        assert json.loads(resp['body'])['status'] == 'PASS'

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'})['Item']
        assert item['rules'][0]['status'] == 'PASS'
        assert item['rules'][0]['feedback_given'] is True

    def test_skip_to_fail_outcome(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import mark_incorrect
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ])

        resp = mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001', 'outcome': 'FAIL'},
        )
        assert resp['statusCode'] == 200
        assert json.loads(resp['body'])['status'] == 'FAIL'

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'})['Item']
        assert item['rules'][0]['status'] == 'FAIL'
        assert item['rules'][0]['feedback_given'] is True

    def test_invalid_outcome_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import mark_incorrect
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001', 'outcome': 'SKIP'},
        )
        assert resp['statusCode'] == 400


class TestConfirmedDocumentLocksMutations:
    """Once a document is confirmed, no rule-level mutations are permitted."""

    def test_confirm_finding_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import confirm_finding
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ], document_confirmed=True)

        resp = confirm_finding(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001'},
        )
        assert resp['statusCode'] == 409

    def test_mark_resolved_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import mark_resolved
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ], document_confirmed=True)

        resp = mark_resolved(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001'},
        )
        assert resp['statusCode'] == 409

    def test_mark_incorrect_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import mark_incorrect
        _seed_doc_with_rules(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ], document_confirmed=True)

        resp = mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001', 'outcome': 'FAIL'},
        )
        assert resp['statusCode'] == 409
