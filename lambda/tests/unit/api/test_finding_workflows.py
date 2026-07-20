"""Tests for finding/document-level reviewer workflows on the document queue.

Covers:
- `queue_confirm_document`: stamps `document_confirmed=true` on
  documents whose rules are all PASS/SKIP, then flips the queue pointer
  to `status='confirmed'`.
- `queue_mark_incorrect`: outcome parameter (PASS or FAIL) so SKIP rules can
  be recategorized into either bucket while capturing feedback.
- Rule-level mutations (`queue_confirm_finding`, `queue_mark_resolved`,
  `queue_mark_incorrect`) are locked once `document_confirmed=true`.
- Resolving the last open FAIL flips the pointer status to `resolved`.

See docs/validation-workflow-states.md for the state machine.
"""

import json
import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def _seed_doc_and_pointer(mock_dynamodb, rules, *, document_confirmed=False):
    """Seed the results-table row + queue-pointer that back a queue entry.

    The queue endpoints read the pointer first and follow its back-pointer
    to the underlying results row for the rules[] and the reviewer-state
    fields, so both must exist.
    """
    results_table = mock_dynamodb.Table('penguin-health-validation-results')
    queue_table = mock_dynamodb.Table('penguin-health-document-queue')

    result_item = {
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
        result_item['document_confirmed'] = True
        result_item['document_confirmed_at'] = '2024-02-01T00:00:00Z'
        result_item['document_confirmed_by'] = 'reviewer@example.com'
    results_table.put_item(Item=result_item)

    pointer_item = {
        'pk': 'ORG#test-org',
        'sk': 'DOC#12345',
        'document_id': '12345',
        'organization_id': 'test-org',
        'status': 'confirmed' if document_confirmed else 'open',
        'gsi1pk': (
            'ORG#test-org#STATUS#confirmed'
            if document_confirmed else 'ORG#test-org#STATUS#open'
        ),
        'gsi1sk': 'LAST_UPDATED#2024-01-15T10:00:00',
        'content_hash': 'seed-hash',
        'latest_version_sk': 'VERSION#2024-01-15T10:00:00',
        'latest_validation_run_id': '20240115-100000',
        'latest_validation_timestamp': '2024-01-15T10:00:00',
        'latest_validation_result_pk': 'DOC#12345',
        'latest_validation_result_sk': 'VALIDATION#2024-01-15T10:00:00',
        'first_seen_run_id': '20240115-100000',
        'first_seen_at': '2024-01-15T10:00:00',
        'last_updated_at': '2024-01-15T10:00:00',
        'last_seen_at': '2024-01-15T10:00:00',
        'version_count': 1,
        'seen_count': 1,
        'field_values_snapshot': {'service_id': '12345'},
    }
    if not document_confirmed:
        # Sparse GSI2: only present while status=open.
        pointer_item['gsi2pk'] = 'STATUS#open'
        pointer_item['gsi2sk'] = 'LAST_UPDATED#2024-01-15T10:00:00'
    queue_table.put_item(Item=pointer_item)
    return results_table, queue_table


class TestQueueConfirmDocument:
    def test_super_admin_can_confirm_all_pass_or_skip(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
            {'rule_id': 'rule-002', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ])

        resp = queue_confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body['document_confirmed'] is True
        assert body['document_confirmed_by'] == 'admin@example.com'
        # Reconciled pointer state comes back on the response.
        assert body['queue_status'] == 'confirmed'

        results_table = mock_dynamodb.Table('penguin-health-validation-results')
        result_row = results_table.get_item(
            Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'}
        )['Item']
        assert result_row['document_confirmed'] is True

        queue_table = mock_dynamodb.Table('penguin-health-document-queue')
        pointer = queue_table.get_item(
            Key={'pk': 'ORG#test-org', 'sk': 'DOC#12345'}
        )['Item']
        assert pointer['status'] == 'confirmed'
        # Terminal state removes the entry from GSI2 (the auto-close scan).
        assert 'gsi2pk' not in pointer

    def test_rejects_when_any_rule_is_fail(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
            {'rule_id': 'rule-002', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = queue_confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409
        assert 'rule-002' in json.loads(resp['body'])['rule_ids']

    def test_rejects_when_any_rule_errored(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'ERROR'},
        ])

        resp = queue_confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_double_confirm_returns_conflict(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ], document_confirmed=True)

        resp = queue_confirm_document(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_member_without_category_view_blocked(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ])
        seed_user_perms('member@example.com', 'test-org')  # no view perms

        resp = queue_confirm_document(
            event=member_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 403

    def test_member_with_view_can_confirm(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms,
    ):
        from api.admin_api import queue_confirm_document
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'PASS'},
        ])
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Compliance Audit': ['view']},
        )

        resp = queue_confirm_document(
            event=member_event,
            path_params={'orgId': 'test-org', 'docId': '12345'},
            body={},
        )
        assert resp['statusCode'] == 200


class TestQueueMarkIncorrectOutcome:
    def test_default_outcome_is_pass_for_fail_flow(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_mark_incorrect
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = queue_mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={},  # no outcome → defaults to PASS
        )
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body['status'] == 'PASS'
        # Only rule was FAIL and reviewer overrode to PASS → no FAILs left,
        # so the pointer's failed_findings count is now 0. Status stays
        # `open` (no work was resolved by the reviewer; the finding is
        # simply not a real fail).
        assert body['failed_findings'] == 0

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(
            Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'}
        )['Item']
        assert item['rules'][0]['status'] == 'PASS'
        assert item['rules'][0]['feedback_given'] is True

    def test_skip_to_fail_outcome(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_mark_incorrect
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ])

        resp = queue_mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={'outcome': 'FAIL'},
        )
        assert resp['statusCode'] == 200
        assert json.loads(resp['body'])['status'] == 'FAIL'

        table = mock_dynamodb.Table('penguin-health-validation-results')
        item = table.get_item(
            Key={'pk': 'DOC#12345', 'sk': 'VALIDATION#2024-01-15T10:00:00'}
        )['Item']
        assert item['rules'][0]['status'] == 'FAIL'
        assert item['rules'][0]['feedback_given'] is True

    def test_invalid_outcome_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_mark_incorrect
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = queue_mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={'outcome': 'SKIP'},
        )
        assert resp['statusCode'] == 400


class TestQueueMarkResolvedFlipsPointerToResolved:
    def test_last_open_fail_resolved_flips_status(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        """The queue's reason for existing: reviewer clears the last open
        FAIL → pointer status flips to `resolved` and the entry drops off
        the auto-close scan."""
        from api.admin_api import queue_mark_resolved

        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ])

        resp = queue_mark_resolved(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={},
        )
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body['fixed'] is True
        assert body['queue_status'] == 'resolved'
        assert body['open_findings'] == 0
        assert body['resolved_findings'] == 1

        pointer = mock_dynamodb.Table('penguin-health-document-queue').get_item(
            Key={'pk': 'ORG#test-org', 'sk': 'DOC#12345'}
        )['Item']
        assert pointer['status'] == 'resolved'
        assert 'gsi2pk' not in pointer  # dropped from auto-close scan


class TestConfirmedDocumentLocksMutations:
    """Once a document is confirmed, no rule-level mutations are permitted."""

    def test_confirm_finding_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_confirm_finding
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ], document_confirmed=True)

        resp = queue_confirm_finding(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_mark_resolved_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_mark_resolved
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'FAIL'},
        ], document_confirmed=True)

        resp = queue_mark_resolved(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={},
        )
        assert resp['statusCode'] == 409

    def test_mark_incorrect_locked(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import queue_mark_incorrect
        _seed_doc_and_pointer(mock_dynamodb, [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit', 'status': 'SKIP'},
        ], document_confirmed=True)

        resp = queue_mark_incorrect(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'docId': '12345', 'ruleId': 'rule-001'},
            body={'outcome': 'FAIL'},
        )
        assert resp['statusCode'] == 409
