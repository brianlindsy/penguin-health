"""
Unit tests for admin_api.py - Authorization and CRUD operations.

Tests the Admin API Lambda function including:
- JWT claim parsing and authorization
- Organization listing with RBAC filtering
- Rule CRUD operations with validation
- Validation workflow operations
"""

import json
import sys
import os

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


class TestAuthorization:
    """Test JWT authorization helpers."""

    def test_get_user_claims_parses_groups_string(self, super_admin_event):
        """Groups come as string '[Admins]' from Cognito and should be parsed to list."""
        from api.admin_api import get_user_claims

        claims = get_user_claims(super_admin_event)

        assert claims['email'] == 'admin@example.com'
        assert 'Admins' in claims['groups']

    def test_get_user_claims_handles_empty_groups(self, org_user_event):
        """Empty groups string '[]' should result in empty list."""
        from api.admin_api import get_user_claims

        claims = get_user_claims(org_user_event)

        assert claims['email'] == 'user@example.com'
        assert claims['groups'] == []
        assert claims['organization_id'] == 'test-org'

    def test_get_user_claims_handles_missing_claims(self, unauthorized_event):
        """Missing claims should return None/empty values."""
        from api.admin_api import get_user_claims

        claims = get_user_claims(unauthorized_event)

        assert claims['email'] is None
        assert claims['groups'] == []
        assert claims['organization_id'] is None

    def test_is_super_admin_returns_true_for_admins_group(self, super_admin_event):
        """User with 'Admins' group should be identified as super admin."""
        from api.admin_api import get_user_claims, is_super_admin

        claims = get_user_claims(super_admin_event)

        assert is_super_admin(claims) is True

    def test_is_super_admin_returns_false_for_org_user(self, org_user_event):
        """User without 'Admins' group should not be super admin."""
        from api.admin_api import get_user_claims, is_super_admin

        claims = get_user_claims(org_user_event)

        assert is_super_admin(claims) is False

    def test_can_access_org_allows_super_admin_any_org(self, super_admin_event):
        """Super admin should be able to access any organization."""
        from api.admin_api import get_user_claims, can_access_org

        claims = get_user_claims(super_admin_event)

        assert can_access_org(claims, 'any-org') is True
        assert can_access_org(claims, 'test-org') is True
        assert can_access_org(claims, 'another-org') is True

    def test_can_access_org_restricts_org_user_to_own_org(self, org_user_event):
        """Org user should only access their assigned organization."""
        from api.admin_api import get_user_claims, can_access_org

        claims = get_user_claims(org_user_event)

        assert can_access_org(claims, 'test-org') is True
        assert can_access_org(claims, 'other-org') is False

    def test_authorize_request_returns_error_for_no_identity(self, unauthorized_event):
        """Request without valid user identity should get 401."""
        from api.admin_api import authorize_request

        claims, error = authorize_request(unauthorized_event)

        assert claims is None
        assert error is not None
        assert error['statusCode'] == 401

    def test_authorize_request_returns_error_for_wrong_org(self, org_user_event):
        """Request for wrong org should get 403."""
        from api.admin_api import authorize_request

        claims, error = authorize_request(org_user_event, org_id='other-org')

        assert claims is None
        assert error is not None
        assert error['statusCode'] == 403

    def test_authorize_request_allows_super_admin(self, super_admin_event):
        """Super admin should be authorized for any org."""
        from api.admin_api import authorize_request

        claims, error = authorize_request(super_admin_event, org_id='any-org')

        assert error is None
        assert claims is not None
        assert claims['email'] == 'admin@example.com'


class TestListOrganizations:
    """Test GET /api/organizations endpoint."""

    def test_super_admin_sees_all_orgs(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Super admin should see all organizations."""
        from api.admin_api import list_organizations

        response = list_organizations(event=super_admin_event)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert 'organizations' in body
        # Should see at least the test-org we created
        org_ids = [o['organization_id'] for o in body['organizations']]
        assert 'test-org' in org_ids

    def test_org_user_sees_only_own_org(self, mock_dynamodb, sample_org_config, org_user_event):
        """Org user should only see their assigned organization."""
        # Add another org that user shouldn't see
        table = mock_dynamodb.Table('penguin-health-org-config')
        table.put_item(Item={
            'pk': 'ORG#other-org',
            'sk': 'METADATA',
            'gsi1pk': 'ORG_METADATA',
            'gsi1sk': 'other-org',
            'organization_id': 'other-org',
            'organization_name': 'Other Organization',
            'enabled': True,
        })

        from api.admin_api import list_organizations

        response = list_organizations(event=org_user_event)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        # Should only see test-org (user's org)
        org_ids = [o['organization_id'] for o in body['organizations']]
        assert 'test-org' in org_ids
        assert 'other-org' not in org_ids


class TestGetOrganization:
    """Test GET /api/organizations/{orgId} endpoint."""

    def test_super_admin_can_get_any_org(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Super admin should be able to get any organization."""
        from api.admin_api import get_organization

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}

        response = get_organization(event=event, path_params=path_params)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['organization_id'] == 'test-org'
        assert body['organization_name'] == 'Test Organization'

    def test_org_user_can_get_own_org(self, mock_dynamodb, sample_org_config, org_user_event):
        """Org user should be able to get their own organization."""
        from api.admin_api import get_organization

        event = {**org_user_event}
        path_params = {'orgId': 'test-org'}

        response = get_organization(event=event, path_params=path_params)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['organization_id'] == 'test-org'

    def test_org_user_cannot_get_other_org(self, mock_dynamodb, sample_org_config, org_user_event):
        """Org user should get 403 when accessing other organization."""
        from api.admin_api import get_organization

        event = {**org_user_event}
        path_params = {'orgId': 'other-org'}

        response = get_organization(event=event, path_params=path_params)

        assert response['statusCode'] == 403

    def test_returns_404_for_nonexistent_org(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should return 404 for organization that doesn't exist."""
        from api.admin_api import get_organization

        event = {**super_admin_event}
        path_params = {'orgId': 'nonexistent-org'}

        response = get_organization(event=event, path_params=path_params)

        assert response['statusCode'] == 404


class TestRuleCRUD:
    """Test rule CRUD operations."""

    def test_list_rules(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should list all rules for an organization."""
        from api.admin_api import list_rules

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}

        response = list_rules(event=event, path_params=path_params)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert 'rules' in body
        assert body['count'] >= 1
        rule_ids = [r['rule_id'] for r in body['rules']]
        assert 'rule-001' in rule_ids

    def test_get_rule(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should get a specific rule."""
        from api.admin_api import get_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org', 'ruleId': 'rule-001'}

        response = get_rule(event=event, path_params=path_params)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['rule_id'] == 'rule-001'
        assert body['name'] == 'Service Date Documentation'

    def test_get_rule_not_found(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should return 404 for nonexistent rule."""
        from api.admin_api import get_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org', 'ruleId': 'nonexistent-rule'}

        response = get_rule(event=event, path_params=path_params)

        assert response['statusCode'] == 404

    def test_create_rule_validates_required_fields(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should reject rule creation with missing required fields."""
        from api.admin_api import create_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}
        body = {'id': 'new-rule'}  # Missing name, category, rule_text

        response = create_rule(event=event, path_params=path_params, body=body)

        assert response['statusCode'] == 400
        response_body = json.loads(response['body'])
        assert 'Missing required fields' in response_body['error']

    def test_create_rule_success(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should successfully create a new rule."""
        from api.admin_api import create_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}
        body = {
            'id': 'new-rule',
            'name': 'New Test Rule',
            'category': 'Compliance Audit',
            'rule_text': 'Verify the documentation is complete.',
        }

        response = create_rule(event=event, path_params=path_params, body=body)

        assert response['statusCode'] == 201
        response_body = json.loads(response['body'])
        assert response_body['rule_id'] == 'new-rule'
        assert response_body['name'] == 'New Test Rule'

    def test_create_rule_rejects_duplicate(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should reject creating a rule that already exists."""
        from api.admin_api import create_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}
        body = {
            'id': 'rule-001',  # Already exists in sample_org_config
            'name': 'Duplicate Rule',
            'category': 'Compliance Audit',
            'rule_text': 'Verify documentation.',
        }

        response = create_rule(event=event, path_params=path_params, body=body)

        assert response['statusCode'] == 409
        response_body = json.loads(response['body'])
        assert 'already exists' in response_body['error']

    def test_create_deterministic_rule_without_rule_text(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Deterministic rules should not require rule_text."""
        from api.admin_api import create_rule

        event = {**super_admin_event}
        path_params = {'orgId': 'test-org'}
        body = {
            'id': 'deterministic-rule',
            'name': 'Deterministic Rule',
            'category': 'Compliance Audit',
            'type': 'deterministic',
            'conditions': [
                {'field': 'status', 'operator': 'equals', 'value': 'approved'}
            ],
        }

        response = create_rule(event=event, path_params=path_params, body=body)

        assert response['statusCode'] == 201


class TestLambdaHandler:
    """Test the main lambda_handler routing."""

    def test_invalid_json_body(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Should return 400 for invalid JSON body."""
        from api.admin_api import lambda_handler

        event = {
            **super_admin_event,
            'routeKey': 'POST /api/organizations/test-org/rules',
            'pathParameters': {'orgId': 'test-org'},
            'body': 'not-valid-json',
        }

        response = lambda_handler(event, None)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Invalid JSON' in body['error']

    def test_route_not_found(self, super_admin_event):
        """Should return 404 for unknown route."""
        from api.admin_api import lambda_handler

        event = {
            **super_admin_event,
            'routeKey': 'GET /api/unknown/route',
        }

        response = lambda_handler(event, None)

        assert response['statusCode'] == 404
        body = json.loads(response['body'])
        assert 'Route not found' in body['error']


class TestMergeDisplayFieldValues:
    """The API overlays `ui_display_fields` onto `field_values` server-side
    so the UI has one dict to read and legacy rows without the mapping fall
    through untouched."""

    def test_overlay_replaces_matching_keys(self):
        from api.admin_api import merge_display_field_values

        item = {
            'field_values': {'provider_display': 'Dr. Smith', 'visit_date': '2026-06-22'},
            'ui_display_fields': {'employee_name': 'Dr. Smith', 'date': '2026-06-22'},
        }
        merged = merge_display_field_values(item)
        # Canonical keys are present alongside the raw source keys.
        assert merged['employee_name'] == 'Dr. Smith'
        assert merged['date'] == '2026-06-22'
        assert merged['provider_display'] == 'Dr. Smith'

    def test_no_ui_display_fields_returns_field_values_unchanged(self):
        """Legacy row / un-configured org: UI reads raw field_values."""
        from api.admin_api import merge_display_field_values

        item = {'field_values': {'employee_name': 'Alice', 'date': '2026-06-22'}}
        assert merge_display_field_values(item) == {
            'employee_name': 'Alice', 'date': '2026-06-22',
        }

    def test_missing_both_returns_empty(self):
        from api.admin_api import merge_display_field_values

        assert merge_display_field_values({}) == {}

    def test_ui_display_fields_wins_when_key_conflicts(self):
        """If the raw field_values happens to already have the canonical
        key, the mapped value takes precedence — the config is the source
        of truth for what the UI shows."""
        from api.admin_api import merge_display_field_values

        item = {
            'field_values': {'employee_name': 'stale'},
            'ui_display_fields': {'employee_name': 'fresh'},
        }
        assert merge_display_field_values(item)['employee_name'] == 'fresh'


class TestUIDisplayFieldsEndpoint:
    """Test GET/PUT /api/organizations/{orgId}/ui-display-fields."""

    def test_get_returns_empty_when_unset(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Missing item → empty mappings dict, which the UI treats as
        the fallback signal."""
        from api.admin_api import get_ui_display_fields

        response = get_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
        )
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['organization_id'] == 'test-org'
        assert body['mappings'] == {}

    def test_put_then_get_round_trip(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import get_ui_display_fields, update_ui_display_fields

        put_body = {'mappings': {'employee_name': 'provider_display', 'date': 'visit_date'}}
        put_resp = update_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body=put_body,
        )
        assert put_resp['statusCode'] == 200

        get_resp = get_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
        )
        assert get_resp['statusCode'] == 200
        got = json.loads(get_resp['body'])
        assert got['mappings'] == put_body['mappings']
        assert 'updated_at' in got

    def test_put_empty_mappings_is_allowed(self, mock_dynamodb, sample_org_config, super_admin_event):
        """Writing an empty dict is the on-record way to turn projection
        off without deleting the item."""
        from api.admin_api import update_ui_display_fields

        resp = update_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'mappings': {}},
        )
        assert resp['statusCode'] == 200

    def test_put_rejects_missing_mappings(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_ui_display_fields

        resp = update_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={},
        )
        assert resp['statusCode'] == 400

    def test_put_rejects_non_dict_mappings(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_ui_display_fields

        resp = update_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'mappings': ['employee_name', 'provider_display']},
        )
        assert resp['statusCode'] == 400

    def test_put_rejects_empty_key_or_value(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_ui_display_fields

        resp = update_ui_display_fields(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'mappings': {'employee_name': ''}},
        )
        assert resp['statusCode'] == 400

    def test_org_user_cannot_edit_other_org(self, mock_dynamodb, sample_org_config, org_user_event):
        from api.admin_api import update_ui_display_fields

        resp = update_ui_display_fields(
            event=org_user_event,
            path_params={'orgId': 'other-org'},
            body={'mappings': {'employee_name': 'provider_display'}},
        )
        assert resp['statusCode'] == 403


class TestOrgProgramsEndpoint:
    """GET/PUT /api/organizations/{orgId}/programs."""

    def test_get_returns_empty_when_unset(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import get_org_programs

        resp = get_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
        )
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body['organization_id'] == 'test-org'
        assert body['programs'] == []

    def test_put_then_get_round_trip(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import get_org_programs, update_org_programs

        put_resp = update_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'programs': ['Program B', 'Program A']},
        )
        assert put_resp['statusCode'] == 200
        # Programs are stored sorted so DDB writes stay stable across roundtrips.
        assert json.loads(put_resp['body'])['programs'] == ['Program A', 'Program B']

        get_resp = get_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
        )
        assert json.loads(get_resp['body'])['programs'] == ['Program A', 'Program B']

    def test_put_dedupes_and_trims(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_org_programs

        resp = update_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'programs': ['Program A', '  Program A  ', 'Program B']},
        )
        assert resp['statusCode'] == 200
        assert json.loads(resp['body'])['programs'] == ['Program A', 'Program B']

    def test_put_rejects_non_list(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_org_programs

        resp = update_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'programs': 'not-a-list'},
        )
        assert resp['statusCode'] == 400

    def test_put_rejects_empty_string(self, mock_dynamodb, sample_org_config, super_admin_event):
        from api.admin_api import update_org_programs

        resp = update_org_programs(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'programs': ['']},
        )
        assert resp['statusCode'] == 400

    def test_put_requires_super_admin(self, mock_dynamodb, sample_org_config,
                                      member_event, seed_user_perms):
        from api.admin_api import update_org_programs
        seed_user_perms('member@example.com', 'test-org', role='org_admin')

        resp = update_org_programs(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'programs': ['Program A']},
        )
        # org-admin is not super-admin
        assert resp['statusCode'] == 403


class TestValidationResultsPagination:
    """Regression tests: DynamoDB Query pages at 1 MB. The list/get/details
    paths that fan a Query out over a whole run's documents must walk
    LastEvaluatedKey — otherwise the tail is silently dropped and the UI
    shows a partial run without any error surfaced to staff."""

    def test_query_all_walks_last_evaluated_key(self):
        """The pagination helper must keep querying until LastEvaluatedKey is absent."""
        from api.admin_api import _query_all

        page1 = [{'document_id': 'a'}, {'document_id': 'b'}]
        page2 = [{'document_id': 'c'}]
        responses = iter([
            {'Items': page1, 'LastEvaluatedKey': {'pk': 'DOC#b', 'sk': 'V#1'}},
            {'Items': page2},  # no LastEvaluatedKey — final page
        ])
        seen_start_keys = []

        class FakeTable:
            def query(self, **kwargs):
                seen_start_keys.append(kwargs.get('ExclusiveStartKey'))
                return next(responses)

        items = _query_all(FakeTable(), IndexName='gsi2', KeyConditionExpression='x')

        assert [i['document_id'] for i in items] == ['a', 'b', 'c']
        # First call: no ExclusiveStartKey. Second call: carries prior LEK.
        assert seen_start_keys == [None, {'pk': 'DOC#b', 'sk': 'V#1'}]

    def test_get_validation_run_returns_all_pages(
        self, mock_dynamodb, sample_org_config, super_admin_event, monkeypatch
    ):
        """get_validation_run must return documents from every DynamoDB page,
        not just the first 1 MB worth."""
        from api.admin_api import get_validation_run
        import api.admin_api as admin_api

        # Two pages of documents for the same run, each with rules the
        # super-admin can see. RBAC filtering happens post-query so both
        # pages must reach the loop for the response to include page 2.
        page1_items = [{
            'organization_id': 'test-org',
            'document_id': f'DOC#{i}',
            'validation_timestamp': '2024-01-15T10:00:00',
            'rules': [{'rule_id': 'r1', 'category': 'Compliance Audit',
                       'status': 'FAIL'}],
            'summary': {'failed': 1},
        } for i in range(3)]
        page2_items = [{
            'organization_id': 'test-org',
            'document_id': f'DOC#{i}',
            'validation_timestamp': '2024-01-15T10:00:00',
            'rules': [{'rule_id': 'r1', 'category': 'Compliance Audit',
                       'status': 'PASS'}],
            'summary': {'passed': 1},
        } for i in range(3, 5)]
        responses = iter([
            {'Items': page1_items, 'LastEvaluatedKey': {'pk': 'DOC#2', 'sk': 'V'}},
            {'Items': page2_items},
        ])

        real_table = admin_api.validation_results_table

        class PagedTable:
            def query(self, **kwargs):
                return next(responses)

        monkeypatch.setattr(admin_api, 'validation_results_table', PagedTable())
        try:
            resp = get_validation_run(
                event=super_admin_event,
                path_params={'orgId': 'test-org', 'runId': 'run-xyz'},
            )
        finally:
            monkeypatch.setattr(admin_api, 'validation_results_table', real_table)

        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        # Without pagination this would be 3 (page 1 only).
        assert body['total_count'] == 5
        assert {d['document_id'] for d in body['documents']} == {
            'DOC#0', 'DOC#1', 'DOC#2', 'DOC#3', 'DOC#4'
        }
