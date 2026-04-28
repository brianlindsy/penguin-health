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
            'category': 'Compliance',
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
            'category': 'Compliance',
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
            'category': 'Compliance',
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
