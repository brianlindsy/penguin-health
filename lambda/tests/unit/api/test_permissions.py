"""Unit tests for the permissions module."""

import os
import sys

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


SUPER_ADMIN_CLAIMS = {'email': 'admin@example.com', 'groups': ['Admins']}
MEMBER_CLAIMS = {'email': 'member@example.com', 'groups': [], 'organization_id': 'test-org'}


class TestLoadPermissions:
    def test_returns_none_when_record_missing(self, mock_dynamodb):
        import permissions

        assert permissions.load_permissions('nobody@example.com', 'test-org') is None

    def test_returns_none_for_missing_inputs(self, mock_dynamodb):
        import permissions

        assert permissions.load_permissions(None, 'test-org') is None
        assert permissions.load_permissions('a@b.com', None) is None

    def test_normalizes_record(self, mock_dynamodb, seed_user_perms):
        import permissions

        seed_user_perms(
            'member@example.com', 'test-org',
            role='member',
            report_permissions={'Billing': ['view'], 'Quality Assurance': ['run']},
            analytics_permissions=['staff_performance', 'unknown_page'],
        )
        perms = permissions.load_permissions('member@example.com', 'test-org')

        assert perms['role'] == 'member'
        assert perms['report_permissions']['Billing'] == ['view']
        assert perms['report_permissions']['Intake'] == []  # filled in
        # Unknown analytics page filtered out by normalization
        assert 'unknown_page' not in perms['analytics_permissions']
        assert 'staff_performance' in perms['analytics_permissions']


class TestSuperAdminShortCircuit:
    def test_super_admin_can_view_every_category(self, mock_dynamodb):
        import permissions
        for cat in permissions.CATEGORIES:
            assert permissions.can_view_category(SUPER_ADMIN_CLAIMS, 'any-org', cat) is True

    def test_super_admin_can_run_every_category(self, mock_dynamodb):
        import permissions
        for cat in permissions.CATEGORIES:
            assert permissions.can_run_category(SUPER_ADMIN_CLAIMS, 'any-org', cat) is True

    def test_super_admin_can_view_every_analytics_page(self, mock_dynamodb):
        import permissions
        for page in permissions.ANALYTICS_PAGES:
            assert permissions.can_view_analytics(SUPER_ADMIN_CLAIMS, 'any-org', page) is True

    def test_super_admin_categories_helpers(self, mock_dynamodb):
        import permissions
        assert permissions.viewable_categories(SUPER_ADMIN_CLAIMS, 'any-org') == set(permissions.CATEGORIES)
        assert permissions.runnable_categories(SUPER_ADMIN_CLAIMS, 'any-org') == set(permissions.CATEGORIES)


class TestOrgAdminShortCircuit:
    def test_org_admin_grants_all_within_their_org(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms('member@example.com', 'test-org', role='org_admin')

        for cat in permissions.CATEGORIES:
            assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', cat) is True
            assert permissions.can_run_category(MEMBER_CLAIMS, 'test-org', cat) is True
        for page in permissions.ANALYTICS_PAGES:
            assert permissions.can_view_analytics(MEMBER_CLAIMS, 'test-org', page) is True

    def test_org_admin_role_does_not_leak_to_other_orgs(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms('member@example.com', 'test-org', role='org_admin')

        # Same user, different org -> no record -> deny
        assert permissions.can_view_category(MEMBER_CLAIMS, 'other-org', 'Billing') is False


class TestMemberWithExplicitPerms:
    def test_view_only_grant(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view']},
        )
        assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', 'Billing') is True
        assert permissions.can_run_category(MEMBER_CLAIMS, 'test-org', 'Billing') is False

    def test_run_only_grant(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Intake': ['run']},
        )
        assert permissions.can_run_category(MEMBER_CLAIMS, 'test-org', 'Intake') is True
        assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', 'Intake') is False

    def test_unlisted_category_is_denied(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view', 'run']},
        )
        for cat in ['Intake', 'Compliance Audit', 'Quality Assurance']:
            assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', cat) is False
            assert permissions.can_run_category(MEMBER_CLAIMS, 'test-org', cat) is False

    def test_no_record_means_deny(self, mock_dynamodb):
        import permissions
        for cat in permissions.CATEGORIES:
            assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', cat) is False
            assert permissions.can_run_category(MEMBER_CLAIMS, 'test-org', cat) is False
        for page in permissions.ANALYTICS_PAGES:
            assert permissions.can_view_analytics(MEMBER_CLAIMS, 'test-org', page) is False

    def test_analytics_grant(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            analytics_permissions=['revenue_analysis'],
        )
        assert permissions.can_view_analytics(MEMBER_CLAIMS, 'test-org', 'revenue_analysis') is True
        assert permissions.can_view_analytics(MEMBER_CLAIMS, 'test-org', 'staff_performance') is False


class TestSerializeForMeEndpoint:
    def test_super_admin_payload(self, mock_dynamodb):
        import permissions
        payload = permissions.serialize_for_me_endpoint(SUPER_ADMIN_CLAIMS)
        assert payload['is_super_admin'] is True
        assert set(payload['report_permissions'].keys()) == set(permissions.CATEGORIES)
        for verbs in payload['report_permissions'].values():
            assert sorted(verbs) == ['run', 'view']

    def test_member_with_no_record(self, mock_dynamodb):
        import permissions
        payload = permissions.serialize_for_me_endpoint(MEMBER_CLAIMS)
        assert payload['is_super_admin'] is False
        assert payload['role'] == 'member'
        for verbs in payload['report_permissions'].values():
            assert verbs == []
        assert payload['analytics_permissions'] == []

    def test_member_with_explicit_perms(self, mock_dynamodb, seed_user_perms):
        import permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view']},
            analytics_permissions=['staff_performance'],
        )
        payload = permissions.serialize_for_me_endpoint(MEMBER_CLAIMS)
        assert payload['report_permissions']['Billing'] == ['view']
        assert payload['analytics_permissions'] == ['staff_performance']


class TestBuildUserPermItem:
    def test_rejects_unknown_role(self, mock_dynamodb):
        import permissions
        import pytest
        with pytest.raises(ValueError, match='Invalid role'):
            permissions.build_user_perm_item(
                'a@b.com', 'org', {'role': 'wizard'},
            )

    def test_rejects_unknown_category(self, mock_dynamodb):
        import permissions
        import pytest
        with pytest.raises(ValueError, match='Unknown category'):
            permissions.build_user_perm_item(
                'a@b.com', 'org',
                {'role': 'member', 'report_permissions': {'Madeup': ['view']}},
            )

    def test_rejects_unknown_analytics_page(self, mock_dynamodb):
        import permissions
        import pytest
        with pytest.raises(ValueError, match='Unknown analytics page'):
            permissions.build_user_perm_item(
                'a@b.com', 'org',
                {'role': 'member', 'analytics_permissions': ['mystery_dashboard']},
            )

    def test_fills_missing_categories_with_empty(self, mock_dynamodb):
        import permissions
        item = permissions.build_user_perm_item(
            'a@b.com', 'org',
            {'role': 'member', 'report_permissions': {'Billing': ['view']}},
        )
        for cat in permissions.CATEGORIES:
            assert cat in item['report_permissions']
        assert item['report_permissions']['Billing'] == ['view']
        assert item['report_permissions']['Intake'] == []
        assert item['gsi1pk'] == 'USER_PERM'
        assert item['gsi1sk'] == 'ORG#org#USER#a@b.com'


class TestCacheInvalidation:
    def test_invalidate_clears_specific_entry(self, mock_dynamodb, seed_user_perms):
        import permissions

        seed_user_perms('member@example.com', 'test-org',
                        report_permissions={'Billing': ['view']})
        # warm cache
        assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', 'Billing') is True

        # mutate underlying record
        seed_user_perms('member@example.com', 'test-org', report_permissions={'Billing': []})
        # still cached -> stale True
        assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', 'Billing') is True

        permissions.invalidate_cache(email='member@example.com', org_id='test-org')
        assert permissions.can_view_category(MEMBER_CLAIMS, 'test-org', 'Billing') is False
