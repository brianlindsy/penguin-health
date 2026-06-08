"""Tests for the org-scoped notification subscription endpoints.

The notifications page is super-admin only: it lists every user with
USER_PERM rows in the org and lets the admin toggle each event on/off
for every user. The self-service `/api/me/subscriptions` endpoints
that existed in an earlier iteration are intentionally gone.
"""

import json

import pytest


@pytest.fixture
def org_with_two_users(mock_dynamodb, sample_org_config, seed_user_perms):
    """Seed two users with permissions in test-org."""
    seed_user_perms('alice@example.com', 'test-org', role='member')
    seed_user_perms('bob@example.com', 'test-org', role='org_admin')
    return ['alice@example.com', 'bob@example.com']


class TestListOrgSubscriptions:
    def test_super_admin_sees_all_users_with_default_disabled(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import list_org_subscriptions
        event = dict(super_admin_event)
        response = list_org_subscriptions(
            event=event, path_params={'orgId': 'test-org'},
        )
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['event_types'] == ['validation_run_complete', 'eligibility_issue']
        emails = [u['email'] for u in body['users']]
        assert emails == sorted(org_with_two_users)
        # Every user starts with both events disabled.
        for user in body['users']:
            for sub in user['subscriptions']:
                assert sub['enabled'] is False
                assert sub['updated_at'] is None

    def test_returns_existing_subscription_state(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from notifications import set_subscription
        set_subscription(
            email='alice@example.com', org_id='test-org',
            event_type='validation_run_complete', enabled=True,
        )

        from admin_api import list_org_subscriptions
        response = list_org_subscriptions(
            event=super_admin_event, path_params={'orgId': 'test-org'},
        )
        body = json.loads(response['body'])
        alice = next(u for u in body['users'] if u['email'] == 'alice@example.com')
        validation = next(
            s for s in alice['subscriptions']
            if s['event_type'] == 'validation_run_complete'
        )
        assert validation['enabled'] is True
        # bob has no subscription rows — both stay False.
        bob = next(u for u in body['users'] if u['email'] == 'bob@example.com')
        assert all(s['enabled'] is False for s in bob['subscriptions'])

    def test_non_super_admin_is_forbidden(
        self, mock_dynamodb, org_with_two_users, org_user_event,
    ):
        from admin_api import list_org_subscriptions
        response = list_org_subscriptions(
            event=org_user_event, path_params={'orgId': 'test-org'},
        )
        assert response['statusCode'] == 403

    def test_empty_org_returns_empty_user_list(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from admin_api import list_org_subscriptions
        response = list_org_subscriptions(
            event=super_admin_event, path_params={'orgId': 'test-org'},
        )
        body = json.loads(response['body'])
        assert body['users'] == []


class TestUpsertOrgUserSubscription:
    def test_super_admin_can_toggle_any_users_subscription(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import upsert_org_user_subscription
        body = {'event_type': 'eligibility_issue', 'enabled': True}
        response = upsert_org_user_subscription(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@example.com'},
            body=body,
        )
        assert response['statusCode'] == 200
        payload = json.loads(response['body'])
        assert payload['enabled'] is True
        assert payload['email'] == 'alice@example.com'
        assert payload['event_type'] == 'eligibility_issue'

    def test_round_trip_via_list_endpoint(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import (
            list_org_subscriptions,
            upsert_org_user_subscription,
        )
        upsert_org_user_subscription(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'bob@example.com'},
            body={'event_type': 'validation_run_complete', 'enabled': True},
        )
        list_response = list_org_subscriptions(
            event=super_admin_event, path_params={'orgId': 'test-org'},
        )
        body = json.loads(list_response['body'])
        bob = next(u for u in body['users'] if u['email'] == 'bob@example.com')
        validation = next(
            s for s in bob['subscriptions']
            if s['event_type'] == 'validation_run_complete'
        )
        assert validation['enabled'] is True

    def test_non_super_admin_is_forbidden(
        self, mock_dynamodb, org_with_two_users, org_user_event,
    ):
        from admin_api import upsert_org_user_subscription
        response = upsert_org_user_subscription(
            event=org_user_event,
            path_params={'orgId': 'test-org', 'email': 'alice@example.com'},
            body={'event_type': 'validation_run_complete', 'enabled': True},
        )
        assert response['statusCode'] == 403

    def test_rejects_unknown_event_type(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import upsert_org_user_subscription
        response = upsert_org_user_subscription(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@example.com'},
            body={'event_type': 'unknown_event', 'enabled': True},
        )
        assert response['statusCode'] == 400

    def test_rejects_non_boolean_enabled(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import upsert_org_user_subscription
        response = upsert_org_user_subscription(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@example.com'},
            body={'event_type': 'validation_run_complete', 'enabled': 'yes'},
        )
        assert response['statusCode'] == 400

    def test_requires_body(
        self, mock_dynamodb, org_with_two_users, super_admin_event,
    ):
        from admin_api import upsert_org_user_subscription
        response = upsert_org_user_subscription(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@example.com'},
            body=None,
        )
        assert response['statusCode'] == 400
