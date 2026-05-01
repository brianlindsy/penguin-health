"""End-to-end RBAC tests for admin_api endpoints with the granular permission model."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


# ---------- list_rules / get_rule ----------

class TestListRulesFiltering:
    def test_member_without_perms_sees_no_rules(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import list_rules
        seed_user_perms('member@example.com', 'test-org')  # member, all empty

        resp = list_rules(event=member_event, path_params={'orgId': 'test-org'})
        body = json.loads(resp['body'])
        assert resp['statusCode'] == 200
        assert body['count'] == 0

    def test_member_with_view_grant_sees_matching_rules(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import list_rules
        # sample_org_config seeds rule-001 with category 'Compliance Audit'
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Compliance Audit': ['view']},
        )

        resp = list_rules(event=member_event, path_params={'orgId': 'test-org'})
        body = json.loads(resp['body'])
        assert resp['statusCode'] == 200
        assert body['count'] == 1

    def test_org_admin_sees_everything(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import list_rules
        seed_user_perms('member@example.com', 'test-org', role='org_admin')

        resp = list_rules(event=member_event, path_params={'orgId': 'test-org'})
        body = json.loads(resp['body'])
        assert body['count'] == 1


class TestGetRulePerCategoryGuard:
    def test_member_without_view_gets_403(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import get_rule
        seed_user_perms('member@example.com', 'test-org')  # no perms

        resp = get_rule(event=member_event,
                        path_params={'orgId': 'test-org', 'ruleId': 'rule-001'})
        assert resp['statusCode'] == 403


# ---------- create_rule / update_rule ----------

class TestRuleManagementRequiresOrgAdmin:
    def test_member_with_view_cannot_create_rule(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import create_rule
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view', 'run']},
        )

        body = {'id': 'r2', 'name': 'r2', 'category': 'Billing', 'rule_text': 'x'}
        resp = create_rule(event=member_event, path_params={'orgId': 'test-org'}, body=body)
        assert resp['statusCode'] == 403

    def test_org_admin_can_create_rule(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms
    ):
        from api.admin_api import create_rule
        seed_user_perms('member@example.com', 'test-org', role='org_admin')

        body = {'id': 'r2', 'name': 'r2', 'category': 'Billing', 'rule_text': 'x'}
        resp = create_rule(event=member_event, path_params={'orgId': 'test-org'}, body=body)
        assert resp['statusCode'] == 201


class TestCategoryValidation:
    def test_invalid_category_rejected_on_create(
        self, mock_dynamodb, sample_org_config, super_admin_event
    ):
        from api.admin_api import create_rule
        body = {'id': 'r3', 'name': 'r3', 'category': 'Madeup', 'rule_text': 'x'}
        resp = create_rule(event=super_admin_event, path_params={'orgId': 'test-org'}, body=body)
        assert resp['statusCode'] == 400
        assert 'Invalid category' in json.loads(resp['body'])['error']

    def test_invalid_category_rejected_on_update(
        self, mock_dynamodb, sample_org_config, super_admin_event
    ):
        from api.admin_api import update_rule
        body = {'category': 'Nonsense'}
        resp = update_rule(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'ruleId': 'rule-001'},
            body=body,
        )
        assert resp['statusCode'] == 400


# ---------- trigger_validation_run ----------

class TestTriggerValidationRunRBAC:
    def test_member_without_runnable_categories_gets_403(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms, mocker
    ):
        from api.admin_api import trigger_validation_run
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view']},
        )
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'categories': ['Billing']},
        )
        assert resp['statusCode'] == 403

    def test_member_with_run_can_trigger_subset(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms, mocker
    ):
        from api.admin_api import trigger_validation_run
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['run'], 'Intake': ['view', 'run']},
        )
        fake_lambda = mocker.patch('api.admin_api.lambda_client')
        fake_lambda.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'categories': ['Billing', 'Intake']},
        )
        assert resp['statusCode'] == 202
        body = json.loads(resp['body'])
        assert sorted(body['categories']) == ['Billing', 'Intake']

        # Forwarded payload includes categories
        invoke_call = fake_lambda.invoke.call_args
        payload = json.loads(invoke_call.kwargs['Payload'])
        assert sorted(payload['categories']) == ['Billing', 'Intake']

    def test_member_requesting_disallowed_category_rejected(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms, mocker
    ):
        from api.admin_api import trigger_validation_run
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['run']},
        )
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'categories': ['Billing', 'Quality Assurance']},
        )
        assert resp['statusCode'] == 403
        assert 'Quality Assurance' in json.loads(resp['body'])['error']

    def test_default_categories_when_omitted(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker
    ):
        from api.admin_api import trigger_validation_run
        fake_lambda = mocker.patch('api.admin_api.lambda_client')
        fake_lambda.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body=None,
        )
        assert resp['statusCode'] == 202
        body = json.loads(resp['body'])
        # super-admin defaults to all categories
        assert set(body['categories']) >= {'Billing', 'Intake', 'Compliance Audit', 'Quality Assurance'}


# ---------- finding actions ----------

class TestConfirmFindingPerCategoryGuard:
    def test_member_without_view_blocked_from_confirm(
        self, mock_dynamodb, sample_org_config, sample_validation_result,
        member_event, seed_user_perms,
    ):
        from api.admin_api import confirm_finding
        seed_user_perms('member@example.com', 'test-org')  # nothing

        resp = confirm_finding(
            event=member_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001'},
        )
        assert resp['statusCode'] == 403

    def test_member_with_view_can_confirm(
        self, mock_dynamodb, sample_org_config, sample_validation_result,
        member_event, seed_user_perms,
    ):
        from api.admin_api import confirm_finding
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Compliance Audit': ['view']},
        )

        resp = confirm_finding(
            event=member_event,
            path_params={'orgId': 'test-org', 'runId': '20240115-100000', 'docId': '12345'},
            body={'rule_id': 'rule-001'},
        )
        assert resp['statusCode'] == 200


# ---------- new endpoints: /api/me/permissions and user CRUD ----------

class TestMyPermissionsEndpoint:
    def test_super_admin_payload(self, mock_dynamodb, super_admin_event):
        from api.admin_api import get_my_permissions
        resp = get_my_permissions(event=super_admin_event)
        body = json.loads(resp['body'])
        assert resp['statusCode'] == 200
        assert body['is_super_admin'] is True

    def test_member_payload(self, mock_dynamodb, member_event, seed_user_perms):
        from api.admin_api import get_my_permissions
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['view']},
        )
        resp = get_my_permissions(event=member_event)
        body = json.loads(resp['body'])
        assert resp['statusCode'] == 200
        assert body['is_super_admin'] is False
        assert body['role'] == 'member'
        assert body['report_permissions']['Billing'] == ['view']


class TestUserCRUDSuperAdminOnly:
    def test_member_cannot_list_users(self, mock_dynamodb, member_event, seed_user_perms):
        from api.admin_api import list_org_users
        seed_user_perms('member@example.com', 'test-org', role='org_admin')
        resp = list_org_users(event=member_event, path_params={'orgId': 'test-org'})
        # org-admin is not super-admin, so 403
        assert resp['statusCode'] == 403

    def test_super_admin_upserts_then_lists(
        self, mock_dynamodb, sample_org_config, super_admin_event,
    ):
        from api.admin_api import upsert_org_user, list_org_users, get_org_user, delete_org_user

        # Create
        resp = upsert_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@clinic.com'},
            body={
                'role': 'member',
                'report_permissions': {'Billing': ['view']},
                'analytics_permissions': ['revenue_analysis'],
            },
        )
        assert resp['statusCode'] == 201
        assert json.loads(resp['body'])['report_permissions']['Billing'] == ['view']

        # List
        resp = list_org_users(event=super_admin_event, path_params={'orgId': 'test-org'})
        body = json.loads(resp['body'])
        assert resp['statusCode'] == 200
        assert body['count'] == 1
        assert body['users'][0]['email'] == 'alice@clinic.com'

        # Get
        resp = get_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@clinic.com'},
        )
        assert resp['statusCode'] == 200

        # Update (idempotent upsert -> 200)
        resp = upsert_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@clinic.com'},
            body={'role': 'org_admin'},
        )
        assert resp['statusCode'] == 200
        assert json.loads(resp['body'])['role'] == 'org_admin'

        # Delete
        resp = delete_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@clinic.com'},
        )
        assert resp['statusCode'] == 204

        # Now missing
        resp = get_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'alice@clinic.com'},
        )
        assert resp['statusCode'] == 404

    def test_upsert_rejects_unknown_role(self, mock_dynamodb, super_admin_event):
        from api.admin_api import upsert_org_user
        resp = upsert_org_user(
            event=super_admin_event,
            path_params={'orgId': 'test-org', 'email': 'a@b.com'},
            body={'role': 'sorcerer'},
        )
        assert resp['statusCode'] == 400
