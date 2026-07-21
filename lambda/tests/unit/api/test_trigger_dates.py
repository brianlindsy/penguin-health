"""
Tests for the `dates` field on POST /validation-runs.

The trigger endpoint validates that requested dates are well-formed,
within the allowed range [2026-05-01, today_utc], and forwards them to
the rules-engine state machine as the SFN execution input.
"""

import json
import os
import sys

from freezegun import freeze_time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


@freeze_time("2026-05-15")
class TestTriggerValidationRunDates:
    def test_default_dates_is_today(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body=None,
        )
        assert resp['statusCode'] == 202
        body = json.loads(resp['body'])
        assert body['dates'] == ['2026-05-15']

        call = rules_engine_sfn.start_execution.call_args
        sfn_input = json.loads(call.kwargs['input'])
        assert sfn_input['dates'] == ['2026-05-15']

    def test_explicit_dates_accepted(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-12', '2026-05-13']},
        )
        assert resp['statusCode'] == 202
        assert json.loads(resp['body'])['dates'] == ['2026-05-12', '2026-05-13']

    def test_pre_cutover_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-04-30']},
        )
        assert resp['statusCode'] == 400
        assert 'cutover' in json.loads(resp['body'])['error']

    def test_future_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-16']},
        )
        assert resp['statusCode'] == 400
        assert 'future' in json.loads(resp['body'])['error']

    def test_malformed_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026/05/01']},
        )
        assert resp['statusCode'] == 400
        assert 'Malformed' in json.loads(resp['body'])['error']

    def test_empty_dates_list_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': []},
        )
        assert resp['statusCode'] == 400

    def test_duplicate_dates_deduplicated(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-10', '2026-05-10', '2026-05-11']},
        )
        assert resp['statusCode'] == 202
        assert json.loads(resp['body'])['dates'] == ['2026-05-10', '2026-05-11']

    def test_dates_and_categories_both_forwarded(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['run']},
        )

        resp = trigger_validation_run(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'categories': ['Billing'], 'dates': ['2026-05-12']},
        )
        assert resp['statusCode'] == 202
        call = rules_engine_sfn.start_execution.call_args
        sfn_input = json.loads(call.kwargs['input'])
        assert sfn_input['categories'] == ['Billing']
        assert sfn_input['dates'] == ['2026-05-12']


@freeze_time("2026-05-15")
class TestTriggerValidationRunAudit:
    """The trigger endpoint touches PHI-bearing pipelines, so every
    click on "Start validation run" must land in the audit trail with
    the caller's identity."""

    def test_success_emits_execute_event(
        self, mock_dynamodb, sample_org_config, super_admin_event,
        rules_engine_sfn,
    ):
        from api.admin_api import trigger_validation_run
        from boto3.dynamodb.conditions import Key

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body=None,
        )
        assert resp['statusCode'] == 202

        rows = mock_dynamodb.Table('penguin-health-audit').query(
            KeyConditionExpression=Key('pk').eq('ORG#test-org')
                                   & Key('sk').begins_with('AUDIT#'),
        )['Items']
        assert len(rows) == 1
        row = rows[0]
        assert row['action'] == 'execute'
        assert row['resource_type'] == 'ValidationRun'
        assert row['outcome'] == 'success'
        assert row['agent_email'] == 'admin@example.com'
        assert row['event']['purpose_of_use'] == 'OPERATIONS'
        assert row['event']['http_status'] == 202

    def test_permission_denied_still_audits(
        self, mock_dynamodb, sample_org_config, org_user_event,
        rules_engine_sfn,
    ):
        """A 403 response is exactly the case we want audited — someone
        without run permission clicked the button. The audit row records
        the attempt with a failure outcome."""
        from api.admin_api import trigger_validation_run
        from boto3.dynamodb.conditions import Key

        resp = trigger_validation_run(
            event=org_user_event,
            path_params={'orgId': 'test-org'},
            body=None,
        )
        assert resp['statusCode'] == 403

        rows = mock_dynamodb.Table('penguin-health-audit').query(
            KeyConditionExpression=Key('pk').eq('ORG#test-org')
                                   & Key('sk').begins_with('AUDIT#'),
        )['Items']
        assert len(rows) == 1
        row = rows[0]
        assert row['action'] == 'execute'
        assert row['resource_type'] == 'ValidationRun'
        assert row['agent_email'] == 'user@example.com'
        assert row['outcome'] != 'success'
        assert row['event']['http_status'] == 403
