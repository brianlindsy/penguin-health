"""
Tests for the new `dates` field on POST /validation-runs.

The trigger endpoint validates that requested dates are well-formed,
within the allowed range [2026-05-01, today_utc], and forwards them to
the rules engine in the async Lambda invocation payload.
"""

import json
import os
import sys

import pytest
from freezegun import freeze_time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


@freeze_time("2026-05-15")
class TestTriggerValidationRunDates:
    def test_default_dates_is_today(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        fake = mocker.patch('api.admin_api.lambda_client')
        fake.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body=None,
        )
        assert resp['statusCode'] == 202
        body = json.loads(resp['body'])
        assert body['dates'] == ['2026-05-15']

        invoke_kwargs = fake.invoke.call_args.kwargs
        payload = json.loads(invoke_kwargs['Payload'])
        assert payload['dates'] == ['2026-05-15']

    def test_explicit_dates_accepted(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        fake = mocker.patch('api.admin_api.lambda_client')
        fake.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-12', '2026-05-13']},
        )
        assert resp['statusCode'] == 202
        assert json.loads(resp['body'])['dates'] == ['2026-05-12', '2026-05-13']

    def test_pre_cutover_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-04-30']},
        )
        assert resp['statusCode'] == 400
        assert 'cutover' in json.loads(resp['body'])['error']

    def test_future_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-16']},
        )
        assert resp['statusCode'] == 400
        assert 'future' in json.loads(resp['body'])['error']

    def test_malformed_date_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026/05/01']},
        )
        assert resp['statusCode'] == 400
        assert 'Malformed' in json.loads(resp['body'])['error']

    def test_empty_dates_list_rejected(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        mocker.patch('api.admin_api.lambda_client')

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': []},
        )
        assert resp['statusCode'] == 400

    def test_duplicate_dates_deduplicated(
        self, mock_dynamodb, sample_org_config, super_admin_event, mocker,
    ):
        from api.admin_api import trigger_validation_run
        fake = mocker.patch('api.admin_api.lambda_client')
        fake.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=super_admin_event,
            path_params={'orgId': 'test-org'},
            body={'dates': ['2026-05-10', '2026-05-10', '2026-05-11']},
        )
        assert resp['statusCode'] == 202
        assert json.loads(resp['body'])['dates'] == ['2026-05-10', '2026-05-11']

    def test_dates_and_categories_both_forwarded(
        self, mock_dynamodb, sample_org_config, member_event, seed_user_perms, mocker,
    ):
        from api.admin_api import trigger_validation_run
        seed_user_perms(
            'member@example.com', 'test-org',
            report_permissions={'Billing': ['run']},
        )
        fake = mocker.patch('api.admin_api.lambda_client')
        fake.invoke.return_value = {'StatusCode': 202}

        resp = trigger_validation_run(
            event=member_event,
            path_params={'orgId': 'test-org'},
            body={'categories': ['Billing'], 'dates': ['2026-05-12']},
        )
        assert resp['statusCode'] == 202
        invoke_kwargs = fake.invoke.call_args.kwargs
        payload = json.loads(invoke_kwargs['Payload'])
        assert payload['categories'] == ['Billing']
        assert payload['dates'] == ['2026-05-12']
