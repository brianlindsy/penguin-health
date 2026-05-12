"""
Unit tests for saved-report CRUD endpoints in admin_api.

Exercises the save / list / get / delete flow against a moto-backed
DynamoDB table, including ordering (newest first), per-org isolation,
oversized-payload rejection, and 404 paths.
"""

import json

import pytest


pytestmark = pytest.mark.unit


def _make_event(super_admin_event, route_key, org_id, body=None, report_id=None):
    """Helper: build an HTTP API v2 event with path params filled in."""
    path_params = {'orgId': org_id}
    if report_id is not None:
        path_params['reportId'] = report_id
    return {
        **super_admin_event,
        'routeKey': route_key,
        'pathParameters': path_params,
        'body': json.dumps(body) if body is not None else None,
    }


def _good_report_body(name='Q1 referral counts'):
    return {
        'name': name,
        'question': 'how many referrals last quarter',
        'sql': 'SELECT count(*) FROM charts_circles_of_care LIMIT 1000',
        'viz_type': 'bar',
        'mode': 'sql',
        'explanation': 'Counts referrals from Q1.',
        'columns': [{'name': '_col0', 'type': 'bigint'}],
        'rows': [['42']],
        'row_count': 1,
    }


class TestSaveReport:
    def test_save_round_trip(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler

        evt = _make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(),
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 201, resp['body']
        saved = json.loads(resp['body'])
        assert saved['report_id']
        assert saved['name'] == 'Q1 referral counts'
        assert saved['organization_id'] == 'circles-of-care'
        assert saved['created_by'] == 'admin@example.com'
        # Internal DynamoDB keys must not leak to the client.
        assert 'pk' not in saved
        assert 'sk' not in saved

    def test_save_rejects_missing_name(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        body = _good_report_body()
        del body['name']
        evt = _make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=body,
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 400

    def test_save_rejects_missing_required_fields(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        body = _good_report_body()
        del body['sql']
        evt = _make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=body,
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 400
        assert 'sql' in json.loads(resp['body'])['error']

    def test_save_rejects_oversized_payload(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        body = _good_report_body()
        # Manufacture a >380KB rows blob (each row ~400 bytes * 1000 rows).
        big_row = ['x' * 400]
        body['rows'] = [big_row for _ in range(1000)]
        body['row_count'] = 1000
        evt = _make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=body,
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 400
        assert json.loads(resp['body'])['code'] == 'REPORT_TOO_LARGE'


class TestListReports:
    def test_list_empty(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        evt = _make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 200
        body = json.loads(resp['body'])
        assert body == {'reports': [], 'count': 0}

    def test_list_returns_metadata_not_rows(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        # Save one report.
        save_evt = _make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(),
        )
        lambda_handler(save_evt, None)

        list_evt = _make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        )
        resp = lambda_handler(list_evt, None)
        body = json.loads(resp['body'])
        assert body['count'] == 1
        item = body['reports'][0]
        # Metadata only: rows/columns/sql must not appear in the list view.
        assert 'rows' not in item
        assert 'columns' not in item
        assert 'sql' not in item
        assert item['name'] == 'Q1 referral counts'
        assert item['viz_type'] == 'bar'
        assert item['mode'] == 'sql'

    def test_list_newest_first(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler

        for n in ('alpha', 'bravo', 'charlie'):
            evt = _make_event(
                super_admin_event,
                'POST /api/organizations/{orgId}/analytics/reports',
                'circles-of-care',
                body=_good_report_body(name=n),
            )
            lambda_handler(evt, None)

        list_evt = _make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        )
        resp = lambda_handler(list_evt, None)
        names = [r['name'] for r in json.loads(resp['body'])['reports']]
        # ISO timestamps in sk sort lexically; newest-first means reverse order.
        assert names == ['charlie', 'bravo', 'alpha']

    def test_list_isolates_orgs(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        # Save one in org A.
        lambda_handler(_make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(name='from-A'),
        ), None)
        # Save one in org B.
        lambda_handler(_make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'demo',
            body=_good_report_body(name='from-B'),
        ), None)

        resp_a = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        ), None)
        resp_b = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'demo',
        ), None)
        names_a = [r['name'] for r in json.loads(resp_a['body'])['reports']]
        names_b = [r['name'] for r in json.loads(resp_b['body'])['reports']]
        assert names_a == ['from-A']
        assert names_b == ['from-B']


class TestGetReport:
    def test_get_returns_full_snapshot(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        save_resp = lambda_handler(_make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(),
        ), None)
        report_id = json.loads(save_resp['body'])['report_id']

        get_resp = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports/{reportId}',
            'circles-of-care',
            report_id=report_id,
        ), None)
        assert get_resp['statusCode'] == 200
        body = json.loads(get_resp['body'])
        assert body['report_id'] == report_id
        assert body['rows'] == [['42']]
        assert body['columns'] == [{'name': '_col0', 'type': 'bigint'}]
        assert 'pk' not in body and 'sk' not in body

    def test_get_404_when_missing(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        resp = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports/{reportId}',
            'circles-of-care',
            report_id='does-not-exist',
        ), None)
        assert resp['statusCode'] == 404

    def test_get_404_across_org(self, mock_dynamodb, super_admin_event):
        """A report saved under org A is not accessible via org B's URL."""
        from admin_api import lambda_handler
        save_resp = lambda_handler(_make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(),
        ), None)
        report_id = json.loads(save_resp['body'])['report_id']

        resp = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports/{reportId}',
            'demo',
            report_id=report_id,
        ), None)
        assert resp['statusCode'] == 404


class TestDeleteReport:
    def test_delete_removes_report(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        save_resp = lambda_handler(_make_event(
            super_admin_event,
            'POST /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
            body=_good_report_body(),
        ), None)
        report_id = json.loads(save_resp['body'])['report_id']

        del_resp = lambda_handler(_make_event(
            super_admin_event,
            'DELETE /api/organizations/{orgId}/analytics/reports/{reportId}',
            'circles-of-care',
            report_id=report_id,
        ), None)
        assert del_resp['statusCode'] == 200
        assert json.loads(del_resp['body'])['deleted'] == report_id

        # Verify list is empty afterwards.
        list_resp = lambda_handler(_make_event(
            super_admin_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        ), None)
        assert json.loads(list_resp['body'])['count'] == 0

    def test_delete_404_when_missing(self, mock_dynamodb, super_admin_event):
        from admin_api import lambda_handler
        resp = lambda_handler(_make_event(
            super_admin_event,
            'DELETE /api/organizations/{orgId}/analytics/reports/{reportId}',
            'circles-of-care',
            report_id='nonexistent',
        ), None)
        assert resp['statusCode'] == 404


class TestAuthz:
    def test_org_user_cannot_access_other_org(
        self, mock_dynamodb, org_user_event,
    ):
        from admin_api import lambda_handler
        # org_user_event has custom:organization_id = 'test-org'.
        # Hitting circles-of-care should be denied.
        evt = _make_event(
            org_user_event,
            'GET /api/organizations/{orgId}/analytics/reports',
            'circles-of-care',
        )
        resp = lambda_handler(evt, None)
        assert resp['statusCode'] == 403
