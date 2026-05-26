"""HTTP handlers for the morning-census worklist.

Wired into admin_api.py's dispatch dict. Four endpoints:
    GET  /api/organizations/{orgId}/eligibility/census/latest
    GET  /api/organizations/{orgId}/eligibility/census/runs
    PUT  /api/organizations/{orgId}/eligibility/census/items/
         {runId}/{patientHash}/resolve
    POST /api/organizations/{orgId}/eligibility/census/items/
         {runId}/{patientHash}/rerun

The census runner Lambda writes the rows; this module reads + the resolve
handler patches the `resolution` sub-object + the rerun handler re-runs
verify with corrected demographics and updates the same item.
"""

import json
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import permissions as perms_module
from stedi import audit as stedi_audit
from stedi import client_factory, config as stedi_config
from stedi import orchestrator
from stedi.exceptions import (
    StediAuthError,
    StediBadRequest,
    StediDailyCapExceeded,
    StediError,
    StediOrgNotConfigured,
    StediRateLimited,
    StediUpstreamError,
)


_TABLE_NAME = os.environ.get('STEDI_TABLE_NAME', 'penguin-health-stedi')
_dynamodb = boto3.resource('dynamodb')
_stedi_table = _dynamodb.Table(_TABLE_NAME)


_VALID_RESOLUTION_STATES = {'unresolved', 'in_progress', 'resolved'}


def _response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body, default=str),
    }


# ---- GET /eligibility/census/latest ------------------------------------

def get_latest_run(event, path_params, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_view_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:view permission required'})

    run = _query_latest_run(org_id)
    if not run:
        return _response(200, {'run': None, 'items': []})
    items = _query_items_for_run(org_id, run['run_date'], run['run_id'])
    return _response(200, {
        'run': _format_run(run),
        'items': [_format_item(i) for i in items],
    })


# ---- GET /eligibility/census/runs --------------------------------------

def list_runs(event, path_params, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_view_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:view permission required'})

    qs = event.get('queryStringParameters') or {}
    try:
        limit = min(int(qs.get('limit') or 30), 100)
    except (TypeError, ValueError):
        limit = 30

    response = _stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq(f'CENSUS_RUN#{org_id}'),
        ScanIndexForward=False,
        Limit=limit,
    )
    runs = [_format_run(r) for r in (response.get('Items') or [])]
    return _response(200, {'runs': runs})


# ---- PUT /eligibility/census/items/{runId}/{patientHash}/resolve --------

def resolve_item(event, path_params, body, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_run_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:run permission required'})

    if not isinstance(body, dict):
        return _response(400, {'error': 'JSON body required'})

    state = body.get('state', 'resolved')
    if state not in _VALID_RESOLUTION_STATES:
        return _response(400, {'error': f'state must be one of {sorted(_VALID_RESOLUTION_STATES)}'})

    action = (body.get('action') or '').strip() or None
    note = (body.get('note') or '').strip() or None
    rerun_audit_id = (body.get('rerun_audit_id') or '').strip() or None

    run_id = path_params.get('runId')
    patient_hash = path_params.get('patientHash')
    if not run_id or not patient_hash:
        return _response(400, {'error': 'runId and patientHash path params required'})

    # We don't have run_date in the URL but it's part of the SK. Query by
    # patient_hash suffix to find the row. Cheap because there's at most a
    # handful of runs per patient per 90 days.
    item = _find_item_by_run_and_patient(org_id, run_id, patient_hash)
    if not item:
        return _response(404, {'error': 'census item not found'})

    now = datetime.now(timezone.utc).isoformat()
    resolution = {
        'state': state,
        'action': action,
        'note': note,
        'resolved_by': claims.get('email') if state == 'resolved' else item.get('resolution', {}).get('resolved_by'),
        'resolved_at': now if state == 'resolved' else item.get('resolution', {}).get('resolved_at'),
        'rerun_audit_id': rerun_audit_id or item.get('resolution', {}).get('rerun_audit_id'),
    }

    try:
        _stedi_table.update_item(
            Key={'pk': item['pk'], 'sk': item['sk']},
            UpdateExpression='SET #r = :r',
            ExpressionAttributeNames={'#r': 'resolution'},
            ExpressionAttributeValues={':r': resolution},
        )
    except ClientError as e:
        return _response(500, {'error': str(e)})

    return _response(200, {'resolution': resolution})


# ---- POST /eligibility/census/items/{runId}/{patientHash}/rerun ---------

# Demographic fields the UI is allowed to send in a rerun. Everything
# else on the body is ignored.
_RERUN_DEMOGRAPHIC_FIELDS = (
    'first_name', 'middle_name', 'last_name', 'suffix',
    'dob', 'gender', 'ssn_last4',
    'address1', 'address2', 'city', 'state', 'postal_code',
    'member_id', 'payer_id',
)


def rerun_census_item(event, path_params, body, authorize_fn, **_):
    """Re-run discovery + eligibility for a single census item with
    corrected demographics. Updates the item in place — result_summary,
    payer_demographics, and corrected_demographics — and appends to the
    rerun_history audit trail."""
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_run_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:run permission required'})

    if not isinstance(body, dict):
        return _response(400, {'error': 'JSON body required'})

    run_id = path_params.get('runId')
    patient_hash = path_params.get('patientHash')
    if not run_id or not patient_hash:
        return _response(400, {'error': 'runId and patientHash path params required'})

    existing = _find_item_by_run_and_patient(org_id, run_id, patient_hash)
    if not existing:
        return _response(404, {'error': 'census item not found'})

    # Build the corrected demographics from the body, falling back to the
    # original submitted_demographics for anything the user didn't touch.
    submitted = existing.get('submitted_demographics') or {}
    corrected = {
        k: body[k] for k in _RERUN_DEMOGRAPHIC_FIELDS
        if k in body and body[k] not in (None, '')
    }
    if not corrected:
        return _response(400, {
            'error': f'at least one of {list(_RERUN_DEMOGRAPHIC_FIELDS)} must be provided',
        })

    # Required fields for orchestrator.verify must still be present after
    # the merge. Validate up front so we don't 500 inside Stedi.
    merged = {**submitted, **corrected}
    for required in ('first_name', 'last_name', 'dob'):
        if not merged.get(required):
            return _response(400, {'error': f'{required} is required'})
    if not (isinstance(merged['dob'], str) and len(merged['dob']) == 8 and merged['dob'].isdigit()):
        return _response(400, {'error': 'dob must be YYYYMMDD'})

    try:
        org_config = stedi_config.load_stedi_config(org_id)
    except StediOrgNotConfigured as e:
        return _response(409, {'error': str(e)})

    client = client_factory.build_client(org_config, client_ip=_client_ip_from(event))
    now = datetime.now(timezone.utc)

    try:
        result = orchestrator.verify(
            merged,
            org_id=org_id,
            org_config=org_config,
            stedi_client=client,
            client_ip=_client_ip_from(event),
            user_email=claims.get('email') or 'unknown@rerun',
        )
    except StediDailyCapExceeded as e:
        return _response(429, {'error': str(e), 'code': 'daily_cap_exceeded'})
    except StediBadRequest as e:
        return _response(400, {'error': str(e)})
    except (StediAuthError, StediRateLimited, StediUpstreamError) as e:
        return _response(502, {'error': str(e)})
    except StediError as e:
        return _response(500, {'error': str(e)})

    new_status, new_summary, new_payer_demographics = _summarize_for_rerun(result)
    rerun_entry = {
        'rerun_id': result.get('audit_ids')[0] if result.get('audit_ids') else None,
        'rerun_by': claims.get('email'),
        'rerun_at': now.isoformat(),
        'corrected_fields': sorted(corrected.keys()),
        'previous_status': existing.get('result_status'),
        'new_status': new_status,
        'audit_ids': result.get('audit_ids') or [],
    }
    new_history = list(existing.get('rerun_history') or []) + [rerun_entry]

    try:
        _stedi_table.update_item(
            Key={'pk': existing['pk'], 'sk': existing['sk']},
            UpdateExpression=(
                'SET corrected_demographics = :cd,'
                '    result_status = :rs,'
                '    result_summary = :rsum,'
                '    payer_demographics = :pd,'
                '    rerun_history = :rh'
            ),
            ExpressionAttributeValues={
                ':cd': corrected,
                ':rs': new_status,
                ':rsum': new_summary,
                ':pd': new_payer_demographics,
                ':rh': new_history,
            },
        )
    except ClientError as e:
        return _response(500, {'error': str(e)})

    refreshed = _find_item_by_run_and_patient(org_id, run_id, patient_hash)
    return _response(200, {'item': _format_item(refreshed)})


def _client_ip_from(event):
    return (event.get('requestContext') or {}).get('http', {}).get('sourceIp')


def _summarize_for_rerun(result):
    """Same shape as census_runner._build_item_row's result_summary, but
    rebuilt here so the runner doesn't have to live in the api Lambda.
    Returns (result_status, result_summary, payer_demographics)."""
    primary = result.get('primary_coverage') or {}
    sub = primary.get('subscriber') or {}
    plan = primary.get('plan') or {}
    payer = primary.get('payer') or {}
    member_id = sub.get('member_id') or ''
    member_id_last4 = member_id[-4:] if len(member_id) >= 4 else (member_id or None)
    secondaries = result.get('secondary_coverages') or []
    review_needed = result.get('discovery_review_needed') or []
    discrepancies = result.get('discrepancies') or []

    # Mirror census_runner._classify for status decision.
    if primary is None or not primary:
        if review_needed:
            new_status = 'review_needed'
        else:
            new_status = 'no_coverage'
    elif primary.get('active') and primary.get('service_type_status') == 'not_covered':
        new_status = 'service_type_denied'
    elif discrepancies:
        new_status = 'discrepancy'
    else:
        new_status = 'verified'

    summary = {
        'payer_name': payer.get('name'),
        'payer_id': payer.get('id'),
        'plan_name': plan.get('name'),
        'member_id_last4': member_id_last4,
        'effective_date': plan.get('effective_date'),
        'expiration_date': plan.get('expiration_date'),
        'auth_required': primary.get('auth_required'),
        'service_type_status': primary.get('service_type_status'),
        'service_types': primary.get('service_types') or [],
        'active': primary.get('active'),
        'discrepancies': discrepancies,
        'secondary_count': len(secondaries),
        'review_needed_count': len(review_needed),
    }

    payer_demographics = None
    if sub.get('member_id') or review_needed:
        first_review = review_needed[0] if review_needed else {}
        payer_demographics = {
            'subscriber': {
                'first_name': sub.get('first_name'),
                'last_name': sub.get('last_name'),
                'member_id': sub.get('member_id'),
                'group_number': sub.get('group_number'),
                'dob': sub.get('dob'),
            } if sub.get('member_id') else first_review.get('subscriber_demographics'),
            'dependent': first_review.get('dependent_demographics'),
            'confidence_level': 'HIGH' if sub.get('member_id') else first_review.get('confidence_level'),
            'confidence_reason': first_review.get('confidence_reason'),
        }
    return new_status, summary, payer_demographics


# ---- helpers ------------------------------------------------------------

def _query_latest_run(org_id):
    response = _stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq(f'CENSUS_RUN#{org_id}'),
        ScanIndexForward=False,
        Limit=1,
    )
    items = response.get('Items') or []
    return items[0] if items else None


def _query_items_for_run(org_id, run_date, run_id):
    response = _stedi_table.query(
        KeyConditionExpression=(
            Key('pk').eq(f'ORG#{org_id}')
            & Key('sk').begins_with(f'CENSUS_ITEM#{run_date}#{run_id}#')
        ),
    )
    return response.get('Items') or []


def _find_item_by_run_and_patient(org_id, run_id, patient_hash):
    """Walk the org's CENSUS_ITEM# rows looking for one whose sk ends with
    #{run_id}#{patient_hash}. Bounded since 90d retention caps the volume."""
    response = _stedi_table.query(
        KeyConditionExpression=(
            Key('pk').eq(f'ORG#{org_id}')
            & Key('sk').begins_with('CENSUS_ITEM#')
        ),
    )
    target_suffix = f'#{run_id}#{patient_hash}'
    for item in (response.get('Items') or []):
        if item.get('sk', '').endswith(target_suffix):
            return item
    return None


def _format_run(item):
    return {
        'run_id': item.get('run_id'),
        'run_date': item.get('run_date'),
        'started_at': item.get('started_at'),
        'completed_at': item.get('completed_at'),
        'status': item.get('status'),
        'source': item.get('source'),
        'total': _i(item.get('total')),
        'verified': _i(item.get('verified')),
        'discrepancy': _i(item.get('discrepancy')),
        'no_coverage': _i(item.get('no_coverage')),
        'review_needed': _i(item.get('review_needed')),
        'pediatric_no_info': _i(item.get('pediatric_no_info')),
        'service_type_denied': _i(item.get('service_type_denied')),
        'error': _i(item.get('error')),
    }


def _format_item(item):
    return {
        'run_id': item.get('run_id'),
        'patient_hash': item.get('patient_hash'),
        'patient_first_name': item.get('patient_first_name'),
        'patient_last_name': item.get('patient_last_name'),
        'patient_dob': item.get('patient_dob'),
        'submitted_demographics': item.get('submitted_demographics') or {},
        'corrected_demographics': item.get('corrected_demographics'),
        'payer_demographics': item.get('payer_demographics'),
        'rerun_history': item.get('rerun_history') or [],
        'result_status': item.get('result_status'),
        'result_summary': item.get('result_summary') or {},
        'audit_ids': item.get('audit_ids') or [],
        'resolution': item.get('resolution') or {'state': 'unresolved'},
    }


def _i(v):
    """Coerce DynamoDB Decimal to int. Returns 0 for None."""
    if v is None:
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# ---- unread-count helper (called from permissions.serialize_for_me_endpoint) ----

def unread_count_for_org(org_id):
    """Number of CENSUS_ITEM# rows in the latest run that need attention
    (any non-verified status) and haven't been resolved. Returns 0 on any
    failure so a flaky DDB call doesn't break the /me/permissions endpoint."""
    try:
        run = _query_latest_run(org_id)
        if not run:
            return 0
        items = _query_items_for_run(org_id, run['run_date'], run['run_id'])
        attention_statuses = {
            'discrepancy', 'no_coverage', 'review_needed',
            'pediatric_no_info', 'service_type_denied', 'error',
        }
        return sum(
            1 for i in items
            if i.get('result_status') in attention_statuses
            and (i.get('resolution') or {}).get('state') != 'resolved'
        )
    except Exception:  # noqa: BLE001
        return 0
