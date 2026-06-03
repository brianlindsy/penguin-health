"""HTTP handlers for the eligibility worklist.

Each row is one Encounter the FHIR poller verified, keyed by encounter_id.
Wired into admin_api.py's dispatch dict. Three endpoints:
    GET   /api/organizations/{orgId}/eligibility/encounters
    PUT   /api/organizations/{orgId}/eligibility/encounters/{encounterId}/resolve
    POST  /api/organizations/{orgId}/eligibility/encounters/{encounterId}/rerun

`fhir_eligibility_poller` writes the rows; this module reads + the resolve
handler patches the `resolution` sub-object + the rerun handler re-runs
verify with corrected demographics and updates the same item in place.
"""

import json
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

import permissions as perms_module
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

_ATTENTION_STATUSES = frozenset({
    'discrepancy', 'no_coverage', 'review_needed',
    'pediatric_no_info', 'service_type_denied', 'error',
})


def _response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body, default=str),
    }


# ---- GET /eligibility/encounters ----------------------------------------

def list_encounters(event, path_params, authorize_fn, **_):
    """Return the rolling list of recently-verified encounters for the org,
    newest first. Drives the worklist UI."""
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_view_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:view permission required'})

    qs = event.get('queryStringParameters') or {}
    try:
        limit = min(int(qs.get('limit') or 100), 500)
    except (TypeError, ValueError):
        limit = 100

    items = _query_encounters(org_id, limit=limit)
    return _response(200, {
        'items': [_format_item(i) for i in items],
        'counts': _count_by_status(items),
    })


# ---- PUT /eligibility/encounters/{encounterId}/resolve ------------------

def resolve_encounter(event, path_params, body, authorize_fn, **_):
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

    note = (body.get('note') or '').strip() or None
    rerun_audit_id = (body.get('rerun_audit_id') or '').strip() or None

    encounter_id = path_params.get('encounterId')
    if not encounter_id:
        return _response(400, {'error': 'encounterId path param required'})

    item = _get_encounter(org_id, encounter_id)
    if not item:
        return _response(404, {'error': 'encounter item not found'})

    now = datetime.now(timezone.utc).isoformat()
    prev_resolution = item.get('resolution') or {}
    resolution = {
        'state': state,
        'note': note,
        'resolved_by': claims.get('email') if state == 'resolved' else prev_resolution.get('resolved_by'),
        'resolved_at': now if state == 'resolved' else prev_resolution.get('resolved_at'),
        'rerun_audit_id': rerun_audit_id or prev_resolution.get('rerun_audit_id'),
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


# ---- POST /eligibility/encounters/{encounterId}/rerun -------------------

# Demographic fields the UI is allowed to send in a rerun. Everything else
# on the body is ignored.
_RERUN_DEMOGRAPHIC_FIELDS = (
    'first_name', 'middle_name', 'last_name', 'suffix',
    'dob', 'gender', 'ssn_last4',
    'address1', 'address2', 'city', 'state', 'postal_code',
    'member_id', 'payer_id',
)


def rerun_encounter(event, path_params, body, authorize_fn, **_):
    """Re-run discovery + eligibility for a single encounter row with
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

    encounter_id = path_params.get('encounterId')
    if not encounter_id:
        return _response(400, {'error': 'encounterId path param required'})

    existing = _get_encounter(org_id, encounter_id)
    if not existing:
        return _response(404, {'error': 'encounter item not found'})

    submitted = existing.get('submitted_demographics') or {}
    corrected = {
        k: body[k] for k in _RERUN_DEMOGRAPHIC_FIELDS
        if k in body and body[k] not in (None, '')
    }
    if not corrected:
        return _response(400, {
            'error': f'at least one of {list(_RERUN_DEMOGRAPHIC_FIELDS)} must be provided',
        })

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

    refreshed = _get_encounter(org_id, encounter_id)
    return _response(200, {'item': _format_item(refreshed)})


def _client_ip_from(event):
    return (event.get('requestContext') or {}).get('http', {}).get('sourceIp')


def _summarize_for_rerun(result):
    """Same shape as fhir_eligibility_poller._build_item_row's result_summary
    + payer_demographics, rebuilt here so the poller doesn't have to live
    in the api Lambda. Returns (result_status, result_summary, payer_demographics)."""
    primary = result.get('primary_coverage') or {}
    sub = primary.get('subscriber') or {}
    plan = primary.get('plan') or {}
    payer = primary.get('payer') or {}
    member_id = sub.get('member_id') or ''
    member_id_last4 = member_id[-4:] if len(member_id) >= 4 else (member_id or None)
    secondaries = result.get('secondary_coverages') or []
    review_needed = result.get('discovery_review_needed') or []
    discrepancies = result.get('discrepancies') or []

    if primary is None or not primary:
        new_status = 'review_needed' if review_needed else 'no_coverage'
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
        'cob_check': result.get('cob_check') or {'checked': False},
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


# ---- DDB helpers --------------------------------------------------------

def _query_encounters(org_id, *, limit):
    """Pull the most recent ENCOUNTER_ITEM# rows via the chronological GSI."""
    response = _stedi_table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq(f'ENCOUNTER_ITEM#{org_id}'),
        ScanIndexForward=False,
        Limit=limit,
    )
    return response.get('Items') or []


def _get_encounter(org_id, encounter_id):
    response = _stedi_table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': f'ENCOUNTER_ITEM#{encounter_id}'},
    )
    return response.get('Item')


def _count_by_status(items):
    counts = {
        'total': len(items),
        'verified': 0, 'discrepancy': 0, 'no_coverage': 0,
        'review_needed': 0, 'pediatric_no_info': 0,
        'service_type_denied': 0, 'error': 0,
        'attention': 0, 'resolved': 0,
    }
    for it in items:
        status = it.get('result_status')
        if status in counts:
            counts[status] += 1
        resolution_state = (it.get('resolution') or {}).get('state')
        if resolution_state == 'resolved':
            counts['resolved'] += 1
        elif status in _ATTENTION_STATUSES:
            counts['attention'] += 1
    return counts


def _format_item(item):
    if item is None:
        return None
    return {
        'encounter_id': item.get('encounter_id'),
        'encounter_class': item.get('encounter_class'),
        'encounter_status': item.get('encounter_status'),
        'encounter_lastUpdated': item.get('encounter_lastUpdated'),
        'patient_hash': item.get('patient_hash'),
        'patient_first_initial': item.get('patient_first_initial'),
        'patient_last_initial': item.get('patient_last_initial'),
        'submitted_demographics': item.get('submitted_demographics') or {},
        'corrected_demographics': item.get('corrected_demographics'),
        'payer_demographics': item.get('payer_demographics'),
        'rerun_history': item.get('rerun_history') or [],
        'result_status': item.get('result_status'),
        'result_summary': item.get('result_summary') or {},
        'audit_ids': item.get('audit_ids') or [],
        'resolution': item.get('resolution') or {'state': 'unresolved'},
    }


# ---- unread-count helper (called from permissions.serialize_for_me_endpoint) ----

def unread_count_for_org(org_id):
    """Number of recent ENCOUNTER_ITEM# rows that need attention (any
    non-verified status) and haven't been resolved. Returns 0 on any
    failure so a flaky DDB call doesn't break the /me/permissions endpoint.

    Looks at the most recent 200 encounters — enough to bound the count
    even when verify is running every 15 minutes."""
    try:
        items = _query_encounters(org_id, limit=200)
        return sum(
            1 for i in items
            if i.get('result_status') in _ATTENTION_STATUSES
            and (i.get('resolution') or {}).get('state') != 'resolved'
        )
    except Exception:  # noqa: BLE001
        return 0
