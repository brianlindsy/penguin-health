"""HTTP handlers for the Stedi-backed insurance eligibility feature.

Imported from admin_api.py; dispatches the four eligibility routes:

    POST /api/organizations/{orgId}/eligibility/verify
    GET  /api/organizations/{orgId}/eligibility/history
    GET  /api/organizations/{orgId}/eligibility/config
    PUT  /api/organizations/{orgId}/eligibility/config

The actual orchestration (Stedi calls + audit/cap) lives in the
`stedi` package; this module just adapts API Gateway events to/from it
and enforces RBAC.
"""

import json
import os
from datetime import datetime, timezone

import boto3

import permissions as perms_module
from stedi import audit as stedi_audit
from stedi import client_factory, config as stedi_config
from stedi import demo_fixtures, payer_registry, orchestrator
from stedi.exceptions import (
    StediAuthError,
    StediBadRequest,
    StediDailyCapExceeded,
    StediError,
    StediOrgNotConfigured,
    StediRateLimited,
    StediUpstreamError,
)


_ORG_CONFIG_TABLE = os.environ.get('DYNAMODB_TABLE', 'penguin-health-org-config')
_dynamodb = boto3.resource('dynamodb')
_org_table = _dynamodb.Table(_ORG_CONFIG_TABLE)


def _response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body, default=str),
    }


def _client_ip(event):
    return (
        (event.get('requestContext') or {})
        .get('http', {})
        .get('sourceIp')
    )


# ---- POST /eligibility/verify ----

def verify(event, path_params, body, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_run_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:run permission required'})

    if not isinstance(body, dict):
        return _response(400, {'error': 'JSON body required'})

    payload = _normalize_verify_input(body)
    if isinstance(payload, dict) and 'error' in payload:
        return _response(400, payload)

    try:
        org_config = stedi_config.load_stedi_config(org_id)
    except StediOrgNotConfigured as e:
        return _response(409, {'error': str(e)})

    client = client_factory.build_client(org_config, client_ip=_client_ip(event))

    try:
        result = orchestrator.verify(
            payload,
            org_id=org_id,
            org_config=org_config,
            stedi_client=client,
            client_ip=_client_ip(event),
            user_email=claims.get('email'),
        )
    except StediDailyCapExceeded as e:
        return _response(429, {'error': str(e), 'code': 'daily_cap_exceeded'})
    except StediBadRequest as e:
        return _response(400, {'error': str(e)})
    except StediAuthError as e:
        return _response(502, {'error': 'Stedi authentication failed', 'detail': str(e)})
    except StediRateLimited as e:
        return _response(502, {'error': 'Stedi rate-limited', 'detail': str(e)})
    except StediUpstreamError as e:
        return _response(502, {'error': 'Stedi upstream error', 'detail': str(e)})
    except StediError as e:
        return _response(500, {'error': str(e)})

    return _response(200, result)


# ---- GET /eligibility/history ----

def history(event, path_params, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.can_view_category(claims, org_id, 'Eligibility'):
        return _response(403, {'error': 'Eligibility:view permission required'})

    qs = (event.get('queryStringParameters') or {})
    first = qs.get('first')
    last = qs.get('last')
    dob = qs.get('dob')
    try:
        limit = min(int(qs.get('limit') or 20), 100)
    except (TypeError, ValueError):
        limit = 20

    if not (first and last and dob):
        return _response(400, {'error': 'first, last, and dob query params required'})

    rows = stedi_audit.recent_checks_for_patient(org_id, first, last, dob, limit=limit)
    return _response(200, {'history': [_format_history_row(r) for r in rows]})


# ---- GET/PUT /eligibility/config ----

def get_config(event, path_params, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.is_org_admin(claims, org_id):
        return _response(403, {'error': 'org_admin required'})
    response = _org_table.get_item(Key={'pk': f'ORG#{org_id}', 'sk': 'STEDI_CONFIG'})
    item = response.get('Item')
    if not item:
        return _response(404, {'error': 'no STEDI_CONFIG'})
    demo_mode = bool(item.get('demo_mode'))
    return _response(200, {
        'enabled': item.get('enabled', False),
        'provider': item.get('provider'),
        'daily_cap': item.get('daily_cap'),
        'preferred_payer_ids': item.get('preferred_payer_ids') or [],
        'available_payers': payer_registry.list_all(),
        'demo_mode': demo_mode,
        'demo_scenarios': demo_fixtures.list_scenarios() if demo_mode else [],
        'created_at': item.get('created_at'),
        'updated_at': item.get('updated_at'),
    })


def update_config(event, path_params, body, authorize_fn, **_):
    org_id = path_params.get('orgId')
    claims, error = authorize_fn(event, org_id=org_id)
    if error:
        return error
    if not perms_module.is_super_admin(claims):
        return _response(403, {'error': 'super-admin required'})
    if not isinstance(body, dict):
        return _response(400, {'error': 'JSON body required'})

    provider_in = body.get('provider') or {}
    npi = provider_in.get('npi')
    if npi is not None and not (isinstance(npi, str) and npi.isdigit() and len(npi) == 10):
        return _response(400, {'error': 'provider.npi must be a 10-digit string'})
    organization_name = provider_in.get('organization_name')
    if organization_name is not None and not (
        isinstance(organization_name, str) and organization_name.strip()
    ):
        return _response(400, {'error': 'provider.organization_name must be a non-empty string'})

    daily_cap = body.get('daily_cap')
    if daily_cap is not None and not (isinstance(daily_cap, int) and daily_cap > 0):
        return _response(400, {'error': 'daily_cap must be a positive integer'})

    existing = _org_table.get_item(
        Key={'pk': f'ORG#{org_id}', 'sk': 'STEDI_CONFIG'}
    ).get('Item') or {}

    # Field-level merge so a PUT that only updates one provider field doesn't
    # wipe out the other (e.g. setting NPI shouldn't clear organization_name).
    merged_provider = dict(existing.get('provider') or {})
    if npi is not None:
        merged_provider['npi'] = npi
    if organization_name is not None:
        merged_provider['organization_name'] = organization_name.strip()

    if not merged_provider.get('npi') or not merged_provider.get('organization_name'):
        return _response(400, {
            'error': 'provider.npi and provider.organization_name are both required',
        })

    now = datetime.now(timezone.utc).isoformat()
    item = {
        'pk': f'ORG#{org_id}',
        'sk': 'STEDI_CONFIG',
        'organization_id': org_id,
        'enabled': bool(body.get('enabled', existing.get('enabled', False))),
        'provider': merged_provider,
        'daily_cap': daily_cap if daily_cap is not None else existing.get('daily_cap'),
        'preferred_payer_ids': body.get('preferred_payer_ids', existing.get('preferred_payer_ids') or []),
        'demo_mode': bool(body.get('demo_mode', existing.get('demo_mode', False))),
        'created_at': existing.get('created_at', now),
        'updated_at': now,
    }
    _org_table.put_item(Item=item)
    stedi_config.invalidate_cache()
    return _response(200, item)


# ---- helpers ----

def _normalize_verify_input(body):
    first = (body.get('first_name') or '').strip()
    last = (body.get('last_name') or '').strip()
    dob = (body.get('dob') or '').strip()
    if not (first and last and dob):
        return {'error': 'first_name, last_name, and dob are required'}
    if not (len(dob) == 8 and dob.isdigit()):
        return {'error': 'dob must be YYYYMMDD'}

    payer_id = body.get('payer_id')
    if not payer_id and body.get('payer_name'):
        resolved = payer_registry.lookup_by_user_input(body['payer_name'])
        if resolved:
            payer_id = resolved['id']

    return {
        'first_name': first,
        'last_name': last,
        'dob': dob,
        'ssn': (body.get('ssn') or '').strip() or None,
        'member_id': (body.get('member_id') or '').strip() or None,
        'payer_id': payer_id,
        'address1': body.get('address1'),
        'city': body.get('city'),
        'state': body.get('state'),
        'postal_code': body.get('postal_code'),
    }


def _format_history_row(item):
    return {
        'request_id': item.get('request_id'),
        'requested_at': item.get('requested_at'),
        'user_email': item.get('user_email'),
        'call_type': item.get('call_type'),
        'payer_name': item.get('payer_name'),
        'result_status': item.get('result_status'),
        'member_id_last4': item.get('member_id_last4'),
        'result_summary': item.get('result_summary'),
    }
