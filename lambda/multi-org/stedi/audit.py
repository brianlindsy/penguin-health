"""Audit log + daily usage counter for Stedi calls.

Two row types share the penguin-health-stedi table:
  sk=AUDIT#{iso_ts}#{request_id}  — one row per Stedi call, immutable, 7y TTL
  sk=USAGE#{yyyy-mm-dd}            — atomic counter for daily cap, 90d TTL

GSI1 keyed by patient_hash drives the "checked already today" dedup
lookup without scanning AUDIT# rows.
"""

import hashlib
import os
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from .exceptions import StediDailyCapExceeded


_TABLE_NAME = os.environ.get('STEDI_TABLE_NAME', 'penguin-health-stedi')
_dynamodb = boto3.resource('dynamodb')
_table = _dynamodb.Table(_TABLE_NAME)

_SEVEN_YEARS_SECONDS = 7 * 365 * 24 * 60 * 60
_NINETY_DAYS_SECONDS = 90 * 24 * 60 * 60


def patient_hash(first_name, last_name, dob):
    """sha256 of normalized identity. Used as a stable bucket so dedup
    queries don't need to store PII on the audit row."""
    raw = f"{(first_name or '').strip().lower()}|{(last_name or '').strip().lower()}|{(dob or '').strip()}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def reserve_capacity(org_id, daily_cap):
    """Atomic conditional ADD on USAGE#{today}. Raises StediDailyCapExceeded
    if the increment would push count >= daily_cap. Returns the post-increment
    count on success."""
    today = date.today().isoformat()
    now = datetime.now(timezone.utc)
    ttl = int(now.timestamp()) + _NINETY_DAYS_SECONDS
    try:
        response = _table.update_item(
            Key={'pk': f'ORG#{org_id}', 'sk': f'USAGE#{today}'},
            UpdateExpression='ADD #c :one SET expires_at = :ttl, updated_at = :now',
            ExpressionAttributeNames={'#c': 'count'},
            ExpressionAttributeValues={
                ':one': 1,
                ':cap': daily_cap,
                ':ttl': ttl,
                ':now': now.isoformat(),
            },
            ConditionExpression='attribute_not_exists(#c) OR #c < :cap',
            ReturnValues='UPDATED_NEW',
        )
        return int(response['Attributes']['count'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise StediDailyCapExceeded(f"daily cap of {daily_cap} reached for org={org_id}")
        raise


def write_audit(*, org_id, user_email, call_type, patient, result, client_ip,
                stedi_control_number=None, duration_ms=None, member_id=None,
                payer=None, request_id=None):
    """Write one immutable audit row. Returns the request_id (caller can
    pre-generate it to echo in the API response)."""
    request_id = request_id or str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    iso_ts = now.isoformat()
    expires_at = int(now.timestamp()) + _SEVEN_YEARS_SECONDS

    p_hash = patient_hash(patient.get('first_name'), patient.get('last_name'), patient.get('dob'))
    member_id_last4 = member_id[-4:] if member_id and len(member_id) >= 4 else None
    payer_name = (payer or {}).get('name')
    payer_id = (payer or {}).get('id')

    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'AUDIT#{iso_ts}#{request_id}',
        'gsi1pk': f'PATIENT#{org_id}#{p_hash}',
        'gsi1sk': iso_ts,
        'request_id': request_id,
        'user_email': user_email,
        'requested_at': iso_ts,
        'call_type': call_type,
        'patient_hash': p_hash,
        'patient_first_initial': (patient.get('first_name') or '?')[:1].upper(),
        'patient_last_initial': (patient.get('last_name') or '?')[:1].upper(),
        'patient_dob': patient.get('dob'),
        'client_ip': client_ip,
        'result_status': (result or {}).get('status'),
        'result_summary': _slim_result(result),
        'expires_at': expires_at,
    }
    if member_id_last4:
        item['member_id_last4'] = member_id_last4
    if payer_name:
        item['payer_name'] = payer_name
    if payer_id:
        item['payer_id'] = payer_id
    if stedi_control_number:
        item['stedi_control_number'] = stedi_control_number
    if duration_ms is not None:
        item['duration_ms'] = int(duration_ms)

    _table.put_item(Item=item, ConditionExpression='attribute_not_exists(pk)')
    return request_id


def recent_checks_for_patient(org_id, first_name, last_name, dob, limit=20, since=None):
    """Return up to `limit` recent audit rows for this patient, newest-first.
    `since` is an optional datetime — only rows >= since are returned."""
    p_hash = patient_hash(first_name, last_name, dob)
    expression_values = {':pk': f'PATIENT#{org_id}#{p_hash}'}
    key_condition = 'gsi1pk = :pk'
    if since:
        expression_values[':since'] = since.isoformat()
        key_condition += ' AND gsi1sk >= :since'

    response = _table.query(
        IndexName='gsi1',
        KeyConditionExpression=key_condition,
        ExpressionAttributeValues=expression_values,
        ScanIndexForward=False,
        Limit=limit,
    )
    return response.get('Items', [])


def recent_check_summary(org_id, first_name, last_name, dob, within_minutes=30):
    """Return the single most-recent audit (if any) in the last
    `within_minutes`. Used by the orchestrator's dedup pre-check."""
    since = datetime.now(timezone.utc) - timedelta(minutes=within_minutes)
    items = recent_checks_for_patient(org_id, first_name, last_name, dob, limit=1, since=since)
    if not items:
        return None
    item = items[0]
    return {
        'checked_by': item.get('user_email'),
        'checked_at': item.get('requested_at'),
        'payer_name': item.get('payer_name'),
        'result_status': item.get('result_status'),
        'request_id': item.get('request_id'),
    }


def _slim_result(result):
    """Keep the audit row well under the 400KB item cap by storing only
    the fields the UI needs for history rendering. The full Stedi response
    is recoverable from Stedi via the controlNumber if ever needed."""
    if not result:
        return None
    plan = result.get('plan') or {}
    return {
        'status': result.get('status'),
        'active': result.get('active'),
        'plan_name': plan.get('name'),
        'effective_date': plan.get('effective_date'),
        'expiration_date': plan.get('expiration_date'),
        'auth_required': result.get('auth_required'),
    }
