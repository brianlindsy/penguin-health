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
_AUDIT_TABLE_NAME = os.environ.get('AUDIT_TABLE_NAME', 'penguin-health-audit')
_dynamodb = boto3.resource('dynamodb')
_table = _dynamodb.Table(_TABLE_NAME)
# Phase 2: dedup + history reads land on the new audit table. The
# legacy table still receives writes (AUDIT# rows) until Phase 3 — see
# write_audit below.
_audit_table = _dynamodb.Table(_AUDIT_TABLE_NAME)

# `_SEVEN_YEARS_SECONDS` is no longer used inside this module after the
# Phase 3 cutover, but `fhir_eligibility_poller._ensure_demo_history_seeds`
# imports it from here for the demo-mode seeds it writes directly to the
# legacy table. Keep the constant exported.
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
    pre-generate it to echo in the API response).

    Phase 3 of the audit-layer migration: legacy `AUDIT#` row writes on
    `penguin-health-stedi` have been removed; this function now only
    emits through the new audit layer (S3 Object Lock WORM + DDB hot
    mirror). The existing 7-year `AUDIT#` rows on `penguin-health-stedi`
    stay in place and age out via their `expires_at` TTL through ~2032 —
    do not delete them; they are still the source of truth for that
    historical window.

    Phases:
      1. Dual-write (legacy AUDIT# + new audit.emit).
      2. Cut reads (`recent_checks_for_patient`) over to the new table.
      3. Stop dual-writing — this is the state of the code right now.
    """
    request_id = request_id or str(uuid.uuid4())

    # Stedi audit rows always describe a PHI access against an external
    # payer/clearinghouse, so action='read' and purpose='ELIGIBILITY'.
    # `discovery` and `cob` calls land here too — same purpose, the
    # call_type field differentiates in Athena.
    from audit import emit as _emit  # imported lazily so a bundle without
    # the audit package fails loudly at first call, not at import-time.

    actor = {
        'agent_type': 'human',
        'agent_id': user_email,
        'agent_email': user_email,
        'agent_groups': [],
        'client_ip': client_ip,
        'user_agent': None,
    }
    _emit(
        action='read',
        resource={'type': 'Coverage', 'id': None, 'org': org_id},
        actor=actor,
        org_id=org_id,
        purpose_of_use='ELIGIBILITY',
        call_type=call_type,
        patient=patient,
        member_id=member_id,
        payer=payer,
        external_control_number=stedi_control_number,
        duration_ms=duration_ms,
        result=result,
        request_id=request_id,
    )
    return request_id


def recent_checks_for_patient(org_id, first_name, last_name, dob, limit=20, since=None):
    """Return up to `limit` recent audit rows for this patient, newest-first.
    `since` is an optional datetime — only rows >= since are returned.

    Phase 2 cutover: reads land on `penguin-health-audit` (the new layer).
    The 30-day backfill (scripts/backfill_audit_layer.py) must be run
    before this code path is deployed, otherwise the eligibility dedup
    window starts empty. Rows produced by the dual-write in write_audit
    use the same gsi1pk/gsi1sk shape as the legacy table, so consumers
    of this function don't need to change.

    Each returned item exposes both the legacy field names (user_email,
    requested_at, result_status) — translated from the new schema — and
    the new ones (agent_email, event_time, etc.). Callers using the
    legacy names (eligibility_api.history._format_history_row) continue
    to work without modification.
    """
    p_hash = patient_hash(first_name, last_name, dob)
    expression_values = {':pk': f'PATIENT#{org_id}#{p_hash}'}
    key_condition = 'gsi1pk = :pk'
    if since:
        expression_values[':since'] = since.isoformat()
        key_condition += ' AND gsi1sk >= :since'

    response = _audit_table.query(
        IndexName='gsi1',
        KeyConditionExpression=key_condition,
        ExpressionAttributeValues=expression_values,
        ScanIndexForward=False,
        Limit=limit,
    )
    return [_legacy_view(it) for it in response.get('Items', [])]


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


def _legacy_view(item):
    """Translate a new-schema audit row into the legacy field names that
    downstream consumers (eligibility_api.history, orchestrator dedup,
    discrepancy derivation) expect. The new fields are kept too — callers
    that already use them keep working.

    Mapping:
      legacy             ← new
      user_email          agent_email
      requested_at        event_time
      result_status       event.result_summary.status OR top-level result_status
      result_summary      event.result_summary
      stedi_control_number event.external_control_number
    """
    if not item:
        return item
    legacy = dict(item)  # don't mutate the underlying boto3 dict
    event = item.get('event') or {}
    legacy.setdefault('user_email', item.get('agent_email'))
    legacy.setdefault('requested_at', item.get('event_time'))
    result_summary = event.get('result_summary') or item.get('result_summary')
    if result_summary is not None:
        legacy.setdefault('result_summary', result_summary)
        legacy.setdefault('result_status',
                          (result_summary or {}).get('status'))
    legacy.setdefault(
        'stedi_control_number', event.get('external_control_number')
    )
    legacy.setdefault('request_id', item.get('event_id'))
    return legacy


