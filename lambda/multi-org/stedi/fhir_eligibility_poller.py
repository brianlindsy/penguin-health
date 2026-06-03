"""FHIR-polling-triggered insurance verification Lambda.

Invoked by EventBridge every ~15 minutes per opted-in org. Polls the org's
FHIR API for new Encounters since the last cursor watermark, fetches each
Encounter's referenced Patient, and runs orchestrator.verify() against it.

This replaces the old morning-census EventBridge cron. The cursor row
(pk=ORG#{org}, sk=FHIR_POLL_CURSOR) records the watermark so we don't
re-process encounters across ticks.

PHI/PII: logs use patient_hash + encounter_id only. Never log
demographics, SSN, member id, or full names. The orchestrator's existing
audit.write_audit() writes the per-call audit row as before.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from . import audit as audit_module
from . import client_factory
from . import config as stedi_config_module
from . import demo_fixtures
from . import fhir_patient_mapper
from . import orchestrator
from .exceptions import (
    StediBadRequest,
    StediDailyCapExceeded,
    StediError,
    StediOrgNotConfigured,
)


logger = logging.getLogger(__name__)
_TABLE_NAME = os.environ.get('STEDI_TABLE_NAME', 'penguin-health-stedi')
_dynamodb = boto3.resource('dynamodb')
_table = _dynamodb.Table(_TABLE_NAME)

_NINETY_DAYS_SECONDS = 90 * 24 * 60 * 60
# Per-invoke cap on encounters processed. At 200 encounters/15-min tick,
# we burn through a worst-case admit surge in under an hour without
# tripping the Lambda 15-min timeout (stedi+fhir latency ~1s each).
_MAX_ENCOUNTERS_PER_POLL = 200
# Default look-back when no cursor row exists yet. Bounds the first
# poll's work so a brand-new org doesn't try to ingest months of history.
_INITIAL_LOOKBACK = timedelta(hours=1)

_RESOLUTION_DEFAULT = {
    'state': 'unresolved',
    'note': None,
    'resolved_by': None,
    'resolved_at': None,
    'rerun_audit_id': None,
}


def handler(event, context):
    """EventBridge entry point. Expected payload: {"organization_id": "..."}."""
    org_id = event.get('organization_id')
    if not org_id:
        raise StediBadRequest("event payload must include organization_id")

    try:
        org_config = stedi_config_module.load_stedi_config(org_id)
    except StediOrgNotConfigured as e:
        logger.warning("fhir-eligibility-poller skipped: %s", e)
        return {'status': 'skipped', 'reason': str(e), 'organization_id': org_id}

    if not org_config.get('census_enabled'):
        logger.info("fhir-eligibility-poller not enabled for org=%s", org_id)
        return {
            'status': 'skipped',
            'reason': 'census_enabled is false',
            'organization_id': org_id,
        }

    return run_poll(org_id, org_config)


def run_poll(org_id, org_config, *, now=None):
    """Poll for new encounters, verify each, advance the cursor.

    `now` is injected for tests so the cursor default + ENCOUNTER_ITEM
    timestamps are deterministic.
    """
    now = now or datetime.now(timezone.utc)
    expires_at = int(now.timestamp()) + _NINETY_DAYS_SECONDS
    cursor = _read_cursor(org_id, default=now - _INITIAL_LOOKBACK)

    # Seed the synthetic prior-Cigna audit row for Linda Sandbox before
    # the first verify runs so the "primary changed" discrepancy fires.
    # Idempotent + only fires in demo mode.
    if org_config.get('demo_mode'):
        _ensure_demo_history_seeds(org_id, now)

    stedi_client = client_factory.build_client(org_config, client_ip=None)

    encounters = _iter_encounters(org_id, org_config, cursor)

    counts = {'verified': 0, 'discrepancy': 0, 'no_coverage': 0,
              'review_needed': 0, 'pediatric_no_info': 0,
              'service_type_denied': 0, 'error': 0}
    processed = 0
    max_seen = cursor
    poll_status = 'complete'

    for encounter in encounters:
        if processed >= _MAX_ENCOUNTERS_PER_POLL:
            poll_status = 'max_per_poll_reached'
            break
        encounter_last_updated = ((encounter.get('meta') or {}).get('lastUpdated')) or ''
        try:
            item_status = _process_one(
                org_id, encounter, org_config, stedi_client, expires_at,
            )
        except StediDailyCapExceeded:
            # Do NOT advance cursor past the failing encounter — next tick
            # (or next day, after the counter resets) retries from here.
            logger.warning(
                "fhir-eligibility-poller hit daily cap org=%s encounter=%s",
                org_id, encounter.get('id'),
            )
            poll_status = 'cap_exceeded'
            break

        counts[item_status] = counts.get(item_status, 0) + 1
        processed += 1
        if encounter_last_updated and encounter_last_updated > max_seen:
            max_seen = encounter_last_updated

    _write_cursor(org_id, max_seen, now, poll_status, processed)

    logger.info(
        "fhir-eligibility-poller complete org=%s processed=%d status=%s cursor=%s",
        org_id, processed, poll_status, max_seen,
    )
    return {
        'status': poll_status,
        'organization_id': org_id,
        'processed': processed,
        'cursor': max_seen,
        **counts,
    }


# ---- encounter source ---------------------------------------------------

def _iter_encounters(org_id, org_config, cursor):
    """Return an iterable of FHIR Encounter resources to process.

    Demo mode pulls from the canned ENCOUNTER_STREAM so the demo flow works
    without a real FHIR endpoint. Real mode delegates to fhir.fhir_query
    (imported lazily — the FHIR package is bundled into the Lambda asset
    but tests that don't exercise the real path shouldn't need it on PATH).
    """
    if org_config.get('demo_mode'):
        return demo_fixtures.encounter_stream_after(cursor)

    from fhir import fhir_query  # noqa: PLC0415 — lazy import; see docstring
    params = _build_encounter_params(org_config.get('encounter_filter') or {}, cursor)
    return fhir_query.search(
        org_id, 'Encounter', params,
        max_results=_MAX_ENCOUNTERS_PER_POLL,
    )


def _build_encounter_params(encounter_filter, cursor_iso):
    """Build the FHIR Encounter search params from the org's filter config.

    FHIR R4 search semantics: repeating a parameter with comma-separated
    values is OR within that field. We pass each list directly so
    urlencode(doseq=True) (used by FhirClient) repeats the key per value.
    """
    params = {
        '_lastUpdated': f'gt{cursor_iso}',
        '_sort': '_lastUpdated',
        '_count': 50,
    }
    if encounter_filter.get('class_codes'):
        params['class'] = list(encounter_filter['class_codes'])
    if encounter_filter.get('type_codes'):
        params['type'] = list(encounter_filter['type_codes'])
    if encounter_filter.get('statuses'):
        params['status'] = list(encounter_filter['statuses'])
    return params


# ---- per-encounter processing ------------------------------------------

def _process_one(org_id, encounter, org_config, stedi_client, expires_at):
    """Verify one encounter; write the ENCOUNTER_ITEM# row; return its
    classified result_status. StediDailyCapExceeded propagates up so the
    caller can stop and avoid advancing the cursor past this encounter."""
    encounter_id = encounter.get('id')
    subject_ref = (encounter.get('subject') or {}).get('reference')

    if not encounter_id:
        # Encounter without an id — we have nowhere to anchor the item row.
        # Log and treat as error; cursor still advances via lastUpdated.
        logger.warning("fhir-eligibility-poller encounter without id, skipping")
        return 'error'

    if not subject_ref:
        item = _build_error_row(
            org_id, encounter, encounter_id, expires_at,
            patient_hash=None, error_kind='missing_subject_reference',
            error_message='Encounter.subject.reference is missing',
        )
        _put_item(item)
        return 'error'

    try:
        patient = _fetch_patient(org_id, subject_ref, org_config)
    except _PatientNotFound:
        item = _build_error_row(
            org_id, encounter, encounter_id, expires_at,
            patient_hash=None, error_kind='patient_not_found',
            error_message=f'Patient not found for reference {subject_ref}',
        )
        _put_item(item)
        return 'error'

    verify_input = fhir_patient_mapper.to_verify_input(patient, encounter)
    if not (verify_input.get('first_name') and verify_input.get('last_name')
            and verify_input.get('dob')):
        # Don't log the demographic values — but the encounter id is safe.
        item = _build_error_row(
            org_id, encounter, encounter_id, expires_at,
            patient_hash=None, error_kind='incomplete_patient',
            error_message='Patient missing required name or birthDate fields',
        )
        _put_item(item)
        return 'error'

    p_hash = audit_module.patient_hash(
        verify_input['first_name'], verify_input['last_name'], verify_input['dob'],
    )

    try:
        result = orchestrator.verify(
            verify_input,
            org_id=org_id,
            org_config=org_config,
            stedi_client=stedi_client,
            client_ip=None,
            user_email='system@fhir-poller',
        )
    except StediDailyCapExceeded:
        # Let the caller halt + skip cursor advance.
        raise
    except (StediError, Exception) as e:  # noqa: BLE001 — persist, don't crash
        logger.exception(
            "fhir-eligibility-poller verify failed org=%s encounter=%s patient_hash=%s",
            org_id, encounter_id, p_hash,
        )
        item = _build_error_row(
            org_id, encounter, encounter_id, expires_at,
            patient_hash=p_hash, error_kind=type(e).__name__, error_message=str(e),
        )
        _put_item(item)
        return 'error'

    status = _classify(result, verify_input)
    item = _build_item_row(
        org_id, encounter, encounter_id, expires_at,
        verify_input, p_hash, status, result,
    )
    _put_item(item)
    return status


class _PatientNotFound(Exception):
    """Internal signal — kept private so we don't leak fhir.exceptions into
    callers and the test harness."""


def _fetch_patient(org_id, subject_ref, org_config):
    """Resolve subject.reference to a Patient resource. Demo mode returns
    canned data; real mode hits the FHIR API."""
    patient_id = _parse_patient_id(subject_ref)
    if not patient_id:
        raise _PatientNotFound(subject_ref)

    if org_config.get('demo_mode'):
        patient = demo_fixtures.lookup_patient_by_reference(subject_ref)
        if patient is None:
            raise _PatientNotFound(subject_ref)
        return patient

    from fhir import fhir_query  # noqa: PLC0415 — lazy import
    from fhir.exceptions import FhirNotFound  # noqa: PLC0415
    try:
        return fhir_query.get_resource(org_id, 'Patient', patient_id)
    except FhirNotFound as e:
        raise _PatientNotFound(subject_ref) from e


def _parse_patient_id(reference):
    """Pull the bare id from a FHIR reference. Handles:
        Patient/123              -> 123
        123                      -> 123
        http://.../Patient/123   -> 123
        urn:uuid:abc             -> abc
        #contained               -> contained
    Returns None for empty/None.
    """
    if not reference:
        return None
    ref = reference.lstrip('#')
    # Split off any 'Patient/' or URL prefix; last segment is the id.
    return ref.rsplit('/', 1)[-1].rsplit(':', 1)[-1] or None


def _classify(verify_result, verify_input):
    """Map a verify result + the patient input into a single status string.
    The status taxonomy is the contract the UI worklist renders against."""
    primary = verify_result.get('primary_coverage')
    discrepancies = verify_result.get('discrepancies') or []
    review_needed = verify_result.get('discovery_review_needed') or []

    if primary is None:
        if review_needed:
            return 'review_needed'
        if _is_pediatric(verify_input.get('dob')):
            return 'pediatric_no_info'
        return 'no_coverage'

    if primary.get('active') and primary.get('service_type_status') == 'not_covered':
        return 'service_type_denied'

    if discrepancies:
        return 'discrepancy'
    return 'verified'


def _is_pediatric(dob_yyyymmdd):
    try:
        dob = datetime.strptime(dob_yyyymmdd, '%Y%m%d').date()
    except (ValueError, TypeError):
        return False
    today = datetime.now(timezone.utc).date()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age < 18


# ---- DynamoDB row builders ---------------------------------------------

def _build_item_row(org_id, encounter, encounter_id, expires_at,
                    verify_input, patient_hash, status, verify_result):
    """Per-encounter eligibility row. result_summary + payer_demographics
    + rerun_history shapes are the worklist UI contract."""
    primary = verify_result.get('primary_coverage') or {}
    sub = primary.get('subscriber') or {}
    plan = primary.get('plan') or {}
    payer = primary.get('payer') or {}
    member_id = sub.get('member_id') or verify_input.get('member_id') or ''
    member_id_last4 = member_id[-4:] if len(member_id) >= 4 else (member_id or None)

    secondaries = verify_result.get('secondary_coverages') or []
    review_needed = verify_result.get('discovery_review_needed') or []
    encounter_last_updated = (encounter.get('meta') or {}).get('lastUpdated')
    encounter_class = (encounter.get('class') or {}).get('code')

    return {
        'pk': f'ORG#{org_id}',
        'sk': f'ENCOUNTER_ITEM#{encounter_id}',
        'gsi1pk': f'ENCOUNTER_ITEM#{org_id}',
        'gsi1sk': encounter_last_updated or '',
        'encounter_id': encounter_id,
        'encounter_class': encounter_class,
        'encounter_status': encounter.get('status'),
        'encounter_lastUpdated': encounter_last_updated,
        'patient_hash': patient_hash,
        'patient_first_initial': (verify_input.get('first_name') or '?')[:1].upper(),
        'patient_last_initial': (verify_input.get('last_name') or '?')[:1].upper(),
        'submitted_demographics': _demographics_from_input(verify_input),
        'payer_demographics': _extract_payer_demographics(verify_result),
        'corrected_demographics': None,
        'rerun_history': [],
        'result_status': status,
        'result_summary': {
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
            'discrepancies': verify_result.get('discrepancies') or [],
            'secondary_count': len(secondaries),
            'review_needed_count': len(review_needed),
            'cob_check': verify_result.get('cob_check') or {'checked': False},
        },
        'audit_ids': verify_result.get('audit_ids') or [],
        'resolution': dict(_RESOLUTION_DEFAULT),
        'expires_at': expires_at,
    }


def _build_error_row(org_id, encounter, encounter_id, expires_at,
                     *, patient_hash, error_kind, error_message):
    encounter_last_updated = (encounter.get('meta') or {}).get('lastUpdated')
    encounter_class = (encounter.get('class') or {}).get('code')
    return {
        'pk': f'ORG#{org_id}',
        'sk': f'ENCOUNTER_ITEM#{encounter_id}',
        'gsi1pk': f'ENCOUNTER_ITEM#{org_id}',
        'gsi1sk': encounter_last_updated or '',
        'encounter_id': encounter_id,
        'encounter_class': encounter_class,
        'encounter_status': encounter.get('status'),
        'encounter_lastUpdated': encounter_last_updated,
        'patient_hash': patient_hash,
        'submitted_demographics': {},
        'payer_demographics': None,
        'corrected_demographics': None,
        'rerun_history': [],
        'result_status': 'error',
        'result_summary': {
            'error_kind': error_kind,
            'error_message': (error_message or '')[:500],
        },
        'audit_ids': [],
        'resolution': dict(_RESOLUTION_DEFAULT),
        'expires_at': expires_at,
    }


def _put_item(item):
    """Idempotent put — if the same encounter_id row already exists from a
    prior retry, treat the second write as a no-op."""
    try:
        _table.put_item(Item=item, ConditionExpression='attribute_not_exists(sk)')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info("encounter item already exists, skipping: %s", item['sk'])
            return
        raise


def _demographics_from_input(verify_input):
    fields = (
        'first_name', 'middle_name', 'last_name', 'suffix',
        'dob', 'gender', 'ssn_last4',
        'address1', 'address2', 'city', 'state', 'postal_code',
    )
    return {k: verify_input.get(k) for k in fields if verify_input.get(k) is not None}


def _extract_payer_demographics(verify_result):
    """Pull the payer-side subscriber/dependent demographics out of the
    matched discovery hit (HIGH or REVIEW_NEEDED). Returns None when there
    were no discovery hits so the UI hides the diff section."""
    primary = verify_result.get('primary_coverage') or {}
    sub_from_primary = primary.get('subscriber') or {}
    review_needed = verify_result.get('discovery_review_needed') or []

    candidate_subscriber = None
    candidate_dependent = None
    candidate_confidence = None
    candidate_reason = None

    if sub_from_primary.get('member_id'):
        candidate_subscriber = {
            'first_name': sub_from_primary.get('first_name'),
            'last_name': sub_from_primary.get('last_name'),
            'member_id': sub_from_primary.get('member_id'),
            'group_number': sub_from_primary.get('group_number'),
            'dob': sub_from_primary.get('dob'),
        }
        candidate_confidence = 'HIGH'

    if review_needed:
        first = review_needed[0]
        if candidate_subscriber is None:
            candidate_subscriber = first.get('subscriber_demographics') or None
            candidate_confidence = first.get('confidence_level')
        candidate_dependent = candidate_dependent or first.get('dependent_demographics')
        candidate_reason = candidate_reason or first.get('confidence_reason')

    if not candidate_subscriber and not candidate_dependent:
        return None
    return {
        'subscriber': candidate_subscriber,
        'dependent': candidate_dependent,
        'confidence_level': candidate_confidence,
        'confidence_reason': candidate_reason,
    }


# ---- cursor row --------------------------------------------------------

def _read_cursor(org_id, *, default):
    response = _table.get_item(Key={'pk': f'ORG#{org_id}', 'sk': 'FHIR_POLL_CURSOR'})
    item = response.get('Item')
    if item and item.get('last_updated_iso'):
        return item['last_updated_iso']
    return _to_iso(default)


def _write_cursor(org_id, last_updated_iso, now, poll_status, processed):
    _table.put_item(Item={
        'pk': f'ORG#{org_id}',
        'sk': 'FHIR_POLL_CURSOR',
        'last_updated_iso': last_updated_iso,
        'updated_at': _to_iso(now),
        'last_poll_status': poll_status,
        'last_processed': processed,
    })


def _to_iso(value):
    if isinstance(value, str):
        return value
    return value.isoformat().replace('+00:00', 'Z')


# ---- demo history seeding (demo_mode only) -----------------------------

def _ensure_demo_history_seeds(org_id, now):
    """For each row in demo_fixtures.DEMO_HISTORY_SEEDS, write a synthetic
    audit row if no audit row exists for that patient yet. This is what
    makes Linda Sandbox's 'primary changed' discrepancy fire on her first
    encounter. Idempotent — checks GSI1 before writing."""
    from .audit import _SEVEN_YEARS_SECONDS, _table as audit_table
    for seed in demo_fixtures.DEMO_HISTORY_SEEDS:
        p_hash = audit_module.patient_hash(
            seed['first_name'], seed['last_name'], seed['dob'],
        )
        existing = audit_module.recent_checks_for_patient(
            org_id, seed['first_name'], seed['last_name'], seed['dob'],
            limit=1,
        )
        if existing:
            continue

        seeded_at = now - timedelta(days=seed.get('days_ago', 60))
        iso_ts = seeded_at.isoformat()
        request_id = f"seed-{p_hash[:8]}"
        item = {
            'pk': f'ORG#{org_id}',
            'sk': f'AUDIT#{iso_ts}#{request_id}',
            'gsi1pk': f'PATIENT#{org_id}#{p_hash}',
            'gsi1sk': iso_ts,
            'request_id': request_id,
            'user_email': 'system@census-seed',
            'requested_at': iso_ts,
            'call_type': seed.get('call_type', 'eligibility'),
            'patient_hash': p_hash,
            'patient_first_initial': seed['first_name'][:1].upper(),
            'patient_last_initial': seed['last_name'][:1].upper(),
            'patient_dob': seed['dob'],
            'client_ip': '0.0.0.0',
            'result_status': seed.get('result_status', 'active'),
            'payer_name': seed['payer_name'],
            'payer_id': seed.get('payer_id'),
            'expires_at': int(seeded_at.timestamp()) + _SEVEN_YEARS_SECONDS,
        }
        try:
            audit_table.put_item(Item=item, ConditionExpression='attribute_not_exists(pk)')
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise
