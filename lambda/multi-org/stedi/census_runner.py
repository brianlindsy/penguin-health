"""Scheduled morning-census Lambda.

Invoked by EventBridge once per opted-in org per morning. Pulls the
roster of new admissions, runs the existing orchestrator.verify() against
each one, and writes per-run + per-patient rows to penguin-health-stedi
so UR can see the worklist when they log in.

The roster source is parameterized so the same runner works when we
later swap demo_roster for FHIR or SFTP — only `_load_roster` changes.
"""

import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from . import audit as audit_module
from . import client_factory
from . import config as stedi_config_module
from . import demo_fixtures
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
_PEDIATRIC_AGE_THRESHOLD = 18

_RESOLUTION_DEFAULT = {
    'state': 'unresolved',
    'action': None,
    'note': None,
    'resolved_by': None,
    'resolved_at': None,
    'rerun_audit_id': None,
}


def handler(event, context):
    """EventBridge entry point. Expected payload: {"organization_id": "..."}.

    Returns a small summary dict (so manual invokes via `aws lambda invoke`
    show something useful in the response).
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise StediBadRequest("event payload must include organization_id")

    try:
        org_config = stedi_config_module.load_stedi_config(org_id)
    except StediOrgNotConfigured as e:
        logger.warning("census skipped: %s", e)
        return {'status': 'skipped', 'reason': str(e), 'organization_id': org_id}

    if not org_config.get('census_enabled'):
        logger.info("census not enabled for org=%s; skipping", org_id)
        return {'status': 'skipped', 'reason': 'census_enabled is false', 'organization_id': org_id}

    return run_census(org_id, org_config)


def run_census(org_id, org_config, *, now=None):
    """Run a single census sweep for an org. Pure-ish (takes `now` as
    injection so tests can pin the timestamp)."""
    now = now or datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    run_date = now.date().isoformat()
    expires_at = int(now.timestamp()) + _NINETY_DAYS_SECONDS

    source = org_config.get('census_roster_source', 'demo_roster')

    # Seed demo history before the run so the primary-changed discrepancy
    # fires on Linda Sandbox's row. No-op outside demo_mode.
    if org_config.get('demo_mode'):
        _ensure_demo_history_seeds(org_id, now)

    roster = _load_roster(org_id, source)

    # Write the initial CENSUS_RUN# row (status=running). We update it at
    # the end with the final totals.
    _put_run_row(org_id, run_id, run_date, expires_at, now, source,
                 total=len(roster), status='running')

    client = client_factory.build_client(org_config, client_ip=None)
    counts = {'verified': 0, 'discrepancy': 0, 'no_coverage': 0,
              'review_needed': 0, 'pediatric_no_info': 0,
              'service_type_denied': 0, 'error': 0}

    for patient in roster:
        item_status = _run_one(
            org_id, run_id, run_date, expires_at,
            patient, org_config, client,
        )
        counts[item_status] = counts.get(item_status, 0) + 1

    # Patch the run row with final totals.
    _put_run_row(org_id, run_id, run_date, expires_at, now, source,
                 total=len(roster), status='complete',
                 completed_at=datetime.now(timezone.utc).isoformat(),
                 counts=counts)

    logger.info("census run complete org=%s run_id=%s total=%d counts=%s",
                org_id, run_id, len(roster), counts)
    return {
        'status': 'complete',
        'organization_id': org_id,
        'run_id': run_id,
        'run_date': run_date,
        'total': len(roster),
        **counts,
    }


# ---- roster loading -----------------------------------------------------

def _load_roster(org_id, source):
    if source == 'demo_roster':
        return list(demo_fixtures.CENSUS_ROSTER)
    if source == 'sftp':
        raise NotImplementedError(
            "SFTP census ingestion is not yet implemented. "
            "Set census_roster_source=demo_roster to use the demo fixtures."
        )
    if source == 'fhir':
        raise NotImplementedError(
            "FHIR census ingestion is not yet implemented. "
            "Set census_roster_source=demo_roster to use the demo fixtures."
        )
    raise StediBadRequest(f"unknown census_roster_source: {source!r}")


# ---- per-patient run ----------------------------------------------------

def _run_one(org_id, run_id, run_date, expires_at, patient, org_config, client):
    """Verify one patient and write the CENSUS_ITEM# row. Returns the
    classified result_status string (used to roll up counts)."""
    patient_hash = audit_module.patient_hash(
        patient['first_name'], patient['last_name'], patient['dob'],
    )
    try:
        verify_input = _build_verify_input(patient)
        result = orchestrator.verify(
            verify_input,
            org_id=org_id,
            org_config=org_config,
            stedi_client=client,
            client_ip=None,
            user_email='system@census',
        )
        status = _classify(result, patient)
        item = _build_item_row(
            org_id, run_id, run_date, expires_at,
            patient, patient_hash, status, result,
        )
    except StediDailyCapExceeded as e:
        logger.warning("census patient skipped (cap): org=%s patient=%s", org_id, patient_hash)
        item = _build_error_row(org_id, run_id, run_date, expires_at,
                                patient, patient_hash, 'daily_cap_exceeded', str(e))
        status = 'error'
    except (StediError, Exception) as e:  # noqa: BLE001 — log + persist, don't crash the run
        logger.exception("census patient failed: org=%s patient=%s", org_id, patient_hash)
        item = _build_error_row(org_id, run_id, run_date, expires_at,
                                patient, patient_hash, type(e).__name__, str(e))
        status = 'error'

    try:
        _table.put_item(Item=item, ConditionExpression='attribute_not_exists(sk)')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            # Idempotency: another invocation already wrote this row for the
            # same run+patient. Treat as success.
            logger.info("census item already exists, skipping: %s", item['sk'])
        else:
            raise
    return status


def _classify(verify_result, patient):
    """Map a VerifyResult dict to a single census-row status string."""
    primary = verify_result.get('primary_coverage')
    discrepancies = verify_result.get('discrepancies') or []
    review_needed = verify_result.get('discovery_review_needed') or []

    if primary is None:
        # No HIGH-confidence coverage. Could be REVIEW_NEEDED (manual
        # portal verify) or pure no-coverage (Stedi found nothing).
        if review_needed:
            return 'review_needed'
        # No-coverage: tag pediatric cases separately so Dawn knows to
        # call a parent before Admissions interviews the patient.
        if _is_pediatric(patient.get('dob')):
            return 'pediatric_no_info'
        return 'no_coverage'

    # We have a primary. Check whether the plan is active overall but
    # explicitly does NOT cover inpatient BH (James Example's case).
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
    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age < _PEDIATRIC_AGE_THRESHOLD


# ---- DynamoDB row builders ----------------------------------------------

def _put_run_row(org_id, run_id, run_date, expires_at, started_at, source, *,
                 total, status, completed_at=None, counts=None):
    counts = counts or {}
    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'CENSUS_RUN#{run_date}#{run_id}',
        'gsi1pk': f'CENSUS_RUN#{org_id}',
        'gsi1sk': f'{run_date}#{run_id}',
        'run_id': run_id,
        'org_id': org_id,
        'run_date': run_date,
        'started_at': started_at.isoformat() if hasattr(started_at, 'isoformat') else started_at,
        'completed_at': completed_at,
        'status': status,
        'source': source,
        'total': total,
        'verified': counts.get('verified', 0),
        'discrepancy': counts.get('discrepancy', 0),
        'no_coverage': counts.get('no_coverage', 0),
        'review_needed': counts.get('review_needed', 0),
        'pediatric_no_info': counts.get('pediatric_no_info', 0),
        'service_type_denied': counts.get('service_type_denied', 0),
        'error': counts.get('error', 0),
        'expires_at': expires_at,
    }
    _table.put_item(Item=item)


def _build_item_row(org_id, run_id, run_date, expires_at, patient, patient_hash,
                    status, verify_result):
    primary = verify_result.get('primary_coverage') or {}
    sub = primary.get('subscriber') or {}
    plan = primary.get('plan') or {}
    payer = primary.get('payer') or {}
    member_id = sub.get('member_id') or patient.get('member_id') or ''
    member_id_last4 = member_id[-4:] if len(member_id) >= 4 else (member_id or None)

    secondaries = verify_result.get('secondary_coverages') or []
    review_needed = verify_result.get('discovery_review_needed') or []

    item = {
        'pk': f'ORG#{org_id}',
        'sk': f'CENSUS_ITEM#{run_date}#{run_id}#{patient_hash}',
        'run_id': run_id,
        'run_date': run_date,
        'patient_hash': patient_hash,
        'patient_first_initial': (patient.get('first_name') or '?')[:1].upper(),
        'patient_last_initial': (patient.get('last_name') or '?')[:1].upper(),
        # Full names are stored here only because this is a demo
        # environment with synthetic patients. For real PHI, store
        # initials + DOB only and join against another system for display.
        'patient_first_name': patient.get('first_name'),
        'patient_last_name': patient.get('last_name'),
        'patient_dob': patient.get('dob'),
        # What intake originally captured — preserved across reruns so a
        # manager can see the source data, not just the corrected version.
        'submitted_demographics': _demographics_from_patient(patient),
        # What the matched discovery hit (HIGH or REVIEW_NEEDED) said the
        # payer has on file. Drives the side-by-side "you sent X, payer
        # has Y" diff on the UI.
        'payer_demographics': _extract_payer_demographics(verify_result),
        # Filled in on rerun (see census_api.rerun_census_item). Null on
        # first run; once UR reruns with corrections this carries the
        # corrected demographic set + who/when.
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
        },
        'audit_ids': verify_result.get('audit_ids') or [],
        'resolution': dict(_RESOLUTION_DEFAULT),
        'expires_at': expires_at,
    }
    return item


def _build_error_row(org_id, run_id, run_date, expires_at, patient, patient_hash,
                     error_kind, error_message):
    return {
        'pk': f'ORG#{org_id}',
        'sk': f'CENSUS_ITEM#{run_date}#{run_id}#{patient_hash}',
        'run_id': run_id,
        'run_date': run_date,
        'patient_hash': patient_hash,
        'patient_first_initial': (patient.get('first_name') or '?')[:1].upper(),
        'patient_last_initial': (patient.get('last_name') or '?')[:1].upper(),
        'patient_first_name': patient.get('first_name'),
        'patient_last_name': patient.get('last_name'),
        'patient_dob': patient.get('dob'),
        'submitted_demographics': _demographics_from_patient(patient),
        'payer_demographics': None,
        'corrected_demographics': None,
        'rerun_history': [],
        'result_status': 'error',
        'result_summary': {
            'error_kind': error_kind,
            'error_message': error_message[:500],
        },
        'audit_ids': [],
        'resolution': dict(_RESOLUTION_DEFAULT),
        'expires_at': expires_at,
    }


# ---- demographic helpers -------------------------------------------------

def _build_verify_input(patient):
    """Map a roster entry (or a corrected-demographics dict) into the input
    shape orchestrator.verify expects."""
    return {
        'first_name': patient.get('first_name'),
        'last_name': patient.get('last_name'),
        'middle_name': patient.get('middle_name'),
        'suffix': patient.get('suffix'),
        'dob': patient.get('dob'),
        'gender': patient.get('gender'),
        'ssn': patient.get('ssn'),
        'ssn_last4': patient.get('ssn_last4'),
        'address1': patient.get('address1'),
        'address2': patient.get('address2'),
        'city': patient.get('city'),
        'state': patient.get('state'),
        'postal_code': patient.get('postal_code'),
        'member_id': patient.get('member_id'),
        'payer_id': patient.get('payer_id'),
    }


def _demographics_from_patient(patient):
    """Pull just the demographic fields off a roster entry. Used to seed
    `submitted_demographics` on the census item. Strips Nones so the
    DDB row doesn't store empty keys."""
    fields = (
        'first_name', 'middle_name', 'last_name', 'suffix',
        'dob', 'gender', 'ssn_last4',
        'address1', 'address2', 'city', 'state', 'postal_code',
    )
    return {k: patient.get(k) for k in fields if patient.get(k) is not None}


def _extract_payer_demographics(verify_result):
    """Pull the payer-side subscriber/dependent demographics out of the
    matched discovery hit (HIGH or REVIEW_NEEDED). Returns None when there
    were no discovery hits (direct-path patient) so the UI knows to hide
    the diff section."""
    # Prefer primary coverage (HIGH that produced an eligibility result),
    # fall back to first REVIEW_NEEDED hit.
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
        # If there's no HIGH match, the REVIEW item is the only payer
        # data we got — surface it as the candidate.
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


# ---- demo history seeding -----------------------------------------------

def _ensure_demo_history_seeds(org_id, now):
    """For each row in demo_fixtures.DEMO_HISTORY_SEEDS, write a synthetic
    audit row if no audit row exists for that patient yet. This is what
    makes Linda Sandbox's 'primary changed' discrepancy fire on the first
    census run."""
    for seed in demo_fixtures.DEMO_HISTORY_SEEDS:
        p_hash = audit_module.patient_hash(
            seed['first_name'], seed['last_name'], seed['dob'],
        )
        # Cheap dedup: query GSI1 for this patient. If anything is there
        # already, we've seeded before.
        existing = audit_module.recent_checks_for_patient(
            org_id, seed['first_name'], seed['last_name'], seed['dob'],
            limit=1,
        )
        if existing:
            continue

        seeded_at = now - timedelta(days=seed.get('days_ago', 60))
        # Write directly to the audit table since audit_module.write_audit()
        # uses datetime.now() — we want to backdate. Mirror the exact item
        # shape audit_module.write_audit produces.
        from .audit import _table as audit_table, _SEVEN_YEARS_SECONDS
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
