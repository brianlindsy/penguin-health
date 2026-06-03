"""Verify-patient orchestration.

Pure function — takes injected client + audit module so it's straight-
forward to unit-test without touching DynamoDB or the Stedi network.

Decision tree:
  A. member_id + payer_id present  -> /eligibility directly  (1 transaction)
  B. payer_id only, no member_id  -> /discovery, then /eligibility for the
                                     matching high-confidence hit  (≤2 tx)
  C. neither present              -> /discovery, then /eligibility for each
                                     HIGH hit (cap at 3, parallel)  (≤4 tx)

Each Stedi call reserves a slot on the daily counter BEFORE executing.
Discrepancies (primary-changed, recent-inactivation) are derived from the
audit history and the eligibility response.
"""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

from . import audit as audit_module
from . import cob_transformer
from . import copy_block as copy_block_module
from . import discovery_transformer
from . import eligibility_transformer
from .exceptions import StediBadRequest, StediDailyCapExceeded, StediError


logger = logging.getLogger(__name__)

_MAX_HIGH_HITS = 3
_DEDUP_WINDOW_MIN = 30
_DISCREPANCY_LOOKBACK_DAYS = 30


def verify(input_, *, org_id, org_config, stedi_client, client_ip, user_email,
           audit=audit_module, eligibility_xform=eligibility_transformer,
           discovery_xform=discovery_transformer,
           cob_xform=cob_transformer):
    """Run the verify decision tree. Returns a VerifyResult dict.

    `input_` keys:
        first_name, last_name, dob (YYYYMMDD),
        middle_name?, suffix?, gender?,
        ssn? (full) | ssn_last4?,
        member_id?, payer_id?,
        address1?, address2?, city?, state?, postal_code?
    `org_config` is the STEDI_CONFIG item (must contain provider.npi).
    `audit`/`eligibility_xform`/`discovery_xform`/`cob_xform` are injected
    for tests.
    """
    _require_fields(input_)
    provider = org_config.get('provider') or {}
    npi = provider.get('npi')
    organization_name = provider.get('organization_name')
    if not npi:
        raise StediBadRequest("org STEDI_CONFIG missing provider.npi")
    if not organization_name:
        raise StediBadRequest("org STEDI_CONFIG missing provider.organization_name")
    provider_payload = {'npi': npi, 'organizationName': organization_name}
    daily_cap = int(org_config.get('daily_cap') or 0)
    if daily_cap <= 0:
        raise StediBadRequest("org STEDI_CONFIG missing daily_cap")

    recent = audit.recent_check_summary(
        org_id, input_['first_name'], input_['last_name'], input_['dob'],
        within_minutes=_DEDUP_WINDOW_MIN,
    )

    member_id = (input_.get('member_id') or '').strip() or None
    payer_id = (input_.get('payer_id') or '').strip() or None

    audit_ids = []
    cob_check = {'checked': False}
    if member_id and payer_id:
        path = "direct"
        primary, secondaries, review_needed = _path_direct(
            input_, provider_payload, payer_id, member_id, org_id, daily_cap,
            stedi_client, audit, eligibility_xform, user_email, client_ip, audit_ids,
        )
    elif payer_id:
        path = "discovery_then_eligibility"
        primary, secondaries, review_needed = _path_payer_only(
            input_, provider_payload, payer_id, org_id, daily_cap,
            stedi_client, audit, eligibility_xform, discovery_xform,
            user_email, client_ip, audit_ids,
        )
    else:
        path = "discovery_first"
        primary, secondaries, review_needed, cob_check = _path_discovery_first(
            input_, provider_payload, org_id, org_config, daily_cap,
            stedi_client, audit, eligibility_xform, discovery_xform, cob_xform,
            user_email, client_ip, audit_ids,
        )

    discrepancies = _derive_discrepancies(
        primary, org_id, audit,
        input_['first_name'], input_['last_name'], input_['dob'],
    )

    # If COB disagreed with our primary pick, surface that as a discrepancy
    # so it lands in the UI's discrepancy banner. ('ok' means COB returned
    # a different order; 'no_change' means it confirmed our pick.)
    if cob_check.get('status') == 'ok':
        primary_payer_name = cob_check.get('primary_payer_name') or 'another payer'
        our_pick = ((primary or {}).get('payer') or {}).get('name') or 'the first active payer'
        discrepancies.append(
            f"COB check reordered primary: was {our_pick}; payer-of-record rules put "
            f"{primary_payer_name} primary."
        )

    result = {
        "path": path,
        "primary_coverage": primary,
        "secondary_coverages": secondaries,
        "discovery_review_needed": review_needed,
        "discrepancies": discrepancies,
        "cob_check": cob_check,
        "recent_check": recent,
        "audit_ids": audit_ids,
    }
    result["copy_block"] = copy_block_module.build(result)
    return result


# ---- path implementations -----------------------------------------------

def _path_direct(input_, provider_payload, payer_id, member_id, org_id, daily_cap,
                 client, audit, xform, user_email, client_ip, audit_ids):
    audit.reserve_capacity(org_id, daily_cap)
    request_id = str(uuid.uuid4())
    payload = _build_eligibility_payload(input_, provider_payload, payer_id, member_id)
    started = datetime.now(timezone.utc)
    response = client.check_eligibility(payload)
    duration_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
    primary = xform.transform(response, requested_payer_id=payer_id)
    audit_ids.append(audit.write_audit(
        org_id=org_id, user_email=user_email, call_type='eligibility',
        patient={'first_name': input_['first_name'], 'last_name': input_['last_name'], 'dob': input_['dob']},
        result=primary, client_ip=client_ip,
        stedi_control_number=response.get('controlNumber'),
        duration_ms=duration_ms, member_id=member_id, payer=primary.get('payer'),
        request_id=request_id,
    ))
    return primary, [], []


def _path_payer_only(input_, provider_payload, payer_id, org_id, daily_cap,
                     client, audit, eligibility_xform, discovery_xform,
                     user_email, client_ip, audit_ids):
    discovery_result, _ = _run_discovery(
        input_, provider_payload, org_id, daily_cap, client, audit, discovery_xform,
        user_email, client_ip, audit_ids,
    )
    match = next(
        (h for h in discovery_result['high_confidence']
         if h.get('trading_partner_service_id') == payer_id),
        None,
    )
    if not match or not match.get('member_id'):
        return None, [], discovery_result['high_confidence'] + discovery_result['review_needed']
    primary, _, _ = _path_direct(
        input_, provider_payload, payer_id, match['member_id'], org_id, daily_cap,
        client, audit, eligibility_xform, user_email, client_ip, audit_ids,
    )
    return primary, [], discovery_result['review_needed']


def _path_discovery_first(input_, provider_payload, org_id, org_config, daily_cap,
                          client, audit, eligibility_xform, discovery_xform, cob_xform,
                          user_email, client_ip, audit_ids):
    discovery_result, _ = _run_discovery(
        input_, provider_payload, org_id, daily_cap, client, audit, discovery_xform,
        user_email, client_ip, audit_ids,
    )
    high = discovery_result['high_confidence'][:_MAX_HIGH_HITS]
    if not high:
        return None, [], discovery_result['review_needed'], {'checked': False}

    def _run_one(hit):
        if not hit.get('trading_partner_service_id') or not hit.get('member_id'):
            return None, None, hit
        try:
            audit.reserve_capacity(org_id, daily_cap)
        except StediDailyCapExceeded:
            return None, None, hit
        request_id = str(uuid.uuid4())
        payload = _build_eligibility_payload(
            input_, provider_payload, hit['trading_partner_service_id'], hit['member_id'],
        )
        started = datetime.now(timezone.utc)
        response = client.check_eligibility(payload)
        duration_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
        coverage = eligibility_xform.transform(
            response, requested_payer_id=hit['trading_partner_service_id'],
        )
        rid = audit.write_audit(
            org_id=org_id, user_email=user_email, call_type='eligibility',
            patient={'first_name': input_['first_name'], 'last_name': input_['last_name'], 'dob': input_['dob']},
            result=coverage, client_ip=client_ip,
            stedi_control_number=response.get('controlNumber'),
            duration_ms=duration_ms, member_id=hit['member_id'], payer=coverage.get('payer'),
            request_id=request_id,
        )
        return coverage, rid, None

    coverages = []
    cap_skipped = []
    with ThreadPoolExecutor(max_workers=_MAX_HIGH_HITS) as pool:
        futures = [pool.submit(_run_one, hit) for hit in high]
        for fut in as_completed(futures):
            coverage, rid, skipped = fut.result()
            if rid:
                audit_ids.append(rid)
            if coverage:
                coverages.append(coverage)
            if skipped:
                cap_skipped.append(skipped)

    if not coverages:
        return None, [], discovery_result['review_needed'] + cap_skipped, {'checked': False}

    # Default ordering: first active wins; fall back to first hit.
    active_coverages = [c for c in coverages if c.get('status') == 'active']
    primary = active_coverages[0] if active_coverages else coverages[0]
    secondaries = [c for c in coverages if c is not primary]

    # COB call: only when ≥2 active coverages AND the org has opted in.
    # One Stedi transaction per call, so it's gated by daily_cap too.
    cob_check = {'checked': False}
    if org_config.get('cob_enabled') and len(active_coverages) >= 2:
        cob_check = _run_cob_check(
            input_, provider_payload, active_coverages, org_id, daily_cap,
            client, audit, cob_xform, user_email, client_ip, audit_ids,
        )
        # Re-rank when COB returns a definitive new order.
        if cob_check.get('status') == 'ok' and cob_check.get('primary_payer_id'):
            new_primary = next(
                (c for c in coverages
                 if (c.get('payer') or {}).get('id') == cob_check['primary_payer_id']),
                None,
            )
            if new_primary is not None:
                primary = new_primary
                secondaries = [c for c in coverages if c is not primary]

    return primary, secondaries, discovery_result['review_needed'] + cap_skipped, cob_check


# ---- helpers ------------------------------------------------------------

def _run_cob_check(input_, provider_payload, active_coverages, org_id, daily_cap,
                   client, audit, xform, user_email, client_ip, audit_ids):
    """Call Stedi COB with the active coverages and normalize the response.

    Returns the cob_xform output (a dict with `checked: True`). On daily-
    cap exhaustion or any Stedi error, returns `{'checked': False, ...}`
    so the rest of verify still succeeds — COB is advisory, not blocking.
    """
    try:
        audit.reserve_capacity(org_id, daily_cap)
    except StediDailyCapExceeded:
        logger.warning("cob skipped: daily cap exhausted org=%s", org_id)
        return {'checked': False, 'status': 'skipped_cap'}

    request_id = str(uuid.uuid4())
    payload = _build_cob_payload(input_, provider_payload, active_coverages)
    started = datetime.now(timezone.utc)
    try:
        response = client.check_coordination_of_benefits(payload)
    except StediError as e:
        logger.warning("cob call failed org=%s err=%s", org_id, type(e).__name__)
        return {'checked': False, 'status': 'error'}
    duration_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000

    cob = xform.transform(response, input_coverages=active_coverages)

    audit_ids.append(audit.write_audit(
        org_id=org_id, user_email=user_email, call_type='cob',
        patient={'first_name': input_['first_name'], 'last_name': input_['last_name'], 'dob': input_['dob']},
        # COB has no notion of "active/inactive plan"; record the primacy
        # signal as the result_status so the audit row is queryable.
        result={'status': cob.get('status') or 'unknown', 'plan': {}},
        client_ip=client_ip,
        stedi_control_number=cob.get('cob_id') or response.get('controlNumber'),
        duration_ms=duration_ms,
        request_id=request_id,
    ))
    return cob


def _build_cob_payload(input_, provider_payload, active_coverages):
    """Stedi COB request: provider + subscriber demographics + the list of
    coverages we want primacy-ranked. Each coverage carries the
    tradingPartnerServiceId + member ID we got back from eligibility."""
    subscriber = {
        'firstName': input_['first_name'],
        'lastName': input_['last_name'],
        'dateOfBirth': input_['dob'],
    }
    if input_.get('gender'):
        subscriber['gender'] = input_['gender']

    coverages_payload = []
    for c in active_coverages:
        payer = c.get('payer') or {}
        sub = c.get('subscriber') or {}
        entry = {
            'tradingPartnerServiceId': payer.get('id'),
        }
        if sub.get('member_id'):
            entry['memberId'] = sub['member_id']
        coverages_payload.append(entry)

    return {
        'provider': dict(provider_payload),
        'subscriber': subscriber,
        'coverages': coverages_payload,
    }


def _run_discovery(input_, provider_payload, org_id, daily_cap, client, audit, xform,
                   user_email, client_ip, audit_ids):
    audit.reserve_capacity(org_id, daily_cap)
    request_id = str(uuid.uuid4())
    payload = _build_discovery_payload(input_, provider_payload)
    started = datetime.now(timezone.utc)
    response = client.check_insurance_discovery(payload)
    duration_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
    discovery = xform.transform(response)
    audit_ids.append(audit.write_audit(
        org_id=org_id, user_email=user_email, call_type='discovery',
        patient={'first_name': input_['first_name'], 'last_name': input_['last_name'], 'dob': input_['dob']},
        result={'status': 'discovery', 'plan': {}},
        client_ip=client_ip,
        stedi_control_number=response.get('discoveryId'),
        duration_ms=duration_ms,
        request_id=request_id,
    ))
    return discovery, request_id


def _build_eligibility_payload(input_, provider_payload, payer_id, member_id):
    subscriber = {
        'firstName': input_['first_name'],
        'lastName': input_['last_name'],
        'dateOfBirth': input_['dob'],
        'memberId': member_id,
    }
    if input_.get('middle_name'):
        subscriber['middleName'] = input_['middle_name']
    if input_.get('suffix'):
        subscriber['suffix'] = input_['suffix']
    if input_.get('gender'):
        subscriber['gender'] = input_['gender']
    if input_.get('address1'):
        subscriber['address1'] = input_['address1']
    if input_.get('address2'):
        subscriber['address2'] = input_['address2']
    if input_.get('city'):
        subscriber['city'] = input_['city']
    if input_.get('state'):
        subscriber['state'] = input_['state']
    if input_.get('postal_code'):
        subscriber['postalCode'] = input_['postal_code']
    return {
        'provider': dict(provider_payload),
        'subscriber': subscriber,
        'tradingPartnerServiceId': payer_id,
        'encounter': {
            'dateOfService': date.today().strftime('%Y%m%d'),
            'serviceTypeCodes': ['30', '45', 'MH', 'AI'],
        },
    }


def _build_discovery_payload(input_, provider_payload):
    subscriber = {
        'firstName': input_['first_name'],
        'lastName': input_['last_name'],
        'dateOfBirth': input_['dob'],
    }
    if input_.get('middle_name'):
        subscriber['middleName'] = input_['middle_name']
    if input_.get('suffix'):
        subscriber['suffix'] = input_['suffix']
    if input_.get('gender'):
        subscriber['gender'] = input_['gender']
    # Stedi accepts either the full SSN or just the last 4. Last-4 is
    # what we persist on the roster; pass it through here.
    if input_.get('ssn'):
        subscriber['ssn'] = input_['ssn']
    elif input_.get('ssn_last4'):
        subscriber['ssn'] = input_['ssn_last4']
    address = {}
    if input_.get('address1'):
        address['address1'] = input_['address1']
    if input_.get('address2'):
        address['address2'] = input_['address2']
    if input_.get('city'):
        address['city'] = input_['city']
    if input_.get('state'):
        address['state'] = input_['state']
    if input_.get('postal_code'):
        address['postalCode'] = input_['postal_code']
    if address:
        subscriber['address'] = address
    return {
        'provider': dict(provider_payload),
        'subscriber': subscriber,
        'encounter': {'dateOfService': date.today().strftime('%Y%m%d')},
    }


def _derive_discrepancies(primary, org_id, audit, first_name, last_name, dob):
    out = []
    if not primary:
        return out

    today = date.today()
    since = datetime.now(timezone.utc) - timedelta(days=_DISCREPANCY_LOOKBACK_DAYS)
    history = audit.recent_checks_for_patient(
        org_id, first_name, last_name, dob, limit=20, since=since,
    )

    new_payer = (primary.get('payer') or {}).get('name')
    prior_payers = []
    for row in history:
        prior_name = row.get('payer_name')
        if prior_name and prior_name != new_payer:
            prior_payers.append(prior_name)
    if prior_payers:
        prior_unique = sorted(set(prior_payers))
        out.append(
            f"Primary payer differs from prior check(s): was {', '.join(prior_unique)}; now {new_payer}."
        )

    exp = (primary.get('plan') or {}).get('expiration_date')
    if primary.get('status') == 'inactive' and exp and _ymd_within(exp, today, days=30):
        out.append(f"Plan terminated on {exp} — was active within the last 30 days.")

    return out


def _ymd_within(ymd_str, ref_date, *, days):
    try:
        parsed = datetime.strptime(ymd_str, '%Y%m%d').date()
    except (ValueError, TypeError):
        try:
            parsed = datetime.fromisoformat(ymd_str).date()
        except (ValueError, TypeError):
            return False
    delta = (ref_date - parsed).days
    return 0 <= delta <= days


def _require_fields(input_):
    missing = [k for k in ('first_name', 'last_name', 'dob') if not input_.get(k)]
    if missing:
        raise StediBadRequest(f"missing required input fields: {missing}")
