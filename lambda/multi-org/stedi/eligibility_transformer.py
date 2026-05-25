"""Stedi /eligibility (271) response → simplified internal shape.

The wire response is a faithful structured rendering of an X12 271. The
agent UI needs five things: active/inactive, the member's identity, plan
period, copays/deductibles, and whether prior auth is required for
behavioral-health admission. Everything else stays in raw_x12 for the
"see full response" toggle.
"""

from . import payer_registry


# X12 Service Type Codes the UR team cares about for inpatient BH:
#   30 = Health Benefit Plan Coverage (overall active flag)
#   45 = Hospital - Inpatient
#   MH = Mental Health
#   AI = Substance Abuse
SERVICE_TYPES_INPATIENT_BH = {"30", "45", "MH", "AI"}

# X12 benefit information codes (EB01):
#   1 = Active Coverage
#   6 = Inactive
#   I = Non-Covered
#   V = Cannot Process
ACTIVE_CODES = {"1"}
INACTIVE_CODES = {"6", "I", "V"}


def transform(stedi_response, *, requested_payer_id=None):
    """Returns:
      {
        active: bool,
        status: "active" | "inactive" | "no_coverage" | "unknown",
        payer: { id, name, payer_name_unknown },
        subscriber: { first_name, last_name, member_id, group_number, dob },
        plan: { name, effective_date, expiration_date },
        copays: [{ service_type, amount }],
        deductibles: [{ in_or_out_of_network, total, remaining }],
        oop_max: [{ in_or_out_of_network, total, remaining }],
        auth_required: bool | None,
        notes: [str, ...],
        raw: <pass-through>,
      }
    """
    payer = _extract_payer(stedi_response, requested_payer_id)
    subscriber = _extract_subscriber(stedi_response)
    plan = _extract_plan(stedi_response)
    benefits = stedi_response.get('benefitsInformation') or []

    status = _derive_status(benefits)
    copays = [b for b in (_extract_copays(benefits)) if b]
    deductibles = _extract_deductibles(benefits)
    oop_max = _extract_oop_max(benefits)
    auth_required = _extract_auth_required(benefits)

    notes = []
    if status == "unknown":
        notes.append("Payer did not return a clear active/inactive determination — verify manually.")

    return {
        "active": status == "active",
        "status": status,
        "payer": payer,
        "subscriber": subscriber,
        "plan": plan,
        "copays": copays,
        "deductibles": deductibles,
        "oop_max": oop_max,
        "auth_required": auth_required,
        "notes": notes,
        "raw": stedi_response,
    }


def _extract_payer(response, requested_payer_id):
    payer_block = response.get('payer') or {}
    trading_partner_id = (
        response.get('tradingPartnerServiceId')
        or payer_block.get('tradingPartnerServiceId')
        or requested_payer_id
    )
    if trading_partner_id:
        return payer_registry.lookup_by_id(trading_partner_id)
    name = payer_block.get('name') or payer_block.get('organizationName')
    if name:
        return {"id": None, "name": name, "payer_name_unknown": True}
    return {"id": None, "name": "Unknown", "payer_name_unknown": True}


def _extract_subscriber(response):
    sub = response.get('subscriber') or {}
    return {
        "first_name": sub.get('firstName'),
        "last_name": sub.get('lastName'),
        "member_id": sub.get('memberId'),
        "group_number": sub.get('groupNumber'),
        "dob": sub.get('dateOfBirth'),
    }


def _extract_plan(response):
    plan_info = response.get('planInformation') or {}
    dates = response.get('planDateInformation') or {}
    eligibility_dates = response.get('eligibilityDates') or {}
    return {
        "name": plan_info.get('planName') or plan_info.get('groupName'),
        "effective_date": (
            dates.get('planBegin')
            or dates.get('eligibilityBegin')
            or eligibility_dates.get('begin')
        ),
        "expiration_date": (
            dates.get('planEnd')
            or dates.get('eligibilityEnd')
            or eligibility_dates.get('end')
        ),
    }


def _derive_status(benefits):
    has_active = any(b.get('code') in ACTIVE_CODES for b in benefits)
    has_inactive = any(b.get('code') in INACTIVE_CODES for b in benefits)
    if has_active:
        return "active"
    if has_inactive:
        return "inactive"
    if not benefits:
        return "no_coverage"
    return "unknown"


def _extract_copays(benefits):
    out = []
    for b in benefits:
        if b.get('name') != 'Co-Payment' and b.get('code') != 'B':
            continue
        amount = b.get('benefitAmount')
        for stc in (b.get('serviceTypeCodes') or [None]):
            if stc is None or stc in SERVICE_TYPES_INPATIENT_BH:
                out.append({"service_type": stc, "amount": amount})
    return out


def _extract_deductibles(benefits):
    out = []
    for b in benefits:
        if b.get('code') != 'C' and b.get('name') != 'Deductible':
            continue
        out.append({
            "in_or_out_of_network": b.get('inPlanNetworkIndicatorCode'),
            "total": b.get('benefitAmount'),
            "time_period": b.get('timeQualifierCode'),
        })
    return out


def _extract_oop_max(benefits):
    out = []
    for b in benefits:
        if b.get('code') != 'G' and b.get('name') != 'Out of Pocket (Stop Loss)':
            continue
        out.append({
            "in_or_out_of_network": b.get('inPlanNetworkIndicatorCode'),
            "total": b.get('benefitAmount'),
        })
    return out


def _extract_auth_required(benefits):
    """Return True/False/None. True if any inpatient-BH benefit has
    authOrCertIndicator=Y; False if explicitly N; None if not stated."""
    saw_y = False
    saw_n = False
    for b in benefits:
        ind = b.get('authOrCertIndicator')
        stcs = set(b.get('serviceTypeCodes') or [])
        if not stcs.intersection(SERVICE_TYPES_INPATIENT_BH) and stcs:
            continue
        if ind == 'Y':
            saw_y = True
        elif ind == 'N':
            saw_n = True
    if saw_y:
        return True
    if saw_n:
        return False
    return None
