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

# Human-readable label for every X12 service type code we have an
# opinion about. Driven by the X12 EB03 code list (truncated to the
# codes Stedi actually returns for BH admits). Anything not listed
# here passes through with the raw code as the label so we never
# silently drop a payer-returned service type.
SERVICE_TYPE_LABELS = {
    "1":   "Medical Care",
    "30":  "Health Benefit Plan Coverage",
    "33":  "Chiropractic",
    "35":  "Dental Care",
    "45":  "Hospital - Inpatient",
    "47":  "Hospital - Outpatient",
    "48":  "Hospital - Emergency",
    "50":  "Hospital - Ambulatory Surgical",
    "51":  "Hospital - Skilled Nursing",
    "52":  "Hospital - Long-Term Care",
    "53":  "Hospital - Hospice",
    "54":  "Long-Term Care",
    "60":  "General Benefits",
    "62":  "MRI/CAT Scan",
    "65":  "Newborn Care",
    "68":  "Well-Baby Care",
    "73":  "Diagnostic Medical",
    "76":  "Dialysis",
    "78":  "Chemotherapy",
    "80":  "Immunizations",
    "81":  "Routine Physical",
    "82":  "Family Planning",
    "86":  "Emergency Services",
    "88":  "Pharmacy",
    "93":  "Podiatry",
    "98":  "Professional (Physician) Visit - Office",
    "A0":  "Professional (Physician) Visit - Inpatient",
    "A3":  "Professional (Physician) Visit - Home",
    "A6":  "Psychotherapy",
    "A7":  "Psychiatric - Inpatient",
    "A8":  "Psychiatric - Outpatient",
    "AD":  "Occupational Therapy",
    "AE":  "Physical Medicine",
    "AF":  "Speech Therapy",
    "AG":  "Skilled Nursing Care",
    "AI":  "Substance Abuse",
    "AL":  "Vision (Optometry)",
    "BG":  "Cardiac Rehabilitation",
    "BH":  "Pediatric",
    "MH":  "Mental Health",
    "UC":  "Urgent Care",
}

# X12 benefit information codes (EB01):
#   1 = Active Coverage
#   6 = Inactive
#   I = Non-Covered
#   V = Cannot Process
ACTIVE_CODES = {"1"}
INACTIVE_CODES = {"6", "I", "V"}

# Human-readable label for EB01 codes that show up most often. Anything
# else passes through with the payer's own name field.
BENEFIT_CODE_LABELS = {
    "1": "Active Coverage",
    "6": "Inactive",
    "I": "Non-Covered",
    "V": "Cannot Process",
    "B": "Co-Payment",
    "C": "Deductible",
    "G": "Out of Pocket (Stop Loss)",
}


def transform(stedi_response, *, requested_payer_id=None):
    """Returns:
      {
        active: bool,
        status: "active" | "inactive" | "no_coverage" | "unknown",
        service_type_status: "covered" | "not_covered" | "unknown",
        service_types: [
          { code, label, status, auth_required, copay, notes }, ...
        ],
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
    service_type_status = _derive_service_type_status(benefits)
    service_types = _build_service_types_breakdown(benefits)
    copays = [b for b in (_extract_copays(benefits)) if b]
    deductibles = _extract_deductibles(benefits)
    oop_max = _extract_oop_max(benefits)
    auth_required = _extract_auth_required(benefits)

    notes = []
    if status == "unknown":
        notes.append("Payer did not return a clear active/inactive determination — verify manually.")
    if status == "active" and service_type_status == "not_covered":
        notes.append("Plan is active overall but does NOT cover inpatient behavioral health.")

    return {
        "active": status == "active",
        "status": status,
        "service_type_status": service_type_status,
        "service_types": service_types,
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


# Service-type codes specific to inpatient behavioral health admission.
# Code 30 ("Health Benefit Plan Coverage") is the overall plan flag and
# is intentionally excluded here — a plan can be active overall (code 30
# == 1) while inpatient/MH service types are explicitly non-covered.
_INPATIENT_BH_ONLY = {"45", "MH", "AI"}


def _derive_service_type_status(benefits):
    """Decide whether the plan covers inpatient behavioral health
    specifically, distinct from whether the plan is active overall.

    Returns "covered" if any benefit targeting 45/MH/AI has an active
    code; "not_covered" if any explicitly inactive code appears for
    those service types and no active code does; "unknown" otherwise
    (no benefit lines mention these service types at all).
    """
    saw_active = False
    saw_inactive = False
    for b in benefits:
        stcs = set(b.get('serviceTypeCodes') or [])
        if not stcs.intersection(_INPATIENT_BH_ONLY):
            continue
        code = b.get('code')
        if code in ACTIVE_CODES:
            saw_active = True
        elif code in INACTIVE_CODES:
            saw_inactive = True
    if saw_active:
        return "covered"
    if saw_inactive:
        return "not_covered"
    return "unknown"


def _build_service_types_breakdown(benefits):
    """Roll the benefit lines up into one entry per service-type code so
    the UI can render exactly what the payer said about each service —
    not just our inpatient-BH summary.

    Output (one entry per distinct serviceTypeCode the payer mentioned):
      [
        {
          code:           "45",
          label:          "Hospital - Inpatient",
          status:         "covered" | "not_covered" | "unknown",
          auth_required:  bool | None,
          copays:         [{ amount, in_or_out_of_network }, ...],
          deductibles:    [{ total, in_or_out_of_network, time_period }, ...],
          notes:          [str, ...],   # payer-supplied free text
        },
        ...
      ]

    Order is the order the codes first appeared in the benefits array —
    payers usually list the overall plan flag (30) first and then drill
    into specific service types, which is the order operators want.
    """
    by_code: dict[str, dict] = {}
    order: list[str] = []

    for b in benefits:
        code = b.get('code')
        amount = b.get('benefitAmount')
        network = b.get('inPlanNetworkIndicatorCode')
        time_period = b.get('timeQualifierCode')
        auth_ind = b.get('authOrCertIndicator')
        # Some payers stash human-readable plan notes here (e.g. "Limited to
        # 30 inpatient days per benefit year"). When present we surface them
        # on the service-type entry so UR doesn't have to read the raw 271.
        message_text = b.get('benefitsAdditionalInformation') or {}

        for stc in (b.get('serviceTypeCodes') or []):
            if stc not in by_code:
                by_code[stc] = {
                    'code': stc,
                    'label': SERVICE_TYPE_LABELS.get(stc, stc),
                    'status': 'unknown',
                    'auth_required': None,
                    'copays': [],
                    'deductibles': [],
                    'notes': [],
                }
                order.append(stc)
            entry = by_code[stc]

            # status: active beats inactive beats unknown. Once a service
            # type has been seen as covered we keep it covered even if a
            # later inactive line for the same code shows up (some payers
            # send both when there's a benefit-limit nuance).
            if code in ACTIVE_CODES:
                entry['status'] = 'covered'
            elif code in INACTIVE_CODES and entry['status'] != 'covered':
                entry['status'] = 'not_covered'

            # auth_required: Y wins over N wins over unset
            if auth_ind == 'Y':
                entry['auth_required'] = True
            elif auth_ind == 'N' and entry['auth_required'] is None:
                entry['auth_required'] = False

            # Copay / deductible lines for THIS service type. Skip the
            # status-only rows (code 1/6/I/V) — those drive `status` above.
            if code == 'B' and amount is not None:
                entry['copays'].append({
                    'amount': amount,
                    'in_or_out_of_network': network,
                })
            elif code == 'C' and amount is not None:
                entry['deductibles'].append({
                    'total': amount,
                    'in_or_out_of_network': network,
                    'time_period': time_period,
                })

            # Payer-supplied notes (rare but useful when present).
            if isinstance(message_text, dict):
                for v in message_text.values():
                    if isinstance(v, str) and v.strip():
                        entry['notes'].append(v.strip())

    return [by_code[c] for c in order]


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
