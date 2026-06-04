"""Stedi /insurance-discovery response → ranked list of candidate coverages.

Discovery returns 0..N items, each tagged with a confidence level (HIGH
or REVIEW_NEEDED). The orchestrator uses HIGH items as the input list for
follow-up /eligibility calls and surfaces REVIEW_NEEDED items to the user
for manual portal verification.
"""

from . import payer_registry


def transform(stedi_response):
    """Returns:
      {
        coverages_found: int,
        discovery_id: str,
        high_confidence: [DiscoveryItem],
        review_needed: [DiscoveryItem],
        errors: [<pass-through>],
      }
    """
    items = stedi_response.get('items') or []
    high = []
    review = []
    for item in items:
        normalized = _normalize_item(item)
        level = (item.get('confidence') or {}).get('level', '').upper()
        if level == 'HIGH':
            high.append(normalized)
        else:
            review.append(normalized)
    return {
        "coverages_found": stedi_response.get('coveragesFound', len(items)),
        "discovery_id": stedi_response.get('discoveryId'),
        "high_confidence": high,
        "review_needed": review,
        "errors": stedi_response.get('errors') or [],
    }


def _normalize_item(item):
    confidence = item.get('confidence') or {}
    sub = item.get('subscriber') or {}
    dep = item.get('dependent') or {}
    payer_block = item.get('payer') or {}
    dates = item.get('planDateInformation') or {}
    trading_partner_id = (
        item.get('tradingPartnerServiceId')
        or payer_block.get('tradingPartnerServiceId')
    )
    payer = (
        payer_registry.lookup_by_id(trading_partner_id)
        if trading_partner_id
        else {"id": None, "name": payer_block.get('name') or 'Unknown', "payer_name_unknown": True}
    )
    return {
        "confidence_level": confidence.get('level'),
        "confidence_reason": confidence.get('reason'),
        "payer": payer,
        "trading_partner_service_id": trading_partner_id,
        "member_id": sub.get('memberId'),
        "group_number": sub.get('groupNumber'),
        # Discovery's premium-paid-through equivalent. Some payers
        # (notably Ambetter/Centene) surface it here as well as on the
        # downstream 271. Carried forward so review-needed rows that
        # skip the eligibility call still expose the signal.
        "premium_paid_through": dates.get('premiumPaidUpTo'),
        "subscriber_first_name": sub.get('firstName'),
        "subscriber_last_name": sub.get('lastName'),
        # Payer-side subscriber demographics — full picture so UR can see
        # exactly what the payer has on file vs. what intake captured.
        # This is the diff that explains REVIEW_NEEDED hits.
        "subscriber_demographics": _strip_none({
            "first_name": sub.get('firstName'),
            "middle_name": sub.get('middleName'),
            "last_name": sub.get('lastName'),
            "suffix": sub.get('suffix'),
            "dob": sub.get('dateOfBirth'),
            "gender": sub.get('gender'),
            "address1": (sub.get('address') or {}).get('address1'),
            "address2": (sub.get('address') or {}).get('address2'),
            "city": (sub.get('address') or {}).get('city'),
            "state": (sub.get('address') or {}).get('state'),
            "postal_code": (sub.get('address') or {}).get('postalCode'),
        }),
        # If discovery returned a dependent block, this is the patient
        # we're admitting and the subscriber is the policyholder (e.g.
        # parent's plan). UR needs both demographics side-by-side.
        "dependent_demographics": _strip_none({
            "first_name": dep.get('firstName'),
            "middle_name": dep.get('middleName'),
            "last_name": dep.get('lastName'),
            "suffix": dep.get('suffix'),
            "dob": dep.get('dateOfBirth'),
            "gender": dep.get('gender'),
            "relation_to_subscriber": dep.get('relationToSubscriber'),
            "address1": (dep.get('address') or {}).get('address1'),
            "address2": (dep.get('address') or {}).get('address2'),
            "city": (dep.get('address') or {}).get('city'),
            "state": (dep.get('address') or {}).get('state'),
            "postal_code": (dep.get('address') or {}).get('postalCode'),
        }) or None,
    }


def _strip_none(d):
    """Drop None / empty-string entries so the DDB write doesn't
    persist a wall of nulls. Returns None instead of {} so callers
    can use `or None` to short-circuit."""
    cleaned = {k: v for k, v in d.items() if v not in (None, '', [])}
    return cleaned if cleaned else None
