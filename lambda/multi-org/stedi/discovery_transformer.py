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
    payer_block = item.get('payer') or {}
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
        "subscriber_first_name": sub.get('firstName'),
        "subscriber_last_name": sub.get('lastName'),
    }
