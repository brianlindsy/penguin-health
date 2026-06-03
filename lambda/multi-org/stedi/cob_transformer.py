"""Stedi /coordination-of-benefits response → normalized primacy ranking.

We call COB only when discovery + parallel eligibility produced ≥2 active
coverages. The response answers "which payer is actually primary?" per
the rules COB tracks (Medicare Secondary Payer, birthday rule, custodial
parent, etc.) — replacing our naive "first active wins" guess.

Output shape (consumed by orchestrator + worklist UI):

    {
        'checked': True,                            # always True when this is called
        'status': 'ok' | 'no_change' | 'no_signal', # see below
        'primary_payer_id':   '68068',              # trading_partner_service_id
        'primary_payer_name': 'Sunshine Health',
        'rankings': [                               # ordered, primary first
            {'rank': 'primary',   'payer_id': '...', 'payer_name': '...'},
            {'rank': 'secondary', 'payer_id': '...', 'payer_name': '...'},
        ],
        'reason': 'Medicaid is secondary by default when commercial coverage exists.',
        'cob_id': 'cob-...',                        # for the audit trail
    }

Status values:
    ok         — COB returned a definitive ranking that differs from input order
    no_change  — COB confirmed the input order (no reorder needed)
    no_signal  — COB couldn't determine primacy (returned empty or low confidence)
"""


def transform(response, *, input_coverages):
    """Normalize a Stedi COB response into the dict shape above.

    `input_coverages` is the list of coverages we sent in (orchestrator's
    active coverages, in their original order). We use it to compute the
    `no_change` status — if COB's ranking matches the input order, the
    UI doesn't need to flag a discrepancy.
    """
    response = response or {}
    cob_id = response.get('cobId') or response.get('controlNumber')

    rankings = _extract_rankings(response)
    if not rankings:
        return {
            'checked': True,
            'status': 'no_signal',
            'primary_payer_id': None,
            'primary_payer_name': None,
            'rankings': [],
            'reason': _extract_reason(response) or 'COB returned no primacy signal.',
            'cob_id': cob_id,
        }

    primary = next((r for r in rankings if r['rank'] == 'primary'), rankings[0])
    input_order = [c.get('payer', {}).get('id') for c in input_coverages]
    cob_order = [r['payer_id'] for r in rankings]

    matches_input = (
        # Trim to the same length — COB may rank fewer than we sent if a
        # payer wasn't recognized; if its prefix matches our prefix we
        # treat that as agreement.
        cob_order == input_order[:len(cob_order)]
    )
    status = 'no_change' if matches_input else 'ok'

    return {
        'checked': True,
        'status': status,
        'primary_payer_id': primary.get('payer_id'),
        'primary_payer_name': primary.get('payer_name'),
        'rankings': rankings,
        'reason': _extract_reason(response),
        'cob_id': cob_id,
    }


def _extract_rankings(response):
    """Pull the ordered (rank, payer) tuples out of the Stedi shape.

    Stedi's documented response carries a `result` object whose
    `primaryCoverage`/`secondaryCoverage`/`tertiaryCoverage` sub-objects
    contain `tradingPartnerServiceId` + payer name. Newer responses may
    use a `coverages: [{rank: 'primary', ...}, ...]` array; we accept
    either shape so we don't break when Stedi adjusts the schema.
    """
    rankings = []
    result = response.get('result') or {}

    # Array shape.
    coverages = result.get('coverages') or response.get('coverages')
    if isinstance(coverages, list):
        for entry in coverages:
            rank = (entry.get('rank') or entry.get('priority') or '').lower()
            payer = entry.get('payer') or {}
            payer_id = (
                entry.get('tradingPartnerServiceId')
                or payer.get('payorIdentification')
                or payer.get('id')
            )
            if not (rank and payer_id):
                continue
            rankings.append({
                'rank': rank,
                'payer_id': payer_id,
                'payer_name': payer.get('name') or entry.get('payerName'),
            })

    # Sub-object shape (one block per rank).
    if not rankings:
        for rank in ('primary', 'secondary', 'tertiary'):
            block = result.get(f'{rank}Coverage')
            if not isinstance(block, dict):
                continue
            payer = block.get('payer') or {}
            payer_id = (
                block.get('tradingPartnerServiceId')
                or payer.get('payorIdentification')
                or payer.get('id')
            )
            if not payer_id:
                continue
            rankings.append({
                'rank': rank,
                'payer_id': payer_id,
                'payer_name': payer.get('name'),
            })

    return rankings


def _extract_reason(response):
    """Best-effort pull of the human-readable explanation Stedi sometimes
    attaches (e.g. 'Patient has Medicare; commercial is secondary')."""
    result = response.get('result') or {}
    for key in ('reason', 'explanation', 'rationale'):
        val = result.get(key) or response.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None
