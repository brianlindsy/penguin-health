"""Build the plaintext block UR pastes into Credible's insurance section.

Format chosen to match what Sheryl/Lynette/Dawn/Cici copy by hand today.
Confirm the exact field order with Barb during UAT — easy to tweak.
"""


def build(verify_result):
    """Given the merged VerifyResult dict, return a plaintext block."""
    lines = []
    primary = verify_result.get('primary_coverage')
    if primary:
        lines.append("=== PRIMARY INSURANCE ===")
        lines.extend(_format_coverage(primary))

    secondaries = verify_result.get('secondary_coverages') or []
    for i, cov in enumerate(secondaries, start=2):
        lines.append("")
        lines.append(f"=== INSURANCE #{i} ===")
        lines.extend(_format_coverage(cov))

    review_needed = verify_result.get('discovery_review_needed') or []
    if review_needed:
        lines.append("")
        lines.append("=== POSSIBLE COVERAGE (NEEDS MANUAL VERIFY) ===")
        for item in review_needed:
            lines.append(f"- {item['payer']['name']} | Member ID: {item.get('member_id') or '?'} | reason: {item.get('confidence_reason') or ''}")

    discrepancies = verify_result.get('discrepancies') or []
    if discrepancies:
        lines.append("")
        lines.append("=== DISCREPANCIES ===")
        for d in discrepancies:
            lines.append(f"! {d}")

    return "\n".join(lines)


def _format_coverage(cov):
    sub = cov.get('subscriber') or {}
    plan = cov.get('plan') or {}
    payer = cov.get('payer') or {}
    out = [
        f"Status: {cov.get('status', '?').upper()}",
        f"Payer: {payer.get('name') or '?'}",
        f"Plan: {plan.get('name') or '?'}",
        f"Member ID: {sub.get('member_id') or '?'}",
        f"Group: {sub.get('group_number') or '?'}",
        f"Effective: {plan.get('effective_date') or '?'} – {plan.get('expiration_date') or 'open'}",
    ]
    auth = cov.get('auth_required')
    if auth is True:
        out.append("Auth required: YES")
    elif auth is False:
        out.append("Auth required: No")
    copays = cov.get('copays') or []
    if copays:
        copay_str = ", ".join(f"{c.get('service_type') or 'any'}=${c.get('amount')}" for c in copays)
        out.append(f"Copays: {copay_str}")
    deductibles = cov.get('deductibles') or []
    if deductibles:
        ded_str = ", ".join(f"{d.get('in_or_out_of_network') or '?'}=${d.get('total')}" for d in deductibles)
        out.append(f"Deductible: {ded_str}")
    return out
