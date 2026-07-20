"""Plain-text email templates.

PHI rule: bodies contain only timestamps, aggregate counts, the eligibility
issue category, and the deep link. No patient names, no patient hashes,
no payer details, no discrepancy strings, no member IDs.
"""

from __future__ import annotations

import os


def _admin_ui_base_url() -> str:
    return os.environ.get("ADMIN_UI_BASE_URL", "https://app.penguinhealth.io").rstrip("/")


def render_validation_run_complete(
    *,
    org_id: str,
    validation_run_id: str,
    summary: dict,
    queue_counters: dict | None = None,
) -> tuple[str, str]:
    """Subject + body for a finished validation run.

    The reviewer-facing story after the document-queue cutover is:
      * How many NEW documents landed in the queue for review tonight?
      * How many resends were byte-identical and correctly skipped?
      * How many prior docs got new versions (content changed)?

    `queue_counters` (from the rules-engine per-run counters) supplies
    the queue delta. Aggregate only — no document ids, no field values,
    no rule messages. `summary` is accepted for the subject fallback
    when queue_counters is absent; the body no longer surfaces rule
    aggregates because reviewers work off the queue, not the run.

    The deep link lands on the queue's default view for this org.
    Reviewers can filter to tonight's run themselves inside the app if
    needed; the email itself carries no run id.
    """
    counters = queue_counters or {}
    new_docs = int(counters.get("new_documents", 0) or 0)
    new_versions = int(counters.get("new_versions", 0) or 0)
    duplicates = int(counters.get("duplicate_skips", 0) or 0)
    needs_review = new_docs + new_versions

    deep_link = f"{_admin_ui_base_url()}/organizations/{org_id}/document-queue"

    if queue_counters is None:
        # Old shape — kept for callers not yet threaded through with the
        # queue counters. Behaves identically to the pre-cutover email.
        failed = summary.get("failed", 0)
        total = summary.get("total", 0)
        subject = f"Validation run finished — {failed} failed of {total}"
        body = (
            "A validation run has finished.\n"
            "\n"
            f"Review documents: {deep_link}\n"
        )
        return subject, body

    subject = (
        f"Document queue update — {needs_review} for review "
        f"({duplicates} unchanged skipped)"
    )
    body = (
        "A validation run has finished.\n"
        "\n"
        "Queue changes:\n"
        f"  New documents:     {new_docs}\n"
        f"  Updated documents: {new_versions}\n"
        f"  Unchanged skipped: {duplicates}\n"
        "\n"
        f"Review new documents: {deep_link}\n"
    )
    return subject, body


_ELIGIBILITY_STATUS_LABEL = {
    "discrepancy": "Eligibility discrepancy",
    "no_coverage": "No coverage found",
    "review_needed": "Coverage match needs review",
    "service_type_denied": "Service type denied",
    "pediatric_no_info": "Pediatric — no info returned",
    "error": "Verification error",
}


def render_eligibility_issue(
    *,
    org_id: str,
    encounter_id: str,
    encounter_datetime: str | None,
    result_status: str,
) -> tuple[str, str]:
    """Subject + body for a single problem encounter.

    Only the encounter date/time and the issue category appear in the body.
    No patient identifiers, no payer name, no discrepancy detail strings.
    Recipients click the link and view the rest in the authenticated UI.
    """
    label = _ELIGIBILITY_STATUS_LABEL.get(result_status, "Eligibility issue")
    deep_link = (
        f"{_admin_ui_base_url()}/organizations/{org_id}/eligibility/worklist"
        f"?encounter={encounter_id}"
    )

    when = encounter_datetime or "Unknown"

    subject = f"{label} — encounter {encounter_datetime or 'time unknown'}"
    body = (
        f"A new eligibility issue was detected.\n"
        f"\n"
        f"Encounter time: {when}\n"
        f"Issue:          {label}\n"
        f"\n"
        f"Review in the worklist: {deep_link}\n"
    )
    return subject, body
