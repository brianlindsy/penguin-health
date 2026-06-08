"""Plain-text email templates.

PHI rule: bodies contain only timestamps, aggregate counts, the eligibility
issue category, and the deep link. No patient names, no patient hashes,
no payer details, no discrepancy strings, no member IDs.
"""

from __future__ import annotations

import os


def _admin_ui_base_url() -> str:
    return os.environ.get("ADMIN_UI_BASE_URL", "https://app.penguinhealth.io").rstrip("/")


def render_validation_run_complete(*, org_id: str, validation_run_id: str, summary: dict) -> tuple[str, str]:
    """Subject + body for a finished validation run.

    `summary` is the dict produced by results_handler.aggregate_run_summary —
    only the aggregate counts are emitted into the body.
    """
    total = summary.get("total", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    skipped = summary.get("skipped", 0)

    deep_link = (
        f"{_admin_ui_base_url()}/organizations/{org_id}/validation-runs/{validation_run_id}"
    )

    subject = f"Validation run finished — {failed} failed of {total}"
    body = (
        f"A validation run has finished.\n"
        f"\n"
        f"Run:     {validation_run_id}\n"
        f"Total:   {total}\n"
        f"Passed:  {passed}\n"
        f"Failed:  {failed}\n"
        f"Skipped: {skipped}\n"
        f"\n"
        f"View the run: {deep_link}\n"
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
