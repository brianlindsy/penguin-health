"""Render-time PHI redaction tests.

The contract with the user is: eligibility emails contain only encounter
date/time, the issue category, and the deep link. No patient identifiers,
no payer details, no discrepancy strings, no member IDs. These tests are
the canary — any future template tweak that pulls PHI into the body
must fail here.
"""

import pytest

from notifications import templates


@pytest.fixture(autouse=True)
def admin_base_url(monkeypatch):
    monkeypatch.setenv("ADMIN_UI_BASE_URL", "https://app.penguinhealth.io")


def test_validation_run_complete_legacy_shape_still_renders():
    """Callers not yet threaded with `queue_counters` fall back to the
    pre-cutover subject shape. Body is minimal — just the deep link into
    the queue's default view. Still no PHI."""
    subject, body = templates.render_validation_run_complete(
        org_id="test-org",
        validation_run_id="20260605-123000",
        summary={"total": 10, "passed": 8, "failed": 1, "skipped": 1},
    )
    # Deep link points at the org's queue default view; no per-run
    # filter, no run id in the URL.
    assert "/organizations/test-org/document-queue" in body
    assert "firstSeenRunId" not in body
    assert "1 failed" in subject
    # Rule aggregates and the run id are not surfaced in the body.
    assert "Total:" not in body
    assert "Failed:" not in body
    assert "20260605-123000" not in body
    for forbidden in ("patient", "dob", "ssn", "member_id"):
        assert forbidden not in body.lower()


def test_validation_run_complete_reports_queue_delta_when_counters_present():
    """The reviewer-facing story: how many docs need review, how many
    unchanged resends we correctly skipped."""
    subject, body = templates.render_validation_run_complete(
        org_id="test-org",
        validation_run_id="20260707-020000",
        summary={"total": 42, "passed": 30, "failed": 10, "skipped": 2},
        queue_counters={
            "new_documents": 12,
            "new_versions": 3,
            "duplicate_skips": 87,
        },
    )
    # Subject leads with the reviewer's number: docs that need attention.
    assert "15 for review" in subject
    assert "87 unchanged skipped" in subject

    # Body carries the queue-delta section only.
    assert "New documents:     12" in body
    assert "Updated documents: 3" in body
    assert "Unchanged skipped: 87" in body

    # Deep link lands on the queue's default view — no run filter, no
    # run id in the URL. Reviewers can narrow inside the app if they want.
    assert "/organizations/test-org/document-queue" in body
    assert "firstSeenRunId" not in body

    # Rule aggregates and the "Run: <id>" line are not surfaced. The
    # run id does not appear in the body at all.
    assert "Total:" not in body
    assert "Failed:" not in body
    assert "Run:" not in body
    assert "20260707-020000" not in body

    for forbidden in ("patient", "dob", "ssn", "member_id"):
        assert forbidden not in body.lower()


def test_validation_run_complete_zero_counters_still_render_cleanly():
    """A run where nothing new happened still emails cleanly — no crash,
    subject reads sensibly, body shows the zeros so recipients aren't
    left wondering whether the counters section was omitted by bug."""
    subject, body = templates.render_validation_run_complete(
        org_id="test-org",
        validation_run_id="20260707-020000",
        summary={"total": 0, "passed": 0, "failed": 0, "skipped": 0},
        queue_counters={"new_documents": 0, "new_versions": 0, "duplicate_skips": 0},
    )
    assert "0 for review" in subject
    assert "New documents:     0" in body


# Sample PHI-ish strings that absolutely must not leak into the email body
# even if a future change accidentally pipes them through.
_PHI_NEEDLES = [
    "Alice Smith",
    "1990-01-01",
    "patient_hash_abc123",
    "Member ID 1234",
    "Cigna",
    "Primary changed",
    "subscriber.first_name",
]


@pytest.mark.parametrize("status", [
    "discrepancy", "no_coverage", "review_needed", "service_type_denied",
])
def test_eligibility_template_body_is_phi_free(status):
    subject, body = templates.render_eligibility_issue(
        org_id="test-org",
        encounter_id="enc-789",
        encounter_datetime="2026-06-05T14:30:00Z",
        result_status=status,
    )
    # The expected pieces ARE present.
    assert "enc-789" in body
    assert "2026-06-05T14:30:00Z" in body
    assert "/organizations/test-org/eligibility/worklist?encounter=enc-789" in body
    # And none of the PHI-ish needles are.
    blob = f"{subject}\n{body}"
    for needle in _PHI_NEEDLES:
        assert needle not in blob, (
            f"PHI-ish needle leaked into eligibility template: {needle!r}"
        )


def test_eligibility_template_handles_missing_datetime():
    subject, body = templates.render_eligibility_issue(
        org_id="test-org",
        encounter_id="enc-789",
        encounter_datetime=None,
        result_status="discrepancy",
    )
    # We still want a sensible subject + body, with no crash.
    assert "enc-789" in body
    assert "Unknown" in body or "time unknown" in subject


def test_eligibility_template_uses_admin_ui_base_url_override(monkeypatch):
    monkeypatch.setenv("ADMIN_UI_BASE_URL", "https://dev.penguinhealth.io/")
    _, body = templates.render_eligibility_issue(
        org_id="test-org", encounter_id="enc-1",
        encounter_datetime="2026-06-05T00:00:00Z", result_status="discrepancy",
    )
    assert "https://dev.penguinhealth.io/organizations/test-org/eligibility/worklist" in body
    assert "//organizations" not in body  # trailing-slash dedupe
