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


def test_validation_run_complete_body_contains_only_aggregate_data():
    subject, body = templates.render_validation_run_complete(
        org_id="test-org",
        validation_run_id="20260605-123000",
        summary={"total": 10, "passed": 8, "failed": 1, "skipped": 1},
    )
    assert "10" in body
    assert "20260605-123000" in body
    assert "/organizations/test-org/validation-runs/20260605-123000" in body
    # The aggregates should appear, but the test asserts on the *kind* of
    # data — there are no patient fields in this template at all.
    assert subject
    for forbidden in ("patient", "dob", "ssn", "member_id"):
        assert forbidden not in body.lower()


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
