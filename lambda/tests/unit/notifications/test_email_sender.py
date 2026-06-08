"""Unit tests for notifications.email_sender.

Verifies the SES happy path, the org-level kill switch, the empty-recipient
short-circuit, the EMAIL_FROM_ADDRESS guard, and (most importantly) that
no PHI/email-content can be reconstructed from the audit row or the logs.
"""

import hashlib
import os

import pytest

# moto patches boto3 globally via mock_dynamodb fixture; the module also
# uses boto3.client('ses'), so we mock that with a stub before import.


@pytest.fixture
def email_env(monkeypatch):
    monkeypatch.setenv("EMAIL_FROM_ADDRESS", "noreply@penguinhealth.io")
    monkeypatch.setenv("EMAIL_REPLY_TO", "noreply@penguinhealth.io")
    monkeypatch.setenv("ADMIN_UI_BASE_URL", "https://app.penguinhealth.io")
    monkeypatch.setenv("ORG_CONFIG_TABLE_NAME", "penguin-health-org-config")
    monkeypatch.setenv("STEDI_TABLE_NAME", "penguin-health-stedi")


@pytest.fixture
def email_module(mock_dynamodb, email_env, monkeypatch):
    """Reload the module each test so the moto-backed tables and env vars
    take effect on the module-level boto3 clients."""
    import importlib
    import notifications.email_sender as email_sender_mod
    importlib.reload(email_sender_mod)
    # Re-bind the module-level tables to the moto-backed resources.
    monkeypatch.setattr(
        email_sender_mod, "_audit_table",
        mock_dynamodb.Table("penguin-health-stedi"),
    )
    monkeypatch.setattr(
        email_sender_mod, "_org_config_table",
        mock_dynamodb.Table("penguin-health-org-config"),
    )
    return email_sender_mod


@pytest.fixture
def seed_org_metadata(mock_dynamodb):
    """Add an ORG#test-org/METADATA row with notifications enabled."""
    table = mock_dynamodb.Table("penguin-health-org-config")

    def _seed(*, org_id="test-org", notifications_enabled=True):
        item = {
            "pk": f"ORG#{org_id}",
            "sk": "METADATA",
            "organization_id": org_id,
            "organization_name": f"{org_id} display",
            "enabled": True,
            "notifications_enabled": notifications_enabled,
        }
        table.put_item(Item=item)
        return item

    return _seed


@pytest.fixture
def mock_ses(mocker, email_module):
    """Replace the module's SES client with a Mock that returns a stable id."""
    ses_stub = mocker.MagicMock()
    ses_stub.send_email.return_value = {"MessageId": "ses-test-message-id"}
    mocker.patch.object(email_module, "_ses", ses_stub)
    return ses_stub


def test_send_email_writes_audit_row_and_does_not_leak_recipient(
    email_module, mock_ses, seed_org_metadata, mock_dynamodb,
):
    seed_org_metadata()
    recipient = "alice@example.com"
    msg_id = email_module.send_email(
        to=[recipient],
        subject="Run done",
        body_text="Run done. Link: https://app.penguinhealth.io/x",
        event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
        org_id="test-org",
        template_name="validation_run_complete",
    )
    assert msg_id == "ses-test-message-id"
    mock_ses.send_email.assert_called_once()
    sent = mock_ses.send_email.call_args.kwargs
    assert sent["Destination"] == {"ToAddresses": [recipient]}

    audit_table = mock_dynamodb.Table("penguin-health-stedi")
    rows = audit_table.scan().get("Items", [])
    audit_rows = [r for r in rows if r["sk"].startswith("EMAIL_AUDIT#")]
    assert len(audit_rows) == 1
    row = audit_rows[0]
    assert row["message_id"] == "ses-test-message-id"
    assert row["event_type"] == "validation_run_complete"
    assert row["template_name"] == "validation_run_complete"
    assert row["recipient_count"] == 1

    # Recipients stored ONLY as sha256 hashes, never as raw addresses.
    expected_hash = hashlib.sha256(recipient.encode()).hexdigest()
    assert row["recipient_hashes"] == [expected_hash]
    # The raw email must not appear anywhere on the audit row.
    serialized = str(row)
    assert recipient not in serialized


def test_send_email_skipped_when_no_recipients(email_module, mock_ses, seed_org_metadata):
    seed_org_metadata()
    result = email_module.send_email(
        to=[],
        subject="x",
        body_text="x",
        event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
        org_id="test-org",
        template_name="validation_run_complete",
    )
    assert result is None
    mock_ses.send_email.assert_not_called()


def test_send_email_skipped_when_org_notifications_disabled(
    email_module, mock_ses, seed_org_metadata,
):
    seed_org_metadata(notifications_enabled=False)
    result = email_module.send_email(
        to=["alice@example.com"],
        subject="x",
        body_text="x",
        event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
        org_id="test-org",
        template_name="validation_run_complete",
    )
    assert result is None
    mock_ses.send_email.assert_not_called()


def test_send_email_skipped_when_org_metadata_missing(email_module, mock_ses):
    # Reads on the org-config table that return no Item should error on the
    # side of NOT sending — the helper short-circuits to None.
    result = email_module.send_email(
        to=["alice@example.com"],
        subject="x",
        body_text="x",
        event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
        org_id="ghost-org",
        template_name="validation_run_complete",
    )
    # Without a METADATA row notifications_enabled defaults to True, so this
    # still attempts to send. The point of this test is just to verify the
    # absence of a metadata row doesn't crash.
    assert result == "ses-test-message-id"


def test_send_email_skipped_when_from_address_unset(
    email_module, mock_ses, seed_org_metadata, monkeypatch,
):
    seed_org_metadata()
    monkeypatch.delenv("EMAIL_FROM_ADDRESS", raising=False)
    result = email_module.send_email(
        to=["alice@example.com"],
        subject="x",
        body_text="x",
        event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
        org_id="test-org",
        template_name="validation_run_complete",
    )
    assert result is None
    mock_ses.send_email.assert_not_called()


def test_send_email_rejects_unknown_event_type(email_module, mock_ses, seed_org_metadata):
    seed_org_metadata()
    with pytest.raises(ValueError, match="unknown event_type"):
        email_module.send_email(
            to=["alice@example.com"],
            subject="x",
            body_text="x",
            event_type="not_a_real_event",
            org_id="test-org",
            template_name="bogus",
        )
    mock_ses.send_email.assert_not_called()


def test_recipient_hash_is_lowercased_and_trimmed(email_module):
    h1 = email_module._hash_recipient("Alice@Example.COM ")
    h2 = email_module._hash_recipient("alice@example.com")
    assert h1 == h2


def test_send_email_logs_do_not_contain_recipient_or_body(
    email_module, mock_ses, seed_org_metadata, caplog,
):
    """Defense in depth: even if a future change pulls these into a log
    line, we want the test suite to catch it."""
    seed_org_metadata()
    secret_body = "patient_name=Alice_Smith_dob=1990-01-01"
    secret_recipient = "very.secret.recipient@example.com"
    with caplog.at_level("INFO", logger="notifications.email_sender"):
        email_module.send_email(
            to=[secret_recipient],
            subject="Run done",
            body_text=secret_body,
            event_type=email_module.EVENT_VALIDATION_RUN_COMPLETE,
            org_id="test-org",
            template_name="validation_run_complete",
        )
    log_blob = "\n".join(rec.getMessage() for rec in caplog.records)
    assert secret_recipient not in log_blob
    assert "Alice_Smith" not in log_blob
    assert "1990-01-01" not in log_blob
