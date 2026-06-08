"""SES email sender + immutable audit trail.

Logs only message_id, sha256 recipient hash, template name, event type,
and org_id. The body and raw recipient address never appear in logs.

Audit rows land on the existing penguin-health-stedi table under
  pk = ORG#{org_id}
  sk = EMAIL_AUDIT#{iso_ts}#{message_id}
mirroring the AUDIT# pattern in stedi/audit.py (7-year TTL).
"""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

EVENT_VALIDATION_RUN_COMPLETE = "validation_run_complete"
EVENT_ELIGIBILITY_ISSUE = "eligibility_issue"

EVENT_TYPES = (EVENT_VALIDATION_RUN_COMPLETE, EVENT_ELIGIBILITY_ISSUE)

_SEVEN_YEARS_SECONDS = 7 * 365 * 24 * 60 * 60

_STEDI_TABLE_NAME = os.environ.get("STEDI_TABLE_NAME", "penguin-health-stedi")
_ORG_CONFIG_TABLE_NAME = os.environ.get("ORG_CONFIG_TABLE_NAME", "penguin-health-org-config")

_dynamodb = boto3.resource("dynamodb")
_audit_table = _dynamodb.Table(_STEDI_TABLE_NAME)
_org_config_table = _dynamodb.Table(_ORG_CONFIG_TABLE_NAME)

_ses = boto3.client("ses")


class _EmailSkipped(Exception):
    """Internal marker — surface a single 'skipped' log line and return cleanly."""


def send_email(
    *,
    to: list[str],
    subject: str,
    body_text: str,
    event_type: str,
    org_id: str,
    template_name: str,
):
    """Send `body_text` to `to` via SES; write one audit row per recipient.

    Skipped silently (with one log line) when:
      - `to` is empty
      - the org has notifications disabled via org config
      - EMAIL_FROM_ADDRESS env var is unset (dev/test convenience)

    `body_text` and `subject` MUST NOT contain PHI — callers are responsible
    for keeping the body to non-PHI fields (timestamps, counts, deep links).
    """
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event_type: {event_type!r}")

    if not to:
        logger.info(
            "email-send skipped: no recipients event=%s org=%s template=%s",
            event_type, org_id, template_name,
        )
        return None

    if not _org_notifications_enabled(org_id):
        logger.info(
            "email-send skipped: notifications_enabled=false event=%s org=%s template=%s",
            event_type, org_id, template_name,
        )
        return None

    from_address = os.environ.get("EMAIL_FROM_ADDRESS")
    if not from_address:
        logger.warning(
            "email-send skipped: EMAIL_FROM_ADDRESS unset event=%s org=%s template=%s",
            event_type, org_id, template_name,
        )
        return None

    reply_to = os.environ.get("EMAIL_REPLY_TO")
    configuration_set = os.environ.get("SES_CONFIGURATION_SET")

    send_kwargs = {
        "Source": from_address,
        "Destination": {"ToAddresses": to},
        "Message": {
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Text": {"Data": body_text, "Charset": "UTF-8"}},
        },
    }
    if reply_to:
        send_kwargs["ReplyToAddresses"] = [reply_to]
    if configuration_set:
        send_kwargs["ConfigurationSetName"] = configuration_set

    try:
        response = _ses.send_email(**send_kwargs)
    except ClientError as e:
        # Log only the SES error code, never the recipient or body.
        code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.error(
            "email-send failed event=%s org=%s template=%s ses_error=%s",
            event_type, org_id, template_name, code,
        )
        raise

    message_id = response.get("MessageId") or str(uuid.uuid4())
    recipient_hashes = [_hash_recipient(addr) for addr in to]

    _write_audit_row(
        org_id=org_id,
        message_id=message_id,
        event_type=event_type,
        template_name=template_name,
        recipient_hashes=recipient_hashes,
        recipient_count=len(to),
    )

    logger.info(
        "email-send ok event=%s org=%s template=%s message_id=%s recipients=%d recipient_hashes=%s",
        event_type, org_id, template_name, message_id, len(to),
        # Truncate hashes for log brevity; the full hashes are on the audit row.
        [h[:12] for h in recipient_hashes],
    )
    return message_id


def _org_notifications_enabled(org_id: str) -> bool:
    """Read the org metadata row's `notifications_enabled` flag. Defaults to
    True if the field is absent — orgs opt out, not in."""
    try:
        response = _org_config_table.get_item(
            Key={"pk": f"ORG#{org_id}", "sk": "METADATA"},
        )
    except ClientError:
        # If the org config can't be read we err on the side of NOT sending.
        logger.exception("email-send: failed to read org metadata org=%s", org_id)
        return False
    item = response.get("Item") or {}
    return item.get("notifications_enabled", True) is not False


def _hash_recipient(email: str) -> str:
    """sha256 of the lowercased email. Lets us reconcile bounce logs against
    audit rows without storing the raw address."""
    return hashlib.sha256((email or "").strip().lower().encode("utf-8")).hexdigest()


def _write_audit_row(
    *,
    org_id: str,
    message_id: str,
    event_type: str,
    template_name: str,
    recipient_hashes: list[str],
    recipient_count: int,
):
    now = datetime.now(timezone.utc)
    iso_ts = now.isoformat()
    item = {
        "pk": f"ORG#{org_id}",
        "sk": f"EMAIL_AUDIT#{iso_ts}#{message_id}",
        "message_id": message_id,
        "event_type": event_type,
        "template_name": template_name,
        "recipient_hashes": recipient_hashes,
        "recipient_count": recipient_count,
        "sent_at": iso_ts,
        "expires_at": int(now.timestamp()) + _SEVEN_YEARS_SECONDS,
    }
    try:
        _audit_table.put_item(Item=item)
    except ClientError:
        # Don't fail the send on audit-write failure, but loudly warn.
        logger.exception(
            "email-send: audit write failed message_id=%s event=%s org=%s",
            message_id, event_type, org_id,
        )
