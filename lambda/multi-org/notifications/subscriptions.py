"""Per-user notification subscriptions.

Stored on penguin-health-org-config:
  pk = USER#{email}
  sk = SUBSCRIPTION#{org_id}#{event_type}

A row's presence with `enabled=True` means the user opts in. Missing row
or `enabled=False` means no email. The GSI mirrors the existing user-perm
pattern so we can query "all subscribers for (org, event)" cheaply.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key


logger = logging.getLogger(__name__)

_TABLE_NAME = os.environ.get("ORG_CONFIG_TABLE_NAME", "penguin-health-org-config")
_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


def get_subscribers(org_id: str, event_type: str) -> list[str]:
    """Return emails opted-in to (org_id, event_type). Empty list if none."""
    if not org_id or not event_type:
        return []
    response = _table.query(
        IndexName="gsi1",
        KeyConditionExpression=Key("gsi1pk").eq("SUBSCRIPTION")
                               & Key("gsi1sk").begins_with(f"ORG#{org_id}#{event_type}#"),
    )
    return [
        item["email"]
        for item in response.get("Items", [])
        if item.get("enabled") and item.get("email")
    ]


def set_subscription(*, email: str, org_id: str, event_type: str, enabled: bool) -> dict:
    """Upsert one subscription row. Returns the stored item."""
    if not email or not org_id or not event_type:
        raise ValueError("email, org_id, and event_type are required")
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "pk": f"USER#{email}",
        "sk": f"SUBSCRIPTION#{org_id}#{event_type}",
        "gsi1pk": "SUBSCRIPTION",
        "gsi1sk": f"ORG#{org_id}#{event_type}#USER#{email}",
        "email": email,
        "organization_id": org_id,
        "event_type": event_type,
        "enabled": bool(enabled),
        "updated_at": now,
    }
    # Preserve created_at if the row already exists.
    existing = _table.get_item(
        Key={"pk": item["pk"], "sk": item["sk"]},
    ).get("Item")
    item["created_at"] = (existing or {}).get("created_at") or now
    _table.put_item(Item=item)
    return item


def list_my_subscriptions(email: str, org_id: str) -> list[dict]:
    """Return the calling user's subscription rows scoped to one org."""
    if not email or not org_id:
        return []
    response = _table.query(
        KeyConditionExpression=Key("pk").eq(f"USER#{email}")
                               & Key("sk").begins_with(f"SUBSCRIPTION#{org_id}#"),
    )
    return [
        {
            "event_type": item.get("event_type"),
            "organization_id": item.get("organization_id"),
            "enabled": bool(item.get("enabled")),
            "updated_at": item.get("updated_at"),
        }
        for item in response.get("Items", [])
    ]
