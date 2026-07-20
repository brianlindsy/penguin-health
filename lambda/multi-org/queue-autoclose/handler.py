"""
Queue auto-close job.

Runs once daily. Queries GSI2 on the document-queue table for open queue
entries whose `last_updated_at` is older than the configured window
(default 90 days, overridable per-org via
org_config `sk=QUEUE_CONFIG`.autoclose_days), then flips each one to
`auto-closed` with a ConditionExpression that guards against a reviewer
racing us to `resolved`/`confirmed`.

Every close emits one audit event. No PHI in the event body — only the
document id (which is the vendor's canonical record id, treated as
sensitive at the record level but standard fare in audit metadata).
"""

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from audit import SystemPrincipal, emit as audit_emit

# Lazily created so tests running inside separate moto contexts don't
# reuse a resource handle bound to a torn-down mock. Callers reach the
# resource via `_ddb()`; production code pays a one-time cache.
_dynamodb = None


def _ddb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _reset_for_tests():
    """Test hook: drop the cached resource so the next `_ddb()` picks up
    the current moto context."""
    global _dynamodb
    _dynamodb = None

_AUDIT_PRINCIPAL = SystemPrincipal(
    os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "queue-autoclose")
)

_DEFAULT_DAYS = int(os.environ.get("DEFAULT_AUTOCLOSE_DAYS", "90"))


def _org_thresholds(org_config_table_name: str) -> dict:
    """Return {org_id: autoclose_days} for any org that has overridden the
    default via a QUEUE_CONFIG row. Missing rows fall through to the
    module-level default.
    """
    if not org_config_table_name:
        return {}
    table = _ddb().Table(org_config_table_name)
    # Cheapest way to find overrides today: scan for sk=QUEUE_CONFIG. There
    # are a handful of orgs, so this is O(orgs) once per invocation and
    # not worth a dedicated GSI.
    thresholds: dict[str, int] = {}
    kwargs = {
        "FilterExpression": "#sk = :qc",
        "ExpressionAttributeNames": {"#sk": "sk"},
        "ExpressionAttributeValues": {":qc": "QUEUE_CONFIG"},
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            pk = item.get("pk", "")
            if not pk.startswith("ORG#"):
                continue
            org_id = pk[len("ORG#"):]
            days = item.get("autoclose_days")
            # DynamoDB returns numeric fields as Decimal; accept int/float/Decimal.
            if isinstance(days, (int, float, Decimal)) and days > 0:
                thresholds[org_id] = int(days)
        if "LastEvaluatedKey" not in resp:
            return thresholds
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def _iter_stale_open_entries(queue_table, cutoff_iso: str):
    """Query GSI2 for open entries older than the earliest cutoff.

    We use the LOOSEST (largest) cutoff up front and let the per-item
    per-org threshold cull inside the loop. This keeps GSI reads to one
    Query per invocation.
    """
    last_evaluated = None
    while True:
        kwargs = {
            "IndexName": "gsi2",
            "KeyConditionExpression": "gsi2pk = :open AND gsi2sk < :cutoff",
            "ExpressionAttributeValues": {
                ":open": "STATUS#open",
                ":cutoff": f"LAST_UPDATED#{cutoff_iso}",
            },
        }
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = queue_table.query(**kwargs)
        for item in resp.get("Items", []):
            yield item
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


def _close_entry(queue_table, item: dict, now_iso: str) -> bool:
    """UpdateItem the pointer row to `auto-closed`. Returns True on close,
    False if the reviewer beat us (ConditionalCheckFailed) — that's a
    normal race, not an error.
    """
    try:
        queue_table.update_item(
            Key={"pk": item["pk"], "sk": item["sk"]},
            UpdateExpression=(
                "SET #status = :closed, "
                "auto_closed_at = :ts, "
                "auto_closed_reason = :reason, "
                "last_updated_at = :ts, "
                "gsi1pk = :gsi1pk"
                " REMOVE gsi2pk, gsi2sk"
            ),
            ConditionExpression="#status = :open",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":closed": "auto-closed",
                ":open": "open",
                ":ts": now_iso,
                ":reason": "idle_over_threshold",
                ":gsi1pk": f"ORG#{item['organization_id']}#STATUS#auto-closed",
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def lambda_handler(event, context):
    queue_table_name = os.environ["DOCUMENT_QUEUE_TABLE"]
    org_config_table_name = os.environ.get("ORG_CONFIG_TABLE_NAME", "")

    queue_table = _ddb().Table(queue_table_name)
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    org_thresholds = _org_thresholds(org_config_table_name)
    # Query cutoff uses the SHORTEST configured window across all orgs so
    # anything potentially eligible under any per-org threshold shows up
    # in the GSI2 result. Per-item filtering below then re-applies the
    # actual per-org threshold and skips items that aren't stale enough
    # under their own org's window.
    shortest_days = min([_DEFAULT_DAYS, *org_thresholds.values()],
                        default=_DEFAULT_DAYS)
    query_cutoff = (now - timedelta(days=shortest_days)).isoformat()

    scanned = 0
    closed = 0
    races = 0

    for item in _iter_stale_open_entries(queue_table, query_cutoff):
        scanned += 1
        org_id = item.get("organization_id", "")
        threshold_days = org_thresholds.get(org_id, _DEFAULT_DAYS)
        per_org_cutoff = now - timedelta(days=threshold_days)

        last_updated = item.get("last_updated_at")
        if not last_updated:
            continue
        try:
            last_updated_dt = datetime.fromisoformat(last_updated)
        except (TypeError, ValueError):
            continue
        if last_updated_dt.tzinfo is None:
            last_updated_dt = last_updated_dt.replace(tzinfo=timezone.utc)
        if last_updated_dt >= per_org_cutoff:
            # Eligible under the loosest window but not the org's own —
            # skip.
            continue
        # last_updated_dt is older than the per-org cutoff → close.

        if _close_entry(queue_table, item, now_iso):
            closed += 1
            audit_emit(
                action="write",
                resource={
                    "type": "DocumentQueueEntry",
                    "id": item.get("document_id", "unknown"),
                    "org": org_id,
                },
                actor=_AUDIT_PRINCIPAL.as_actor(),
                org_id=org_id,
                purpose_of_use="OPERATIONS",
                call_type="queue_auto_close",
            )
        else:
            races += 1

    print(
        f"queue-autoclose: scanned={scanned} closed={closed} races={races} "
        f"shortest_days={shortest_days}"
    )
    return {"scanned": scanned, "closed": closed, "races": races}
