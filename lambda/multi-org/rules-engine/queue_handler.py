"""
Document queue handler.

Owns the content-hash + pointer/version writes that transform the daily
audit runs into an ongoing per-document review queue. Two shapes of
work land here:

  * ``compute_content_hash`` / ``lookup_pointer`` — invoked at the top
    of ``document_validator.validate_document`` to short-circuit
    byte-identical resends before extraction and rule evaluation ever
    fire.
  * ``upsert_new_or_version`` / ``record_duplicate_skip`` /
    ``write_sentinel_row`` — invoked from ``rules_engine_rag.process_file``
    after ``validate_document`` returns, to persist the queue state and
    keep continuation legs' "already-processed" tracking honest.

Feature-flagged via ``QUEUE_WRITE_ENABLED``; callers guard on
``is_enabled()``.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Optional

import boto3
from botocore.exceptions import ClientError

from audit import SystemPrincipal, emit as audit_emit


_dynamodb = boto3.resource("dynamodb")

_AUDIT_PRINCIPAL = SystemPrincipal(
    os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "rules-engine-rag")
)

# Denormalized filter columns copied off latest field_values onto the
# pointer row. Reviewer chip filters run as DynamoDB FilterExpression
# against these; keeping the list narrow keeps the pointer row compact.
_DENORMALIZED_FILTERS = ("program", "service_type", "payer_description", "date")


def is_enabled() -> bool:
    """Feature flag. Rules engine writes to the queue only when true."""
    return os.environ.get("QUEUE_WRITE_ENABLED", "false").lower() == "true"


def _queue_table_name() -> str:
    return os.environ["DOCUMENT_QUEUE_TABLE"]


def _queue_table():
    return _dynamodb.Table(_queue_table_name())


def _to_ddb_item(item: dict) -> dict:
    """Convert nested Python floats to Decimal for boto3 put_item.

    boto3's DynamoDB resource layer rejects `float` — every numeric
    attribute has to be `Decimal`. CR-ingested records carry raw
    floats on billing fields (rates, charges, mileage) which flow
    into `field_values_snapshot`, so a naked `put_item` blows up with
    "Float types are not supported."

    Mirror the same round-trip trick `results_handler.store_results`
    uses: JSON round-trip with `parse_float=Decimal`. The `default=str`
    covers datetime/Decimal/other non-JSON-native types so the dump
    step doesn't raise before the load ever runs.
    """
    return json.loads(json.dumps(item, default=str), parse_float=Decimal)


# ----- Canonicalization + hash --------------------------------------------

def _canonicalize(value: Any) -> Any:
    """Recursively normalize a value to a stable, JSON-serializable form.

    Dicts land as sorted-key dicts; Decimals coerce to strings so the
    hash is stable across DynamoDB round-trips; sets get sorted; tuples
    collapse to lists.
    """
    if isinstance(value, dict):
        return {k: _canonicalize(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(v) for v in value]
    if isinstance(value, set):
        return sorted(_canonicalize(v) for v in value)
    if isinstance(value, Decimal):
        return str(value)
    return value


def canonicalize_record(data: dict) -> str:
    """Deterministic JSON of the raw ingested record.

    Contract: byte-for-byte identical output for records that hash to the
    same content. A change here silently invalidates every stored hash;
    lock it with tests.
    """
    return json.dumps(_canonicalize(data), separators=(",", ":"), default=str)


def compute_content_hash(data: dict) -> str:
    """SHA-256 hex of the canonicalized record."""
    return hashlib.sha256(canonicalize_record(data).encode("utf-8")).hexdigest()


# ----- Pointer + version reads / writes -----------------------------------

def _pointer_pk(org_id: str) -> str:
    return f"ORG#{org_id}"


def _pointer_sk(document_id: str) -> str:
    return f"DOC#{document_id}"


def _version_pk(org_id: str, document_id: str) -> str:
    return f"ORG#{org_id}#DOC#{document_id}"


def _version_sk(iso_ts: str) -> str:
    return f"VERSION#{iso_ts}"


def _gsi1_pk(org_id: str, status: str) -> str:
    return f"ORG#{org_id}#STATUS#{status}"


def _gsi1_sk(last_updated: str) -> str:
    return f"LAST_UPDATED#{last_updated}"


def lookup_pointer(org_id: str, document_id: str) -> Optional[dict]:
    """Return the pointer row for (org, document_id), or None."""
    if not org_id or not document_id or document_id == "UNKNOWN":
        return None
    try:
        resp = _queue_table().get_item(
            Key={"pk": _pointer_pk(org_id), "sk": _pointer_sk(document_id)}
        )
    except ClientError as e:
        # Fail closed: if we can't read the queue, treat as unseen so we
        # don't accidentally skip a validation. The write path will
        # attempt a create; if it collides the ConditionExpression will
        # surface the true state.
        print(f"queue_handler: pointer GetItem failed for {org_id}/{document_id}: {e}")
        return None
    return resp.get("Item")


def _finding_counts(rules: list[dict]) -> tuple[int, int, int, int, int]:
    """Return (total, failed, resolved_fails, confirmed_fails, open_fails).

    Mirrors the reviewer-facing state on individual result rows:
      * `fixed=True` — reviewer marked the FAIL as resolved
      * `finding_confirmed=True` — reviewer confirmed the finding (but
        hasn't fixed it yet)
      * else it's an open failing finding
    """
    total = len(rules)
    failed = 0
    resolved = 0
    confirmed = 0
    open_ = 0
    for r in rules:
        if r.get("status") != "FAIL":
            continue
        failed += 1
        if r.get("fixed"):
            resolved += 1
        elif r.get("finding_confirmed"):
            confirmed += 1
        else:
            open_ += 1
    return total, failed, resolved, confirmed, open_


def _denormalize_filters(field_values: dict) -> dict:
    out = {}
    for key in _DENORMALIZED_FILTERS:
        value = field_values.get(key)
        if value in (None, ""):
            continue
        out[key] = value
    return out


def upsert_new_or_version(results: dict, pointer: Optional[dict]) -> str:
    """Write the version row and create/refresh the pointer row.

    Returns one of:
      * "queue_create"     — first time we've seen this document
      * "queue_new_version" — content changed since latest version

    The caller emits the corresponding audit event; keeping the emit
    outside this helper makes tests easier.
    """
    org_id = results["organization_id"]
    document_id = results["document_id"]
    content_hash = results["content_hash"]
    validation_run_id = results["validation_run_id"]
    validation_ts = results["validation_timestamp"]
    field_values = results.get("field_values", {})
    summary = results.get("summary", {})
    rules = results.get("rules", [])

    total, failed, resolved, confirmed, open_ = _finding_counts(rules)

    version_sk_value = _version_sk(validation_ts)
    now_iso = datetime.now(timezone.utc).isoformat()

    version_item = {
        "pk": _version_pk(org_id, document_id),
        "sk": version_sk_value,
        "document_id": document_id,
        "organization_id": org_id,
        "content_hash": content_hash,
        "validation_run_id": validation_run_id,
        "validation_timestamp": validation_ts,
        "field_values_snapshot": field_values,
        "summary": summary,
        "validation_result_pk": f"DOC#{document_id}",
        "validation_result_sk": f"VALIDATION#{validation_ts}",
        "previous_version_sk": pointer.get("latest_version_sk") if pointer else None,
    }

    table = _queue_table()
    # Version rows are append-only; a naked put is safe (sk is
    # timestamp-based so collisions require identical microseconds,
    # unlikely in practice; if it does happen the last-writer wins and
    # the summary is unchanged).
    table.put_item(Item=_to_ddb_item(version_item))

    pointer_item = {
        "pk": _pointer_pk(org_id),
        "sk": _pointer_sk(document_id),
        "document_id": document_id,
        "organization_id": org_id,
        "status": "open",
        "content_hash": content_hash,
        "latest_version_sk": version_sk_value,
        "latest_validation_run_id": validation_run_id,
        "latest_validation_timestamp": validation_ts,
        "last_updated_at": now_iso,
        "last_seen_at": now_iso,
        "field_values_snapshot": field_values,
        "total_findings": total,
        "failed_findings": failed,
        "resolved_findings": resolved,
        "confirmed_findings": confirmed,
        "open_findings": open_,
        "latest_validation_result_pk": f"DOC#{document_id}",
        "latest_validation_result_sk": f"VALIDATION#{validation_ts}",
        "gsi1pk": _gsi1_pk(org_id, "open"),
        "gsi1sk": _gsi1_sk(now_iso),
        # Sparse GSI2 — only present while status=open. Reset on status
        # flips (see admin_api and queue-autoclose handlers).
        "gsi2pk": "STATUS#open",
        "gsi2sk": _gsi1_sk(now_iso),
    }
    pointer_item.update(_denormalize_filters(field_values))

    if pointer:
        # Preserve first-seen metadata across new versions.
        pointer_item["first_seen_run_id"] = pointer.get("first_seen_run_id") or validation_run_id
        pointer_item["first_seen_at"] = pointer.get("first_seen_at") or validation_ts
        pointer_item["seen_count"] = int(pointer.get("seen_count") or 0) + 1
        pointer_item["version_count"] = int(pointer.get("version_count") or 0) + 1
        table.put_item(Item=_to_ddb_item(pointer_item))
        return "queue_new_version"

    pointer_item["first_seen_run_id"] = validation_run_id
    pointer_item["first_seen_at"] = validation_ts
    pointer_item["seen_count"] = 1
    pointer_item["version_count"] = 1
    table.put_item(Item=_to_ddb_item(pointer_item))
    return "queue_create"


def record_duplicate_skip(pointer: dict, validation_run_id: str) -> None:
    """Bump `seen_count` and `last_seen_at` on the pointer row.

    Does not touch `last_updated_at` — a duplicate skip is not
    reviewer-visible work and must not reset the auto-close idle clock.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    _queue_table().update_item(
        Key={"pk": pointer["pk"], "sk": pointer["sk"]},
        UpdateExpression=(
            "SET last_seen_at = :ts, "
            "seen_count = if_not_exists(seen_count, :zero) + :one"
        ),
        ExpressionAttributeValues={
            ":ts": now_iso,
            ":zero": 0,
            ":one": 1,
        },
    )
    audit_emit(
        action="execute",
        resource={
            "type": "DocumentQueueEntry",
            "id": pointer.get("document_id", "unknown"),
            "org": pointer.get("organization_id", "unknown"),
        },
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=pointer.get("organization_id", "unknown"),
        purpose_of_use="DOC_PROCESSING",
        call_type="queue_duplicate_skip",
        external_control_number=validation_run_id,
    )


def emit_queue_write_audit(*, call_type: str, org_id: str, document_id: str,
                           validation_run_id: str) -> None:
    """Audit for a create or new-version write. Kept as its own function so
    the emit type is centralized alongside record_duplicate_skip."""
    audit_emit(
        action="write",
        resource={
            "type": "DocumentQueueEntry",
            "id": document_id,
            "org": org_id,
        },
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=org_id,
        purpose_of_use="DOC_PROCESSING",
        call_type=call_type,
        external_control_number=validation_run_id,
    )


# ----- Sentinel row on the validation-results table -----------------------

def write_sentinel_row(*, org_id: str, document_id: str,
                       validation_run_id: str, s3_key: str,
                       duplicate_of_version_sk: Optional[str],
                       results_table_name: str) -> None:
    """Skinny "already processed" marker on the results table.

    Same shape as a real per-doc result row so
    ``results_handler.get_processed_s3_keys`` finds it and continuation
    legs won't re-list this file. `duplicate_of_version_sk` marks it as
    a sentinel so ``aggregate_run_summary`` can ignore it.
    """
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "pk": f"DOC#{document_id}#SKIPPED#{validation_run_id}",
        "sk": f"VALIDATION#{now}",
        "gsi1pk": f"DATE#{now[:10]}",
        "gsi1sk": f"DOC#{document_id}#SKIPPED",
        "gsi2pk": f"RUN#{validation_run_id}",
        "gsi2sk": f"DOC#{document_id}#SKIPPED",
        "organization_id": org_id,
        "document_id": document_id,
        "validation_run_id": validation_run_id,
        "validation_timestamp": now,
        "s3_key": s3_key,
        "summary": {"total_rules": 0, "passed": 0, "failed": 0, "skipped": 0},
        "rules": [],
        "duplicate_of_version_sk": duplicate_of_version_sk,
    }
    _dynamodb.Table(results_table_name).put_item(Item=_to_ddb_item(item))


def is_sentinel_row(item: dict) -> bool:
    """True if a results-table row was written by ``write_sentinel_row``."""
    return bool(item.get("duplicate_of_version_sk")) or (
        isinstance(item.get("pk"), str) and "#SKIPPED#" in item["pk"]
    )
