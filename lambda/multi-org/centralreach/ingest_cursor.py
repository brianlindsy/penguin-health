"""Presence-only "already ingested" cursor for centralreach entries.

The runner checks this at the top of per-entry processing, before any
CR API calls or Bedrock work, to skip entries a prior run has already
ingested. Once a row lands here, that entry is not re-ingested even
if CR's `modified_date` later changes — provider re-signs and note
edits are intentionally out of scope for auto-reprocessing.

Item shape (single presence-only row per entry):
    pk = ORG#{org_id}
    sk = ENTRY#{source_record_id}
    first_ingested_at    : ISO-8601 UTC of the successful ingest
    first_ingest_run_id  : the run that first ingested the entry
    pdf_s3_key           : the PDF's S3 key (diagnostic only)
    record_s3_key        : the record JSON's S3 key (diagnostic only)

Feature-flagged via `CENTRALREACH_INGEST_DEDUPE_ENABLED`. When off,
`has_ingested` returns False and `mark_ingested` is a no-op, so
neither the infra rollout nor a mid-run flip surprises callers.

Operator escape hatch: if a specific entry needs re-ingestion, delete
its row from the cursor table by `(pk=ORG#{org_id}, sk=ENTRY#{id})`.
The runner sees no row on the next pass and processes the entry
again.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Callable

import boto3
from botocore.exceptions import ClientError


_TABLE_NAME_ENV = "CENTRALREACH_INGEST_CURSOR_TABLE"
_FEATURE_FLAG_ENV = "CENTRALREACH_INGEST_DEDUPE_ENABLED"
# Runner env var (not an infra default): operator sets this on a manual
# re-run to force reprocessing even when a cursor row exists.
_FORCE_REINGEST_ENV = "CENTRALREACH_FORCE_REINGEST"


def is_enabled(env: dict | None = None) -> bool:
    """True when the ingest-cursor dedupe path is active."""
    env = env if env is not None else os.environ
    return env.get(_FEATURE_FLAG_ENV, "false").lower() == "true"


def is_force_reingest(env: dict | None = None) -> bool:
    """True when the operator has opted into re-ingesting entries that
    already have a cursor row. Only checked when `is_enabled()`."""
    env = env if env is not None else os.environ
    return env.get(_FORCE_REINGEST_ENV, "false").lower() == "true"


def _pk(org_id: str) -> str:
    return f"ORG#{org_id}"


def _sk(source_record_id: str) -> str:
    return f"ENTRY#{source_record_id}"


def _default_table() -> Any:
    """Lazily resolve the DDB table so the module imports even when
    the env var is unset (e.g. in unit tests that inject a fake)."""
    return boto3.resource("dynamodb").Table(os.environ[_TABLE_NAME_ENV])


def has_ingested(
    org_id: str,
    source_record_id: str,
    *,
    table: Any | None = None,
) -> bool:
    """Return True if we have already ingested this entry.

    Fail-closed to False on any DDB error: re-ingesting is safer than
    skipping when we can't read state. The subsequent `mark_ingested`
    conditional put will catch a race with a concurrent run.

    `table` is injectable for tests; production callers omit it.
    """
    tbl = table if table is not None else _default_table()
    try:
        resp = tbl.get_item(
            Key={"pk": _pk(org_id), "sk": _sk(source_record_id)},
        )
    except ClientError as e:
        print(
            f"centralreach-ingest-cursor: has_ingested read failed for "
            f"{org_id}/{source_record_id}: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return False
    return "Item" in resp


def mark_ingested(
    org_id: str,
    source_record_id: str,
    *,
    ingest_run_id: str,
    pdf_s3_key: str,
    record_s3_key: str,
    now_iso: str,
    table: Any | None = None,
) -> bool:
    """Record a successful ingest.

    Uses a ConditionExpression so a concurrent run that already wrote
    the row is a silent no-op (returns False) rather than clobbering
    the first-ingest metadata. Any other DDB error is logged and
    swallowed — the ingest itself already succeeded, and losing the
    cursor row only means the next run reprocesses one entry.

    Returns True if this call wrote the row, False if it was already
    present or the write failed.
    """
    tbl = table if table is not None else _default_table()
    item = {
        "pk": _pk(org_id),
        "sk": _sk(source_record_id),
        "first_ingested_at": now_iso,
        "first_ingest_run_id": ingest_run_id,
        "pdf_s3_key": pdf_s3_key,
        "record_s3_key": record_s3_key,
    }
    try:
        tbl.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        print(
            f"centralreach-ingest-cursor: mark_ingested write failed for "
            f"{org_id}/{source_record_id}: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return False
