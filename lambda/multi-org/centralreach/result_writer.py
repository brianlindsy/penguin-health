"""Persist one centralreach-extracted clinical note: JSON to S3 + audit.

Single PHI-writing path for the centralreach integration. The pipeline
calls `persist_note()` once per signed entry after the PDF has already
been uploaded via `pdf_storage.write_pdf()` and the record built via
`record_builder.build_record()`.

PHI handling:
  * Raw `first_name`, `last_name`, `client_id` are required by the
    audit emitter to derive the patient hash and initials. They live
    only in the in-memory `IdentityForAudit` dataclass; the persisted
    record carries the hash and initials, never raw names.
  * `audit.emit` derives the hash + initials and drops the raw values
    per `lambda/multi-org/audit/schema.py`.
  * Note body never enters audit; for centralreach records `text` is
    None anyway. The PDF bytes live only in the encrypted S3 prefix
    written by `pdf_storage.write_pdf`.
  * Bucket-level KMS encryption applies to the per-org bucket; no
    per-object override required.

Audit-hash subtlety:
  CR's billing entries are date-stamped by ClientId, not DOB. The
  record's `patient_hash` is derived from `(first_name, last_name,
  client_id)`. To keep audit cross-references consistent with the
  record, we pass `client_id` into the audit emitter's `dob` slot —
  the audit emitter's hash function only concatenates and SHA-256s
  three strings, so the slot is semantically a "stable per-org
  identity tail." Future audit schema work may rename the field, but
  the hash value stays consistent across both.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

import boto3

from audit import emit as audit_emit

from .record import CentralReachNoteRecord


_s3_client = boto3.client("s3")


_AUDIT_CALL_TYPE = "centralreach_note_ingest"
_AUDIT_PURPOSE = "OPERATIONS"


def _bucket_for_org(org_id: str) -> str:
    return f"penguin-health-{org_id}"


def _record_s3_key(
    *, record: CentralReachNoteRecord,
    captured_at_compact: str, ingest_date: str,
) -> str:
    """`data/{YYYY-MM-DD}/{YYYYMMDDTHHMMSSZ}__{source_record_id}.json`.

    Matches `rpa.result_writer.s3_key_for` so the rules engine's
    existing `data/` reads consume centralreach records without any
    path change. (PDFs use a sibling `pdfs/` prefix; see
    `pdf_storage.pdf_s3_key`.)
    """
    return (
        f"data/{ingest_date}/"
        f"{captured_at_compact}__{record.source_record_id}.json"
    )


@dataclass(frozen=True)
class IdentityForAudit:
    """Raw identity values the audit emitter needs to derive the hash.

    Held in-memory just long enough to call `persist_note`. Never
    persisted, never logged. The `client_id` populates the audit
    emitter's `dob` slot so the audit-derived hash matches the
    record's `patient_hash` — see the module docstring's
    "Audit-hash subtlety" note.
    """

    first_name: str
    last_name: str
    client_id: str    # str(BillingEntry.client_id)


def persist_note(
    *,
    record: CentralReachNoteRecord,
    identity: IdentityForAudit,
    captured_at_compact: str,
    ingest_date: str,
    actor: dict,
    s3_client: Any | None = None,
    audit_emit_fn: Callable = audit_emit,
) -> dict:
    """Write the record JSON to S3 and emit the audit event.

    Returns `{s3_bucket, s3_key, source_record_id, patient_hash}` so
    the pipeline can include counts and identifiers in the
    run-completed event.

    The S3 client and audit emitter are injectable so tests can pin
    them to moto-backed or recording fakes; production callers omit
    them.
    """
    bucket = _bucket_for_org(record.org_id)
    key = _record_s3_key(
        record=record,
        captured_at_compact=captured_at_compact,
        ingest_date=ingest_date,
    )
    body = json.dumps(record.to_json_dict(), separators=(",", ":")).encode("utf-8")

    client = s3_client if s3_client is not None else _s3_client
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    # Audit: per the audit emitter contract, only the hash + initials
    # persist; raw identity is dropped at the emitter boundary. We
    # pass `client_id` into the `dob` slot so the audit-derived hash
    # matches the record's hash (both use the same SHA-256 of
    # first|last|client_id).
    audit_emit_fn(
        action="read",
        resource={
            "type": "ClinicalNote",
            "id": record.source_record_id,
            "org": record.org_id,
        },
        actor=actor,
        org_id=record.org_id,
        purpose_of_use=_AUDIT_PURPOSE,
        call_type=_AUDIT_CALL_TYPE,
        external_control_number=record.ingest_run_id,
        patient={
            "first_name": identity.first_name,
            "last_name": identity.last_name,
            "dob": identity.client_id,  # see Audit-hash subtlety in module docstring
        },
        member_id=record.patient.source_patient_id,
        result={
            "vendor": record.vendor,
            "visit_date": record.encounter.visit_date,
            "note_type": record.encounter.note_type,
            "s3_key": key,
            "pdf_s3_key": record.extracted_fields.get("pdf_s3_key"),
            "template_id": record.extracted_fields.get("template_id"),
        },
    )

    return {
        "s3_bucket": bucket,
        "s3_key": key,
        "source_record_id": record.source_record_id,
        "patient_hash": record.patient.patient_hash,
    }
