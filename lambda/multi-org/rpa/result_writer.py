"""Persist one extracted clinical note: JSON to S3 + audit event.

Single PHI-writing path for the RPA integration. The playbook engine
yields an extraction dict (vendor-shaped); this module shapes it into an
`RpaNoteRecord`, writes the JSON file the rules engine will consume, and
emits the `read ClinicalNote` audit event.

PHI handling:
  * Raw `first_name`, `last_name`, `dob`, `source_patient_id` are required
    to construct the patient hash and initials but live only in the in-memory
    extraction dict — they are NEVER written to logs or audit event fields.
  * `audit.schema.patient_hash` produces the hash used in the record and the
    audit event.
  * Audit event records only the hash + last-4 of `source_patient_id`.
  * The JSON payload itself is encrypted at rest via the per-org bucket's
    bucket-level KMS configuration; no per-object override required.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Any, Callable

import boto3

from audit import emit as audit_emit
from audit.schema import patient_hash as compute_patient_hash

from .record import RpaEncounter, RpaNoteRecord, RpaPatient, narrative_hash


_s3_client = boto3.client("s3")


def _bucket_for_org(org_id: str) -> str:
    """Mirror of `lambda/api/nl_agent_tools.py:org_data_bucket` + the SFTP/
    FHIR ingestion convention. Kept inline because the existing helper
    lives in the API Lambda asset; the Fargate runner ships separately.
    """
    return f"penguin-health-{org_id}"


def _initials(first_name: str, last_name: str) -> str:
    f = (first_name or "").strip()[:1].upper() or "?"
    l = (last_name or "").strip()[:1].upper() or "?"
    return f"{f}{l}"


def _last4(value: str) -> str:
    v = (value or "").strip()
    return v[-4:] if len(v) >= 4 else v


def build_record(
    *,
    extraction: dict,
    org_id: str,
    vendor: str,
    playbook_run_id: str,
    captured_at: str,
) -> RpaNoteRecord:
    """Shape a raw extraction dict into an `RpaNoteRecord`.

    `extraction` is what the playbook engine emits via the `emit_note`
    op. Required keys:
        source_record_id   (vendor's stable id for the note)
        first_name, last_name, dob, source_patient_id (for hash + initials)
        visit_date, provider_display, note_type
        text               (plain-text rendering for the rules engine)
        body_html          (original HTML for reproducibility)
        extracted_fields   (everything else from the playbook — the clinical
                            fields driving the rule suite)
    """
    patient = RpaPatient(
        patient_hash=compute_patient_hash(
            extraction["first_name"],
            extraction["last_name"],
            extraction["dob"],
        ),
        source_patient_id=extraction["source_patient_id"],
        initials=_initials(extraction["first_name"], extraction["last_name"]),
    )
    encounter = RpaEncounter(
        visit_date=extraction["visit_date"],
        provider_display=extraction["provider_display"],
        note_type=extraction["note_type"],
    )
    extracted_fields = dict(extraction.get("extracted_fields") or {})
    extracted_fields.setdefault("narrative_hash", narrative_hash(extraction["text"]))
    return RpaNoteRecord(
        schema_version=1,
        source=f"rpa.{vendor}",
        source_record_id=extraction["source_record_id"],
        captured_at=captured_at,
        playbook_run_id=playbook_run_id,
        vendor=vendor,
        org_id=org_id,
        patient=patient,
        encounter=encounter,
        text=extraction["text"],
        body_html=extraction["body_html"],
        extracted_fields=extracted_fields,
    )


def s3_key_for(record: RpaNoteRecord, *, captured_at_compact: str,
               ingest_date: str) -> str:
    """`data/{YYYY-MM-DD}/{YYYYMMDDTHHMMSSZ}__{source_record_id}.json`."""
    return (
        f"data/{ingest_date}/"
        f"{captured_at_compact}__{record.source_record_id}.json"
    )


def persist_note(
    *,
    extraction: dict,
    org_id: str,
    vendor: str,
    playbook_run_id: str,
    captured_at: str,
    ingest_date: str,
    captured_at_compact: str,
    actor: dict,
    s3_client: Any = None,
    audit_emit_fn: Callable = audit_emit,
) -> dict:
    """Build the record, PUT it to the per-org bucket, emit the audit event.

    Returns `{s3_bucket, s3_key, source_record_id, patient_hash}` so the
    runner can include counts + identifiers in the run-completed event.

    The S3 client and audit emitter are injectable so tests can pin them
    to moto-backed instances; defaults wire to the module-level boto3
    client and the real `audit.emit`.
    """
    record = build_record(
        extraction=extraction,
        org_id=org_id,
        vendor=vendor,
        playbook_run_id=playbook_run_id,
        captured_at=captured_at,
    )
    bucket = _bucket_for_org(org_id)
    key = s3_key_for(
        record,
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

    # Audit: hand the trusted audit module raw identity fields so it can
    # derive the canonical hash + initials and the `member_id_last4` form
    # itself. Per the existing emitter contract (lambda/multi-org/audit/
    # schema.py:144-156), only the hash + initials persist; raw values
    # are dropped at the emitter boundary. Note body and HTML are NEVER
    # passed in — they live only in the encrypted S3 payload.
    audit_emit_fn(
        action="read",
        resource={
            "type": "ClinicalNote",
            "id": record.source_record_id,
            "org": org_id,
        },
        actor=actor,
        org_id=org_id,
        purpose_of_use="OPERATIONS",
        call_type="rpa_note_extraction",
        external_control_number=playbook_run_id,
        patient={
            "first_name": extraction["first_name"],
            "last_name": extraction["last_name"],
            "dob": extraction["dob"],
        },
        member_id=extraction["source_patient_id"],
        result={
            "vendor": vendor,
            "visit_date": record.encounter.visit_date,
            "note_type": record.encounter.note_type,
            "s3_key": key,
        },
    )

    return {
        "s3_bucket": bucket,
        "s3_key": key,
        "source_record_id": record.source_record_id,
        "patient_hash": record.patient.patient_hash,
    }
