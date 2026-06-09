"""Audit event schema — single source of truth for field names and values.

The Glue table in infra/components/audit_layer.py mirrors `EVENT_COLUMNS`
below. Adding a field means:
  1. Bump SCHEMA_VERSION.
  2. Add it here and to _AUDIT_EVENT_COLUMNS in audit_layer.py.
  3. Re-deploy (Firehose schema is read from Glue on each batch).

Forbidden in events under any circumstances:
  * Full SSN, full member IDs, full names — store hashes / initials / last4
  * Bedrock prompt bodies, FHIR resource bodies, Textract JSON, chart text
  * Exception messages — `error_class` is the type name only
"""

from __future__ import annotations

import hashlib

SCHEMA_VERSION = "1"


# ----- Enum values --------------------------------------------------------
#
# Kept as plain string constants (not Enum) so events stay JSON-serializable
# without converters. Callers should reference these by name to avoid typos
# becoming silent unmapped values in Athena.

ACTION_READ = "read"
ACTION_WRITE = "write"
ACTION_EXECUTE = "execute"
ACTION_LOGIN = "login"
ACTION_EXPORT = "export"
ACTIONS = (ACTION_READ, ACTION_WRITE, ACTION_EXECUTE, ACTION_LOGIN, ACTION_EXPORT)

OUTCOME_SUCCESS = "success"
OUTCOME_MINOR_FAILURE = "minor-failure"
OUTCOME_SERIOUS_FAILURE = "serious-failure"
OUTCOME_MAJOR_FAILURE = "major-failure"
OUTCOMES = (
    OUTCOME_SUCCESS,
    OUTCOME_MINOR_FAILURE,
    OUTCOME_SERIOUS_FAILURE,
    OUTCOME_MAJOR_FAILURE,
)

PURPOSE_TREATMENT = "TREATMENT"
PURPOSE_PAYMENT = "PAYMENT"
PURPOSE_OPERATIONS = "OPERATIONS"
PURPOSE_ELIGIBILITY = "ELIGIBILITY"
PURPOSE_DOC_PROCESSING = "DOC_PROCESSING"
PURPOSE_ANALYTICS = "ANALYTICS"
PURPOSE_DEMOGRAPHIC_SEARCH = "DEMOGRAPHIC_SEARCH"
PURPOSE_ADMIN_CONFIG = "ADMIN_CONFIG"

AGENT_HUMAN = "human"
AGENT_SYSTEM = "system"


def outcome_for_status(status_code: int | None) -> str:
    """Map an HTTP status code to a HIPAA outcome value.

    A None status (handler didn't return one) is treated as success since
    the only way we reach this branch is via the decorator's try block
    completing normally.
    """
    if status_code is None or 200 <= status_code < 300:
        return OUTCOME_SUCCESS
    if 400 <= status_code < 500:
        return OUTCOME_MINOR_FAILURE
    if 500 <= status_code < 600:
        return OUTCOME_SERIOUS_FAILURE
    # 1xx / 3xx — unusual for an API handler but still success-coded.
    return OUTCOME_SUCCESS


# ----- Patient identity ---------------------------------------------------
#
# Must stay identical to stedi/audit.py:patient_hash so the dedup queries
# on the new GSI keep returning the same rows after cutover. The
# normalization is intentionally permissive (lowercase + strip) — DOB is
# kept verbatim because it arrives as a structured YYYYMMDD string.

def patient_hash(first_name: str | None,
                 last_name: str | None,
                 dob: str | None) -> str:
    raw = (
        f"{(first_name or '').strip().lower()}|"
        f"{(last_name or '').strip().lower()}|"
        f"{(dob or '').strip()}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ----- Result slimming ----------------------------------------------------
#
# Mirrors stedi/audit.py:_slim_result. The Stedi response object is the
# canonical input shape, but the function tolerates dicts that don't have
# `plan` populated so generic write paths can pass through their summary.

def slim_result(result: dict | None) -> dict | None:
    if not result:
        return None
    plan = (result.get("plan") or {}) if isinstance(result, dict) else {}
    return {
        "status": result.get("status"),
        "active": result.get("active"),
        "plan_name": plan.get("name"),
        "effective_date": plan.get("effective_date"),
        "expiration_date": plan.get("expiration_date"),
        "auth_required": result.get("auth_required"),
    }


# ----- Event builder ------------------------------------------------------

def build_event(*,
                event_id: str,
                event_time: str,
                action: str,
                outcome: str,
                purpose_of_use: str,
                org_id: str,
                actor: dict,
                resource: dict,
                source_lambda: str | None = None,
                request_id: str | None = None,
                patient: dict | None = None,
                member_id: str | None = None,
                payer: dict | None = None,
                call_type: str | None = None,
                external_control_number: str | None = None,
                duration_ms: int | None = None,
                result: dict | None = None,
                http_status: int | None = None,
                error_class: str | None = None) -> dict:
    """Assemble the flat event dict. Field omissions become null in
    Athena rather than missing keys — Firehose's JSON deserializer
    handles either, but explicit nulls make Glue partition projection
    easier to reason about."""
    p_hash = None
    p_first = None
    p_last = None
    p_dob = None
    if patient:
        p_hash = patient_hash(
            patient.get("first_name"),
            patient.get("last_name"),
            patient.get("dob"),
        )
        p_first = (patient.get("first_name") or "?")[:1].upper()
        p_last = (patient.get("last_name") or "?")[:1].upper()
        p_dob = patient.get("dob")

    member_id_last4 = None
    if member_id and len(member_id) >= 4:
        member_id_last4 = member_id[-4:]

    payer = payer or {}

    return {
        "event_id": event_id,
        "event_time": event_time,
        "schema_version": SCHEMA_VERSION,
        "action": action,
        "outcome": outcome,
        "purpose_of_use": purpose_of_use,
        "org_id": org_id,
        "agent_type": actor.get("agent_type"),
        "agent_id": actor.get("agent_id"),
        "agent_email": actor.get("agent_email"),
        "agent_groups": actor.get("agent_groups") or [],
        "client_ip": actor.get("client_ip"),
        "user_agent": actor.get("user_agent"),
        "source_lambda": source_lambda,
        "request_id": request_id,
        "resource_type": resource.get("type"),
        "resource_id": resource.get("id"),
        "patient_hash": p_hash,
        "patient_first_initial": p_first,
        "patient_last_initial": p_last,
        "patient_dob": p_dob,
        "member_id_last4": member_id_last4,
        "payer_id": payer.get("id"),
        "payer_name": payer.get("name"),
        "call_type": call_type,
        "external_control_number": external_control_number,
        "duration_ms": int(duration_ms) if duration_ms is not None else None,
        # Glue stores result_summary as a JSON-encoded string to keep the
        # Parquet schema simple — flatten via Athena `json_extract` when
        # querying.
        "result_summary": slim_result(result),
        "http_status": http_status,
        "error_class": error_class,
    }
