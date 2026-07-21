"""Build a `CentralReachNoteRecord` from `BillingEntry` + `PreviewResponse`.

The runner calls `build_record()` once per signed entry after PDF
upload. The function projects the typed wrappers and the resolved S3
key into the record dataclass, which validates and then serializes to
JSON for upload to the data prefix.

Field mapping (per the design doc):

| Record field                    | Source                              |
|---------------------------------|-------------------------------------|
| source_record_id                | BillingEntry.id (CR's billing id)   |
| patient.patient_hash            | first+last+ClientId hash            |
| patient.source_patient_id       | BillingEntry.client_id              |
| patient.initials                | derived from first+last names       |
| encounter.visit_date            | derived from DateOfService          |
| encounter.provider_display      | provider first+last names           |
| encounter.note_type             | split ProcedureCodeString on `: `   |
| text                            | None (PDF path)                     |
| body_html                       | None (PDF path)                     |
| extracted_fields.pdf_s3_key     | written by pdf_storage              |
| extracted_fields.preview_file_id | CR file id from preview.files       |
| extracted_fields.template_id    | preview.template_id                 |
| extracted_fields.service_code   | split ProcedureCodeString left      |
| extracted_fields.signed_at      | preview.signed_at                   |
| extracted_fields.provider_signature | preview.provider_signature_present |
| extracted_fields.supervisor_signature | same (v1 templates)         |
| extracted_fields.billing_list_* | Every non-identity column CR        |
|                                 | returned on the list endpoint,      |
|                                 | mapped 1:1 from PascalCase to       |
|                                 | snake_case. `billing_list_` is the  |
|                                 | canonical form for list-endpoint    |
|                                 | values — no short-name aliases.     |
|                                 | See `_BILLING_LIST_MAP`.            |
| extracted_fields.note_provider_location       | NoteFields (from PDF) |
| extracted_fields.note_provider_billed_time    | NoteFields (from PDF) |
| extracted_fields.note_provider_billed         | NoteFields (from PDF) |
| extracted_fields.note_provider_signature_name | NoteFields (from PDF) |
| extracted_fields.note_supervisor_name         | NoteFields (from PDF) — from the supervisor-attribution section |
| extracted_fields.note_supervisor_signature_names | NoteFields (from PDF) — list of every name signed as a supervisor; a note may carry more than one |

Two prefixes signal source unambiguously so rules and dashboards can
tell where a value came from at a glance:
  * `billing_list_*` — from CR's `/billing/query` list endpoint,
    PascalCase → snake_case. Canonical form for list-endpoint values.
  * `note_*` — Bedrock-extracted from the rendered PDF at ingest time
    (`note_fields_extractor.extract_note_fields`).

Naming policy: list-endpoint values live ONLY under `billing_list_*`.
Rule authors reference e.g. `billing_list_time_worked_in_mins`, not
a short-name alias. This keeps one value under one key — no
ambiguity for rule authors, no dead aliases to keep in sync.

Fields sourced from the preview endpoint (`signed_at`, `provider_signature`,
`supervisor_signature`) do not use a prefix; the preview endpoint's
values are the record's authoritative signature metadata, and adding
a `preview_` prefix would create noise without resolving ambiguity
(nothing else sources those names).

The list-endpoint dump skips patient/provider identity columns (they
already flow through `patient.patient_hash` / `encounter.provider_display`
and the record dataclass validator rejects records that smuggle them
into `extracted_fields`).
"""

from __future__ import annotations

from typing import Any

from .list_query import BillingEntry
from .note_fields_extractor import NoteFields
from .preview import PreviewResponse
from .record import (
    CentralReachEncounter,
    CentralReachNoteRecord,
    CentralReachPatient,
    SOURCE,
    narrative_hash,
    patient_hash_from_client_id,
)


_VENDOR = "centralreach"


# BillingEntry attribute → `extracted_fields.billing_list_<name>` key.
# EVERY list-endpoint column is emitted under this prefix; the
# `billing_list_` name is the canonical form for any value that came
# off `/crxapi/internal/billing/query`. Only patient/provider identity
# columns are omitted:
#   * `id` is `source_record_id` on the record
#   * `client_id` is `patient.source_patient_id`
#   * `client_first_name`, `client_last_name` are patient PHI —
#     `patient.patient_hash` + `patient.initials` cover the need
#   * `provider_id`, `provider_first_name`, `provider_last_name` are
#     workforce PII — `encounter.provider_display` covers the need
#
# `date_of_service` and `procedure_code_string` are also emitted here
# even though they feed the top-level `encounter.visit_date` /
# `encounter.note_type` fields; the encounter block carries the
# processed form (sliced date, split note-type), while
# `billing_list_*` carries the raw column verbatim.
_BILLING_LIST_MAP: tuple[str, ...] = (
    "date_of_service",
    "procedure_code_string",
    "procedure_code_id",
    "location",
    "time_worked_in_mins",
    "units_of_service",
    "date_time_from",
    "date_time_to",
    "creation_date",
    "is_void",
    "is_deleted",
    "is_locked",
    "timezone",
    "voided_date",
    "deleted_date",
    "last_paid",
    "last_billed",
    "first_billed_date",
    "modified_date",
    "schedule_date",
    "authorization_id",
    "authorization_resource_id",
    "service_location_id",
    "calc_type",
    "drive_time_minutes",
    "mileage",
    "labels",
    "code_labels",
    "resource_count",
    "payor_id",
    "payor_insurance_id",
    "payor_name",
    "rate_client",
    "rate_client_agreed",
    "rate_client_drive_hourly",
    "rate_client_drive_mileage",
    "invoiced",
    "payments_made",
    "exported",
    "claims",
    "claims_exported",
    "group_count",
    "group_id",
    "client_charges",
    "client_charges_agreed",
    "drive_time_charges",
    "mileage_charges",
    "client_charges_total",
    "client_charges_total_agreed",
    "amount_owed",
    "amount_owed_agreed",
    "amount_paid",
    "amount_adjustment",
    "copay_owed",
    "copay_amount",
    "tasks",
    "tasks_completed",
    "show_agreed",
    "schedule_course",
    "schedule_auth",
    "schedule_code",
    "schedule_ordinal",
    "time_worked_from_utc_offset",
    "time_zone_abbr",
)


def _initials(first_name: str, last_name: str) -> str:
    """Build `"FL"` from first and last names.

    Falls back to `"?"` if either is empty. Matches
    `rpa.result_writer._initials` so downstream consumers see the same
    shape regardless of source module.
    """
    f = (first_name or "").strip()[:1].upper() or "?"
    l = (last_name or "").strip()[:1].upper() or "?"
    return f"{f}{l}"


def _visit_date(date_of_service: str | None) -> str:
    """Slice the YYYY-MM-DD prefix from CR's ISO timestamp.

    The record's `encounter.visit_date` validator requires a string
    starting with YYYY-MM-DD; CR's DateOfService is a full ISO
    timestamp like `"2026-06-28T17:00:00.0000000"`. Slicing the
    first 10 chars matches the validator's expectation.
    """
    if not date_of_service:
        return ""
    return str(date_of_service)[:10]


def _split_procedure_code(raw: str | None) -> tuple[str, str]:
    """Split CR's `"97155: Treatment Planning - BCBA"` into
    `("97155", "Treatment Planning - BCBA")`.

    Returns `("", raw or "")` if there's no `: ` separator — the
    record's `note_type` is allowed to be the whole string, but
    `service_code` is intentionally empty rather than guessed.
    """
    if not raw:
        return "", ""
    parts = raw.split(": ", 1)
    if len(parts) != 2:
        return "", raw.strip()
    return parts[0].strip(), parts[1].strip()


def _provider_display(entry: BillingEntry) -> str:
    """Concatenate the billing entry's provider first and last name.

    The billing entry's provider fields are the session's rendering
    provider — the person who delivered the service, distinct from
    any signer on the note.
    """
    return f"{entry.provider_first_name} {entry.provider_last_name}".strip()


def build_record(
    *,
    entry: BillingEntry,
    preview: PreviewResponse,
    pdf_s3_key: str,
    preview_file_id: int,
    narrative_text: str,
    note_fields: NoteFields,
    org_id: str,
    ingest_run_id: str,
    captured_at: str,
) -> CentralReachNoteRecord:
    """Project the typed API wrappers into a CentralReachNoteRecord.

    Required inputs:
      * `entry` from the list query
      * `preview` from the per-entry preview endpoint
      * `pdf_s3_key` from a prior call to `pdf_storage.write_pdf`
      * `preview_file_id` — the CR file/resource id the pipeline chose
        from `preview.files` (via `first_accessible_file`). Surfaces on
        the record so the document validation list can deep-link to
        the file screen in CentralReach.
      * `narrative_text` from a prior call to
        `narrative_extractor.extract_narrative` — the provider's prose
        Bedrock pulled out of the PDF. The record's `text` field gets
        this verbatim, and `extracted_fields.narrative_hash` is
        computed from it (rule 1 dedup signal).
      * `note_fields` from a prior call to
        `note_fields_extractor.extract_note_fields` — six structured
        fields Bedrock read off the rendered PDF (location, billed
        time, provider/supervisor names, and the list of every name
        that signed as a supervisor). Every field is optional; the
        builder omits `note_*` keys that are None (or, for the
        supervisor-signature list, an empty tuple) so downstream rules
        can distinguish "extractor found nothing there" from a bogus
        empty-string match.
      * `org_id` and `ingest_run_id` from the runner
      * `captured_at` ISO-8601 UTC string from the runner's clock

    Raises `ValueError` if any of the record dataclass validators
    reject — typically missing visit_date (CR omitted DateOfService)
    or empty source_record_id.
    """
    service_code, note_type = _split_procedure_code(entry.procedure_code_string)

    patient = CentralReachPatient(
        patient_hash=patient_hash_from_client_id(
            entry.client_first_name,
            entry.client_last_name,
            entry.client_id,
        ),
        source_patient_id=str(entry.client_id),
        initials=_initials(entry.client_first_name, entry.client_last_name),
    )

    encounter = CentralReachEncounter(
        visit_date=_visit_date(entry.date_of_service),
        provider_display=_provider_display(entry),
        note_type=note_type,
    )

    # extracted_fields carries everything the rules engine consumes
    # that isn't in the dataclass's structured shape. Identity keys
    # are forbidden — the dataclass's validator rejects records that
    # leak first_name/last_name/dob/etc. here.
    #
    # No canonical-short-name aliases for list-endpoint columns:
    # every list-endpoint value lives under a `billing_list_*` key
    # (see the `_BILLING_LIST_MAP` loop below). Rules reference the
    # `billing_list_*` name directly.
    extracted_fields: dict[str, Any] = {
        "pdf_s3_key": pdf_s3_key,
        "preview_file_id": preview_file_id,
        "text_source": "pdf_bedrock_extracted",
        "narrative_hash": narrative_hash(narrative_text),
        "template_id": preview.template_id,
        "service_code": service_code,
        "provider_signature": preview.provider_signature_present,
        "supervisor_signature": preview.provider_signature_present,  # same in v1 templates
    }
    if preview.signed_at:
        extracted_fields["signed_at"] = preview.signed_at

    # Bedrock-extracted note fields. Every one is optional; None means
    # "not present on the note" and gets omitted so a rule can
    # distinguish absence from a match against an empty string.
    if note_fields.provider_location is not None:
        extracted_fields["note_provider_location"] = note_fields.provider_location
    if note_fields.provider_billed_time is not None:
        extracted_fields["note_provider_billed_time"] = note_fields.provider_billed_time
    if note_fields.provider_billed is not None:
        extracted_fields["note_provider_billed"] = note_fields.provider_billed
    if note_fields.provider_signature_name is not None:
        extracted_fields["note_provider_signature_name"] = note_fields.provider_signature_name
    if note_fields.supervisor_name is not None:
        extracted_fields["note_supervisor_name"] = note_fields.supervisor_name
    if note_fields.supervisor_signature_names:
        # Emit as a list so downstream rules see the full set of
        # supervisor signers. Rule 7's supervisor-name match passes
        # when the expected name equals any element.
        extracted_fields["note_supervisor_signature_names"] = list(
            note_fields.supervisor_signature_names,
        )

    # `billing_list_*` — everything CR sent on the list endpoint for
    # this entry, mapped 1:1 to snake_case. Emitted verbatim (empty
    # strings and zeros included) so rules can distinguish "CR sent
    # zero" from "CR omitted the field entirely" via the raw record —
    # the record dataclass reserializes the dict as-is.
    for attr in _BILLING_LIST_MAP:
        extracted_fields[f"billing_list_{attr}"] = getattr(entry, attr)

    return CentralReachNoteRecord(
        schema_version=1,
        source=SOURCE,
        source_record_id=str(entry.id),
        captured_at=captured_at,
        ingest_run_id=ingest_run_id,
        vendor=_VENDOR,
        org_id=org_id,
        patient=patient,
        encounter=encounter,
        text=narrative_text,
        body_html=None,
        extracted_fields=extracted_fields,
    )
