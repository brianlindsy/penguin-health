"""The on-disk JSON shape for one centralreach-extracted clinical note.

The rules engine consumes records via the same path it reads any other
chart record — see `lambda/multi-org/rules-engine/document_validator.py`.
The keys on this dataclass match what the engine reads; renaming or
reshuffling them breaks downstream compatibility.

Differences from `rpa.record.RpaNoteRecord`:
  * `text` and `body_html` are Optional and always None for centralreach.
    The rules engine's narrative-derived rules (2, 3, 11) detect the
    None and route through Bedrock against the PDF instead. See the
    design doc's "Bedrock rule evaluation" section.
  * `extracted_fields.pdf_s3_key` is required — points to the PDF bytes
    on the per-org bucket.
  * `extracted_fields.template_id` is the CR templateId for ops
    visibility. Not used for dispatch (the design is template-agnostic).
  * `patient_hash` is derived from
    `(first_name, last_name, str(client_id))` rather than
    `(first_name, last_name, dob)`. DOB isn't available without
    additional Bedrock work; ClientId is more stable per-org and we
    don't need cross-org dedup. See the design doc's locked decision #6.
  * `source` is `"centralreach.api"` (was `"rpa.{vendor}"`).
  * `ingest_run_id` replaces `playbook_run_id` — the centralreach path
    has no playbook, but the field name needs to stay distinct for
    every-run trace correlation in the audit table.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from typing import Any


_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")  # tolerates date-only or full ISO
_WHITESPACE = re.compile(r"\s+")


# Identity keys we refuse to allow in `extracted_fields` so a buggy
# upstream parser cannot sidestep the patient hash by smuggling raw
# names/DOB into the record body. Mirrors `rpa.record._FORBIDDEN_PATIENT_KEYS`.
_FORBIDDEN_PATIENT_KEYS = frozenset({
    "first_name", "last_name", "first", "last", "name",
    "dob", "date_of_birth", "birth_date", "ssn",
})

# Source attribution. Identifies records produced by this module across
# the storage layer regardless of which vendor they came from.
SOURCE = "centralreach.api"


def patient_hash_from_client_id(
    first_name: str | None,
    last_name: str | None,
    client_id: int | str | None,
) -> str:
    """SHA-256 hash of `first|last|client_id` (lowercased + stripped).

    The third slot is CR's per-org client id rather than DOB. The
    hash is per-org stable (ClientIds are per-org) and we do not
    rely on it for cross-org deduplication — the rules engine looks
    up records by (org_id, patient_hash) together.

    Why not reuse `audit.schema.patient_hash`: that function names
    its third parameter `dob`, which would mislead readers when we
    pass a ClientId. We define our own with explicit naming.
    """
    raw = (
        f"{(first_name or '').strip().lower()}|"
        f"{(last_name or '').strip().lower()}|"
        f"{(str(client_id) if client_id is not None else '').strip()}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def narrative_hash(narrative_text: str) -> str:
    """SHA-256 of the narrative after lowercasing and collapsing
    whitespace.

    Identical to `rpa.record.narrative_hash` so deterministic_evaluator's
    op_narrative_hash_unique rule produces the same key for the same
    text regardless of source module. Empty/missing text returns the
    hash of the empty string — the rules engine treats those as
    "no narrative" rather than a collision.
    """
    normalized = _WHITESPACE.sub(" ", (narrative_text or "").strip().lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CentralReachPatient:
    """Patient identity envelope on the record.

    The runner holds raw first/last/DOB in memory just long enough to
    compute the hash, then drops them. The persisted record contains
    only `patient_hash`, `source_patient_id` (CR's ClientId), and
    initials.
    """

    patient_hash: str
    source_patient_id: str   # str(ClientId)
    initials: str

    def __post_init__(self) -> None:
        if not _HEX64.match(self.patient_hash):
            raise ValueError(
                "patient_hash must be a 64-char lowercase hex sha256; got "
                f"{len(self.patient_hash)} chars"
            )
        if not self.source_patient_id:
            raise ValueError("source_patient_id is required")
        if not self.initials:
            raise ValueError("initials is required (e.g., 'JD')")


@dataclass(frozen=True)
class CentralReachEncounter:
    """Visit-level fields the rules engine reads."""

    visit_date: str          # must start with YYYY-MM-DD
    provider_display: str
    note_type: str

    def __post_init__(self) -> None:
        if not _ISO_DATE.match(self.visit_date):
            raise ValueError(
                f"visit_date must start with YYYY-MM-DD; got {self.visit_date!r}"
            )


@dataclass(frozen=True)
class CentralReachNoteRecord:
    """One ingested centralreach clinical note, serialized as one JSON
    file in S3.

    Filename: `data/{YYYY-MM-DD}/{YYYYMMDDTHHMMSSZ}__{source_record_id}.json`
    The rules engine reads from this prefix; do not move it.
    """

    schema_version: int
    source: str
    source_record_id: str
    captured_at: str          # ISO-8601 UTC; from the ingest moment
    ingest_run_id: str
    vendor: str               # always "centralreach" for this module
    org_id: str

    patient: CentralReachPatient
    encounter: CentralReachEncounter

    # PDF-only records: both None. The rules engine detects None
    # `text` and routes to Bedrock via `extracted_fields.pdf_s3_key`.
    text: str | None
    body_html: str | None

    extracted_fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError(
                f"schema_version must be 1 in this codebase; got {self.schema_version}"
            )
        if self.source != SOURCE:
            raise ValueError(
                f"source must be {SOURCE!r}; got {self.source!r}"
            )
        if not self.source_record_id:
            raise ValueError("source_record_id is required")

        # Either text (HTML path, future) or pdf_s3_key (PDF path,
        # current) must be set. The rules engine reads one or the
        # other depending on which is populated.
        has_text = bool(self.text)
        has_pdf = bool(self.extracted_fields.get("pdf_s3_key"))
        if not (has_text or has_pdf):
            raise ValueError(
                "either `text` or `extracted_fields.pdf_s3_key` must be set"
            )

        bad = _FORBIDDEN_PATIENT_KEYS & set(self.extracted_fields.keys())
        if bad:
            raise ValueError(
                "extracted_fields must not carry raw PHI identity fields; "
                f"forbidden keys present: {sorted(bad)}"
            )

    def to_json_dict(self) -> dict[str, Any]:
        """Plain dict ready for json.dumps. The rules engine reads
        from this exact shape — field names here must match what the
        engine consumes."""
        return asdict(self)
