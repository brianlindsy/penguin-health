"""The on-disk JSON shape for a single RPA-extracted clinical note.

This is the contract the rules engine reads — the top-level `text` field
satisfies the existing `data['text']` consumer at
lambda/multi-org/rules-engine/document_validator.py without any engine
changes.

The class uses stdlib dataclasses + manual validation rather than Pydantic
to avoid adding a runtime dependency for one consumer. If a second module
needs structured validation we can revisit.

Forbidden fields are rejected to keep raw PHI (names, DOB) out of the
record body — the `patient_hash` derived in audit.schema.patient_hash is
the only patient identity that should travel with the record.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Any
import re


_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")  # tolerates date-only or full iso

_FORBIDDEN_PATIENT_KEYS = frozenset({
    "first_name", "last_name", "first", "last", "name",
    "dob", "date_of_birth", "birth_date", "ssn",
})


@dataclass(frozen=True)
class RpaPatient:
    patient_hash: str
    source_patient_id: str
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
class RpaEncounter:
    visit_date: str
    provider_display: str
    note_type: str

    def __post_init__(self) -> None:
        if not _ISO_DATE.match(self.visit_date):
            raise ValueError(
                f"visit_date must start with YYYY-MM-DD; got {self.visit_date!r}"
            )


@dataclass(frozen=True)
class RpaNoteRecord:
    """One extracted clinical note, serialized as one JSON file in S3.

    Filename: `data/{YYYY-MM-DD}/{YYYYMMDDTHHMMSSZ}__{source_record_id}.json`
    where source_record_id matches this record's `source_record_id`.
    """

    schema_version: int
    source: str  # "rpa.{vendor}"
    source_record_id: str
    captured_at: str  # iso8601
    playbook_run_id: str
    vendor: str
    org_id: str

    patient: RpaPatient
    encounter: RpaEncounter

    text: str  # plain-text rendering — the rules engine's data['text']
    body_html: str
    extracted_fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError(
                f"schema_version must be 1 in this codebase; got {self.schema_version}"
            )
        if not self.source.startswith("rpa."):
            raise ValueError(
                f"source must start with 'rpa.'; got {self.source!r}"
            )
        if not self.source_record_id:
            raise ValueError("source_record_id is required")
        if not self.text:
            raise ValueError(
                "text is required — this is what the rules engine reads"
            )
        bad = _FORBIDDEN_PATIENT_KEYS & set(self.extracted_fields.keys())
        if bad:
            raise ValueError(
                "extracted_fields must not carry raw PHI identity fields; "
                f"forbidden keys present: {sorted(bad)}"
            )

    def to_json_dict(self) -> dict[str, Any]:
        """Plain dict ready for json.dumps. Frozen dataclasses serialize fine,
        but we use asdict so consumers see plain dicts, not dataclass instances.
        """
        return asdict(self)
