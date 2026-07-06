"""Preview endpoint — `GET /crxapi/billing/billing-entries/{id}/preview`.

Returns the per-entry metadata the pipeline reads: signature info,
supervising provider, templateId, and pointers to the structured note
(`betterNotes[0].id`) and the rendered PDF resource
(`files[0].id` — the input to `resources.getresourceurl`).

Two pipeline-level conditions are diagnosed from this response:

  * `has_pdf_available()` — `files == []` means the entry is unsigned
    or otherwise has no rendered PDF; the pipeline skips with
    `no_pdf_available`.
  * `has_better_note()` — `betterNotes == []` means there is no
    structured note. The PDF-only pipeline doesn't read better notes
    so this is informational, retained for future use.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .client import CentralReachClient


def _preview_path(billing_entry_id: int) -> str:
    return f"/crxapi/billing/billing-entries/{billing_entry_id}/preview"


# Names of the "second file" the pipeline should prefer look like
# `07/02/2026 <client> ... Note by <provider>` — a leading MM/DD/YYYY
# date. Anchoring on the date is enough to distinguish the clinical
# note from the other attachment that comes back first.
_MMDDYYYY = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")


@dataclass(frozen=True)
class PreviewFile:
    """One file resource attached to the billing entry.

    `id` is what the runner sends to `resources.getresourceurl` as the
    `resourceId` — NOT the billing entry id. (This distinction tripped
    up the early design; see the design doc's pipeline section.)
    """

    id: int
    name: str
    is_archived: bool
    has_access: bool

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "PreviewFile":
        return cls(
            id=int(raw["id"]),
            name=str(raw.get("name") or ""),
            is_archived=bool(raw.get("isArchived", False)),
            has_access=bool(raw.get("hasAccess", False)),
        )


@dataclass(frozen=True)
class PreviewBetterNote:
    """One structured note attached to the entry.

    The pipeline does not currently read structured notes — the
    PDF-only ingest path goes through `files` instead. Retained on the
    dataclass for diagnostic logging and to keep the door open if a
    future PR adds HTML-based extraction for a subset of templates.
    """

    id: int
    template_id: int
    name: str

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "PreviewBetterNote":
        return cls(
            id=int(raw["id"]),
            template_id=int(raw.get("templateId") or 0),
            name=str(raw.get("name") or ""),
        )


@dataclass(frozen=True)
class PreviewResponse:
    """The fields the pipeline reads from the preview response.

    The full response has 60+ fields; we model only what's used. Raw
    response is retained on the dataclass so callers can pull
    additional fields without round-tripping CR.
    """

    billing_entry_id: int

    # Fields from the `fields` subobject
    provider_full_name: str          # from providerSignatureName when signed; else providerName
    provider_signature_present: bool  # bool from providerSignature length
    signed_at: str | None             # ISO from providerSignatureCreationDate; None if unsigned

    # Resource references
    files: tuple[PreviewFile, ...]
    better_notes: tuple[PreviewBetterNote, ...]

    # Retained raw response so the record builder can read additional
    # fields without re-parsing the JSON.
    raw: dict[str, Any]

    @property
    def has_pdf_available(self) -> bool:
        """True if at least one file the bot has access to is
        attached and not archived. Drives the pipeline's no_pdf_available
        skip branch."""
        return any(
            f.has_access and not f.is_archived for f in self.files
        )

    @property
    def first_accessible_file(self) -> PreviewFile | None:
        """The file the pipeline should ingest.

        With 2+ accessible files, CR returns an unrelated attachment
        first and the clinical note second. When the second accessible
        file's name carries a MM/DD/YYYY date (matching the
        `07/02/2026 <client> Note by <provider>` shape), prefer it.
        Otherwise fall back to the first accessible file so the
        single-file case is unchanged.
        """
        accessible = [
            f for f in self.files if f.has_access and not f.is_archived
        ]
        if not accessible:
            return None
        if len(accessible) >= 2 and _MMDDYYYY.search(accessible[1].name):
            return accessible[1]
        return accessible[0]

    @property
    def template_id(self) -> int | None:
        """The CR templateId for the first better note, if any. Retained
        as `extracted_fields.template_id` on the record for ops
        visibility."""
        if not self.better_notes:
            return None
        return self.better_notes[0].template_id

    @classmethod
    def from_json(cls, billing_entry_id: int, raw: dict[str, Any]) -> "PreviewResponse":
        fields = raw.get("fields") or {}
        signature_blob = fields.get("providerSignature")
        signature_present = bool(signature_blob) and len(str(signature_blob)) > 0
        signed_at = fields.get("providerSignatureCreationDate") or None
        provider_name = (
            fields.get("providerSignatureName")
            or fields.get("providerName")
            or ""
        )

        files = tuple(
            PreviewFile.from_json(f)
            for f in (raw.get("files") or [])
        )
        better_notes = tuple(
            PreviewBetterNote.from_json(n)
            for n in (raw.get("betterNotes") or [])
        )

        return cls(
            billing_entry_id=billing_entry_id,
            provider_full_name=str(provider_name),
            provider_signature_present=signature_present,
            signed_at=str(signed_at) if signed_at else None,
            files=files,
            better_notes=better_notes,
            raw=raw,
        )


def get_preview(
    client: CentralReachClient, billing_entry_id: int,
) -> PreviewResponse:
    """Fetch the preview for one billing entry.

    Pipeline checks `response.has_pdf_available` before calling
    `resources.get_resource_url`; entries without a PDF skip with
    `no_pdf_available`.
    """
    raw = client.get_json(_preview_path(billing_entry_id))
    return PreviewResponse.from_json(billing_entry_id, raw)
