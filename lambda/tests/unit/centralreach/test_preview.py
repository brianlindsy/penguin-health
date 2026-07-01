"""Tests for centralreach.preview.

Pins three contracts:
  1. The response shape we parse matches the captured CR response
  2. `has_pdf_available` correctly distinguishes the unsigned/draft
     skip case from the normal-ingest case
  3. Signature fields surface correctly (presence as bool, never raw
     bytes)
"""

from __future__ import annotations

from centralreach.preview import (
    PreviewBetterNote,
    PreviewFile,
    PreviewResponse,
    get_preview,
)


# ----- fixtures -------------------------------------------------------------


# Redacted version of the captured preview response. Field names match
# what we observed in `centralreach-api-preview-response.txt`.
_SIGNED_PREVIEW_RESPONSE = {
    "fields": {
        "id": 1234,
        "organizationId": 5678,
        "clientId": 9012,
        "clientName": "[FIRST]",
        "providerId": 3456,
        "providerName": "[PFIRST]",
        "timeWorkedFrom": "2026-06-28T17:45:00.0000000Z",
        "timeWorkedTo": "2026-06-28T19:00:00.0000000Z",
        "timeWorkedMins": 75,
        "serviceLocationName": "12: Home",
        "procedureCodeId": 7890,
        "procedureCodeString": "97155: Treatment Planning - BCBA",
        "creationDate": "2026-06-28T22:59:37.1870000Z",
        # The signed-note signals
        "providerSignature": "",   # empty in this capture but signed
        "providerSignatureName": "[FIRST] [LAST]",
        "providerSignatureCreationDate": "2026-06-28T22:59:32.0000000Z",
        "savedSignatureProviderId": 4567,
    },
    "files": [
        {
            "id": 8901,
            "name": "[REDACTED FILENAME]",
            "isArchived": False,
            "hasAccess": True,
        },
    ],
    "notes": [],
    "betterNotes": [
        {
            "id": 1357,
            "organizationId": 5678,
            "createdById": 2468,
            "createdOn": "2026-06-28T22:59:15.0300000Z",
            "name": "[REDACTED]",
            "templateId": 113875,
            "hasPermission": True,
        },
    ],
    "result": "OK",
    "failed": False,
}


# An unsigned/draft entry — no files, no betterNotes, no signature.
# Triggers the pipeline's `no_pdf_available` skip branch.
_DRAFT_PREVIEW_RESPONSE = {
    "fields": {
        "id": 1234,
        "providerName": "[FIRST]",
        "providerSignature": "",
        "providerSignatureName": "",
        "providerSignatureCreationDate": None,
    },
    "files": [],
    "notes": [],
    "betterNotes": [],
    "result": "OK",
    "failed": False,
}


class _StubClient:
    """Test double — returns canned `get_json` payloads and records
    request paths."""

    def __init__(self, payload):
        self._payload = payload
        self.last_path: str | None = None

    def get_json(self, path):
        self.last_path = path
        return self._payload


# ----- request path --------------------------------------------------------


def test_get_preview_uses_correct_path():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    get_preview(client, billing_entry_id=42)
    assert client.last_path == "/crxapi/billing/billing-entries/42/preview"


# ----- response parsing ----------------------------------------------------


def test_signed_response_parses_fields():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)

    assert response.billing_entry_id == 1234
    assert response.signed_at == "2026-06-28T22:59:32.0000000Z"
    assert response.provider_full_name == "[FIRST] [LAST]"


def test_signed_response_files_parsed():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert len(response.files) == 1
    f = response.files[0]
    assert isinstance(f, PreviewFile)
    assert f.id == 8901
    assert f.has_access is True
    assert f.is_archived is False


def test_signed_response_better_notes_parsed():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert len(response.better_notes) == 1
    n = response.better_notes[0]
    assert isinstance(n, PreviewBetterNote)
    assert n.id == 1357
    assert n.template_id == 113875


def test_template_id_property():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.template_id == 113875


def test_template_id_returns_none_when_no_better_notes():
    client = _StubClient(_DRAFT_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.template_id is None


# ----- signature presence --------------------------------------------------


def test_signature_present_when_blob_is_populated():
    """Signed entries have a non-empty `providerSignature` field
    (base64 PNG). We only retain a boolean — never the bytes."""
    payload = {**_SIGNED_PREVIEW_RESPONSE,
               "fields": {**_SIGNED_PREVIEW_RESPONSE["fields"],
                          "providerSignature": "data:image/png;base64,iVBOR..."}}
    client = _StubClient(payload)
    response = get_preview(client, billing_entry_id=1234)
    assert response.provider_signature_present is True


def test_signature_absent_when_blob_is_empty_string():
    """The captured 'signed' fixture has empty string for the
    signature itself but timestamps populated. Provider signature
    presence is determined by the blob length, not the timestamp."""
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.provider_signature_present is False


def test_signature_absent_on_draft():
    client = _StubClient(_DRAFT_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.provider_signature_present is False
    assert response.signed_at is None


# ----- pipeline dispatch helpers -------------------------------------------


def test_has_pdf_available_true_when_accessible_file_present():
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.has_pdf_available is True
    assert response.first_accessible_file is not None
    assert response.first_accessible_file.id == 8901


def test_has_pdf_available_false_on_draft():
    """Unsigned/draft entries have no files → skip branch."""
    client = _StubClient(_DRAFT_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.has_pdf_available is False
    assert response.first_accessible_file is None


def test_has_pdf_available_false_when_only_archived_files():
    payload = {**_SIGNED_PREVIEW_RESPONSE,
               "files": [{"id": 1, "name": "x",
                          "isArchived": True, "hasAccess": True}]}
    client = _StubClient(payload)
    response = get_preview(client, billing_entry_id=1234)
    assert response.has_pdf_available is False


def test_has_pdf_available_false_when_only_inaccessible_files():
    payload = {**_SIGNED_PREVIEW_RESPONSE,
               "files": [{"id": 1, "name": "x",
                          "isArchived": False, "hasAccess": False}]}
    client = _StubClient(payload)
    response = get_preview(client, billing_entry_id=1234)
    assert response.has_pdf_available is False


# ----- raw payload retention -----------------------------------------------


def test_raw_response_retained_for_record_builder():
    """The record builder needs additional fields (billed times,
    diagnosis code, etc.) from the raw response. Keep it accessible
    rather than re-parsing every field on the dataclass."""
    client = _StubClient(_SIGNED_PREVIEW_RESPONSE)
    response = get_preview(client, billing_entry_id=1234)
    assert response.raw is _SIGNED_PREVIEW_RESPONSE
    # Spot-check that a non-dataclass field is reachable
    assert response.raw["fields"]["timeWorkedFrom"].startswith("2026-06-28")
