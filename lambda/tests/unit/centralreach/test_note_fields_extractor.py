"""Tests for centralreach.note_fields_extractor.

Pins the contracts around the six-field Bedrock extraction:
  1. Happy path: all six fields present → typed NoteFields
  2. Field-level absence (`null`) is normal → None on the result
  3. Field-level whitespace-only string → None (empty extraction)
  4. Bedrock invocation failure → NoteFieldsExtractionError
  5. Non-dict Bedrock response → NoteFieldsExtractionError
  6. Wrong type on a string field → NoteFieldsExtractionError
  7. Oversized field value → NoteFieldsExtractionError
  8. PDF ships as base64 document content block
  9. Cost attribution kwargs (call_type, parent_request_id, org_id)
 10. Unknown response keys are ignored (defensive)
 11. supervisor_name and supervisor_signature_name capture independently
"""

from __future__ import annotations

import base64

import pytest

from centralreach.note_fields_extractor import (
    NoteFields,
    NoteFieldsExtractionError,
    extract_note_fields,
)


_PDF_BYTES = b"%PDF-1.4\nfake note pdf\n%%EOF"


_FULL_RESPONSE = {
    "provider_location": "10: Telehealth Provided in Patient's Home",
    "provider_billed_time": "75 minutes",
    "provider_billed": "Ann Smith, BCBA",
    "provider_signature_name": "Ann Smith, BCBA",
    "supervisor_name": "Dr. Jane Doe",
    "supervisor_signature_name": "Dr. Jane Doe",
}


class _FakeInvoker:
    def __init__(self, response):
        self._response = response
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _invoke(response, **overrides):
    invoker = _FakeInvoker(response)
    kwargs = {
        "org_id": "demo",
        "ingest_run_id": "run-abc",
        "invoke_claude_model": invoker,
        "model_id": "stub-model",
        **overrides,
    }
    return extract_note_fields(_PDF_BYTES, **kwargs), invoker


# ----- happy path ----------------------------------------------------------


def test_returns_all_six_fields_when_bedrock_populates_them():
    result, _ = _invoke(_FULL_RESPONSE)
    assert isinstance(result, NoteFields)
    assert result.provider_location == "10: Telehealth Provided in Patient's Home"
    assert result.provider_billed_time == "75 minutes"
    assert result.provider_billed == "Ann Smith, BCBA"
    assert result.provider_signature_name == "Ann Smith, BCBA"
    assert result.supervisor_name == "Dr. Jane Doe"
    assert result.supervisor_signature_name == "Dr. Jane Doe"


def test_supervisor_name_and_signature_name_can_diverge():
    """The attribution section names the supervisor of record, while
    the signature line names whoever actually signed. When those
    differ, the cross-check rule should catch it — verify the two
    fields are captured independently."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "supervisor_name": "Dr. Jane Doe",
        "supervisor_signature_name": "J. Roe, BCBA-D",
    })
    assert result.supervisor_name == "Dr. Jane Doe"
    assert result.supervisor_signature_name == "J. Roe, BCBA-D"


def test_provider_and_supervisor_signature_names_can_be_the_same_person():
    """On a solo ABA note whose sole signature block reads e.g. "Jane
    Doe, Supervising Analyst, BCBA-D", the same name populates both
    fields: the block is the provider signature (it sits under the
    provider signature line) and its role label also identifies the
    signer as a supervisor. The extractor must accept and preserve
    that overlap — the two fields are semantically independent."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "provider_signature_name": "Jane Doe",
        "supervisor_signature_name": "Jane Doe",
    })
    assert result.provider_signature_name == "Jane Doe"
    assert result.supervisor_signature_name == "Jane Doe"


def test_provider_billed_and_supervisor_name_can_be_the_same_person():
    """The attribution-side mirror of the signature-side case: a note
    with no dedicated Supervisor section whose Provider section reads
    "Jane Doe, Supervising Analyst, BCBA-D" populates both
    `provider_billed` and `supervisor_name` with "Jane Doe" — the
    role label identifies the sole named provider as also being the
    supervising provider of record."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "provider_billed": "Jane Doe",
        "supervisor_name": "Jane Doe",
    })
    assert result.provider_billed == "Jane Doe"
    assert result.supervisor_name == "Jane Doe"


def test_provider_billed_and_signature_name_can_diverge():
    """The whole reason to extract `provider_billed` separately is
    that the billed-provider name (top of note) and the signature
    name (bottom of note) can differ — rule 7 catches that. Verify
    the two fields are captured independently."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "provider_billed": "Ann Smith, BCBA",
        "provider_signature_name": "J. Doe, RBT",
    })
    assert result.provider_billed == "Ann Smith, BCBA"
    assert result.provider_signature_name == "J. Doe, RBT"


def test_strips_whitespace_from_string_fields():
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "provider_location": "  Clinic - Room 3  ",
    })
    assert result.provider_location == "Clinic - Room 3"


def test_pdf_ships_as_base64_document_block():
    _, invoker = _invoke(_FULL_RESPONSE)
    body = invoker.calls[0]["body"]
    user_message = body["messages"][0]
    doc_blocks = [
        b for b in user_message["content"] if b.get("type") == "document"
    ]
    assert len(doc_blocks) == 1
    src = doc_blocks[0]["source"]
    assert src["type"] == "base64"
    assert src["media_type"] == "application/pdf"
    assert base64.b64decode(src["data"]) == _PDF_BYTES


def test_cost_attribution_kwargs_flow_through_to_invoker():
    """Per-org dashboards split Bedrock spend by `call_type`. The
    note-fields extraction uses its own tag so it doesn't get
    lumped in with the narrative-extraction cost."""
    _, invoker = _invoke(_FULL_RESPONSE)
    call = invoker.calls[0]
    assert call["org_id"] == "demo"
    assert call["call_type"] == "centralreach_note_fields_extract"
    assert call["parent_request_id"] == "run-abc"
    assert call["return_json_only"] is True


def test_system_prompt_names_all_six_fields():
    _, invoker = _invoke(_FULL_RESPONSE)
    system = invoker.calls[0]["body"]["system"].lower()
    assert "provider_location" in system
    assert "provider_billed_time" in system
    assert "provider_billed" in system
    assert "provider_signature_name" in system
    assert "supervisor_name" in system
    assert "supervisor_signature_name" in system


def test_system_prompt_defines_supervisor_signature_by_role_label():
    """The `supervisor_signature_name` field is defined by the
    signature block's role text ("supervis" case-insensitive
    contains), NOT by the label of the line above the block. This is
    what lets solo notes signed by a supervising BCBA under a
    "Provider Signature" line land the signer's name on the record.
    The prompt has to keep that rule explicit — if a future edit
    reverts to line-label semantics, that class of note silently
    stops populating the field again."""
    _, invoker = _invoke(_FULL_RESPONSE)
    system = invoker.calls[0]["body"]["system"].lower()
    assert "supervis" in system
    assert "role" in system


def test_system_prompt_defines_supervisor_name_with_role_fallback():
    """`supervisor_name` falls back to a supervis-role-labeled name in
    the provider section when the note has no dedicated supervisor
    section. Without this, notes like "Provider: Jane Doe,
    Supervising Analyst, BCBA-D" (no separate Supervisor line) leave
    the field null even though the note names a supervisor of record.
    The prompt must keep both the primary supervisor-section path AND
    the provider-section fallback documented."""
    _, invoker = _invoke(_FULL_RESPONSE)
    system = invoker.calls[0]["body"]["system"]
    supervisor_name_entry = system.split("- supervisor_name:", 1)[1]
    supervisor_name_entry = supervisor_name_entry.split("- supervisor_signature_name:", 1)[0].lower()
    assert "supervisor" in supervisor_name_entry
    assert "provider" in supervisor_name_entry
    assert "supervis" in supervisor_name_entry


def test_system_prompt_defines_supervisor_name_participants_checkbox_fallback():
    """`supervisor_name` also falls back to a Participants-style
    checkbox block whose top-level "Supervisor" checkbox is checked,
    with BCBA / BCaBA / QBA credential sub-options and the name
    written below. The checkbox is the signal — a supervisor named
    only in that block, with no role label near their name, still
    needs to land on the record. The prompt must keep the credential
    sub-options and the ambiguity fallback (multiple sub-boxes
    checked → use signature-line name) documented so a future edit
    doesn't drop the third source silently."""
    _, invoker = _invoke(_FULL_RESPONSE)
    system = invoker.calls[0]["body"]["system"]
    supervisor_name_entry = system.split("- supervisor_name:", 1)[1]
    supervisor_name_entry = supervisor_name_entry.split("- supervisor_signature_name:", 1)[0]
    lowered = supervisor_name_entry.lower()
    assert "checkbox" in lowered
    assert "participants" in lowered
    # All three credential sub-options must be named so the model
    # doesn't drift toward some other credential set.
    assert "bcba" in lowered
    assert "bcaba" in lowered
    assert "qba" in lowered
    # Ambiguity fallback to the signature-line name.
    assert "supervisor_signature_name" in supervisor_name_entry


# ----- field-level absence -------------------------------------------------


def test_null_on_field_returns_none():
    """Bedrock returns null when a field isn't present on the note.
    This is normal — the corresponding NoteFields attribute is None,
    NOT an error."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "supervisor_name": None,
        "supervisor_signature_name": None,
    })
    assert result.supervisor_name is None
    assert result.supervisor_signature_name is None
    # The other fields are still populated.
    assert result.provider_location is not None


def test_missing_key_treated_as_null():
    result, _ = _invoke({
        "provider_location": "Clinic",
        "provider_billed_time": "60 min",
        # provider_signature_name / supervisor_name / supervisor_signature_name absent
    })
    assert result.provider_signature_name is None
    assert result.supervisor_name is None
    assert result.supervisor_signature_name is None


def test_whitespace_only_string_becomes_none():
    """A field that Bedrock returned as `"   "` is not a real
    extraction — treat as absent so a rule doesn't match against
    the empty string."""
    result, _ = _invoke({
        **_FULL_RESPONSE,
        "provider_location": "   ",
    })
    assert result.provider_location is None


def test_all_null_response_is_valid():
    """Every field null is a valid outcome (note had none of them)."""
    result, _ = _invoke({
        "provider_location": None,
        "provider_billed_time": None,
        "provider_billed": None,
        "provider_signature_name": None,
        "supervisor_name": None,
        "supervisor_signature_name": None,
    })
    assert result == NoteFields(
        provider_location=None,
        provider_billed_time=None,
        provider_billed=None,
        provider_signature_name=None,
        supervisor_name=None,
        supervisor_signature_name=None,
    )


def test_unknown_response_keys_are_ignored():
    """A prompt drift or model quirk might add extra keys. Ignore
    them rather than crash — the pipeline should stay resilient."""
    result, _ = _invoke({**_FULL_RESPONSE, "unexpected_field": "hello"})
    assert result.provider_location is not None


# ----- failure modes -------------------------------------------------------


def test_bedrock_invocation_failure_wraps():
    with pytest.raises(NoteFieldsExtractionError) as exc:
        _invoke(RuntimeError("net down"))
    assert "RuntimeError" in str(exc.value)


def test_non_dict_response_raises():
    with pytest.raises(NoteFieldsExtractionError):
        _invoke("just a string")


def test_non_string_type_on_string_field_raises():
    """String field returned as int/list/etc. is a prompt drift signal;
    skip the entry rather than coerce."""
    with pytest.raises(NoteFieldsExtractionError):
        _invoke({**_FULL_RESPONSE, "provider_location": 42})


def test_oversized_string_field_raises():
    """A 5000-char 'provider_location' means Bedrock lost track and
    dumped surrounding context. Better to skip loudly than persist."""
    with pytest.raises(NoteFieldsExtractionError) as exc:
        _invoke({**_FULL_RESPONSE, "provider_location": "x" * 600})
    assert "exceeds" in str(exc.value).lower()
