"""Bedrock-driven structured-field extraction from a CR PDF at ingest time.

Complements `narrative_extractor.py`. That module pulls the free-text
narrative; this one pulls five structured fields that appear on the
rendered note but need not match what CR's API returned on the list /
preview endpoints:

  * `note_provider_location`      — the location string on the note
  * `note_provider_billed_time`   — the billed time string on the note
  * `note_provider_signature_name`— the name at the provider signature line
  * `note_supervisor_signature`   — whether a supervisor signature exists
  * `note_supervisor_name`        — the supervisor name text on the note

The `note_` prefix on every returned field is deliberate: downstream
rules and dashboards can distinguish "what the vendor API asserted"
(`location`, `billed_minutes`, `supervisor_name`, `provider_signature`)
from "what the provider wrote on the note itself" (`note_*`) without
guessing.

Why ingest-time and not eval-time:
  * Same immutability + cost arguments as `narrative_extractor.py` —
    records are immutable, rule engine runs read the extracted values
    directly rather than paying Bedrock per rule.
  * These fields feed rules 6 (location matches API) and 7 (supervisor
    documentation present); both would otherwise send the whole PDF
    to Bedrock at eval time.

Cost attribution:
  Bedrock calls use `call_type=centralreach_note_fields_extract` so
  per-org dashboards can split this extraction cost from the narrative
  extraction (`centralreach_narrative_extract`) and rule-eval cost.
"""

from __future__ import annotations

import base64
import sys
from dataclasses import dataclass
from pathlib import Path

from .exceptions import CentralReachError


class NoteFieldsExtractionError(CentralReachError):
    """Bedrock extraction failed or returned an unparseable result.

    Caught by the pipeline at the per-entry boundary — the entry is
    skipped with reason `note_fields_extract_failed` rather than
    half-ingested with a missing extraction.
    """


@dataclass(frozen=True)
class NoteFields:
    """The six fields extracted verbatim from the rendered PDF.

    Every field is optional (`None` when Bedrock could not find it on
    the note). A missing field is a data-quality signal to downstream
    rules — Rule 7 (supervisor signature present) treats
    `note_supervisor_signature=False` as a failing state, but
    `None` on the same field means "extractor couldn't tell," which
    should SKIP rather than fail.
    """

    provider_location: str | None
    provider_billed_time: str | None
    # The provider's name as it appears in the billed-provider header
    # section of the note (e.g. "Provider: Ann Smith"). Distinct from
    # `provider_signature_name`, which is the name at the signature
    # line at the bottom. Rule 7's three-way match compares the CR
    # API's `provider_display` against this value and the signature
    # name to catch cases where the note was billed under one
    # provider but signed by another.
    provider_billed: str | None
    provider_signature_name: str | None
    supervisor_signature: bool | None
    supervisor_name: str | None


# The extraction prompt. Kept here as a module constant rather than in
# operator-owned DynamoDB — this is a structural extraction step, not a
# per-org compliance rule. Changing the prompt requires a code change
# with a held-out-PDF regression check.
_SYSTEM_PROMPT = (
    "You are extracting structured fields from a CentralReach billing-"
    "note PDF. Return the exact text as it appears on the note — do NOT "
    "paraphrase, translate, or normalize. If a field is not present on "
    "the note, return null for that field. Never guess.\n\n"
    "Fields to extract:\n"
    "  - provider_location: the location string shown on the note for "
    "the session (e.g. \"10: Telehealth\" or \"Clinic - Room 3\"). "
    "This is the location the provider wrote on the note, NOT any "
    "value the billing system may have added afterward.\n"
    "  - provider_billed_time: the billed time as it appears on the "
    "note, verbatim including units (e.g. \"75 minutes\", \"1.25 "
    "hours\", \"1:15\"). Do not convert units.\n"
    "  - provider_billed: the provider's name as it appears in the "
    "billed-provider or header section of the note (e.g. after a "
    "\"Provider:\" label, or in a top-of-page provider block). This "
    "is the name attributed to the billed session, distinct from the "
    "name written or typed at the signature line at the bottom of "
    "the note. Return the name exactly as it appears.\n"
    "  - provider_signature_name: the name that appears at the "
    "provider signature line, exactly as typed or printed.\n"
    "  - supervisor_signature: true if a supervisor signature (image, "
    "typed name, or 'e-signed by' block) is present at the supervisor "
    "signature line; false if the note has a supervisor line but it is "
    "blank; null if the note has no supervisor line at all.\n"
    "  - supervisor_name: the supervisor's name as it appears at the "
    "supervisor signature line, exactly as typed or printed. null if "
    "the supervisor line is blank or absent.\n\n"
    "Respond with JSON in the form:\n"
    "  {\"provider_location\": ..., \"provider_billed_time\": ..., "
    "\"provider_billed\": ..., \"provider_signature_name\": ..., "
    "\"supervisor_signature\": ..., \"supervisor_name\": ...}"
)


_USER_INSTRUCTION = (
    "Extract the six structured fields from the attached PDF and "
    "return them as the JSON object documented in the system prompt."
)


# Any single extracted string longer than this is almost certainly the
# model returning surrounding context by mistake. 512 chars comfortably
# fits every legitimate location / time / name string we've seen.
_MAX_FIELD_CHARS = 512


_ALLOWED_KEYS = frozenset({
    "provider_location",
    "provider_billed_time",
    "provider_billed",
    "provider_signature_name",
    "supervisor_signature",
    "supervisor_name",
})


def extract_note_fields(
    pdf_bytes: bytes,
    *,
    org_id: str,
    ingest_run_id: str,
    invoke_claude_model=None,
    model_id: str | None = None,
) -> NoteFields:
    """Send `pdf_bytes` to Bedrock and return the extracted NoteFields.

    The runner calls this once per record between PDF fetch and record
    build, right after `narrative_extractor.extract_narrative`. Raises
    `NoteFieldsExtractionError` on:
      * Bedrock invocation failure
      * Response with no parseable JSON
      * Response is not a JSON object
      * A string field exceeds `_MAX_FIELD_CHARS` (sanity cap on
        runaway extraction)

    Field-level absence (a `null` value on any of the six keys) is
    NOT an error — that's the normal signal for "not present on the
    note." The resulting `NoteFields` carries `None` for that field.

    Cost attribution flows through `claude_cost.record_cost` via the
    wrapped `invoke_claude_model`. `parent_request_id` is
    `ingest_run_id` so cost dashboards group every extraction call
    under the same Fargate task.
    """
    if invoke_claude_model is None or model_id is None:
        # Lazy import: same reasoning as narrative_extractor — keeps
        # the centralreach module importable in contexts (CLI helpers,
        # config tooling) that don't ship the rules-engine code.
        bedrock_client_mod = _load_rules_engine_bedrock_client()
        if invoke_claude_model is None:
            invoke_claude_model = bedrock_client_mod.invoke_claude_model
        if model_id is None:
            model_id = bedrock_client_mod.MODEL_ID

    body = {
        "system": _SYSTEM_PROMPT,
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "temperature": 0.01,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _USER_INSTRUCTION},
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": base64.b64encode(pdf_bytes).decode("ascii"),
                        },
                    },
                ],
            },
        ],
    }

    try:
        response = invoke_claude_model(
            inference_profile_id=model_id,
            body=body,
            return_json_only=True,
            raise_on_error=True,
            retries=1,
            org_id=org_id,
            call_type="centralreach_note_fields_extract",
            parent_request_id=ingest_run_id,
        )
    except Exception as e:  # noqa: BLE001
        raise NoteFieldsExtractionError(
            f"Bedrock invocation failed: {type(e).__name__}: {e}",
        ) from e

    if not isinstance(response, dict):
        raise NoteFieldsExtractionError(
            "Bedrock response is not a JSON object",
        )

    # Unknown keys are ignored (defensive — a prompt change or model
    # quirk shouldn't crash the pipeline). Missing keys become None.
    return NoteFields(
        provider_location=_string_field(
            response.get("provider_location"), "provider_location",
        ),
        provider_billed_time=_string_field(
            response.get("provider_billed_time"), "provider_billed_time",
        ),
        provider_billed=_string_field(
            response.get("provider_billed"), "provider_billed",
        ),
        provider_signature_name=_string_field(
            response.get("provider_signature_name"),
            "provider_signature_name",
        ),
        supervisor_signature=_bool_field(
            response.get("supervisor_signature"), "supervisor_signature",
        ),
        supervisor_name=_string_field(
            response.get("supervisor_name"), "supervisor_name",
        ),
    )


def _string_field(raw, name):
    """Normalize a string-typed response field.

    Returns None if raw is None (Bedrock said "not present"). Strips
    whitespace. Raises `NoteFieldsExtractionError` for the runaway-
    extraction case (a string that's clearly not a single field
    value) — the pipeline treats that as a bad extraction and skips
    the entry rather than persisting garbage.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise NoteFieldsExtractionError(
            f"Bedrock returned non-string for '{name}': {type(raw).__name__}",
        )
    stripped = raw.strip()
    if not stripped:
        return None
    if len(stripped) > _MAX_FIELD_CHARS:
        raise NoteFieldsExtractionError(
            f"extracted '{name}' exceeds {_MAX_FIELD_CHARS}-char cap "
            f"({len(stripped)} chars); likely Bedrock returned "
            "surrounding context by mistake",
        )
    return stripped


def _bool_field(raw, name):
    """Normalize a bool-typed response field.

    Bedrock reliably returns JSON true/false/null for the schema we
    described. Anything else here means the prompt drifted; raise so
    the entry gets skipped with a clear reason rather than silently
    coerced.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    raise NoteFieldsExtractionError(
        f"Bedrock returned non-bool for '{name}': {type(raw).__name__}",
    )


def _load_rules_engine_bedrock_client():
    """Resolve `lambda/multi-org/rules-engine/bedrock_client.py`.

    Same lazy-import trick as `narrative_extractor._load_rules_engine_bedrock_client`.
    """
    try:
        import bedrock_client  # type: ignore
        return bedrock_client
    except ImportError:
        rules_engine_dir = (
            Path(__file__).resolve().parents[1] / "rules-engine"
        )
        if str(rules_engine_dir) not in sys.path:
            sys.path.insert(0, str(rules_engine_dir))
        import bedrock_client  # type: ignore
        return bedrock_client
