"""Bedrock-driven structured-field extraction from a CR PDF at ingest time.

Complements `narrative_extractor.py`. That module pulls the free-text
narrative; this one pulls six structured fields that appear on the
rendered note but need not match what CR's API returned on the list /
preview endpoints:

  * `note_provider_location`          — the location string on the note
  * `note_provider_billed_time`       — the billed time string on the note
  * `note_provider_billed`            — the billed-provider name on the note
  * `note_provider_signature_name`    — the name at the provider signature line
  * `note_supervisor_name`            — the supervisor name from the note's
                                        supervisor-attribution section (NOT
                                        the signature line)
  * `note_supervisor_signature_names` — every name signed as a supervisor
                                        on the note (a note can carry
                                        more than one supervisor signature)

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

    Every field is optional (`None` / empty tuple when Bedrock could
    not find it on the note). A missing field is a data-quality signal
    to downstream rules — Rule 7 (supervisor sign-off) treats an empty
    `supervisor_signature_names` as "no signature present" and
    otherwise passes as long as the expected name matches any element.
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
    # The name attributed to the supervising role in the note's
    # header/attribution area, NOT the signature line. Extractor
    # tries three sources in order and takes the first that yields a
    # name:
    #   1. A dedicated supervisor section (e.g. "Supervisor: Dr.
    #      Jane Doe").
    #   2. The provider-attribution section, when the name inside
    #      it carries a supervising role label (e.g. "Provider:
    #      Jane Doe, Supervising Analyst, BCBA-D"). Case-insensitive
    #      contains of "supervis" captures "Supervising Analyst",
    #      "Supervisor", "Supervised by".
    #   3. A Participants-style checkbox block: a top-level
    #      "Supervisor" checkbox that's checked, with credential
    #      sub-options (BCBA / BCaBA / QBA) beneath and the
    #      supervisor's name written below. The checkbox is the
    #      signal — the name itself doesn't need any role label.
    #      Unchecked top-level Supervisor box → skip this source.
    #      Multiple credential sub-boxes checked → ambiguous, fall
    #      back to the first entry of `supervisor_signature_names`.
    # Independent of `provider_billed`: both can carry the same name
    # when the sole provider on the note is a supervisor. Distinct
    # from `supervisor_signature_names` (bottom-of-note signature
    # blocks). The seed rule comparing `supervisor_name` and
    # `supervisor_signature_names` catches cases where a supervisor
    # of record is named but a different (or no) supervisor signs.
    supervisor_name: str | None
    # Every name that signed the note as a supervisor. A note can
    # legitimately carry more than one supervisor signature (co-
    # supervisors, multi-approver forms), so this is a tuple rather
    # than a single string; downstream rules that check for a specific
    # supervisor pass as long as the expected name matches any entry.
    # A signer counts as a supervisor when the signature block contains
    # a role label that includes the text "supervis" (case-insensitive)
    # — matches "Supervisor", "Supervising Analyst", "Supervised by",
    # and similar variants. Solo notes signed by a supervisor-
    # credentialed provider land here even when the signature block is
    # labeled "Provider Signature," which is the normal case on ABA
    # notes signed by the supervising BCBA. Independent of
    # `provider_signature_name`: the two can carry the same name (e.g.
    # a solo note whose sole signer is the supervisor of record).
    # Empty tuple when no signature block on the note identifies its
    # signer as a supervisor.
    supervisor_signature_names: tuple[str, ...]


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
    "  - supervisor_name: the name attributed to the supervising role "
    "in the note's header/attribution area — NOT the signature line "
    "at the bottom of the note. Return the name exactly as it "
    "appears. Look in this order and take the first source that "
    "yields a name:\n"
    "    1. A dedicated supervisor section (e.g. after a "
    "\"Supervisor:\" label, or in a header block that names the "
    "supervising provider for the session).\n"
    "    2. Inside the provider-attribution section, a name whose "
    "accompanying role text contains \"supervis\" (case-insensitive) "
    "— this matches \"Supervising Analyst\", \"Supervisor\", "
    "\"Supervised by\", and similar variants. Example: a note with "
    "only a \"Provider:\" section reading \"Jane Doe, Supervising "
    "Analyst, BCBA-D\" populates supervisor_name with \"Jane Doe\", "
    "because the role label identifies the person as the "
    "supervising provider.\n"
    "    3. A Participants-style checkbox block whose top-level "
    "\"Supervisor\" option is checked. The block layout is: a "
    "\"Supervisor\" checkbox at the top, then three credential sub-"
    "options (\"BCBA\", \"BCaBA\", \"QBA\") each with their own "
    "checkbox, then the supervisor's name written below. When the "
    "top-level Supervisor checkbox is checked (a literal check mark "
    "inside the box), take the name written in the block. Which "
    "specific credential sub-box (BCBA / BCaBA / QBA) is checked "
    "does not matter — it's an attribute of the supervisor, not the "
    "signal for whether to extract. The name does NOT need to "
    "contain \"supervisor\" or any role label — the checkbox is the "
    "signal. If the top-level Supervisor checkbox is unchecked, "
    "skip this source (do not read the name even if one is written "
    "in the block). If multiple credential sub-boxes appear "
    "checked, the block is ambiguous — fall back to the first entry "
    "in supervisor_signature_names (i.e. return the first supervisor "
    "signature-line name). Only consider a checkbox checked when the "
    "mark inside it is clear; do not guess.\n"
    "    4. null if no source above yields a name.\n"
    "This field is INDEPENDENT of `provider_billed` — the same "
    "person can populate both when the sole provider on the note is "
    "a supervisor.\n"
    "  - supervisor_signature_names: the names of every person who "
    "signed the note as a supervisor, returned as a JSON array of "
    "strings. A signer counts as a supervisor when the signature "
    "block contains a role label that includes the text \"supervis\" "
    "(case-insensitive) — this matches \"Supervisor\", \"Supervising "
    "Analyst\", \"Supervised by\", and similar variants. Look at the "
    "role text inside each signature block, NOT the label of the "
    "line above it: a note whose only signature block reads \"Jane "
    "Doe, Supervising Analyst, BCBA-D\" counts even if that block "
    "sits under a \"Provider Signature\" line. Return EVERY "
    "qualifying signature block — a note can carry more than one "
    "supervisor signature (co-supervisors, multi-approver forms). "
    "Preserve document order: earlier signature blocks first. Return "
    "each signer's name exactly as it appears (name text only, drop "
    "the role label and credentials). Do not deduplicate — if the "
    "same name qualifies twice, return it twice. This field is "
    "INDEPENDENT of `provider_signature_name` — the same signer can "
    "populate both. In the solo-note case above, "
    "`provider_signature_name` is \"Jane Doe\" and "
    "`supervisor_signature_names` is [\"Jane Doe\"]: the block is the "
    "provider signature (it sits under the provider line) and its "
    "role label also identifies the signer as a supervisor. Return "
    "an empty array [] if no signature block on the note carries a "
    "supervising-role label.\n\n"
    "Respond with JSON in the form:\n"
    "  {\"provider_location\": ..., \"provider_billed_time\": ..., "
    "\"provider_billed\": ..., \"provider_signature_name\": ..., "
    "\"supervisor_name\": ..., \"supervisor_signature_names\": [...]}"
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
    "supervisor_name",
    "supervisor_signature_names",
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
      * `supervisor_signature_names` is not a list, or an entry is
        not a string / exceeds the char cap

    Field-level absence (a `null` value on any of the string keys, or
    missing/`null`/`[]` for `supervisor_signature_names`) is NOT an
    error — that's the normal signal for "not present on the note."
    The resulting `NoteFields` carries `None` (or `()`) for that field.

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
        supervisor_name=_string_field(
            response.get("supervisor_name"), "supervisor_name",
        ),
        supervisor_signature_names=_string_list_field(
            response.get("supervisor_signature_names"),
            "supervisor_signature_names",
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


def _string_list_field(raw, name) -> tuple[str, ...]:
    """Normalize a list-of-strings response field.

    Returns an empty tuple when raw is None or missing. Applies the
    same per-item hygiene as `_string_field` (must be string, strip
    whitespace, drop empties, cap at `_MAX_FIELD_CHARS`) so a bogus
    element can't slip through the list wrapper.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise NoteFieldsExtractionError(
            f"Bedrock returned non-list for '{name}': {type(raw).__name__}",
        )
    cleaned: list[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, str):
            raise NoteFieldsExtractionError(
                f"Bedrock returned non-string in '{name}[{idx}]': "
                f"{type(item).__name__}",
            )
        stripped = item.strip()
        if not stripped:
            continue
        if len(stripped) > _MAX_FIELD_CHARS:
            raise NoteFieldsExtractionError(
                f"extracted '{name}[{idx}]' exceeds "
                f"{_MAX_FIELD_CHARS}-char cap ({len(stripped)} chars); "
                "likely Bedrock returned surrounding context by mistake",
            )
        cleaned.append(stripped)
    return tuple(cleaned)


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
