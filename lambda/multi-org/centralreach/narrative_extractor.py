"""Bedrock-driven narrative extraction from a CR PDF at ingest time.

CR's billing-note PDFs contain a clinical narrative — the Session
Summary or Summary/Narrative paragraphs the provider wrote — alongside
template chrome (checkbox lists, signature blocks, header tables with
patient identifiers, etc.). The rules engine's text-based rules (1, 2,
3) operate on just the narrative; rule 11 needs the whole PDF because
it asks about charts/graphs/percentages that live outside the prose.

This module extracts the narrative prose once per record at ingest
time, hashes it, and stores both on the record. The text-based rules
then use the legacy text path; rule 11 still reads the PDF directly
via the centralreach branch in document_validator (kept for that
single rule via the `requires_pdf` flag on its rule_config row).

Why ingest-time, not eval-time:
  * Records are immutable once written; computing the hash at ingest
    means the hash is part of the record's identity from the start.
  * Rule 1 (narrative_hash_unique) is a mandatory audit rule and must
    have a hash to evaluate against. Computing at eval time means
    every rule-engine run pays for extraction; doing it at ingest
    means one extraction per record total.
  * Rules 2 and 3 cost ~10x less when run against extracted text
    (~1K tokens) vs the raw PDF document block (~10K tokens). Pulling
    the extraction forward unblocks that cost reduction.

Cost attribution:
  Bedrock calls from this module use `call_type=centralreach_narrative_extract`
  so per-org dashboards can split extraction cost from rule-eval cost
  (which uses `centralreach_rule_validate:{rule_id}` and similar).
"""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

from .exceptions import CentralReachError


class NarrativeExtractionError(CentralReachError):
    """Bedrock extraction failed or returned an unusable result.

    Caught by the pipeline at the per-entry boundary — the entry is
    skipped with reason `narrative_extract_failed` rather than
    half-ingested with a missing narrative.
    """


# The extraction prompt. Kept here as a module constant rather than in
# operator-owned DynamoDB because this is a structural extraction step,
# not a per-org rule. Changing the prompt requires a code change with
# a held-out-PDF regression check; see the design doc's open questions.
_SYSTEM_PROMPT = (
    "You are extracting the clinical narrative from a CentralReach "
    "billing-note PDF. Return only the prose paragraphs the provider "
    "wrote in the Session Summary or Summary/Narrative section. Do not "
    "include section headers, field labels, checkbox lists, signature "
    "blocks, patient identifiers, dates, times, billing metadata, or "
    "table contents. If multiple narrative sections exist, concatenate "
    "them in the order they appear separated by a blank line.\n\n"
    "Respond with JSON in the form {\"narrative\": \"<verbatim prose "
    "text here>\"}. The narrative value must be the provider's prose "
    "verbatim, preserving the original capitalization and punctuation. "
    "Do not paraphrase or summarize."
)


_USER_INSTRUCTION = (
    "Extract the clinical narrative from the attached PDF and return "
    "it as the JSON object documented in the system prompt."
)


# Maximum narrative length we'll accept from Bedrock. CR's narratives
# top out around 2000 chars in the captures we've seen; 8K is a safety
# ceiling that catches "Bedrock extracted the whole document by
# mistake" without rejecting any realistic provider note.
_MAX_NARRATIVE_CHARS = 8192


def extract_narrative(
    pdf_bytes: bytes,
    *,
    org_id: str,
    ingest_run_id: str,
    invoke_claude_model=None,
    model_id: str | None = None,
) -> str:
    """Send `pdf_bytes` to Bedrock and return the extracted narrative.

    The runner calls this once per record between PDF fetch and record
    build. Raises `NarrativeExtractionError` on:
      * Bedrock invocation failure
      * Response with no parseable JSON
      * Response missing the `narrative` key
      * `narrative` value that isn't a non-empty string
      * `narrative` value exceeding `_MAX_NARRATIVE_CHARS` (sanity cap
        on runaway extraction)

    Cost attribution flows through the existing `claude_cost.record_cost`
    via the wrapped `invoke_claude_model`. `parent_request_id` is
    `ingest_run_id` so cost dashboards group every extraction call
    under the same Fargate task.

    `invoke_claude_model` and `model_id` are injected for testing; the
    production path imports them lazily from the rules-engine module
    so the centralreach module's main code path doesn't depend on
    Bedrock at import time.
    """
    if invoke_claude_model is None or model_id is None:
        # Lazy import keeps the centralreach module importable in
        # contexts (CLI helpers, config tooling) that don't ship
        # the rules-engine code.
        bedrock_client_mod = _load_rules_engine_bedrock_client()
        if invoke_claude_model is None:
            invoke_claude_model = bedrock_client_mod.invoke_claude_model
        if model_id is None:
            model_id = bedrock_client_mod.MODEL_ID

    body = {
        "system": _SYSTEM_PROMPT,
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
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
            call_type="centralreach_narrative_extract",
            parent_request_id=ingest_run_id,
        )
    except Exception as e:  # noqa: BLE001
        raise NarrativeExtractionError(
            f"Bedrock invocation failed: {type(e).__name__}: {e}",
        ) from e

    if not isinstance(response, dict):
        raise NarrativeExtractionError(
            "Bedrock response is not a JSON object",
        )

    narrative = response.get("narrative")
    if not isinstance(narrative, str) or not narrative.strip():
        raise NarrativeExtractionError(
            "Bedrock response missing non-empty 'narrative' string",
        )

    narrative = narrative.strip()
    if len(narrative) > _MAX_NARRATIVE_CHARS:
        raise NarrativeExtractionError(
            f"extracted narrative exceeds {_MAX_NARRATIVE_CHARS}-char cap "
            f"({len(narrative)} chars); likely Bedrock returned the whole "
            "document by mistake",
        )

    return narrative


def _load_rules_engine_bedrock_client():
    """Resolve `lambda/multi-org/rules-engine/bedrock_client.py`.

    The rules-engine module is on `sys.path` in production (the Fargate
    runner's Dockerfile / the Lambda layout puts rules-engine sibling
    to centralreach). Tests inject their own `invoke_claude_model`
    callable, so this import path only matters at production runtime.
    """
    # Already on sys.path in production. Best-effort fallback so tests
    # importing this module without rules-engine on sys.path don't
    # crash at import time.
    try:
        import bedrock_client  # type: ignore
        return bedrock_client
    except ImportError:
        # Walk up from this file to find rules-engine in the same
        # multi-org tree. Lets the lazy-import succeed in dev shells
        # without sys.path manipulation.
        rules_engine_dir = (
            Path(__file__).resolve().parents[1] / "rules-engine"
        )
        if str(rules_engine_dir) not in sys.path:
            sys.path.insert(0, str(rules_engine_dir))
        import bedrock_client  # type: ignore
        return bedrock_client
