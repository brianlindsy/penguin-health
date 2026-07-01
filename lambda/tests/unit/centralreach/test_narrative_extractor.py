"""Tests for centralreach.narrative_extractor.

Pins five contracts:
  1. Happy path: Bedrock returns the narrative; the function returns it
     trimmed and the PDF was sent as a document content block
  2. Bedrock failure (exception during invoke) -> NarrativeExtractionError
  3. Non-dict Bedrock response -> NarrativeExtractionError
  4. Missing/empty narrative key -> NarrativeExtractionError
  5. Oversized narrative (>8192 chars) -> NarrativeExtractionError
  6. Cost-attribution wiring: call_type and parent_request_id flow through
"""

from __future__ import annotations

import base64

import pytest

from centralreach.narrative_extractor import (
    NarrativeExtractionError,
    extract_narrative,
)


_PDF_BYTES = b"%PDF-1.4\nfake content\n%%EOF"


class _FakeInvoker:
    """Records every kwargs set the production code passes to
    `invoke_claude_model` so the tests can assert on the cost-attribution
    fields without needing a real Bedrock client."""

    def __init__(self, response):
        self._response = response
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _invoke(response, **overrides):
    """Run the extractor with a stubbed invoker. Returns
    `(result_or_exception, invoker)`."""
    invoker = _FakeInvoker(response)
    kwargs = {
        "org_id": "demo",
        "ingest_run_id": "run-abc",
        "invoke_claude_model": invoker,
        "model_id": "stub-model",
        **overrides,
    }
    return extract_narrative(_PDF_BYTES, **kwargs), invoker


# ----- happy path -----------------------------------------------------------


def test_returns_narrative_when_bedrock_responds_with_json():
    narrative, invoker = _invoke({"narrative": "Provider observed task latency."})
    assert narrative == "Provider observed task latency."
    assert len(invoker.calls) == 1


def test_trims_leading_and_trailing_whitespace():
    narrative, _ = _invoke({"narrative": "   narrative text\n  "})
    assert narrative == "narrative text"


def test_pdf_is_sent_as_base64_document_content_block():
    """Bedrock expects PDFs as a `{type: document, source.type: base64}`
    block. The encoded data must match the input PDF bytes verbatim."""
    _, invoker = _invoke({"narrative": "ok"})
    body = invoker.calls[0]["body"]
    user_message = body["messages"][0]
    document_blocks = [
        b for b in user_message["content"] if b.get("type") == "document"
    ]
    assert len(document_blocks) == 1
    src = document_blocks[0]["source"]
    assert src["type"] == "base64"
    assert src["media_type"] == "application/pdf"
    assert base64.b64decode(src["data"]) == _PDF_BYTES


def test_cost_attribution_kwargs_flow_through_to_invoker():
    """`org_id`, `call_type`, and `parent_request_id` are the three
    fields the existing `claude_cost.record_cost` hook reads. They must
    arrive at `invoke_claude_model` so per-org dashboards can break out
    extraction spend from rule-eval spend.
    """
    _, invoker = _invoke({"narrative": "ok"})
    call = invoker.calls[0]
    assert call["org_id"] == "demo"
    assert call["call_type"] == "centralreach_narrative_extract"
    assert call["parent_request_id"] == "run-abc"
    assert call["return_json_only"] is True


def test_system_prompt_instructs_verbatim_prose_only():
    """The prompt must require verbatim prose extraction (no
    paraphrase, no headers, no signature blocks). Without this
    instruction, Bedrock would summarize and break Rule 1's
    narrative_hash dedup."""
    _, invoker = _invoke({"narrative": "ok"})
    system = invoker.calls[0]["body"]["system"]
    assert "verbatim" in system.lower()
    assert "narrative" in system.lower()


# ----- failure modes --------------------------------------------------------


def test_bedrock_invocation_failure_is_wrapped():
    with pytest.raises(NarrativeExtractionError) as exc:
        _invoke(RuntimeError("network is down"))
    assert "RuntimeError" in str(exc.value)


def test_non_dict_response_raises():
    with pytest.raises(NarrativeExtractionError):
        _invoke("just a string")


def test_missing_narrative_key_raises():
    with pytest.raises(NarrativeExtractionError):
        _invoke({"other": "field"})


def test_empty_narrative_raises():
    with pytest.raises(NarrativeExtractionError):
        _invoke({"narrative": ""})


def test_whitespace_only_narrative_raises():
    with pytest.raises(NarrativeExtractionError):
        _invoke({"narrative": "   \n  "})


def test_non_string_narrative_raises():
    with pytest.raises(NarrativeExtractionError):
        _invoke({"narrative": ["a", "b"]})


def test_oversized_narrative_raises():
    """Sanity cap catches the "Bedrock returned the whole document by
    mistake" case before it gets stored on the record."""
    huge = "x" * 9000
    with pytest.raises(NarrativeExtractionError) as exc:
        _invoke({"narrative": huge})
    assert "exceeds" in str(exc.value).lower()
