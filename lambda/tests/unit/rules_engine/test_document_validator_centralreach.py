"""Tests for the centralreach branch in document_validator.

centralreach records now ship with the narrative `text` populated at
ingest time AND keep the `pdf_s3_key` reference. Dispatch is per-rule:
* Default rules (1, 2, 3): use the text path, no PDF cost.
* Rules opting in via `requires_pdf: true` (rule 11): use the PDF
  document-block path.

These tests focus on the dispatch logic, content-block construction,
S3 PDF fetch, and the `bedrock_rule_eval` audit emission.

Mocks:
  * S3 via moto for the PDF GET
  * `invoke_claude_model` via patch — returns canned JSON responses
  * `audit_emit` via patch — captures emitted events for assertion
"""

from __future__ import annotations

import base64
import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import document_validator as dv


_PDF_BYTES = b"%PDF-1.4\nfake pdf for testing\n%%EOF"
_PDF_S3_KEY = "pdfs/2026-06-28/20260628T220000Z__502614593.pdf"


def _text_record():
    """A non-centralreach record with populated text. Exercises the
    legacy code path."""
    return {
        "source_record_id": "rec-1",
        "org_id": "demo",
        "text": "Patient presents in stable condition.",
        "extracted_fields": {},
    }


def _centralreach_record():
    """A centralreach record. As of the ingest-time narrative-extract
    change, `text` carries the Bedrock-extracted narrative and
    `extracted_fields.pdf_s3_key` references the original PDF for
    requires_pdf rules (e.g. rule 11)."""
    return {
        "source_record_id": "502614593",
        "org_id": "demo",
        "source": "centralreach.api",
        "text": "Provider observed independent responding at 80% across trials.",
        "extracted_fields": {
            "pdf_s3_key": _PDF_S3_KEY,
            "template_id": 113875,
            "narrative_hash": "deadbeef" * 8,
        },
    }


@pytest.fixture
def s3_with_pdf():
    """moto-backed S3 with a fake PDF already at the expected key."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        client.put_object(
            Bucket="penguin-health-demo",
            Key=_PDF_S3_KEY,
            Body=_PDF_BYTES,
            ContentType="application/pdf",
        )
        # Reset the lazily-cached client so the patched moto session
        # is used. Same trick the audit emitter uses for its own
        # cached clients.
        dv._s3_client = client
        try:
            yield client
        finally:
            dv._s3_client = None


# ----- _load_chart_input ----------------------------------------------------


def test_load_chart_input_returns_text_for_populated_text():
    kind, payload = dv._load_chart_input(_text_record())
    assert kind == "text"
    assert payload == "Patient presents in stable condition."


def test_load_chart_input_centralreach_default_rule_takes_text_path(
    s3_with_pdf,
):
    """Default rules (no `requires_pdf`) read the extracted narrative
    text — this is the cheap path. S3 is NOT touched."""
    kind, payload = dv._load_chart_input(
        _centralreach_record(), rule_config={"rule_id": "r1"},
    )
    assert kind == "text"
    assert payload.startswith("Provider observed")


def test_load_chart_input_centralreach_requires_pdf_rule_takes_pdf_path(
    s3_with_pdf,
):
    """`requires_pdf: true` opts into the PDF document block (rule 11
    case — needs charts/percentages the narrative doesn't carry)."""
    kind, payload = dv._load_chart_input(
        _centralreach_record(),
        rule_config={"rule_id": "r11", "requires_pdf": True},
    )
    assert kind == "pdf"
    assert payload == _PDF_BYTES


def test_load_chart_input_requires_pdf_falls_back_to_text_when_no_pdf_key():
    """A record without a `pdf_s3_key` cannot honor `requires_pdf`;
    the dispatcher falls back to the text path rather than failing."""
    record = {
        "source_record_id": "x", "org_id": "demo",
        "text": "narrative only", "extracted_fields": {},
    }
    kind, payload = dv._load_chart_input(
        record, rule_config={"requires_pdf": True},
    )
    assert kind == "text"
    assert payload == "narrative only"


def test_load_chart_input_returns_empty_text_when_no_text_and_no_pdf():
    """A record without text and without pdf_s3_key gets an empty
    text payload — caller's fallback (`json.dumps(fields)`) takes
    over."""
    record = {"source_record_id": "x", "org_id": "demo", "text": None,
              "extracted_fields": {}}
    kind, payload = dv._load_chart_input(record)
    assert kind == "text"
    assert payload == ""


def test_load_chart_input_handles_none_data():
    kind, payload = dv._load_chart_input(None)
    assert kind == "text"
    assert payload == ""


def test_load_chart_input_requires_pdf_path_requires_org_id(s3_with_pdf):
    """When the dispatcher routes a record to the PDF path it needs
    `org_id` to resolve the per-org bucket. Missing org_id raises so
    operators see the misconfiguration loudly."""
    record = _centralreach_record()
    record["org_id"] = None
    with pytest.raises(ValueError, match="org_id"):
        dv._load_chart_input(record, rule_config={"requires_pdf": True})


# ----- _pdf_document_content_block ------------------------------------------


def test_pdf_document_content_block_shape():
    block = dv._pdf_document_content_block(b"hello")
    assert block["type"] == "document"
    assert block["source"]["type"] == "base64"
    assert block["source"]["media_type"] == "application/pdf"
    decoded = base64.b64decode(block["source"]["data"])
    assert decoded == b"hello"


# ----- _emit_pdf_read_audit -------------------------------------------------


def test_emit_pdf_read_audit_event_shape():
    captured = []
    record = _centralreach_record()
    with patch.object(dv, "audit_emit", side_effect=lambda **kw: captured.append(kw)):
        dv._emit_pdf_read_audit(record, "rule-2", "run-abc")
    assert len(captured) == 1
    event = captured[0]
    assert event["action"] == "read"
    assert event["resource"] == {
        "type": "ClinicalNote",
        "id": "502614593",
        "org": "demo",
    }
    assert event["call_type"] == "bedrock_rule_eval"
    assert event["purpose_of_use"] == "DOC_PROCESSING"
    assert event["external_control_number"] == "run-abc"
    assert event["result"] == {"rule_id": "rule-2"}


# ----- evaluate_llm_rule end-to-end (centralreach PDF) ---------------------


@pytest.fixture
def captured_bedrock_calls():
    """Replace invoke_claude_model with a stub that captures every
    call's body and returns a canned PASS response."""
    calls = []

    def _stub(*, body, **kw):
        calls.append({"body": body, "kwargs": kw})
        return {"status": "PASS", "reasoning": "looks good"}

    with patch.object(dv, "invoke_claude_model", side_effect=_stub):
        yield calls


@pytest.fixture
def captured_audit():
    """Replace audit_emit with a capture stub."""
    events = []

    def _stub(**kw):
        events.append(kw)
        return "fake-event-id"

    with patch.object(dv, "audit_emit", side_effect=_stub):
        yield events


def test_evaluate_llm_rule_uses_pdf_content_block_when_requires_pdf(
    s3_with_pdf, captured_bedrock_calls, captured_audit,
):
    """Pin: a rule with `requires_pdf: true` (rule 11) drives the
    validator to send a document content block to Bedrock, not the
    narrative text. The text blocks for rule_text/notes/schema still
    appear alongside."""
    rule_config = {
        "rule_id": "r11_chart_visuals",
        "name": "Chart visuals present",
        "rule_text": "Note must include a graph with percentages.",
        "notes": [],
        "requires_pdf": True,
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_centralreach_record(),
        org_id="demo", validation_run_id="run-abc",
    )

    # One Bedrock call (no fields_to_extract → skip step 1)
    assert len(captured_bedrock_calls) == 1
    body = captured_bedrock_calls[0]["body"]
    user_content = body["messages"][0]["content"]

    # Find the document block
    doc_blocks = [b for b in user_content if b.get("type") == "document"]
    assert len(doc_blocks) == 1
    assert doc_blocks[0]["source"]["media_type"] == "application/pdf"
    assert base64.b64decode(doc_blocks[0]["source"]["data"]) == _PDF_BYTES

    # Text blocks for rule + schema still present
    text_blocks = [b for b in user_content if b.get("type") == "text"]
    assert any("Rule:" in b["text"] for b in text_blocks)
    assert any("JSON schema:" in b["text"] for b in text_blocks)
    # Crucially: NO `Chart narrative:` block (the PDF replaces it)
    assert not any("Chart narrative:" in b["text"] for b in text_blocks)


def test_evaluate_llm_rule_uses_text_path_for_default_centralreach_rule(
    captured_bedrock_calls, captured_audit,
):
    """Pin: rules without `requires_pdf` evaluate against the
    extracted narrative text on centralreach records — no S3 fetch,
    no document block. This is the default for rules 1/2/3."""
    rule_config = {
        "rule_id": "r2_sentences",
        "name": "Sentences per hour",
        "rule_text": "There must be at least 2 sentences per hour.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_centralreach_record(),
        org_id="demo", validation_run_id="run-abc",
    )

    user_content = captured_bedrock_calls[0]["body"]["messages"][0]["content"]
    doc_blocks = [b for b in user_content if b.get("type") == "document"]
    text_blocks = [b for b in user_content if b.get("type") == "text"]
    assert doc_blocks == []
    assert any("Chart narrative:" in b["text"] for b in text_blocks)
    assert any(
        "Provider observed independent responding" in b["text"]
        for b in text_blocks
    )


def test_evaluate_llm_rule_text_path_unchanged_for_legacy_records(
    captured_bedrock_calls, captured_audit,
):
    """Pin: records with populated text still ship the text content
    block. The PR-E change must be additive — no regression in the
    existing text path."""
    rule_config = {
        "rule_id": "r1",
        "name": "Test",
        "rule_text": "Some rule.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    assert len(captured_bedrock_calls) == 1
    user_content = captured_bedrock_calls[0]["body"]["messages"][0]["content"]
    doc_blocks = [b for b in user_content if b.get("type") == "document"]
    text_blocks = [b for b in user_content if b.get("type") == "text"]
    assert doc_blocks == []
    assert any("Chart narrative:" in b["text"] for b in text_blocks)
    assert any("Patient presents in stable condition" in b["text"]
               for b in text_blocks)


def test_evaluate_llm_rule_emits_pdf_read_audit_for_requires_pdf_rule(
    s3_with_pdf, captured_bedrock_calls, captured_audit,
):
    """Pin: the bedrock_rule_eval audit fires once per LLM rule
    evaluation on a PDF read. Distinct from the bedrock_invoke audit
    which fires per Bedrock call. Default text-path rules do not emit
    this audit (see the next test)."""
    rule_config = {
        "rule_id": "r11_chart_visuals",
        "name": "Chart visuals",
        "rule_text": "Note must include a graph with percentages.",
        "notes": [],
        "requires_pdf": True,
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_centralreach_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    bedrock_rule_eval_events = [
        e for e in captured_audit
        if e.get("call_type") == "bedrock_rule_eval"
    ]
    assert len(bedrock_rule_eval_events) == 1
    assert bedrock_rule_eval_events[0]["result"] == {
        "rule_id": "r11_chart_visuals",
    }


def test_evaluate_llm_rule_does_not_emit_pdf_read_audit_for_text_path_rule(
    captured_bedrock_calls, captured_audit,
):
    """Pin: centralreach records evaluated against a default
    (text-path) rule must NOT emit the bedrock_rule_eval PDF-read
    audit — we didn't actually read the PDF."""
    rule_config = {
        "rule_id": "r3_third_person",
        "name": "Third person",
        "rule_text": "Note must be in third person.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_centralreach_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    bedrock_rule_eval_events = [
        e for e in captured_audit
        if e.get("call_type") == "bedrock_rule_eval"
    ]
    assert bedrock_rule_eval_events == []


def test_evaluate_llm_rule_text_path_does_not_emit_pdf_read_audit(
    captured_bedrock_calls, captured_audit,
):
    """Pin: the bedrock_rule_eval audit is ONLY for PDF reads. Text
    records do not produce this audit event."""
    rule_config = {
        "rule_id": "r1",
        "name": "Test",
        "rule_text": "Some rule.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    bedrock_rule_eval_events = [
        e for e in captured_audit
        if e.get("call_type") == "bedrock_rule_eval"
    ]
    assert bedrock_rule_eval_events == []


def test_evaluate_llm_rule_call_type_uses_centralreach_prefix_for_pdf(
    s3_with_pdf, captured_bedrock_calls, captured_audit,
):
    """The Bedrock `call_type` cost-attribution tag uses
    `centralreach_rule_validate:{rule_id}` for `requires_pdf` rules so
    per-org cost dashboards can split out the expensive document-block
    spend explicitly."""
    rule_config = {
        "rule_id": "r11_chart_visuals",
        "name": "Chart visuals",
        "rule_text": "Test rule.",
        "notes": [],
        "requires_pdf": True,
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_centralreach_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    assert captured_bedrock_calls[0]["kwargs"]["call_type"] == \
        "centralreach_rule_validate:r11_chart_visuals"


def test_evaluate_llm_rule_call_type_unchanged_for_text_path(
    captured_bedrock_calls, captured_audit,
):
    """Text path keeps the legacy `chart_rule_validate` call_type so
    existing cost dashboards do not change shape."""
    rule_config = {
        "rule_id": "r1",
        "name": "Test",
        "rule_text": "Test rule.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    assert captured_bedrock_calls[0]["kwargs"]["call_type"] == "chart_rule_validate"


# ----- field extraction step uses PDF input too ----------------------------


def test_field_extract_for_requires_pdf_rule_uses_pdf_block(
    s3_with_pdf, captured_audit,
):
    """When a `requires_pdf` rule has `fields_to_extract` defined,
    step 1 (field extract) ALSO runs against the PDF — not against
    text. Two Bedrock calls fire: extract, then validate, both with
    PDF content blocks."""
    # Step 1 returns extracted fields; step 2 returns PASS
    invoke_calls = []

    def _stub(*, body, **kw):
        invoke_calls.append({"body": body, "kwargs": kw})
        # First call is field extract -> return {"fields": {...}}
        # Second call is validate -> return {"status": ..., "reasoning": ...}
        if len(invoke_calls) == 1:
            return {"fields": {"graph_present": "true"}}
        return {"status": "PASS", "reasoning": "ok"}

    rule_config = {
        "rule_id": "r11_chart_visuals",
        "name": "Chart visuals",
        "rule_text": "Note must include a graph with percentages.",
        "notes": [],
        "requires_pdf": True,
        "fields_to_extract": [
            {"name": "graph_present", "type": "string",
             "description": "y/n"},
        ],
    }

    with patch.object(dv, "invoke_claude_model", side_effect=_stub):
        dv.evaluate_llm_rule(
            rule_config, fields={},
            data=_centralreach_record(),
            org_id="demo", validation_run_id="run-abc",
        )

    assert len(invoke_calls) == 2
    # Both calls included a document content block
    for call in invoke_calls:
        blocks = call["body"]["messages"][0]["content"]
        assert any(b.get("type") == "document" for b in blocks)


# ----- failure modes -------------------------------------------------------


def test_pdf_fetch_failure_surfaces_as_error_status(captured_audit):
    """If S3 GET fails for a `requires_pdf` rule, `evaluate_llm_rule`'s
    top-level except clause returns 'ERROR' with the exception
    message. The Bedrock call is NOT attempted."""
    rule_config = {
        "rule_id": "r11_chart_visuals",
        "name": "Chart visuals",
        "rule_text": "rule.",
        "notes": [],
        "requires_pdf": True,
    }
    # No moto fixture this time → S3 GET fails
    with patch.object(dv, "invoke_claude_model") as mock_invoke, \
         patch.object(dv, "_s3_client", None):
        # Force the s3 lazy resolver to mint a real boto3 client that
        # will fail to find the bucket (since we're not in moto).
        result = dv.evaluate_llm_rule(
            rule_config, fields={},
            data=_centralreach_record(),
            org_id="demo", validation_run_id="run-abc",
        )
    status, message, _ = result
    assert status == "ERROR"
    assert "LLM evaluation error" in message
    # Bedrock was not called
    assert mock_invoke.call_count == 0


# ----- chart_fields injection (LLM rules read record fields, not just text) --


def test_chart_fields_are_sent_alongside_narrative_in_validate_step(
    captured_bedrock_calls, captured_audit,
):
    """Rules like rule 6 (location cross-check) need record-level
    fields that the narrative prose doesn't carry. The full flat
    `fields` dict must ship as its own content block on both the
    field-extract and rule-validate Bedrock calls, so the model can
    look up e.g. `billing_list_location` and
    `note_provider_location` without asking Bedrock to guess them
    from prose."""
    rule_config = {
        "rule_id": "r6_location",
        "name": "Location match",
        "rule_text": "Compare billing_list_location and note_provider_location.",
        "notes": [],
    }
    fields = {
        "billing_list_location": "10: Telehealth Provided in Patient's Home",
        "note_provider_location": "12 - Home",
        "billed_minutes": 30,
    }
    dv.evaluate_llm_rule(
        rule_config, fields=fields,
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )

    # Single Bedrock call (no fields_to_extract → step 1 skipped).
    assert len(captured_bedrock_calls) == 1
    user_content = captured_bedrock_calls[0]["body"]["messages"][0]["content"]
    chart_fields_blocks = [
        b for b in user_content
        if b.get("type") == "text"
        and b["text"].startswith("Chart fields:")
    ]
    assert len(chart_fields_blocks) == 1
    payload = chart_fields_blocks[0]["text"]
    assert "billing_list_location" in payload
    assert "note_provider_location" in payload
    assert "12 - Home" in payload


def test_chart_fields_are_sent_on_step_1_field_extraction(captured_audit):
    """Same injection must fire on the field-extraction call so
    step 1's `fields_to_extract` can pull passthrough values (e.g.
    rule 7's `provider_display_value`) from the record's flat
    fields, not from the narrative."""
    calls = []

    def _stub(*, body, **kw):
        calls.append({"body": body, "kwargs": kw})
        # Step 1 shape (extract), then step 2 (validate).
        if len(calls) == 1:
            return {"fields": {"provider_display_value": "Ann Smith, BCBA"}}
        return {"status": "PASS", "reasoning": "match"}

    rule_config = {
        "rule_id": "r7_provider",
        "name": "Provider 3-way match",
        "rule_text": "Compare provider names across sources.",
        "notes": [],
        "fields_to_extract": [
            {"name": "provider_display_value", "type": "string",
             "description": "Passthrough of provider_display."},
        ],
    }
    fields = {
        "provider_display": "Ann Smith, BCBA",
        "note_provider_signature_name": "A. Smith",
    }

    with patch.object(dv, "invoke_claude_model", side_effect=_stub):
        dv.evaluate_llm_rule(
            rule_config, fields=fields,
            data=_text_record(),
            org_id="demo", validation_run_id="run-abc",
        )

    # Both calls must carry the chart fields block.
    for call in calls:
        content = call["body"]["messages"][0]["content"]
        chart_fields_blocks = [
            b for b in content
            if b.get("type") == "text" and b["text"].startswith("Chart fields:")
        ]
        assert len(chart_fields_blocks) == 1, (
            "Chart fields must ship on both step-1 (extract) and step-2 (validate)"
        )
        assert "provider_display" in chart_fields_blocks[0]["text"]


def test_empty_fields_omits_the_chart_fields_block(
    captured_bedrock_calls, captured_audit,
):
    """Legacy path: a record with no pre-extracted fields (or an
    empty dict) should NOT ship an empty `Chart fields: {}` block —
    just noise for the model."""
    rule_config = {
        "rule_id": "r_x",
        "name": "T",
        "rule_text": "Something.",
        "notes": [],
    }
    dv.evaluate_llm_rule(
        rule_config, fields={},
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    user_content = captured_bedrock_calls[0]["body"]["messages"][0]["content"]
    assert not any(
        b.get("type") == "text" and b["text"].startswith("Chart fields:")
        for b in user_content
    )


def test_chart_fields_serialization_survives_decimal_values(
    captured_bedrock_calls, captured_audit,
):
    """`fields` frequently contains `Decimal` values (they come out
    of DynamoDB that way and can also land in the record). Straight
    `json.dumps` would raise TypeError; the evaluator must use
    `default=str` so the block ships as prompt text without
    crashing the whole rule eval."""
    from decimal import Decimal
    rule_config = {
        "rule_id": "r_x",
        "name": "T",
        "rule_text": "Something.",
        "notes": [],
    }
    fields = {
        "billing_list_rate_client": Decimal("115.00"),
        "billing_list_time_worked_in_mins": Decimal("30"),
    }
    dv.evaluate_llm_rule(
        rule_config, fields=fields,
        data=_text_record(),
        org_id="demo", validation_run_id="run-abc",
    )
    # If Decimal handling was broken, the call would ERROR out
    # before reaching Bedrock. Check we made it to Bedrock and the
    # values landed as strings inside the payload.
    assert len(captured_bedrock_calls) == 1
    payload = next(
        b["text"] for b in captured_bedrock_calls[0]["body"]["messages"][0]["content"]
        if b.get("type") == "text" and b["text"].startswith("Chart fields:")
    )
    assert "115.00" in payload
    assert "30" in payload


# ----- document_id resolution -----------------------------------------------


def test_validate_document_uses_source_record_id_for_json_records():
    """Regression: JSON records (RPA + centralreach) must land in
    DynamoDB under `DOC#{source_record_id}` so the UI can link
    validation results back to the underlying document. Previously
    `validate_document` only extracted an id from `.csv` filenames
    and fell back to a nonexistent `document_id` field, so every
    centralreach record got `document_id='UNKNOWN'` and the UI
    couldn't tell them apart."""
    config = {'rules': [], 'field_mappings': {}}
    fields = {'source_record_id': '502614593', 'org_id': 'demo'}
    # Filename shape used by centralreach ingest (`.json`, not `.csv`).
    filename = (
        'data/2026-06-28/20260628T220000Z__502614593.json'
    )
    result = dv.validate_document(
        {'extracted_fields': fields}, filename, config,
        org_id='demo', validation_run_id='run-abc',
    )
    assert result['document_id'] == '502614593'


def test_validate_document_falls_back_to_csv_filename_id():
    """Legacy CSV path continues to work — id is parsed from the
    `{timestamp}__{visitID}.csv` filename shape."""
    config = {'rules': [], 'field_mappings': {}}
    filename = 'csv/2026-06-28/20260628T120000Z__98765.csv'
    result = dv.validate_document(
        {'text': 'col1,col2\nval1,val2', 'extracted_fields': {}},
        filename, config,
        org_id='demo', validation_run_id='run-abc',
    )
    assert result['document_id'] == '98765'


def test_validate_document_source_record_id_wins_over_csv_filename():
    """If somehow both are present, prefer `source_record_id` — it's
    the vendor's canonical id, not a filename convention."""
    config = {'rules': [], 'field_mappings': {}}
    filename = 'data/2026-06-28/20260628T220000Z__CSVID.csv'
    result = dv.validate_document(
        {'extracted_fields': {'source_record_id': 'RECORD-ID'}},
        filename, config,
        org_id='demo', validation_run_id='run-abc',
    )
    assert result['document_id'] == 'RECORD-ID'


def test_validate_document_falls_back_to_unknown_only_when_no_id_anywhere():
    """UNKNOWN is a loud last-resort signal that ingest-side id
    plumbing regressed. Any record hitting UNKNOWN disappears from
    the UI's per-doc view."""
    config = {'rules': [], 'field_mappings': {}}
    result = dv.validate_document(
        {'text': 'no comma, no csv'},
        'data/2026-06-28/some-file.txt', config,
        org_id='demo', validation_run_id='run-abc',
    )
    assert result['document_id'] == 'UNKNOWN'


# ----- ui_display_fields projection ----------------------------------------


def test_project_ui_display_fields_copies_by_source_key():
    """Mapping `{canonical: source}` copies `fields[source]` under `canonical`."""
    fields = {'provider_display': 'Dr. Smith', 'visit_date': '2026-06-22'}
    mapping = {'employee_name': 'provider_display', 'date': 'visit_date'}
    assert dv.project_ui_display_fields(fields, mapping) == {
        'employee_name': 'Dr. Smith',
        'date': '2026-06-22',
    }


def test_project_ui_display_fields_skips_missing_and_empty_sources():
    """Missing/None/empty values are omitted so the UI's fallback to raw
    `field_values` continues to work per-key."""
    fields = {'provider_display': 'Dr. Smith', 'visit_date': None, 'note': ''}
    mapping = {
        'employee_name': 'provider_display',
        'date': 'visit_date',
        'notes': 'note',
        'program': 'not_present',
    }
    assert dv.project_ui_display_fields(fields, mapping) == {
        'employee_name': 'Dr. Smith',
    }


def test_project_ui_display_fields_empty_mapping_returns_empty():
    """No mapping configured → empty dict → caller omits the field from
    the DDB item; API/UI fall back to raw `field_values`."""
    fields = {'provider_display': 'Dr. Smith'}
    assert dv.project_ui_display_fields(fields, {}) == {}
    assert dv.project_ui_display_fields(fields, None) == {}


def test_validate_document_emits_ui_display_fields_when_mapping_set():
    """End-to-end: an org config with `ui_display_fields` produces a
    top-level `ui_display_fields` dict on the result the DDB row
    inherits."""
    config = {
        'rules': [],
        'field_mappings': {},
        'ui_display_fields': {
            'employee_name': 'provider_display',
            'date': 'visit_date',
        },
    }
    data = {
        'extracted_fields': {
            'source_record_id': 'note-1',
            'provider_display': 'Dr. Alice',
            'visit_date': '2026-06-22',
        },
    }
    result = dv.validate_document(
        data, 'data/2026-06-22/note-1.json', config,
        org_id='demo', validation_run_id='run-1',
    )
    assert result['ui_display_fields'] == {
        'employee_name': 'Dr. Alice',
        'date': '2026-06-22',
    }
    # Raw field_values is untouched — LLM/deterministic paths still work.
    assert result['field_values']['provider_display'] == 'Dr. Alice'
    assert result['field_values']['visit_date'] == '2026-06-22'


def test_validate_document_omits_ui_display_fields_when_no_mapping():
    """Legacy orgs without the mapping produce no `ui_display_fields` key,
    so old rows and new rows for un-configured orgs stay identical."""
    config = {'rules': [], 'field_mappings': {}}
    data = {'extracted_fields': {'source_record_id': 'note-1',
                                 'provider_display': 'Dr. Alice'}}
    result = dv.validate_document(
        data, 'data/2026-06-22/note-1.json', config,
        org_id='demo', validation_run_id='run-1',
    )
    assert 'ui_display_fields' not in result


# ----- deterministic rules with LLM-extracted fields -----------------------


def test_deterministic_rule_with_fields_to_extract_runs_extraction_then_math():
    """Rule 2 pattern: LLM extracts sentence_count as a scalar, Python
    owns the PASS/FAIL math. Pins the wiring — one Bedrock call (extract
    only), and the extracted value feeds the operator so 7 sentences at
    135 min PASSes."""
    invoke_calls = []

    def _stub(*, body, **kw):
        invoke_calls.append({"body": body, "kwargs": kw})
        return {"fields": {"sentence_count": 7}}

    rule_config = {
        "rule_id": "r2_sentences",
        "name": "Session narrative has >= 2 sentences per hour",
        "type": "deterministic",
        "rule_text": "Count complete sentences in the narrative.",
        "notes": [],
        "fields_to_extract": [
            {"name": "sentence_count", "type": "integer",
             "description": "Complete sentences in the narrative."},
        ],
        "conditions": [{
            "field": "sentence_count",
            "operator": "sentence_count_meets_hourly_minimum",
            "compare_to": "billing_list_time_worked_in_mins",
        }],
        "logic": "all",
    }
    data = _centralreach_record()
    fields = {"billing_list_time_worked_in_mins": 135}

    with patch.object(dv, "invoke_claude_model", side_effect=_stub), \
         patch.object(dv, "audit_emit", side_effect=lambda **kw: None):
        result = dv.evaluate_rule(
            rule_config, fields, data=data,
            org_id="demo", validation_run_id="run-abc",
        )

    # Exactly one Bedrock call — the extract step. No validate step, no
    # second LLM call whose reasoning could flip.
    assert len(invoke_calls) == 1
    body = invoke_calls[0]["body"]
    assert body["messages"][0]["content"][0]["text"].startswith("Rule:")
    # The extraction call carries the extraction system prompt, not
    # the PASS/FAIL validate one.
    assert "extract" in body["system"].lower()

    assert result['status'] == 'PASS'
    assert result['rule_type'] == 'deterministic'
    assert '7 sentences' in result['message']
    assert 'required 6' in result['message']


def test_deterministic_rule_extract_failure_surfaces_as_error():
    """If the extraction call returns no JSON, the rule reports ERROR
    rather than silently SKIPping to a wrong verdict."""
    def _stub(*, body, **kw):
        return None

    rule_config = {
        "rule_id": "r2_sentences",
        "type": "deterministic",
        "rule_text": "Count sentences.",
        "notes": [],
        "fields_to_extract": [
            {"name": "sentence_count", "type": "integer",
             "description": "count"},
        ],
        "conditions": [{
            "field": "sentence_count",
            "operator": "sentence_count_meets_hourly_minimum",
            "compare_to": "billing_list_time_worked_in_mins",
        }],
    }

    with patch.object(dv, "invoke_claude_model", side_effect=_stub), \
         patch.object(dv, "audit_emit", side_effect=lambda **kw: None):
        result = dv.evaluate_rule(
            rule_config,
            fields={"billing_list_time_worked_in_mins": 60},
            data=_centralreach_record(),
            org_id="demo", validation_run_id="run-abc",
        )
    assert result['status'] == 'ERROR'
