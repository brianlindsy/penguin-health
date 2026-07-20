"""
Document Validator for per-rule LLM validation with multi-threading.

Validates documents against configurable rules using per-rule LLM calls:
- 1 call to extract fields (if fields_to_extract is defined)
- 1 call to validate the rule

Records produced by the centralreach ingest path (source =
"centralreach.api") populate `text` with the Bedrock-extracted
clinical narrative at ingest time AND carry the original PDF's
`pdf_s3_key` in `extracted_fields`. By default these records use the
text path (cheaper, ~1K tokens). Rules with `requires_pdf: true` in
their rule_config (e.g. rule 11, which asks about
charts/percentages/graphs not present in the narrative prose) opt
into the PDF document-block path and pay the document-extraction
cost on every eval. See `docs/centralreach-api-integration.md`
"Bedrock rule evaluation" section.
"""

import base64
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import os

import boto3

from audit import SystemPrincipal, emit as audit_emit
from bedrock_client import invoke_claude_model, MODEL_ID

_AUDIT_PRINCIPAL = SystemPrincipal(
    os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'rules-engine-rag')
)
from field_extractor import extract_fields
from deterministic_evaluator import evaluate_deterministic_rule
import queue_handler


_s3_client = None


def _get_s3_client():
    """Lazily resolve a Bedrock-region S3 client.

    Module-load-time client creation breaks tests that wrap calls in
    moto contexts after the module is already imported. This pattern
    matches `audit.emitter._resolve_table` for the same reason.
    """
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client


def _load_chart_input(data, rule_config=None):
    """Return `(kind, payload)` for the Bedrock content block.

    Dispatch order:
      * Rule opts in via `rule_config['requires_pdf']: true` AND the
        record carries `extracted_fields.pdf_s3_key` → `("pdf", bytes)`.
        This is the path for rule 11 — the prose narrative doesn't
        contain the charts/percentages/graphs it asks about, so we
        pay the document-block cost on every eval of that rule.
      * Record has populated `text` → `("text", str)`. centralreach
        records hit this path by default — the narrative was
        extracted at ingest time and stored in `text`, so rules 1,
        2, 3 read it directly.
      * Empty fallback → `("text", "")`. Callers substitute the
        fields dict serialized as JSON to preserve legacy behavior.
    """
    if not data:
        return 'text', ''

    requires_pdf = bool((rule_config or {}).get('requires_pdf'))
    pdf_s3_key = (data.get('extracted_fields') or {}).get('pdf_s3_key')
    if requires_pdf and pdf_s3_key:
        return 'pdf', _fetch_pdf_bytes(pdf_s3_key, data)

    text = data.get('text')
    if text:
        return 'text', text

    return 'text', ''


def _fetch_pdf_bytes(pdf_s3_key, data):
    """Download PDF bytes from the per-org bucket.

    `data` carries `org_id`, which the per-org bucket name derives
    from. Bucket convention matches `lambda/api/nl_agent_tools.py::
    org_data_bucket` and `centralreach.pdf_storage._bucket_for_org`.
    """
    org_id = data.get('org_id')
    if not org_id:
        raise ValueError(
            "centralreach record missing org_id; cannot resolve "
            "per-org bucket for pdf_s3_key fetch"
        )
    bucket = f"penguin-health-{org_id}"
    response = _get_s3_client().get_object(Bucket=bucket, Key=pdf_s3_key)
    return response['Body'].read()


def _pdf_document_content_block(pdf_bytes):
    """Build the Bedrock `{type: document}` block for PDF input.

    Anthropic's Claude on Bedrock accepts PDFs as base64-encoded
    document blocks; the model reads visible content (text and
    rendered tables) directly.
    """
    return {
        "type": "document",
        "source": {
            "type": "base64",
            "media_type": "application/pdf",
            "data": base64.b64encode(pdf_bytes).decode("ascii"),
        },
    }


def _emit_pdf_read_audit(data, rule_id, validation_run_id):
    """Emit the standard ClinicalNote read audit for a PDF fetch.

    Mirrors the audit pattern that runs at ingest time
    (`centralreach.result_writer.persist_note`). The
    `bedrock_invoke` audit that wraps every Bedrock call continues
    to fire; this is the per-PDF-read event the design doc names.
    """
    org_id = data.get('org_id') or 'unknown'
    record_id = data.get('source_record_id') or ''
    audit_emit(
        action='read',
        resource={
            'type': 'ClinicalNote',
            'id': record_id,
            'org': org_id,
        },
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=org_id,
        purpose_of_use='DOC_PROCESSING',
        call_type='bedrock_rule_eval',
        external_control_number=validation_run_id,
        result={'rule_id': rule_id},
    )


def extract_document_id_from_filename(filename):
    """
    Extract document ID from CSV filename in format {timestamp}__{visitID}.csv.
    Falls back to the whole stem if no `__` separator is present (legacy files).

    Args:
        filename: The filename or path (e.g., 'path/to/20260503T120000__12345.csv')

    Returns:
        str: The document ID or None if not a .csv file
    """
    basename = os.path.basename(filename)
    if not basename.endswith('.csv'):
        return None
    stem = basename[:-4]
    return stem.rsplit('__', 1)[-1]


def evaluate_rule(rule_config, fields, data=None, *,
                  org_id=None, validation_run_id=None):
    """Evaluate a single rule against the document fields.

    `org_id` and `validation_run_id` are forwarded to the LLM cost hook
    so per-org spend rolls up by validation run in CloudWatch."""
    rule_type = rule_config.get('type', 'llm')

    result = {
        'rule_id': rule_config.get('rule_id'),
        'rule_name': rule_config.get('name'),
        'category': rule_config.get('category'),
        'rule_type': rule_type,
    }

    try:
        if rule_type == 'llm':
            status, message, _ = evaluate_llm_rule(
                rule_config, fields, data,
                org_id=org_id, validation_run_id=validation_run_id,
            )
        elif rule_type == 'deterministic':
            # If the rule declares fields_to_extract, run the same
            # single-scalar LLM extraction step LLM rules use, merge the
            # result into fields, then hand to the deterministic evaluator.
            # This lets a rule outsource "read a number off the prose" to
            # the LLM while keeping the PASS/FAIL decision in Python.
            eval_fields = fields
            if rule_config.get('fields_to_extract'):
                extracted = _extract_fields_for_deterministic(
                    rule_config, fields, data,
                    org_id=org_id, validation_run_id=validation_run_id,
                )
                if extracted is None:
                    status = 'ERROR'
                    message = 'No JSON found in Claude response (field extraction)'
                    result['status'] = status
                    result['message'] = message
                    return result
                eval_fields = {**fields, **extracted}
            status, message = evaluate_deterministic_rule(
                rule_config, eval_fields, data,
            )
        else:
            status = 'SKIP'
            message = f'Unsupported rule type: {rule_type}. Only "llm" and "deterministic" are supported.'

        result['status'] = status
        result['message'] = message

    except Exception as e:
        result['status'] = 'ERROR'
        result['message'] = f'Error evaluating rule: {str(e)}'

    return result


def evaluate_llm_rule(rule_config, fields, data=None, *,
                      org_id=None, validation_run_id=None):
    """
    Evaluate a rule using AWS Bedrock Claude with structured JSON output.
    Uses the flat schema with rule_text, fields_to_extract, and notes.

    Two-step approach:
    1. Extract fields from chart text (if fields_to_extract is defined)
    2. Validate the rule using extracted fields

    Chart input dispatch (via `_load_chart_input`):
    * Rules with `requires_pdf: true` in their rule_config use the
      PDF document-block path when the record has a `pdf_s3_key`
      (centralreach rule 11). Pays the document-extraction cost on
      every eval; reserved for rules whose content lives in chart
      visuals not present in the prose narrative.
    * All other records with populated `data.text` use the text path
      — including centralreach records, whose narrative was
      extracted at ingest time.
    """
    # Flat schema fields
    rule_text = rule_config.get('rule_text', '')
    fields_to_extract = rule_config.get('fields_to_extract', [])
    notes = rule_config.get('notes', [])
    rule_id = rule_config.get('rule_id', '')

    print(f"Evaluating rule {rule_id} - {rule_config.get('name')}")

    try:
        chart_kind, chart_input = _load_chart_input(data, rule_config)

        # Text-path fallback: when no chart text exists and the record
        # has no PDF, fall back to the fields dict as JSON so legacy
        # behavior continues to work.
        if chart_kind == 'text' and not chart_input:
            chart_input = json.dumps(fields, indent=2)
            print(f"No text found, using fields JSON: {len(chart_input)} characters")
        elif chart_kind == 'text':
            print(f"Chart text length: {len(chart_input)} characters")
        elif chart_kind == 'pdf':
            print(f"Chart PDF length: {len(chart_input)} bytes")
            _emit_pdf_read_audit(data, rule_id, validation_run_id)

        extracted_fields = None

        # Step 1: Extract fields if fields_to_extract is defined
        if fields_to_extract:
            extracted_fields = _extract_rule_fields(
                MODEL_ID, rule_text, notes, fields_to_extract,
                chart_kind, chart_input, fields,
                org_id=org_id, validation_run_id=validation_run_id,
                rule_id=rule_id,
            )
            if extracted_fields is None:
                return 'ERROR', 'No JSON found in Claude response (field extraction)', ''

        # Step 2: Validate the rule
        return _validate_rule(
            MODEL_ID, rule_text, notes,
            chart_kind, chart_input, fields, extracted_fields,
            org_id=org_id, validation_run_id=validation_run_id,
            rule_id=rule_id,
        )

    except Exception as e:
        print(f"LLM ERROR: {str(e)}")
        import traceback
        print(f"LLM TRACEBACK: {traceback.format_exc()}")
        error_msg = f'LLM evaluation error: {str(e)}'
        return 'ERROR', error_msg, error_msg


def _extract_rule_fields(model_id, rule_text, notes, fields_to_extract,
                         chart_kind, chart_input, chart_fields,
                         *, org_id=None, validation_run_id=None,
                         rule_id=''):
    """
    Step 1: Extract fields from chart text/PDF to help validate the rule.

    `chart_kind` is `"text"` or `"pdf"` from `_load_chart_input`. The
    content block list adapts: text records send `{type: "text"}` as
    before; PDF records send a `{type: "document"}` block in addition
    to the existing rule/notes/schema text blocks.

    `chart_fields` is the flat pre-extracted field dict from
    `field_extractor.extract_fields_from_json_record` (or CSV columns
    in the legacy path). It ships as a JSON content block alongside
    the chart text/PDF so the model can pull values directly from
    structured record fields (e.g. `billing_list_location`,
    `note_provider_signature_name`) that don't appear in the
    narrative prose.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Narrative, a JSON object of Chart Fields already extracted from the record, and a list of fields to extract. Your only purpose is to extract the fields, using the Chart Fields when the value is present there and the Chart Narrative otherwise, and return them in a JSON object.
Please respond with JSON, with the key: 'fields'. The value should be an object with the field names as keys."""

    # Build JSON schema for field extraction
    properties = {
        f['name']: {
            'type': f.get('type', 'string'),
            'description': f.get('description', '')
        } for f in fields_to_extract
    }
    field_names = [f['name'] for f in fields_to_extract]

    json_schema = {
        "type": "object",
        "properties": {
            "fields": {
                "type": "object",
                "properties": properties,
                "required": field_names
            }
        },
        "required": ["fields"]
    }

    # Format notes as string
    notes_text = '\n'.join(f"- {note}" for note in notes) if notes else 'None'

    content = [
        {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
    ]
    if chart_kind == 'pdf':
        content.append(_pdf_document_content_block(chart_input))
    else:
        content.append({"type": "text", "text": f"Chart narrative:\n\n{chart_input}"})
    if chart_fields:
        content.append({
            "type": "text",
            "text": f"Chart fields:\n\n{json.dumps(chart_fields, default=str)}",
        })
    content.append({"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"})

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [{"role": "user", "content": content}]
    }

    # Audit the Bedrock call BEFORE invocation so a Lambda crash during
    # invocation still leaves a trail of what we sent. The model_id is
    # the resource_id; the validation_run_id is the parent so an Athena
    # query can group all per-rule Bedrock calls under a single run.
    audit_emit(
        action='execute',
        resource={'type': 'BedrockPrompt', 'id': model_id, 'org': org_id},
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=org_id or 'unknown',
        purpose_of_use='DOC_PROCESSING',
        call_type='bedrock_invoke',
        external_control_number=validation_run_id,
    )

    call_type = (
        f'centralreach_field_extract:{rule_id}'
        if chart_kind == 'pdf'
        else 'chart_field_extract'
    )
    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1,
        org_id=org_id,
        call_type=call_type,
        parent_request_id=validation_run_id,
    )

    if response_json is None:
        return None

    extracted = response_json.get('fields', {})
    print(f"Fields extracted: {len(extracted)} fields, names={list(extracted.keys())}")
    return extracted


def _extract_fields_for_deterministic(rule_config, fields, data, *,
                                      org_id=None, validation_run_id=None):
    """Run the LLM field-extraction step so a deterministic rule can
    read a scalar (e.g. `sentence_count`) off the prose.

    The extraction call is the reliable half of the two-step LLM
    pattern — single-scalar JSON output, temp 0.01, no verdict text
    to flip mid-response. Returns the extracted dict or None if the
    call produced no JSON.
    """
    rule_id = rule_config.get('rule_id', '')
    rule_text = rule_config.get('rule_text', '')
    notes = rule_config.get('notes', [])
    fields_to_extract = rule_config.get('fields_to_extract', [])

    chart_kind, chart_input = _load_chart_input(data, rule_config)
    if chart_kind == 'text' and not chart_input:
        chart_input = json.dumps(fields, indent=2)
    if chart_kind == 'pdf':
        _emit_pdf_read_audit(data, rule_id, validation_run_id)

    return _extract_rule_fields(
        MODEL_ID, rule_text, notes, fields_to_extract,
        chart_kind, chart_input, fields,
        org_id=org_id, validation_run_id=validation_run_id,
        rule_id=rule_id,
    )


def _validate_rule(model_id, rule_text, notes,
                   chart_kind, chart_input, chart_fields=None,
                   extracted_fields=None,
                   *, org_id=None, validation_run_id=None,
                   rule_id=''):
    """
    Step 2: Validate the rule using extracted fields (if any).
    Returns (status, message, reasoning) tuple.

    `chart_kind` is `"text"` or `"pdf"`. PDF input ships as a
    `{type: "document"}` content block; text input ships as a
    `{type: "text"}` block.

    `chart_fields` is the same flat pre-extracted field dict passed
    to `_extract_rule_fields`, shipped alongside the chart so the
    model can consult values that only exist in structured record
    fields (e.g. `billing_list_location`,
    `note_provider_signature_name`) — data the narrative prose does
    not carry. `extracted_fields` is the step-1 output, which may
    itself have been populated from those chart fields.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Narrative, a JSON object of Chart Fields already extracted from the record, and optionally some pre-extracted fields. Validate whether the rule passes or fails, using the Chart Fields and Chart Narrative together as needed.
Please respond with JSON, with the keys: 'status' and 'reasoning'. The status should be one of: 'PASS', 'FAIL', 'SKIP'. The reasoning should be a short explanation of the reason for the status."""

    json_schema = {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "description": "The status of the rule. One of: 'PASS', 'FAIL', 'SKIP'.",
                "enum": ["PASS", "FAIL", "SKIP"]
            },
            "reasoning": {
                "type": "string",
                "description": "The reasoning for the status.",
            },
        },
        "required": ["status", "reasoning"]
    }

    # Format notes as string
    notes_text = '\n'.join(f"- {note}" for note in notes) if notes else 'None'

    # Build message content
    content = [
        {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
    ]
    if chart_kind == 'pdf':
        content.append(_pdf_document_content_block(chart_input))
    else:
        content.append({"type": "text", "text": f"Chart narrative:\n\n{chart_input}"})

    if chart_fields:
        content.append({
            "type": "text",
            "text": f"Chart fields:\n\n{json.dumps(chart_fields, default=str)}",
        })

    if extracted_fields:
        content.append({"type": "text", "text": f"Extracted fields:\n\n{json.dumps(extracted_fields, default=str)}"})

    content.append({"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"})

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [{"role": "user", "content": content}]
    }

    audit_emit(
        action='execute',
        resource={'type': 'BedrockPrompt', 'id': model_id, 'org': org_id},
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=org_id or 'unknown',
        purpose_of_use='DOC_PROCESSING',
        call_type='bedrock_invoke',
        external_control_number=validation_run_id,
    )

    call_type = (
        f'centralreach_rule_validate:{rule_id}'
        if chart_kind == 'pdf'
        else 'chart_rule_validate'
    )
    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1,
        org_id=org_id,
        call_type=call_type,
        parent_request_id=validation_run_id,
    )

    if response_json is None:
        return 'ERROR', 'No JSON found in Claude response', ''

    status = response_json['status']
    reasoning = response_json['reasoning']

    return status, f"{status} - {reasoning}", reasoning


def project_ui_display_fields(fields, mapping):
    """
    Build the canonical display dict from `fields` per the org's UI mapping.

    `mapping` is `{canonical_name: source_key}` — e.g.
    ``{"employee_name": "provider_display"}`` copies
    `fields["provider_display"]` into the result as `employee_name`.
    Source keys missing from `fields` (or present with a None/empty value)
    are skipped so the UI's `??` fallback keeps working.

    An empty `mapping` yields `{}` — callers omit the field from the DDB
    item entirely, and the UI reads `field_values` unchanged.
    """
    if not mapping or not fields:
        return {}
    out = {}
    for canonical_name, source_key in mapping.items():
        value = fields.get(source_key)
        if value in (None, ''):
            continue
        out[canonical_name] = value
    return out


def _resolve_document_id(data, filename, fields=None):
    """Resolve the vendor's stable document id from the incoming record.

    Preference order matches the original inline logic that used to live
    at the tail end of `validate_document`:
      1. `source_record_id` on the record (JSON: top-level; extracted
         fields dict: `fields['source_record_id']`).
      2. Filename parse (`{timestamp}__{visitID}.csv` → visitID).
      3. `document_id` on the extracted fields (org-configured mapping).
      4. `"UNKNOWN"` — same last-resort sentinel as before.
    """
    document_id = None
    if isinstance(data, dict):
        document_id = data.get('source_record_id')
    if not document_id and fields:
        document_id = fields.get('source_record_id')
    if not document_id:
        document_id = extract_document_id_from_filename(filename)
    if not document_id and fields:
        document_id = fields.get('document_id')
    return document_id or 'UNKNOWN'


def validate_document(data, filename, config, org_id, validation_run_id):
    """
    Run all validation rules against a document using multi-threaded per-rule evaluation.

    Args:
        data: Document data dict with 'text' key
        filename: Source filename for the document
        config: Organization config with rules and field_mappings
        org_id: Organization ID
        validation_run_id: ID for this validation run

    Returns:
        dict: Validation results including rule statuses and field values.
        If ``QUEUE_WRITE_ENABLED`` is on and the incoming record matches
        the latest queue entry's ``content_hash`` for this document, we
        short-circuit and return
        ``{'skipped_duplicate': True, ...}`` — the caller in
        ``rules_engine_rag`` handles the sentinel row + audit + queue
        bookkeeping. Field extraction and rule evaluation are skipped
        entirely in that branch.
    """
    # Dedup fork — must run BEFORE extract_fields so byte-identical
    # resends never pay LLM/rules-engine cost. Feature-flagged so the
    # write path can be shipped dark and turned on after backfill.
    pointer = None
    content_hash = None
    doc_id_for_lookup = _resolve_document_id(data, filename)
    if queue_handler.is_enabled() and doc_id_for_lookup != 'UNKNOWN':
        content_hash = queue_handler.compute_content_hash(data)
        pointer = queue_handler.lookup_pointer(org_id, doc_id_for_lookup)
        if pointer and pointer.get('content_hash') == content_hash:
            return {
                'skipped_duplicate': True,
                'document_id': doc_id_for_lookup,
                'organization_id': org_id,
                'filename': filename,
                'content_hash': content_hash,
                'duplicate_of_version_sk': pointer.get('latest_version_sk'),
                'validation_run_id': validation_run_id,
            }

    field_mappings = config.get('field_mappings', {})
    csv_column_mappings = config.get('csv_column_mappings', {})
    fields = extract_fields(data, field_mappings, csv_column_mappings)
    ui_display_fields = project_ui_display_fields(
        fields, config.get('ui_display_fields') or {}
    )

    enabled_rules = [rule for rule in config.get('rules', []) if rule.get('enabled', True)]

    if not enabled_rules:
        rule_results = []
    else:
        print(f"Evaluating {len(enabled_rules)} rules in parallel...")
        max_workers = min(10, len(enabled_rules))

        rule_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_rule = {
                executor.submit(
                    evaluate_rule, rule_config, fields, data,
                    org_id=org_id, validation_run_id=validation_run_id,
                ): rule_config
                for rule_config in enabled_rules
            }

            for future in as_completed(future_to_rule):
                rule_config = future_to_rule[future]
                try:
                    result = future.result()
                    rule_results.append(result)
                except Exception as e:
                    print(f"Error evaluating rule {rule_config.get('rule_id')}: {str(e)}")
                    rule_results.append({
                        'rule_id': rule_config.get('rule_id'),
                        'rule_name': rule_config.get('name'),
                        'category': rule_config.get('category'),
                        'status': 'ERROR',
                        'message': f'Exception during parallel execution: {str(e)}'
                    })

    passed = sum(1 for r in rule_results if r['status'] == 'PASS')
    failed = sum(1 for r in rule_results if r['status'] == 'FAIL')
    skipped = sum(1 for r in rule_results if r['status'] == 'SKIP')

    # Any record whose id can't be resolved lands under `DOC#UNKNOWN`
    # in DynamoDB and disappears from the UI's per-document view, so
    # it's a loud signal that ingest-side id plumbing regressed.
    document_id = _resolve_document_id(data, filename, fields)

    result = {
        'validation_run_id': validation_run_id,
        'organization_id': org_id,
        'document_id': document_id,
        'filename': filename,
        'validation_timestamp': datetime.utcnow().isoformat(),
        'config_version': config.get('version', 'unknown'),
        'summary': {
            'total_rules': len(rule_results),
            'passed': passed,
            'failed': failed,
            'skipped': skipped
        },
        'rules': rule_results,
        'field_values': fields
    }
    if ui_display_fields:
        result['ui_display_fields'] = ui_display_fields
    if content_hash is not None:
        # Hoisted so process_file can persist it on the pointer/version
        # rows without recomputing.
        result['content_hash'] = content_hash
    return result
