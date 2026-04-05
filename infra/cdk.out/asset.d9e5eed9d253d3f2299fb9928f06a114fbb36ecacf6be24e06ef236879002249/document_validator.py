"""
Document Validator for per-rule LLM validation with multi-threading.

Validates documents against configurable rules using per-rule LLM calls:
- 1 call to extract fields (if fields_to_extract is defined)
- 1 call to validate the rule
"""

import json
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from bedrock_client import invoke_claude_model, MODEL_ID
from field_extractor import extract_fields


def extract_document_id_from_filename(filename):
    """
    Extract document ID from CSV filename in format visit_documentID.csv.

    Args:
        filename: The filename or path (e.g., 'path/to/visit_12345.csv')

    Returns:
        str: The document ID or None if not matched
    """
    basename = os.path.basename(filename)
    if basename.endswith('.csv'):
        # Match pattern: visit_<documentID>.csv
        match = re.match(r'visit_(.+)\.csv$', basename)
        if match:
            return match.group(1)
    return None


def evaluate_rule(rule_config, fields, data=None):
    """Evaluate a single rule against the document fields."""
    rule_type = rule_config.get('type', 'llm')

    result = {
        'rule_id': rule_config.get('rule_id'),
        'rule_name': rule_config.get('name'),
        'category': rule_config.get('category')
    }

    try:
        if rule_type == 'llm':
            status, message, _ = evaluate_llm_rule(rule_config, fields, data)
        else:
            status = 'SKIP'
            message = f'Unsupported rule type: {rule_type}. Only "llm" is supported.'

        result['status'] = status
        result['message'] = message

    except Exception as e:
        result['status'] = 'ERROR'
        result['message'] = f'Error evaluating rule: {str(e)}'

    return result


def evaluate_llm_rule(rule_config, fields, data=None):
    """
    Evaluate a rule using AWS Bedrock Claude with structured JSON output.
    Uses the flat schema with rule_text, fields_to_extract, and notes.

    Two-step approach:
    1. Extract fields from chart text (if fields_to_extract is defined)
    2. Validate the rule using extracted fields
    """
    # Flat schema fields
    rule_text = rule_config.get('rule_text', '')
    fields_to_extract = rule_config.get('fields_to_extract', [])
    notes = rule_config.get('notes', [])

    print(f"Evaluating rule {rule_config.get('rule_id')} - {rule_config.get('name')}")

    # Get the full text from the document
    chart_text = ''
    if data:
        chart_text = data.get('text', '')
        print(f"Chart text length: {len(chart_text)} characters")

    # If no text available, fall back to fields
    if not chart_text:
        chart_text = json.dumps(fields, indent=2)
        print(f"No text found, using fields JSON: {len(chart_text)} characters")

    try:
        extracted_fields = None

        # Step 1: Extract fields if fields_to_extract is defined
        if fields_to_extract:
            extracted_fields = _extract_rule_fields(
                MODEL_ID, rule_text, notes, fields_to_extract, chart_text
            )
            if extracted_fields is None:
                return 'ERROR', 'No JSON found in Claude response (field extraction)', ''

        # Step 2: Validate the rule
        return _validate_rule(
            MODEL_ID, rule_text, notes, chart_text, extracted_fields
        )

    except Exception as e:
        print(f"LLM ERROR: {str(e)}")
        import traceback
        print(f"LLM TRACEBACK: {traceback.format_exc()}")
        error_msg = f'LLM evaluation error: {str(e)}'
        return 'ERROR', error_msg, error_msg


def _extract_rule_fields(model_id, rule_text, notes, fields_to_extract, chart_text):
    """
    Step 1: Extract fields from chart text to help validate the rule.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Text, and a list of fields to extract from the Chart Text. Your only purpose is to extract the fields, and return them in a JSON object.
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

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
                    {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
                    {"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"},
                ]
            }
        ]
    }

    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1
    )

    if response_json is None:
        return None

    extracted = response_json.get('fields', {})
    print(f"Fields extracted: {extracted}")
    return extracted


def _validate_rule(model_id, rule_text, notes, chart_text, extracted_fields=None):
    """
    Step 2: Validate the rule using extracted fields (if any).
    Returns (status, message, reasoning) tuple.
    """
    system_prompt = """You are a Healthcare Compliance Auditor. You will be given a Rule to validate, the patient Chart Text, and optionally some pre-extracted fields. Validate whether the rule passes or fails.
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
        {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
    ]

    if extracted_fields:
        content.append({"type": "text", "text": f"Extracted fields:\n\n{json.dumps(extracted_fields)}"})

    content.append({"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"})

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 1024,
        'temperature': 0.01,
        'messages': [{"role": "user", "content": content}]
    }

    response_json = invoke_claude_model(
        inference_profile_id=model_id,
        body=body,
        return_json_only=True,
        raise_on_error=True,
        retries=1
    )

    if response_json is None:
        return 'ERROR', 'No JSON found in Claude response', ''

    status = response_json['status']
    reasoning = response_json['reasoning']

    return status, f"{status} - {reasoning}", reasoning


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
        dict: Validation results including rule statuses and field values
    """
    field_mappings = config.get('field_mappings', {})
    csv_column_mappings = config.get('csv_column_mappings', {})
    fields = extract_fields(data, field_mappings, csv_column_mappings)

    enabled_rules = [rule for rule in config.get('rules', []) if rule.get('enabled', True)]

    if not enabled_rules:
        rule_results = []
    else:
        print(f"Evaluating {len(enabled_rules)} rules in parallel...")
        max_workers = min(10, len(enabled_rules))

        rule_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_rule = {
                executor.submit(evaluate_rule, rule_config, fields, data): rule_config
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

    # For CSV files, extract document_id from filename (format: visit_documentID.csv)
    # Otherwise use the field value from the document
    document_id = extract_document_id_from_filename(filename)
    if not document_id:
        document_id = fields.get('document_id', 'UNKNOWN')

    return {
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
