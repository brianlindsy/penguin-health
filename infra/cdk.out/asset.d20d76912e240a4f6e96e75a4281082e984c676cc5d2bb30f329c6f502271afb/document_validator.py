"""
Document Validator for multi-rule batched LLM validation.

Validates documents against configurable rules using batched LLM calls:
- 1 call to extract fields for all rules
- 1 call to validate all rules
"""

import json
import os
import re
from datetime import datetime

from bedrock_client import invoke_claude_model, MODEL_ID
from field_extractor import extract_fields_from_text


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


def extract_fields_for_all_rules(rules, chart_text):
    """
    Extract fields for ALL rules in a single LLM call.

    Args:
        rules: List of rule configs, each with optional 'fields_to_extract'
        chart_text: The full document text

    Returns:
        dict: {rule_id: {field_name: value, ...}, ...}
    """
    # Collect all fields by rule
    all_fields_by_rule = {}
    for rule in rules:
        rule_id = rule.get('rule_id')
        fields = rule.get('fields_to_extract', [])
        if fields:
            all_fields_by_rule[rule_id] = fields

    if not all_fields_by_rule:
        print("No fields to extract for any rules")
        return {}

    print(f"Extracting fields for {len(all_fields_by_rule)} rules in single call")

    # Build the prompt describing all fields needed
    rules_fields_text = []
    for rule_id, fields in all_fields_by_rule.items():
        field_descriptions = [
            f"  - {f['name']} ({f.get('type', 'string')}): {f.get('description', '')}"
            for f in fields
        ]
        rules_fields_text.append(f"Rule {rule_id}:\n" + "\n".join(field_descriptions))

    fields_prompt = "\n\n".join(rules_fields_text)

    system_prompt = """You are a Healthcare Compliance Auditor. Extract the specified fields for each rule from the chart text.

Return a JSON object where keys are rule IDs and values are objects containing the extracted field values.
Example format:
{
  "rule_1": {"recipient": "Face to Face", "service_location": "Main Office"},
  "rule_2": {"start_time": "9:00 AM", "end_time": "10:00 AM"}
}

If a field cannot be found, use null for its value."""

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 2048,
        'temperature': 0.01,
        'messages': [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"Fields to extract by rule:\n\n{fields_prompt}"},
                    {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
                ]
            }
        ]
    }

    try:
        response_json = invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body=body,
            return_json_only=True,
            raise_on_error=True,
            retries=1
        )

        if response_json is None:
            print("Failed to extract fields - no JSON in response")
            return {}

        print(f"Extracted fields for {len(response_json)} rules")
        return response_json

    except Exception as e:
        print(f"Error extracting fields: {str(e)}")
        return {}


def validate_all_rules(rules, chart_text, extracted_fields_by_rule):
    """
    Validate ALL rules in a single LLM call.

    Args:
        rules: List of rule configs with rule_text, notes, etc.
        chart_text: The full document text
        extracted_fields_by_rule: Dict from extract_fields_for_all_rules

    Returns:
        list: [{rule_id, status, reasoning}, ...]
    """
    print(f"Validating {len(rules)} rules in single call")

    # Build rules description
    rules_text_parts = []
    for rule in rules:
        rule_id = rule.get('rule_id')
        rule_text = rule.get('rule_text', '')
        notes = rule.get('notes', [])
        notes_text = '\n'.join(f"  - {note}" for note in notes) if notes else '  None'

        # Include extracted fields if available
        fields_text = ""
        if rule_id in extracted_fields_by_rule:
            fields = extracted_fields_by_rule[rule_id]
            fields_text = f"\n  Extracted fields: {json.dumps(fields)}"

        rules_text_parts.append(
            f"Rule {rule_id}: {rule.get('name', '')}\n"
            f"  Criteria: {rule_text}\n"
            f"  Notes:\n{notes_text}{fields_text}"
        )

    all_rules_text = "\n\n".join(rules_text_parts)

    # Get list of rule IDs for the prompt
    rule_ids = [rule.get('rule_id') for rule in rules]
    rule_ids_str = ', '.join(f'"{rid}"' for rid in rule_ids)

    system_prompt = f"""You are a Healthcare Compliance Auditor. Validate each rule against the chart text.

For each rule, determine if it PASSES, FAILS, or should be SKIPPED (if not applicable).

IMPORTANT: Use the EXACT rule_id values provided: {rule_ids_str}

Return a JSON array with an object for each rule:
[
  {{"rule_id": "<exact_rule_id>", "status": "PASS", "reasoning": "Brief explanation"}},
  {{"rule_id": "<exact_rule_id>", "status": "FAIL", "reasoning": "Brief explanation of failure"}}
]

Status must be exactly one of: PASS, FAIL, SKIP
The rule_id must exactly match the IDs provided above."""

    body = {
        "system": system_prompt,
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 4096,
        'temperature': 0.01,
        'messages': [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"Rules to validate:\n\n{all_rules_text}"},
                    {"type": "text", "text": f"Chart text:\n\n{chart_text}"},
                ]
            }
        ]
    }

    try:
        response_json = invoke_claude_model(
            inference_profile_id=MODEL_ID,
            body=body,
            return_json_only=True,
            raise_on_error=True,
            retries=1
        )

        if response_json is None:
            print("Failed to validate rules - no JSON in response")
            return []

        # Handle both array response and object with 'results' key
        if isinstance(response_json, list):
            results = response_json
        elif isinstance(response_json, dict) and 'results' in response_json:
            results = response_json['results']
        else:
            # Try to find array in response
            results = response_json if isinstance(response_json, list) else []

        print(f"Validated {len(results)} rules")
        return results

    except Exception as e:
        print(f"Error validating rules: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return []


def validate_document(data, filename, config, org_id, validation_run_id):
    """
    Run all validation rules against a document using batched LLM calls.

    Uses 2 LLM calls total (instead of 2 per rule):
    1. Extract fields for all rules
    2. Validate all rules

    Args:
        data: Document data dict with 'text' key
        filename: Source filename for the document
        config: Organization config with rules and field_mappings
        org_id: Organization ID
        validation_run_id: ID for this validation run

    Returns:
        dict: Validation results including rule statuses and field values
    """
    text = data.get('text', '')
    field_mappings = config.get('field_mappings', {})
    fields = extract_fields_from_text(text, field_mappings)

    enabled_rules = [rule for rule in config.get('rules', []) if rule.get('enabled', True)]

    if not enabled_rules:
        print("No enabled rules to evaluate")
        rule_results = []
    else:
        print(f"Validating document against {len(enabled_rules)} rules (batched)")

        # Step 1: Extract fields for all rules (1 LLM call)
        extracted_fields_by_rule = extract_fields_for_all_rules(enabled_rules, text)

        # Step 2: Validate all rules (1 LLM call)
        validation_results = validate_all_rules(enabled_rules, text, extracted_fields_by_rule)

        # Convert validation results to expected format
        rule_results = []
        results_by_id = {str(r.get('rule_id')): r for r in validation_results}

        # Debug logging
        expected_ids = [rule.get('rule_id') for rule in enabled_rules]
        returned_ids = list(results_by_id.keys())
        print(f"Expected rule IDs: {expected_ids}")
        print(f"Returned rule IDs: {returned_ids}")

        for rule in enabled_rules:
            rule_id = str(rule.get('rule_id'))
            result = results_by_id.get(rule_id)

            if result:
                status = result.get('status', 'ERROR')
                reasoning = result.get('reasoning', '')
                rule_results.append({
                    'rule_id': rule_id,
                    'rule_name': rule.get('name'),
                    'category': rule.get('category'),
                    'status': status,
                    'message': f"{status} - {reasoning}" if reasoning else status
                })
            else:
                # Rule not in response - mark as error
                rule_results.append({
                    'rule_id': rule_id,
                    'rule_name': rule.get('name'),
                    'category': rule.get('category'),
                    'status': 'ERROR',
                    'message': 'Rule not found in LLM validation response'
                })

    passed = sum(1 for r in rule_results if r['status'] == 'PASS')
    failed = sum(1 for r in rule_results if r['status'] == 'FAIL')
    skipped = sum(1 for r in rule_results if r['status'] == 'SKIP')
    errors = sum(1 for r in rule_results if r['status'] == 'ERROR')

    print(f"Document validation complete: {passed} PASS, {failed} FAIL, {skipped} SKIP, {errors} ERROR")

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
