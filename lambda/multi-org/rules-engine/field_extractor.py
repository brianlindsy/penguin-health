"""
Field Extractor for extracting values from document text and CSV content.

Supports three extraction modes:
1. JSON-record mode: Read `extracted_fields` from an RpaNoteRecord JSON payload,
   plus selected top-level keys (org_id, source_record_id) and the flattened
   `encounter` block (visit_date, provider_display, note_type). Used by the
   RPA ingestion path.
2. CSV mode: Column-based extraction using csv_column_mappings (for SFTP CSVs).
3. Text mode: Pattern matching on text lines (for PDFs/text documents).

Uses organization-specific field mappings from DynamoDB RULES_CONFIG.
"""

import csv
from io import StringIO


def extract_fields_from_text(text, field_mappings):
    """
    Extract field values from text using simple pattern matching.

    For each field mapping (e.g., "document_id": "Consumer Service ID:"):
    - Searches for lines containing the key
    - Extracts the value after the key on the same line

    Args:
        text: Document text to search
        field_mappings: Dict mapping field names to key patterns
            e.g., {"document_id": "Consumer Service ID:", "consumer_name": "Consumer Name:"}

    Returns:
        dict: Extracted field values, with None for fields not found
    """
    fields = {}

    if not text or not field_mappings:
        return fields

    print(f"Extracting fields from text ({len(text)} chars)")

    lines = text.split('\n')
    print(f"Text split into {len(lines)} lines")

    for field_name, key_pattern in field_mappings.items():
        value = None

        for line in lines:
            if key_pattern in line:
                parts = line.split(key_pattern, 1)
                if len(parts) > 1:
                    value = parts[1].strip()
                    print(f"Extracted {field_name} from key '{key_pattern}'")
                    break

        fields[field_name] = value if value else None

        if value is None:
            print(f"Field '{field_name}' not found (looking for key: '{key_pattern}')")

    return fields


def extract_fields_from_csv(csv_content, csv_column_mappings):
    """
    Extract fields from CSV content using column mappings.

    Args:
        csv_content: Raw CSV string (with headers)
        csv_column_mappings: Dict mapping internal field names to CSV column names
            e.g., {"service_id": "1_Service_ID", "date": "8_Service_Date"}

    Returns:
        dict: Extracted field values, with None for fields not found
    """
    fields = {}

    if not csv_content or not csv_column_mappings:
        return fields

    try:
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)

        if not rows:
            print("CSV has no data rows")
            return fields

        # For single-row charts, use that row
        # For multi-row charts (e.g., Circles of Care), use first row for most fields
        first_row = rows[0]

        print(f"Extracting fields from CSV ({len(rows)} rows). Columns: {list(first_row.keys())[:10]}...")

        for internal_name, csv_column in csv_column_mappings.items():
            if csv_column is None:
                # Field not available in this org's CSV
                fields[internal_name] = None
                print(f"Field '{internal_name}' not mapped (csv_column is null)")
                continue

            # Get value from first row
            value = first_row.get(csv_column, '').strip()
            fields[internal_name] = value if value else None

            if value:
                print(f"Extracted {internal_name} from column '{csv_column}'")
            else:
                print(f"Field '{internal_name}' not found in column '{csv_column}'")

    except Exception as e:
        print(f"Error parsing CSV: {e}")

    return fields


def extract_fields_from_json_record(data, field_mappings):
    """
    Extract fields from an RPA JSON record (RpaNoteRecord shape).

    The record's `extracted_fields` block is the primary source. Selected
    top-level keys and the `encounter` block are flattened on top so
    deterministic rules can reference them without dot notation:
      - org_id, source_record_id (top-level identifiers)
      - visit_date, provider_display, note_type (from encounter)

    Per-org `field_mappings` are applied as a final pass: each entry remaps
    a source key (or list of fallback keys) to a target field name. This
    lets an org rename a vendor-specific extracted field into the canonical
    name a rule expects without changing the rule.

    Args:
        data: Parsed JSON dict (must contain 'extracted_fields' key)
        field_mappings: Per-org renaming map. Each value is either a string
            (source key) or a list of strings (fallback keys to try in order).

    Returns:
        dict: Flat dict of field values
    """
    fields = {}

    extracted = data.get('extracted_fields') or {}
    if isinstance(extracted, dict):
        fields.update(extracted)

    for top_key in ('org_id', 'source_record_id'):
        if top_key in data and top_key not in fields:
            fields[top_key] = data[top_key]

    encounter = data.get('encounter') or {}
    if isinstance(encounter, dict):
        for enc_key in ('visit_date', 'provider_display', 'note_type'):
            if enc_key in encounter and enc_key not in fields:
                fields[enc_key] = encounter[enc_key]

    if field_mappings:
        for target_name, source_spec in field_mappings.items():
            source_keys = source_spec if isinstance(source_spec, list) else [source_spec]
            for source_key in source_keys:
                if source_key in fields and fields[source_key] is not None:
                    fields[target_name] = fields[source_key]
                    break

    print(f"Extracted {len(fields)} fields from JSON record")
    return fields


def extract_fields(data, field_mappings, csv_column_mappings=None):
    """
    Extract fields from document data (JSON record, CSV, or text).

    Detection order:
      1. JSON record — `data` has an `extracted_fields` dict (RpaNoteRecord).
      2. CSV — `data['text']` has a comma in the first line AND csv_column_mappings
         is configured.
      3. Text — fall back to pattern matching on `data['text']`.

    Args:
        data: Either a parsed JSON dict (RpaNoteRecord) or a dict with 'text' key.
        field_mappings: Text pattern mappings (text mode) or rename map (JSON mode).
        csv_column_mappings: CSV column mappings (CSV mode), optional.

    Returns:
        dict: Extracted field values
    """
    if isinstance(data.get('extracted_fields'), dict):
        return extract_fields_from_json_record(data, field_mappings)

    text = data.get('text', '')

    if not text:
        return {}

    first_line = text.split('\n')[0] if text else ''
    looks_like_csv = csv_column_mappings and ',' in first_line

    if looks_like_csv:
        try:
            csv_fields = extract_fields_from_csv(text, csv_column_mappings)
            if csv_fields:
                print(f"Extracted {len(csv_fields)} fields from CSV")
                return csv_fields
        except Exception as e:
            print(f"CSV extraction failed, falling back to text: {e}")

    return extract_fields_from_text(text, field_mappings)
