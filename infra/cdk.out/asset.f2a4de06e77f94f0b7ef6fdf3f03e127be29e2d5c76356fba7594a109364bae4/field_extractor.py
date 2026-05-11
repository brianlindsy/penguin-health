"""
Field Extractor for extracting values from document text and CSV content.

Supports two extraction modes:
1. Text mode: Pattern matching on text lines (for PDFs/text documents)
2. CSV mode: Column-based extraction using csv_column_mappings (for SFTP CSVs)

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

    # Log first 500 chars of text for debugging
    print(f"Extracting fields from text ({len(text)} chars). First 500 chars: {text[:500]}")

    lines = text.split('\n')
    print(f"Text split into {len(lines)} lines")

    for field_name, key_pattern in field_mappings.items():
        value = None

        for line in lines:
            if key_pattern in line:
                parts = line.split(key_pattern, 1)
                if len(parts) > 1:
                    value = parts[1].strip()
                    print(f"Extracted {field_name}: '{value}' from key '{key_pattern}'")
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
                print(f"Extracted {internal_name}: '{value}' from column '{csv_column}'")
            else:
                print(f"Field '{internal_name}' not found in column '{csv_column}'")

    except Exception as e:
        print(f"Error parsing CSV: {e}")

    return fields


def extract_fields(data, field_mappings, csv_column_mappings=None):
    """
    Extract fields from document data (text or CSV).

    Automatically detects CSV content and uses appropriate extraction method.

    Args:
        data: Dict with 'text' key containing document content
        field_mappings: Text pattern mappings (for PDFs/text)
        csv_column_mappings: CSV column mappings (for CSV files), optional

    Returns:
        dict: Extracted field values
    """
    text = data.get('text', '')

    if not text:
        return {}

    # Check if content looks like CSV (has comma-separated header row)
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

    # Fall back to text extraction
    return extract_fields_from_text(text, field_mappings)
