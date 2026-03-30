"""
Field Extractor for extracting values from document text.

Uses simple pattern matching to extract field values based on
organization-specific field mappings.
"""


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

    lines = text.split('\n')

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
