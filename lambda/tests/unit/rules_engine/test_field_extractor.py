"""Tests for field_extractor's JSON-record extraction mode.

CSV and text-pattern modes are covered indirectly via the rules-engine
end-to-end tests; this file focuses on the new RPA JSON path added for
the supportive-care org.
"""

import os
import sys

# Match the sys.path setup other tests in this directory use.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..',
                                'multi-org', 'rules-engine'))


def _rpa_record(extracted_fields=None, encounter=None, **top):
    """Build a minimal RpaNoteRecord-shaped dict (as it lands on disk)."""
    return {
        'schema_version': 1,
        'source': 'rpa.centralreach',
        'source_record_id': top.get('source_record_id', 'note-1'),
        'org_id': top.get('org_id', 'supportive-care'),
        'text': 'Client engaged in DTT.',
        'extracted_fields': extracted_fields or {},
        'encounter': encounter or {
            'visit_date': '2026-06-22',
            'provider_display': 'Dr. Alice Smith',
            'note_type': 'Progress Note',
        },
    }


class TestExtractFieldsFromJsonRecord:

    def test_extracted_fields_pass_through(self):
        from field_extractor import extract_fields
        rec = _rpa_record(extracted_fields={
            'signed_at': '2026-06-22T10:30:00Z',
            'billed_duration_minutes': 60,
        })
        result = extract_fields(rec, field_mappings={})
        assert result['signed_at'] == '2026-06-22T10:30:00Z'
        assert result['billed_duration_minutes'] == 60

    def test_encounter_fields_are_surfaced_top_level(self):
        """Rule 12 needs visit_date at top level; rule 7 needs provider_display."""
        from field_extractor import extract_fields
        result = extract_fields(_rpa_record(), field_mappings={})
        assert result['visit_date'] == '2026-06-22'
        assert result['provider_display'] == 'Dr. Alice Smith'
        assert result['note_type'] == 'Progress Note'

    def test_top_level_identifiers_surfaced(self):
        """Rule 1 (narrative_hash_unique) reads org_id and source_record_id from fields."""
        from field_extractor import extract_fields
        result = extract_fields(
            _rpa_record(org_id='org-x', source_record_id='note-42'),
            field_mappings={},
        )
        assert result['org_id'] == 'org-x'
        assert result['source_record_id'] == 'note-42'

    def test_extracted_fields_win_over_encounter_on_collision(self):
        """If both define `visit_date`, the one in extracted_fields wins
        (closer to the vendor's authoritative value for that note)."""
        from field_extractor import extract_fields
        rec = _rpa_record(extracted_fields={'visit_date': '2026-06-21'})
        result = extract_fields(rec, field_mappings={})
        assert result['visit_date'] == '2026-06-21'

    def test_field_mappings_remap_source_to_target(self):
        """A per-org mapping renames a vendor field into a canonical name."""
        from field_extractor import extract_fields
        rec = _rpa_record(extracted_fields={'vendor_signed_at': '2026-06-22T10:30:00Z'})
        result = extract_fields(rec, field_mappings={'signed_at': 'vendor_signed_at'})
        assert result['signed_at'] == '2026-06-22T10:30:00Z'

    def test_field_mappings_fallback_list(self):
        """A list of source keys is tried in order; first non-None wins."""
        from field_extractor import extract_fields
        rec = _rpa_record(extracted_fields={'sig_b': '2026-06-22T10:30:00Z'})
        result = extract_fields(
            rec,
            field_mappings={'signed_at': ['sig_a', 'sig_b', 'sig_c']},
        )
        assert result['signed_at'] == '2026-06-22T10:30:00Z'

    def test_missing_encounter_is_tolerated(self):
        from field_extractor import extract_fields
        rec = _rpa_record()
        rec.pop('encounter')
        result = extract_fields(rec, field_mappings={})
        assert 'visit_date' not in result
        assert result['org_id'] == 'supportive-care'

    def test_routing_prefers_json_when_extracted_fields_present(self):
        """When a record has extracted_fields, JSON mode wins even if `text`
        looks CSV-ish — extracted_fields is authoritative."""
        from field_extractor import extract_fields
        rec = _rpa_record(extracted_fields={'foo': 'bar'})
        rec['text'] = 'a,b,c\n1,2,3'  # looks like CSV
        result = extract_fields(rec, field_mappings={}, csv_column_mappings={'x': 'a'})
        assert result['foo'] == 'bar'
        # CSV columns should not have been parsed
        assert 'x' not in result
