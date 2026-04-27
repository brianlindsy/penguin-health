"""
Unit tests for Circles of Care CSV splitter.

Tests the CSV parsing and grouping logic including:
- Visit ID detection and grouping
- Comma-shift handling in InitialAppt field
- Row padding/normalization
- Intake Screening filtering
"""

import sys
import os
import pytest
from io import StringIO

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'csv-splitter'))


class TestCirclesOfCareSplitter:
    """Test Circles of Care CSV splitter logic."""

    @pytest.fixture
    def splitter(self):
        from splitters.circles_of_care import CirclesOfCareSplitter
        return CirclesOfCareSplitter()

    @pytest.fixture
    def canonical_headers(self, splitter):
        """Get the canonical column headers."""
        return splitter.CANONICAL_COLUMNS

    def test_org_id_property(self, splitter):
        """Should return correct organization ID."""
        assert splitter.org_id == 'circles-of-care'

    def test_is_valid_visit_id_valid_ids(self, splitter):
        """Should accept valid visit IDs (6-15 digits)."""
        assert splitter._is_valid_visit_id('123456') is True      # 6 digits
        assert splitter._is_valid_visit_id('1234567890') is True  # 10 digits
        assert splitter._is_valid_visit_id('12345678901234') is True  # 14 digits
        assert splitter._is_valid_visit_id('123456789012345') is True  # 15 digits

    def test_is_valid_visit_id_invalid_ids(self, splitter):
        """Should reject invalid visit IDs."""
        assert splitter._is_valid_visit_id('12345') is False      # Too short (5 digits)
        assert splitter._is_valid_visit_id('1234567890123456') is False  # Too long (16 digits)
        assert splitter._is_valid_visit_id('abc123') is False     # Non-numeric
        assert splitter._is_valid_visit_id('') is False           # Empty
        assert splitter._is_valid_visit_id('12 345') is False     # Contains space

    def test_fix_comma_shift_no_shift(self, splitter):
        """Row with exactly 31 columns needs no fix."""
        row = [''] * 31
        row[0] = 'client_001'
        row[1] = '123456'
        row[28] = 'Agency Referral'  # InitialAppt
        row[29] = 'Adult'             # AGEGROUP
        row[30] = 'F32.1'             # DiagnoseOnVisit

        fixed = splitter._fix_comma_shift(row)

        assert len(fixed) == 31
        assert fixed[28] == 'Agency Referral'
        assert fixed[29] == 'Adult'
        assert fixed[30] == 'F32.1'

    def test_fix_comma_shift_with_one_extra_comma(self, splitter):
        """Row with embedded comma in InitialAppt (32 columns total)."""
        # Original: 31 columns, but InitialAppt has comma: "Hospital, LEO"
        # This becomes: 32 columns
        row = [''] * 28 + ['Agency Referral (Hospital', 'LEO)', 'Adult', 'F32.1']
        assert len(row) == 32

        fixed = splitter._fix_comma_shift(row)

        assert len(fixed) == 31
        assert fixed[28] == 'Agency Referral (Hospital, LEO)'
        assert fixed[29] == 'Adult'
        assert fixed[30] == 'F32.1'

    def test_fix_comma_shift_with_multiple_extra_commas(self, splitter):
        """Row with multiple embedded commas in InitialAppt (33 columns total)."""
        # "Agency Referral (Hospital, LEO, School)" has 2 extra commas
        row = [''] * 28 + ['Agency Referral (Hospital', 'LEO', 'School)', 'Adult', 'F32.1']
        assert len(row) == 33

        fixed = splitter._fix_comma_shift(row)

        assert len(fixed) == 31
        assert fixed[28] == 'Agency Referral (Hospital, LEO, School)'
        assert fixed[29] == 'Adult'
        assert fixed[30] == 'F32.1'

    def test_fix_comma_shift_pads_short_rows(self, splitter):
        """Short rows should be padded to 31 columns."""
        row = [''] * 20

        fixed = splitter._fix_comma_shift(row)

        assert len(fixed) == 31
        assert fixed[20] == ''  # Padded columns are empty

    def test_fix_comma_shift_preserves_stable_columns(self, splitter):
        """Columns 0-27 should remain unchanged after fix."""
        row = [f'col{i}' for i in range(28)] + ['InitialAppt', 'Adult', 'F32.1']
        assert len(row) == 31

        fixed = splitter._fix_comma_shift(row)

        # First 28 columns unchanged
        for i in range(28):
            assert fixed[i] == f'col{i}'

    def test_split_groups_by_visit_id(self, splitter):
        """Multiple rows with same visit ID should be grouped."""
        # Create CSV with 2 rows for same visit ID
        row1 = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22
        row2 = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22

        csv_content = '\n'.join([
            ','.join(row1),
            ','.join(row2),
        ])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        visit_id, content = results[0]
        assert visit_id == '123456'

        # Count data rows (excluding header)
        lines = content.strip().split('\n')
        assert len(lines) == 3  # Header + 2 data rows

    def test_split_separates_different_visit_ids(self, splitter):
        """Rows with different visit IDs should be in separate results."""
        # Visit ID is at index 1
        row1 = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22
        row2 = ['client2', '654321'] + [''] * 6 + ['Intake Screening'] + [''] * 22

        csv_content = '\n'.join([
            ','.join(row1),
            ','.join(row2),
        ])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 2
        visit_ids = [r[0] for r in results]
        assert '123456' in visit_ids
        assert '654321' in visit_ids

    def test_split_filters_non_intake_visits(self, splitter):
        """Should filter out non-Intake Screening visits when INTAKE_ONLY=True."""
        # Row without "Intake Screening" in visittype (index 8)
        row = ['client1', '123456'] + [''] * 6 + ['Regular Visit'] + [''] * 22

        csv_content = ','.join(row)

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 0

    def test_split_accepts_intake_screening_visits(self, splitter):
        """Should accept Intake Screening visits."""
        # Row with "Intake Screening" in visittype (index 8)
        row = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22

        csv_content = ','.join(row)

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_output_includes_canonical_headers(self, splitter, canonical_headers):
        """Output CSV should include canonical column headers."""
        row = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22

        csv_content = ','.join(row)

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        _, content = results[0]

        lines = content.strip().split('\n')
        header_line = lines[0]

        # Check some canonical headers are present
        assert 'fake_client_ID' in header_line
        assert 'clientvisit_ID' in header_line
        assert 'InitialAppt' in header_line
        assert 'AGEGROUP' in header_line

    def test_split_handles_empty_rows(self, splitter):
        """Should skip empty rows."""
        row = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22

        csv_content = '\n'.join([
            '',  # Empty row
            ','.join(row),
            '',  # Another empty row
        ])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_continues_rows_until_new_visit_id(self, splitter):
        """Continuation rows (no valid visit ID) should be added to current group."""
        # First row has valid visit ID
        row1 = ['client1', '123456'] + [''] * 6 + ['Intake Screening'] + [''] * 22
        # Second row doesn't have valid visit ID at index 1 (continuation)
        row2 = ['', ''] + [''] * 6 + [''] + [''] * 22

        csv_content = '\n'.join([
            ','.join(row1),
            ','.join(row2),
        ])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        _, content = results[0]

        lines = content.strip().split('\n')
        # Should have header + 2 data rows
        assert len(lines) == 3

    def test_num_columns_constant(self, splitter):
        """NUM_COLUMNS should be 31."""
        assert splitter.NUM_COLUMNS == 31
        assert len(splitter.CANONICAL_COLUMNS) == 31

    def test_initial_appt_idx_constant(self, splitter):
        """InitialAppt should be at index 28."""
        assert splitter.INITIAL_APPT_IDX == 28
        assert splitter.CANONICAL_COLUMNS[28] == 'InitialAppt'

    def test_visittype_idx_constant(self, splitter):
        """visittype should be at index 8."""
        assert splitter.VISITTYPE_IDX == 8
        assert splitter.CANONICAL_COLUMNS[8] == 'visittype'
