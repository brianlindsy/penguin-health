"""
Unit tests for Circles of Care CSV splitter.

Tests the CSV parsing and grouping logic including:
- Visit ID detection and grouping
- Comma-shift handling in InitialAppt field
- Row padding/normalization
- Intake Screening filtering
- Header-driven column lookup
"""

import sys
import os
import pytest

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'csv-splitter'))


# The exact upstream header for Circles of Care CSVs. Note the duplicate
# column names (fake_client_ID, clientvisit_id, episode_id) and the
# upstream typos (DiagnosiOnService, first_Referrral).
UPSTREAM_HEADER = [
    "fake_client_ID", "clientvisit_ID", "Grade", "Race_Desc", "Ethnicity_Desc",
    "sex", "marital_status", "age_at_service", "visittype", "plan_id",
    "service_date", "episode_id", "program_desc", "admission_date", "discharge_date",
    "icd10_codes", "problem_list_order", "DiagnosiOnService", "fake_client_ID", "clientvisit_id",
    "first_Referrral", "question_text", "answer", "Type", "episode_id",
    "cptcode", "first_name", "last_name", "rate", "InitialAppt",
    "AGEGROUP", "DiagnoseOnVisit ",
]
HEADER_LINE = ",".join(UPSTREAM_HEADER)
NUM_COLUMNS = len(UPSTREAM_HEADER)  # 32


def _build_csv(data_rows: list) -> str:
    """Prepend the upstream header to a list of comma-joined data rows."""
    return "\n".join([HEADER_LINE] + data_rows)


class TestCirclesOfCareSplitter:
    """Test Circles of Care CSV splitter logic."""

    @pytest.fixture
    def splitter(self):
        from splitters.circles_of_care import CirclesOfCareSplitter
        return CirclesOfCareSplitter()

    @pytest.fixture
    def indices(self, splitter):
        return splitter._locate_columns(UPSTREAM_HEADER)

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

    def test_locate_columns_returns_first_occurrence(self, splitter, indices):
        """Duplicate names in the header should resolve to their first index."""
        # fake_client_ID appears at 0 and 18; we use the first.
        assert UPSTREAM_HEADER[indices["visit_id"]] == "clientvisit_ID"
        assert indices["visittype"] == 8
        assert indices["initial_appt"] == 29
        assert indices["agegroup"] == 30
        assert indices["last"] == 31

    def test_locate_columns_missing_raises(self, splitter):
        """Missing required column should raise KeyError with the header echoed."""
        bad_header = ["a", "b", "c"]
        with pytest.raises(KeyError, match="clientvisit_ID"):
            splitter._locate_columns(bad_header)

    def test_fix_comma_shift_no_shift(self, splitter, indices):
        """Row with exactly NUM_COLUMNS columns needs no fix."""
        row = [''] * NUM_COLUMNS
        row[0] = 'client_001'
        row[indices["visit_id"]] = '123456'
        row[indices["initial_appt"]] = 'Agency Referral'
        row[indices["agegroup"]] = 'Adult'
        row[indices["last"]] = 'F32.1'

        fixed = splitter._fix_comma_shift(row, indices, NUM_COLUMNS)

        assert len(fixed) == NUM_COLUMNS
        assert fixed[indices["initial_appt"]] == 'Agency Referral'
        assert fixed[indices["agegroup"]] == 'Adult'
        assert fixed[indices["last"]] == 'F32.1'

    def test_fix_comma_shift_with_one_extra_comma(self, splitter, indices):
        """Row with one embedded comma in InitialAppt (33 columns total)."""
        row = [''] * 29 + ['Agency Referral (Hospital', 'LEO)', 'Adult', 'F32.1']
        assert len(row) == NUM_COLUMNS + 1

        fixed = splitter._fix_comma_shift(row, indices, NUM_COLUMNS)

        assert len(fixed) == NUM_COLUMNS
        assert fixed[indices["initial_appt"]] == 'Agency Referral (Hospital, LEO)'
        assert fixed[indices["agegroup"]] == 'Adult'
        assert fixed[indices["last"]] == 'F32.1'

    def test_fix_comma_shift_with_multiple_extra_commas(self, splitter, indices):
        """Row with multiple embedded commas in InitialAppt (34 columns total)."""
        row = [''] * 29 + ['Agency Referral (Hospital', 'LEO', 'School)', 'Adult', 'F32.1']
        assert len(row) == NUM_COLUMNS + 2

        fixed = splitter._fix_comma_shift(row, indices, NUM_COLUMNS)

        assert len(fixed) == NUM_COLUMNS
        assert fixed[indices["initial_appt"]] == 'Agency Referral (Hospital, LEO, School)'
        assert fixed[indices["agegroup"]] == 'Adult'
        assert fixed[indices["last"]] == 'F32.1'

    def test_fix_comma_shift_pads_short_rows(self, splitter, indices):
        """Short rows should be padded to NUM_COLUMNS."""
        row = [''] * 20

        fixed = splitter._fix_comma_shift(row, indices, NUM_COLUMNS)

        assert len(fixed) == NUM_COLUMNS
        assert fixed[20] == ''  # Padded columns are empty

    def test_fix_comma_shift_preserves_stable_columns(self, splitter, indices):
        """Columns before InitialAppt should remain unchanged after fix."""
        row = [f'col{i}' for i in range(29)] + ['InitialAppt', 'Adult', 'F32.1']
        assert len(row) == NUM_COLUMNS

        fixed = splitter._fix_comma_shift(row, indices, NUM_COLUMNS)

        for i in range(29):
            assert fixed[i] == f'col{i}'

    def _row_with(self, visit_id: str = '123456', visittype: str = 'Intake Screening') -> list:
        """Build a NUM_COLUMNS-wide data row with the given visit ID and visittype."""
        row = ['client1'] + [''] * (NUM_COLUMNS - 1)
        row[1] = visit_id        # clientvisit_ID
        row[8] = visittype       # visittype
        return row

    def test_split_groups_by_visit_id(self, splitter):
        """Multiple rows with same visit ID should be grouped."""
        row1 = self._row_with(visit_id='123456')
        row2 = self._row_with(visit_id='123456')

        csv_content = _build_csv([','.join(row1), ','.join(row2)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        visit_id, content = results[0]
        assert visit_id == '123456'

        lines = content.strip().split('\n')
        assert len(lines) == 3  # Header + 2 data rows

    def test_split_separates_different_visit_ids(self, splitter):
        """Rows with different visit IDs should be in separate results."""
        row1 = self._row_with(visit_id='123456')
        row2 = self._row_with(visit_id='654321')

        csv_content = _build_csv([','.join(row1), ','.join(row2)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 2
        visit_ids = [r[0] for r in results]
        assert '123456' in visit_ids
        assert '654321' in visit_ids

    def test_split_filters_non_intake_visits(self, splitter):
        """Should filter out non-Intake Screening visits when INTAKE_ONLY=True."""
        row = self._row_with(visit_id='123456', visittype='Regular Visit')

        csv_content = _build_csv([','.join(row)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 0

    def test_split_accepts_intake_screening_visits(self, splitter):
        """Should accept Intake Screening visits."""
        row = self._row_with(visit_id='123456', visittype='Intake Screening')

        csv_content = _build_csv([','.join(row)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_accepts_bedday_psych_visits(self, splitter):
        """Should accept BedDay-Psych visits."""
        row = self._row_with(visit_id='123456', visittype='BedDay-Psych')

        csv_content = _build_csv([','.join(row)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_accepts_bedday_detox_visits(self, splitter):
        """Should accept BedDay-Detox visits."""
        row = self._row_with(visit_id='123456', visittype='BedDay-Detox')

        csv_content = _build_csv([','.join(row)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_output_uses_upstream_header(self, splitter):
        """Output CSV should echo the upstream header verbatim."""
        row = self._row_with(visit_id='123456')

        csv_content = _build_csv([','.join(row)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        _, content = results[0]

        lines = content.strip().splitlines()
        assert lines[0] == HEADER_LINE

    def test_split_handles_empty_rows(self, splitter):
        """Should skip empty rows."""
        row = self._row_with(visit_id='123456')

        csv_content = _build_csv(['', ','.join(row), ''])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1

    def test_split_continues_rows_until_new_visit_id(self, splitter):
        """Continuation rows (no valid visit ID) should be added to current group."""
        row1 = self._row_with(visit_id='123456')
        # Continuation row: no visit ID at clientvisit_ID column
        row2 = [''] * NUM_COLUMNS

        csv_content = _build_csv([','.join(row1), ','.join(row2)])

        results = splitter.split(csv_content, 'test.csv')

        assert len(results) == 1
        _, content = results[0]

        lines = content.strip().split('\n')
        # Header + 2 data rows
        assert len(lines) == 3

    def test_split_empty_input_returns_empty(self, splitter):
        """Empty CSV content should return an empty result list, not crash."""
        assert splitter.split('', 'test.csv') == []

    def test_split_raises_on_missing_required_column(self, splitter):
        """A header that lacks a required column should raise KeyError."""
        bad_csv = "a,b,c\n1,2,3\n"
        with pytest.raises(KeyError):
            splitter.split(bad_csv, 'test.csv')
