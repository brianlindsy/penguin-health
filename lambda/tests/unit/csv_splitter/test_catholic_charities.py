"""
Unit tests for Catholic Charities CSV splitter.

Tests the CSV parsing and filtering logic including:
- Row filtering (approved, billable, program, date)
- Date format parsing
- Chart ID extraction
- CSV output formatting
"""

import sys
import os
import pytest
from datetime import datetime, timedelta
from io import StringIO

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'csv-splitter'))


class TestCatholicCharitiesSplitter:
    """Test Catholic Charities CSV splitter logic."""

    @pytest.fixture
    def splitter(self):
        from splitters.catholic_charities import CatholicCharitiesSplitter
        return CatholicCharitiesSplitter()

    @pytest.fixture
    def today_str(self):
        """Get today's date as a string."""
        return datetime.now().strftime('%m/%d/%Y')

    @pytest.fixture
    def old_date_str(self):
        """Get a date 30 days ago as a string."""
        old_date = datetime.now() - timedelta(days=30)
        return old_date.strftime('%m/%d/%Y')

    @pytest.fixture
    def valid_csv(self, today_str):
        """CSV with one valid row that should be processed."""
        return f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable,Consumer_Name
12345,Med Somatic Services,{today_str},no,no,John Doe
"""

    @pytest.fixture
    def multiple_rows_csv(self, today_str):
        """CSV with multiple rows, some valid and some filtered."""
        return f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable,Consumer_Name
12345,Med Somatic Services,{today_str},no,no,John Doe
12346,Counseling,{today_str},no,no,Jane Smith
12347,Invalid Program,{today_str},no,no,Bob Wilson
12348,Med Somatic Services,{today_str},yes,no,Alice Brown
12349,Med Somatic Services,{today_str},no,yes,Charlie Davis
"""

    def test_org_id_property(self, splitter):
        """Should return correct organization ID."""
        assert splitter.org_id == 'catholic-charities-multi-org'

    def test_split_processes_valid_rows(self, splitter, valid_csv):
        """Should process rows that pass all filters."""
        results = splitter.split(valid_csv, 'test.csv')

        assert len(results) == 1
        chart_id, csv_content = results[0]
        assert chart_id == '12345'
        assert '12345' in csv_content
        assert 'John Doe' in csv_content

    def test_split_filters_approved_visits(self, splitter, today_str):
        """Should filter out rows where 24_Approved is not 'no'."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{today_str},yes,no
12346,Med Somatic Services,{today_str},Yes,no
12347,Med Somatic Services,{today_str},approved,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 0

    def test_split_filters_non_billable_visits(self, splitter, today_str):
        """Should filter out rows where 25_Non_Billable is not 'no'."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{today_str},no,yes
12346,Med Somatic Services,{today_str},no,Yes
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 0

    def test_split_filters_invalid_programs(self, splitter, today_str):
        """Should filter out rows with programs not in ALLOWED_PROGRAMS."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Invalid Program,{today_str},no,no
12346,Another Invalid,{today_str},no,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 0

    def test_split_accepts_all_allowed_programs(self, splitter, today_str):
        """Should accept all programs in ALLOWED_PROGRAMS."""
        allowed_programs = [
            'Med Somatic Services',
            'Free Standing Mental Health Clinic - Med Services',
            'Free Standing Mental Health Clinic - Counseling',
            'Counseling',
            'Compass',
            'Community Support',
            'ACT',
        ]

        for i, program in enumerate(allowed_programs):
            csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
{12345 + i},{program},{today_str},no,no
"""
            results = splitter.split(csv, 'test.csv')
            assert len(results) == 1, f"Program '{program}' should be accepted"

    def test_split_filters_old_dates(self, splitter, old_date_str):
        """Should filter out rows with dates older than DAYS_AGO."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{old_date_str},no,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 0

    def test_split_accepts_recent_dates(self, splitter):
        """Should accept dates within DAYS_AGO days."""
        # Test dates within the last 7 days
        for days_ago in range(0, 7):
            recent_date = (datetime.now() - timedelta(days=days_ago)).strftime('%m/%d/%Y')
            csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{recent_date},no,no
"""
            results = splitter.split(csv, 'test.csv')
            assert len(results) == 1, f"Date {recent_date} ({days_ago} days ago) should be accepted"

    def test_split_handles_multiple_date_formats(self, splitter):
        """Should parse multiple date formats correctly."""
        today = datetime.now()

        date_formats = [
            today.strftime('%m/%d/%Y'),   # 01/15/2024
            today.strftime('%Y-%m-%d'),   # 2024-01-15
            today.strftime('%m/%d/%y'),   # 01/15/24
            today.strftime('%m-%d-%Y'),   # 01-15-2024
        ]

        for date_str in date_formats:
            csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{date_str},no,no
"""
            results = splitter.split(csv, 'test.csv')
            assert len(results) == 1, f"Date format '{date_str}' should be parsed"

    def test_split_skips_rows_without_chart_id(self, splitter, today_str):
        """Should skip rows with empty 1_Service_ID."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
,Med Somatic Services,{today_str},no,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 0

    def test_split_processes_multiple_valid_rows(self, splitter, multiple_rows_csv):
        """Should process multiple valid rows from the same CSV."""
        results = splitter.split(multiple_rows_csv, 'test.csv')

        # Should have 2 results: 12345 (Med Somatic) and 12346 (Counseling)
        # 12347 (Invalid Program), 12348 (approved), 12349 (non-billable) should be filtered
        assert len(results) == 2
        chart_ids = [r[0] for r in results]
        assert '12345' in chart_ids
        assert '12346' in chart_ids
        assert '12347' not in chart_ids
        assert '12348' not in chart_ids
        assert '12349' not in chart_ids

    def test_output_includes_headers(self, splitter, valid_csv):
        """Output CSV should include header row."""
        results = splitter.split(valid_csv, 'test.csv')

        assert len(results) == 1
        _, csv_content = results[0]

        lines = csv_content.strip().split('\n')
        assert len(lines) == 2  # Header + data row
        assert '1_Service_ID' in lines[0]

    def test_is_within_recent_days_empty_string(self, splitter):
        """Empty date string should return False."""
        assert splitter._is_within_recent_days('') is False

    def test_is_within_recent_days_invalid_format(self, splitter):
        """Invalid date format should return False."""
        assert splitter._is_within_recent_days('not-a-date') is False

    def test_is_within_recent_days_future_date(self, splitter):
        """Future dates should return False."""
        future_date = (datetime.now() + timedelta(days=10)).strftime('%m/%d/%Y')
        assert splitter._is_within_recent_days(future_date) is False

    def test_case_insensitive_approved_filter(self, splitter, today_str):
        """Approved filter should be case-insensitive."""
        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{today_str},NO,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 1

    def test_handles_datetime_strings_in_date_field(self, splitter):
        """Should handle datetime strings by extracting date portion."""
        today = datetime.now()
        datetime_str = today.strftime('%m/%d/%Y') + ' 10:30:00 AM'

        csv = f"""1_Service_ID,7b_Program_Name,8_Service_Date,24_Approved,25_Non_Billable
12345,Med Somatic Services,{datetime_str},no,no
"""
        results = splitter.split(csv, 'test.csv')

        assert len(results) == 1
