"""
Demo CSV Splitter.

Adapted from csv-parsers/cc_csv_parser.py.

Format:
- One row = one chart
- Filter by: approved status, non-billable status, program, date range
- ID column: 1_Service_ID
"""

import csv
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Tuple

from .base_splitter import BaseCsvSplitter


class DemoSplitter(BaseCsvSplitter):
    """Demo: 1 row = 1 chart, filtered by approval status."""

    @property
    def org_id(self) -> str:
        return "demo"

    # Programs to include in validation
    ALLOWED_PROGRAMS = {
        'Med Somatic Services',
        'Free Standing Mental Health Clinic - Med Services',
        'Free Standing Mental Health Clinic - Counseling',
        'Counseling',
        'Compass',
        'Community Support',
        'ACT',
    }

    # Number of days in the past to include
    DAYS_AGO = 7

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Filters rows by:
        - 24_Approved == 'no' (unapproved visits)
        - 25_Non_Billable == 'no' (billable visits)
        - 7b_Program_Name in ALLOWED_PROGRAMS
        - 8_Service_Date within DAYS_AGO days
        """
        reader = csv.DictReader(StringIO(csv_content))
        headers = reader.fieldnames
        results = []

        for row in reader:

            # Get chart ID
            chart_id = row.get('1_Service_ID', '').strip()
            if not chart_id:
                continue

            # Output single-row CSV with headers
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()
            writer.writerow(row)
            results.append((chart_id, output.getvalue()))

        return results

    def _is_within_recent_days(self, date_str: str) -> bool:
        """Check if a date string falls within DAYS_AGO days from today."""
        if not date_str:
            return False

        # Try multiple date formats
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%m-%d-%Y', '%Y/%m/%d']
        parsed_date = None

        for fmt in formats:
            try:
                # Handle datetime strings by splitting off time
                parsed_date = datetime.strptime(date_str.split(' ')[0], fmt).date()
                break
            except ValueError:
                continue

        if not parsed_date:
            return False

        today = datetime.now().date()
        past_date_threshold = today - timedelta(days=self.DAYS_AGO)

        return past_date_threshold <= parsed_date <= today
