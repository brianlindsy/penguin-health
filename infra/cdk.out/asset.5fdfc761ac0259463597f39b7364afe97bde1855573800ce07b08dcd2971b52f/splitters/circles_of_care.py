"""
Circles of Care CSV Splitter.

Format:
- Multiple rows can belong to the same chart (grouped by visit ID)
- Handles "comma shift" corruption where unquoted commas add extra columns
- ID column: clientvisit_id (index 1)
- Input CSV has NO header - splitter adds the canonical header to each output
"""

import csv
from collections import defaultdict
from io import StringIO
from typing import List, Tuple

from .base_splitter import BaseCsvSplitter


class CirclesOfCareSplitter(BaseCsvSplitter):
    """Circles of Care: Group rows by visit ID, handle comma-shifted data."""

    @property
    def org_id(self) -> str:
        return "circles-of-care"

    # The canonical columns to add as header to each output CSV
    # Update this list when the source CSV format changes
    CANONICAL_COLUMNS = [
        "fake_client_ID", "clientvisit_ID", "Grade", "Race_Desc",
        "Ethnicity_Desc", "sex", "marital_status", "age_at_service",
        "visittype", "plan_id", "service_date", "episode_id", "program_desc",
        "admission_date", "discharge_date", "icd10_codes", "problem_list_order",
        "fake_client_ID", "clientvisit_id", "first_Referrral", "question_text",
        "answer", "Type", "episode_id", "cptcode", "first_name", "last_name", "rate",
        "InitialAppt", "AGEGROUP", "DiagnoseOnVisit"
    ]

    # Column index for visit ID (0-based)
    VISIT_ID_INDEX = 1

    # Column index for visit type (used for Intake Screening filter)
    VISIT_TYPE_INDEX = 8

    # Column index for diagnosis code (last canonical column)
    DIAGNOSIS_INDEX = None  # Computed from CANONICAL_COLUMNS length

    # Whether to filter for Intake Screening visits only
    INTAKE_ONLY = True

    def __init__(self):
        super().__init__()
        # Diagnosis is the last column
        self.DIAGNOSIS_INDEX = len(self.CANONICAL_COLUMNS) - 1

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Groups rows by clientvisit_id and handles comma-shifted data.
        Adds CANONICAL_COLUMNS as header to each output CSV.
        Optionally filters for Intake Screening visits only.
        """
        groups = defaultdict(list)
        current_visit_id = None
        num_columns = len(self.CANONICAL_COLUMNS)

        reader = csv.reader(StringIO(csv_content))
        for row in reader:
            if not row:
                continue

            # Detect start of a new record (ID is in VISIT_ID_INDEX column)
            if len(row) > self.VISIT_ID_INDEX and self._is_valid_visit_id(row[self.VISIT_ID_INDEX]):
                current_visit_id = row[self.VISIT_ID_INDEX].strip()

            if current_visit_id:
                # Handle comma shift and normalize to expected columns
                fixed_row = self._fix_comma_shift(row, num_columns)
                groups[current_visit_id].append(fixed_row)

        # Create CSV for each visit
        results = []
        for visit_id, rows in groups.items():
            # Filter for Intake Screening only (unless disabled)
            if self.INTAKE_ONLY:
                is_intake = any(
                    len(r) > self.VISIT_TYPE_INDEX and "Intake Screening" in str(r[self.VISIT_TYPE_INDEX])
                    for r in rows
                )
                if not is_intake:
                    continue

            # Create CSV output with canonical header
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(self.CANONICAL_COLUMNS)
            writer.writerows(rows)
            results.append((visit_id, output.getvalue()))

        print(f"Split into {len(results)} charts from {len(groups)} visit IDs")
        return results

    def _is_valid_visit_id(self, val: str) -> bool:
        """Check if a value is a valid visit ID (6-15 digit number)."""
        val = str(val).strip()
        return val.isdigit() and 6 <= len(val) <= 15

    def _fix_comma_shift(self, row: list, num_columns: int) -> list:
        """
        Fix rows corrupted by unquoted commas ("comma shift").

        When a field contains unquoted commas, the row ends up with extra columns.
        The diagnosis code (last expected field) gets pushed to the very end.
        We detect this and move the diagnosis back to its correct position.

        Args:
            row: Original CSV row (may have extra columns due to comma shift)
            num_columns: Expected number of columns

        Returns:
            list: Normalized row with exactly num_columns elements
        """
        # If row has more columns than expected, it's comma-shifted
        if len(row) > num_columns:
            # The actual diagnosis code is at the very end (shifted there)
            actual_dx = row[-1].strip()

            # Truncate to expected column count
            new_row = row[:num_columns]

            # Place the diagnosis code in the correct position (last column)
            # Only if it looks like a valid diagnosis code (not random text)
            if actual_dx and len(actual_dx) < 10 and any(c.isdigit() for c in actual_dx):
                new_row[self.DIAGNOSIS_INDEX] = actual_dx

            return new_row

        # If row has fewer columns, pad with empty strings
        elif len(row) < num_columns:
            return row + [""] * (num_columns - len(row))

        # Row has exactly the right number of columns
        return row
