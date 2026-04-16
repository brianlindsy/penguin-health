"""
Circles of Care CSV Splitter.

Adapted from csv-parsers/coc_csv_parser.py.

Format:
- Multiple rows can belong to the same chart (grouped by visit ID)
- Handles "comma shift" corruption where unquoted commas add extra columns
- ID column: clientvisit_id (index 1)
- Standardizes rows to 27 canonical columns
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

    # The columns expected by the system
    CANONICAL_COLUMNS = [
        "fake_client_ID","clientvisit_ID","Grade","Race_Desc",
    "Ethnicity_Desc","sex","marital_status","age_at_service",
    "visittype","plan_id","service_date","episode_id","program_desc",
    "admission_date","discharge_date","icd10_codes","problem_list_order",
    "fake_client_ID","clientvisit_id","first_Referrral","question_text",
    "answer","Type","episode_id","cptcode","first_name","last_name","rate",
    "InitialAppt","AGEGROUP","DiagnoseOnVisit"
    ]

    # Whether to filter for Intake Screening visits only
    INTAKE_ONLY = True

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Groups rows by clientvisit_id and handles comma-shifted data.
        Optionally filters for Intake Screening visits only.
        """
        groups = defaultdict(list)
        current_visit_id = None

        reader = csv.reader(StringIO(csv_content))
        for row in reader:
            if not row:
                continue

            # Detect start of a new record (ID is in Column 2 / Index 1)
            if len(row) > 1 and self._is_valid_visit_id(row[1]):
                current_visit_id = row[1].strip()

            if current_visit_id:
                # Handle comma shift - fix row to 27 columns
                fixed_row = self._fix_comma_shift(row)
                groups[current_visit_id].append(fixed_row)

        # Create CSV for each visit
        results = []
        for visit_id, rows in groups.items():
            # Filter for Intake Screening only (unless disabled)
            if self.INTAKE_ONLY:
                is_intake = any(
                    len(r) > 8 and "Intake Screening" in str(r[8])
                    for r in rows
                )
                if not is_intake:
                    continue

            # Create CSV output
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(self.CANONICAL_COLUMNS)
            writer.writerows(rows)
            results.append((visit_id, output.getvalue()))

        return results

    def _is_valid_visit_id(self, val: str) -> bool:
        """Check if a value is a valid visit ID (6-15 digit number)."""
        val = str(val).strip()
        return val.isdigit() and 6 <= len(val) <= 15

    def _fix_comma_shift(self, row: list) -> list:
        """
        Fix rows corrupted by unquoted commas ("comma shift").

        If the row is longer than 27 columns, it means commas shifted the data.
        We grab the diagnosis code from the very end and force it into the 27th slot.
        """
        # Get the actual diagnosis code from the end (if shifted)
        actual_dx = row[-1].strip() if len(row) > 0 else ""

        # Standardize the row to 27 columns
        new_row = row[:27]
        if len(new_row) < 27:
            new_row.extend([""] * (27 - len(new_row)))

        # Place the diagnosis code in the correct canonical column (index 26)
        # Only overwrite if the actual_dx looks like a code (not 'Adult' or 'School')
        if actual_dx and len(actual_dx) < 10 and any(c.isdigit() for c in actual_dx):
            new_row[26] = actual_dx

        return new_row
