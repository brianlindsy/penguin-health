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

    # The 32 canonical columns derived from the actual data structure.
    # Columns 0-16:  demographic / visit header fields
    # Columns 17-24: repeated IDs + question/answer block
    # Columns 25-31: CPT, name, rate, referral source, age-group, diagnosis (repeated)
    CANONICAL_COLUMNS = [
        "fake_client_ID",        # 0
        "clientvisit_ID",        # 1
        "Grade",                 # 2
        "Race_Desc",             # 3
        "Ethnicity_Desc",        # 4
        "sex",                   # 5
        "marital_status",        # 6
        "age_at_service",        # 7
        "visittype",             # 8
        "plan_id",               # 9
        "service_date",          # 10
        "episode_id",            # 11
        "program_desc",          # 12
        "admission_date",        # 13
        "discharge_date",        # 14
        "icd10_codes",           # 15
        "problem_list_order",    # 16
        "DiagnoseOnVisitBedday",       # 17  (repeat started on May 11 2026) (bed day diagnosis)
        "fake_client_ID2",       # 18  (repeated)
        "clientvisit_id2",       # 19  (repeated)
        "first_Referral",        # 20
        "question_text",         # 21
        "answer",                # 22
        "Type",                  # 23
        "episode_id2",           # 24  (repeated)
        "cptcode",               # 25
        "first_name",            # 26
        "last_name",             # 27
        "rate",                  # 28
        "InitialAppt",           # 29  (may contain commas → comma-shift source)
        "AGEGROUP",              # 30
        "DiagnoseOnVisit2",      # 31
    ]

    # Total number of canonical columns
    NUM_COLUMNS = len(CANONICAL_COLUMNS)  # 32

    # Index of the field that suffers comma-shift (InitialAppt / referral source)
    INITIAL_APPT_IDX = 29

    # Whether to filter for Intake Screening visits only
    INTAKE_ONLY = True

    # Column index used for the Intake Screening check
    VISITTYPE_IDX = 8

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Groups rows by clientvisit_id and handles comma-shifted data.
        Optionally filters for Intake Screening visits only.
        """
        groups: defaultdict[str, list] = defaultdict(list)
        current_visit_id: str | None = None

        reader = csv.reader(StringIO(csv_content))
        for row in reader:
            if not row:
                continue

            # Detect the start of a new record (visit ID lives at index 1)
            if len(row) > 1 and self._is_valid_visit_id(row[1]):
                current_visit_id = row[1].strip()

            if current_visit_id:
                fixed_row = self._fix_comma_shift(row)
                groups[current_visit_id].append(fixed_row)

        results: List[Tuple[str, str]] = []
        for visit_id, rows in groups.items():
            # Optional: keep only Intake Screening visits
            if self.INTAKE_ONLY:
                is_intake = any(
                    len(r) > self.VISITTYPE_IDX
                    and "Intake Screening" in str(r[self.VISITTYPE_IDX])
                    for r in rows
                )
                if not is_intake:
                    continue

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(self.CANONICAL_COLUMNS)
            writer.writerows(rows)
            results.append((visit_id, output.getvalue()))

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_valid_visit_id(self, val: str) -> bool:
        """Return True if *val* looks like a 6-to-15-digit visit ID."""
        val = str(val).strip()
        return val.isdigit() and 6 <= len(val) <= 15

    def _fix_comma_shift(self, row: list) -> list:
        """
        Normalise a raw row to exactly NUM_COLUMNS columns.

        The 'InitialAppt' field (index 29) occasionally contains unquoted
        commas e.g. "Agency Referral (Hospital, LEO, School)".

        When NO comma-shift has occurred the row has exactly NUM_COLUMNS columns.
        When a comma-shift HAS occurred the row is longer — every extra column
        beyond NUM_COLUMNS represents an extra comma inside the InitialAppt field.

        In both cases:
          - Columns 0-28  are stable.
          - Column  -2    is AGEGROUP.
          - Column  -1    is DiagnoseOnVisit2.
          - Everything in between (index 29 through -3 inclusive) belongs
            to InitialAppt and is rejoined with ", ".
        """
        n = len(row)
        target = self.NUM_COLUMNS  # 32

        if n < target:
            # Pad short rows
            return row + [""] * (target - n)

        if n == target:
            # Perfect row — no comma-shift
            return list(row)

        # n > target — comma-shift occurred in InitialAppt field
        # Columns 0-27 are always stable
        stable = list(row[:self.INITIAL_APPT_IDX])   # indices 0-28

        tail = row[self.INITIAL_APPT_IDX:]            # everything from index 29 onward

        # Last two columns are always AGEGROUP and DiagnoseOnVisit2
        diagnose_on_visit = tail[-1].strip()
        age_group = tail[-2].strip()

        # Everything between index 28 and the last two columns is the
        # fragmented InitialAppt value — rejoin the pieces
        initial_appt_parts = tail[:-2]
        initial_appt = ", ".join(part.strip() for part in initial_appt_parts)

        return stable + [initial_appt, age_group, diagnose_on_visit]