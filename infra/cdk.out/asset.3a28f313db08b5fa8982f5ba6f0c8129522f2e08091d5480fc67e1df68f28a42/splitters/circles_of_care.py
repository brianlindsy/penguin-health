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

    # Names of the columns we need to locate in the upstream header.
    # The upstream CSV header contains duplicate names (fake_client_ID,
    # clientvisit_id, episode_id all appear twice); we only need to find
    # the first occurrence of each name below.
    VISIT_ID_COLUMN = "clientvisit_ID"
    VISITTYPE_COLUMN = "visittype"
    INITIAL_APPT_COLUMN = "InitialAppt"
    AGEGROUP_COLUMN = "AGEGROUP"
    LAST_COLUMN = "DiagnoseOnVisit"

    # Whether to filter for Intake Screening visits only
    INTAKE_ONLY = True

    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Groups rows by clientvisit_ID and handles comma-shifted data.
        Optionally filters for Intake Screening visits only.
        """
        reader = csv.reader(StringIO(csv_content))

        try:
            header = next(reader)
        except StopIteration:
            return []

        indices = self._locate_columns(header)
        num_columns = len(header)

        groups: defaultdict[str, list] = defaultdict(list)
        current_visit_id: str | None = None

        for row in reader:
            if not row:
                continue

            visit_id_idx = indices["visit_id"]
            if len(row) > visit_id_idx and self._is_valid_visit_id(row[visit_id_idx]):
                current_visit_id = row[visit_id_idx].strip()

            if current_visit_id:
                fixed_row = self._fix_comma_shift(row, indices, num_columns)
                groups[current_visit_id].append(fixed_row)

        results: List[Tuple[str, str]] = []
        for visit_id, rows in groups.items():
            if self.INTAKE_ONLY:
                visittype_idx = indices["visittype"]
                is_intake = any(
                    len(r) > visittype_idx
                    and "Intake Screening" in str(r[visittype_idx])
                    for r in rows
                )
                if not is_intake:
                    continue

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(header)
            writer.writerows(rows)
            results.append((visit_id, output.getvalue()))

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _locate_columns(self, header: list) -> dict:
        """
        Build a map of role -> column index from the CSV header.

        Raises KeyError with a clear message if a required column is missing.
        Uses the first occurrence when a name appears more than once.
        """
        def find(name: str) -> int:
            try:
                return header.index(name)
            except ValueError as e:
                raise KeyError(
                    f"Circles of Care CSV header is missing required column "
                    f"{name!r}. Header was: {header}"
                ) from e

        return {
            "visit_id": find(self.VISIT_ID_COLUMN),
            "visittype": find(self.VISITTYPE_COLUMN),
            "initial_appt": find(self.INITIAL_APPT_COLUMN),
            "agegroup": find(self.AGEGROUP_COLUMN),
            "last": find(self.LAST_COLUMN),
        }

    def _is_valid_visit_id(self, val: str) -> bool:
        """Return True if *val* looks like a 6-to-15-digit visit ID."""
        val = str(val).strip()
        return val.isdigit() and 6 <= len(val) <= 15

    def _fix_comma_shift(self, row: list, indices: dict, num_columns: int) -> list:
        """
        Normalise a raw row to exactly num_columns columns.

        The 'InitialAppt' field occasionally contains unquoted commas, e.g.
        "Agency Referral (Hospital, LEO, School)". This shifts every column
        after it to the right and makes the row longer than num_columns.

        Recovery strategy:
          - Columns before InitialAppt are stable.
          - The last two columns are always AGEGROUP and the final column
            (DiagnoseOnVisit).
          - Everything between InitialAppt's index and the last two columns
            is the fragmented InitialAppt value — rejoin with ", ".
        """
        n = len(row)

        if n < num_columns:
            return list(row) + [""] * (num_columns - n)

        if n == num_columns:
            return list(row)

        initial_appt_idx = indices["initial_appt"]
        stable = list(row[:initial_appt_idx])
        tail = row[initial_appt_idx:]

        last_value = tail[-1].strip()
        agegroup_value = tail[-2].strip()
        initial_appt_parts = tail[:-2]
        initial_appt = ", ".join(part.strip() for part in initial_appt_parts)

        return stable + [initial_appt, agegroup_value, last_value]
