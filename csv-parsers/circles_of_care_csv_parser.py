#!/usr/bin/env python3
"""
split_intake_csv.py (CSV OUTPUT VERSION)

This script splits a large intake file into individual CSV files per visit.
It automatically fixes 'shifted' rows caused by unquoted commas and 
ensures the diagnosis code is placed in the correct column.
"""

import argparse
import csv
import logging
import os
import re
from collections import defaultdict
from pathlib import Path

# The standard 27 columns expected by the system
CANONICAL_COLUMNS = [
    "fake_client_id", "clientvisit_id", "grade", "race_desc", "ethnicity_desc",
    "sex", "marital_status", "age_at_service", "visittype", "plan_id",
    "service_date", "episode_id", "program_desc", "admission_date", "discharge_date",
    "icd10_codes", "problem_list_order", "fake_client_id_2", "clientvisit_id_2",
    "first_referral", "question_text", "answer", "type", "episode_id_2",
    "initial_appt", "age_group", "diagnose_on_visit"
]

def is_valid_visit_id(val):
    val = str(val).strip()
    return val.isdigit() and 6 <= len(val) <= 15

def process(input_path: Path, output_dir: Path, intake_only: bool):
    output_dir.mkdir(parents=True, exist_ok=True)
    
    groups = defaultdict(list)
    current_visit_id = None
    
    # Read and Grouping Logic
    for enc in ['utf-8-sig', 'cp1252', 'latin-1']:
        try:
            with open(input_path, newline='', encoding=enc) as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row: continue
                    
                    # Detect start of a new record (ID is in Column 2 / Index 1)
                    if len(row) > 1 and is_valid_visit_id(row[1]):
                        current_visit_id = row[1].strip()
                    
                    if current_visit_id:
                        # --- THE FIX: Handle the "Comma Shift" ---
                        # If the row is longer than 27, it means commas shifted the data.
                        # We grab the DX from the very end and force it into the 27th slot.
                        actual_dx = row[-1].strip() if len(row) > 0 else ""
                        
                        # Standardize the row to 27 columns
                        new_row = row[:27]
                        if len(new_row) < 27:
                            new_row.extend([""] * (27 - len(new_row)))
                        
                        # Place the diagnosis code in the correct canonical column (index 26)
                        # We only overwrite if the actual_dx looks like a code (not 'Adult' or 'School')
                        if len(actual_dx) < 10 and any(c.isdigit() for c in actual_dx):
                            new_row[26] = actual_dx
                            
                        groups[current_visit_id].append(new_row)
            break 
        except UnicodeDecodeError:
            continue

    written = 0
    for visit_id, rows in groups.items():
        # Filter for Intake Screening only (unless --all is used)
        is_intake = any(len(r) > 8 and "Intake Screening" in r[8] for r in rows)
        if intake_only and not is_intake:
            continue

        out_file = output_dir / f"visit_{visit_id}.csv"
        with open(out_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(CANONICAL_COLUMNS)
            # Write data rows
            writer.writerows(rows)
        written += 1

    return written

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="The raw Penguin_COC CSV file")
    parser.add_argument("--output-dir", default="./split_csv_files", help="Folder to save the output")
    parser.add_argument("--all", action="store_true", help="Include all visits, not just Intake Screening")
    args = parser.parse_args()

    print(f"Reading {args.input_file}...")
    count = process(Path(args.input_file), Path(args.output_dir), not args.all)
    print(f"Done! Successfully created {count} individual CSV files in: {args.output_dir}")