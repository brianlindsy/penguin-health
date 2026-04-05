import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

def safe_filename(value: str) -> str:
    val_str = str(value).replace('.0', '').strip()
    return re.sub(r'[^\w\-]', '_', val_str)

def detect_encoding(path: Path) -> str:
    candidates = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']
    for enc in candidates:
        try:
            with open(path, encoding=enc) as f:
                f.read()
            logging.debug('Detected file encoding: %s', enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    logging.warning('Could not detect encoding cleanly; falling back to latin-1')
    return 'latin-1'

def is_within_recent_days(date_str: str, days_ago: int) -> bool:
    """Checks if a given date string falls within the specified number of days from today."""
    if not date_str:
        return False
        
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%m-%d-%Y', '%Y/%m/%d']
    parsed_date = None
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str.split(' ')[0], fmt).date()
            break
        except ValueError:
            continue
            
    if not parsed_date:
        logging.debug('Could not parse date format for: %s', date_str)
        return False
        
    today = datetime.now().date()
    past_date_threshold = today - timedelta(days=days_ago)
    
    return past_date_threshold <= parsed_date <= today

def build_textract_json(row_dict: dict, index: int) -> dict:
    def get_val(key):
        return str(row_dict.get(key, '')).strip()
    
    narrative_simple = get_val('27_Narrative_Simple')
    narrative_rich = get_val('28_Narrative_Rich')
    session_summary = f'{narrative_simple} {narrative_rich}'.strip()
    
    # --- UPDATED TO MATCH EXACT HEADERS FROM CSV ---
    text_lines = [
        f'Visit Link: {get_val("50_Visit_Link")}',
        f'Service ID: {get_val("1_Service_ID")}',
        f'Consumer Name: {get_val("2_Consumer_Name")}',
        f'Consumer DOB: {get_val("3_Consumer_DOB")}',
        f'Episode ID: {get_val("4_Episode_ID")}',
        f'Staff Name: {get_val("5_Staff_Name")}',
        f'Staff ID: {get_val("6_Staff_ID")}',
        f'Program Code: {get_val("7_Program")}',
        f'Program Name: {get_val("7b_Program_Name")}',
        f'Service Date: {get_val("8_Service_Date")}',
        f'Start Time: {get_val("9_Start_Time")}',
        f'End Time: {get_val("10_End_Time")}',
        f'Revised Start: {get_val("11_Revised_Start")}',
        f'Revised End: {get_val("12_Revised_End")}',
        f'Duration: {get_val("13_Duration")}',
        f'Signed Time: {get_val("14_Signed_Time")}',
        f'Transferred Time: {get_val("15_Transferred_Time")}',
        f'CPT Code: {get_val("16_CPT_Code")}',
        f'Client Insurance Order: {get_val("26s_Client_Insurance_Order")}',
        f'Billing Order: {get_val("26b_Billing_Order")}',
        f'Billing Sequence: {get_val("26c_Billing_Sequence")}',
        f'Billing Group ID: {get_val("26t_Billing_Group_ID")}',
        f'Billing Group Name: {get_val("26u_Billing_Group_Name")}',
        f'Units: {get_val("17_Units")}',
        f'Rate: {get_val("18_Rate")}',
        f'Modifier 1: {get_val("19_Modifier_1")}',
        f'Modifier 2: {get_val("20_Modifier_2")}',
        f'Visit Type ID: {get_val("21_Visit_Type_ID")}',
        f'Visit Type Name: {get_val("21b_Visit_Type_Name")}',
        f'Location Code: {get_val("22_Location_Code")}',
        f'Location Label: {get_val("22b_Location_Label")}',
        f'Recipient Code: {get_val("23_Recipient_Code")}',
        f'Recipient Label: {get_val("23b_Recipient_Label")}',
        f'Approved: {get_val("24_Approved")}',
        f'Non Billable: {get_val("25_Non_Billable")}',
        f'Authorization ID: {get_val("26_Authorization_ID")}',
        f'Narrative Simple: {get_val("27_Narrative_Simple")}',
        f'Narrative Rich: {get_val("28_Narrative_Rich")}',
        f'Next Steps Effective: {get_val("29c_Next_Steps_Effective")}',
        f'Next Steps Form Only: {get_val("29b_Next_Steps_Form_Only")}',
        f'Next Steps: {get_val("29_Next_Steps")}',
        f'Form Q and A: {get_val("30_31_Form_Q_and_A")}',
        f'Vitals BP: {get_val("32_Vitals_BP")}',
        f'Vitals Pulse: {get_val("33_Vitals_Pulse")}',
        f'Vitals Temp: {get_val("34_Vitals_Temp")}',
        f'Vitals Weight: {get_val("35_Vitals_Weight")}',
        f'Plan Start Date: {get_val("38_Plan_Start_Date")}',
        f'Plan End Date: {get_val("39_Plan_End_Date")}',
        f'Plan Status: {get_val("40_Plan_Status")}',
        f'Diagnosis Code Primary: {get_val("41b_Diagnosis_Code_Primary")}',
        f'External ID: {get_val("44_External_ID")}',
        f'Insurance: {get_val("45_Insurance")}',
        f'Has Next Insurance Flag: {get_val("26o_Has_Next_Insurance_Flag")}',
        f'Plan Type: {get_val("46_Plan_Type")}',
        f'Plan Signed Date: {get_val("48_Plan_Signed_Date")}',
        f'Plan Signer: {get_val("49_Plan_Signer")}',
        f'Combined Session Summary: {session_summary}'
    ]
    # -----------------------------------------------
    
    text_block = '\n'.join(text_lines)
    json_obj = {
        'text': text_block,
        'metadata': {
            'document_pages': 1,
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'job_status': 'SUCCEEDED',
            'encounter_index': index,
            'is_split_encounter': False,
            'line_count': len(text_lines)
        }
    }
    return json_obj

def process(input_path: Path, output_dir: Path, days_ago: int) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    encoding = detect_encoding(input_path)
    total_rows = 0
    skipped_filtered = 0
    skipped_missing_id = 0
    files_written = 0
    
    allowed_programs = {
        'Med Somatic Services',
        'Free Standing Mental Health Clinic - Med Services',
        'Free Standing Mental Health Clinic - Counseling',
        'Counseling',
        'Compass',
        'Community Support',
        'ACT'
    }

    with open(input_path, newline='', encoding=encoding) as f:
        reader = csv.DictReader(f)
        for line_num, row_dict in enumerate(reader, start=2):
            total_rows += 1
            
            approved_status = str(row_dict.get('24_Approved', '')).strip().lower()
            non_billable_status = str(row_dict.get('25_Non_Billable', '')).strip().lower()
            program_name = str(row_dict.get('7b_Program_Name', '')).strip()
            service_date_str = str(row_dict.get('8_Service_Date', '')).strip()
            
            if (approved_status != 'no' or 
                non_billable_status != 'no' or 
                program_name not in allowed_programs or
                not is_within_recent_days(service_date_str, days_ago)):
                
                skipped_filtered += 1
                continue

            service_id = str(row_dict.get('1_Service_ID', '')).strip()
            if not service_id:
                logging.debug('Line %d: missing Service ID, generating fallback ID.', line_num)
                service_id = f'index_{total_rows}'
                skipped_missing_id += 1
                
            json_data = build_textract_json(row_dict, total_rows)
            filename = f'{safe_filename(service_id)}.json'
            out_path = output_dir / filename
            with open(out_path, 'w', encoding='utf-8') as out_f:
                json.dump(json_data, out_f, indent=2, ensure_ascii=False)
            files_written += 1
            if files_written % 100 == 0:
                logging.info('  Written %d files so far...', files_written)
                
    return {
        'total_rows': total_rows,
        'files_written': files_written,
        'skipped_filtered': skipped_filtered,
        'skipped_missing_id': skipped_missing_id,
        'output_dir': str(output_dir),
        'days_filter_applied': days_ago
    }

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Convert Catholic Charities CSV to Textract-style JSONs.')
    parser.add_argument('input_file', help='Path to the source CSV file')
    parser.add_argument('--output-dir', default='./unapproved_encounters', help='Directory for output files')
    parser.add_argument('--days-ago', type=int, default=7, help='Number of days in the past to include (default: 7)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    input_path = Path(args.input_file)
    
    if not input_path.exists():
        logging.error('Input file not found: %s', input_path)
        sys.exit(1)
        
    output_dir = Path(args.output_dir)
    logging.info('Input:  %s', input_path)
    
    try:
        summary = process(input_path=input_path, output_dir=output_dir, days_ago=args.days_ago)
    except Exception as e:
        logging.error('Fatal error: %s', e)
        sys.exit(1)
        
    print("\n" + "=" * 50)
    print("  SUMMARY")
    print("=" * 50)
    print(f"  Total CSV rows read   : {summary['total_rows']:,}")
    print(f"  JSON Files generated  : {summary['files_written']:,}")
    print(f"  Skipped (Filtered out): {summary['skipped_filtered']:,}")
    print(f"  Date filter applied   : Last {summary['days_filter_applied']} days")
    print(f"  Output directory      : {summary['output_dir']}")
    print("=" * 50)

if __name__ == "__main__":
    main()