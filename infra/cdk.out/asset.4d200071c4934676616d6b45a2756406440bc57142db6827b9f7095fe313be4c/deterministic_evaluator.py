"""
Deterministic Rule Evaluator for code-based validation without LLM calls.

Evaluates rules using declarative conditions that compare CSV column values.
Supports date, string, and numeric comparisons with AND/OR logic.

The 'field' in conditions refers directly to CSV column headers.
"""

import csv
import re
from datetime import datetime
from io import StringIO


# Date formats to try when parsing date strings
DATE_FORMATS = [
    '%Y-%m-%d',      # 2024-01-15
    '%m/%d/%Y',      # 01/15/2024
    '%m/%d/%y',      # 01/15/24
    '%m-%d-%Y',      # 01-15-2024
    '%d/%m/%Y',      # 15/01/2024
    '%Y/%m/%d',      # 2024/01/15
]

# DateTime formats (date + time) to try when parsing
DATETIME_FORMATS = [
    '%m/%d/%Y %I:%M:%S %p',  # 01/15/2024 02:30:00 PM
    '%m/%d/%Y %I:%M %p',     # 01/15/2024 02:30 PM
    '%Y-%m-%d %H:%M:%S',     # 2024-01-15 14:30:00
    '%Y-%m-%d %H:%M',        # 2024-01-15 14:30
    '%m/%d/%Y %H:%M:%S',     # 01/15/2024 14:30:00
    '%m/%d/%Y %H:%M',        # 01/15/2024 14:30
    '%m-%d-%Y %I:%M:%S %p',  # 01-15-2024 02:30:00 PM
    '%m-%d-%Y %I:%M %p',     # 01-15-2024 02:30 PM
    '%Y/%m/%d %H:%M:%S',     # 2024/01/15 14:30:00
    '%Y/%m/%d %H:%M',        # 2024/01/15 14:30
]

# Time-only formats
TIME_FORMATS = [
    '%I:%M:%S %p',   # 02:30:00 PM
    '%I:%M %p',      # 02:30 PM
    '%H:%M:%S',      # 14:30:00
    '%H:%M',         # 14:30
]


def parse_time(value):
    """
    Parse a time string into a datetime.time object.

    Args:
        value: Time string to parse (e.g., "2:30 PM", "14:30")

    Returns:
        datetime.time object or None if parsing fails
    """
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue

    return None


def parse_datetime(value):
    """
    Parse a datetime string into a datetime object.
    Tries datetime formats first, then falls back to date-only formats.

    Args:
        value: DateTime string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    value = str(value).strip()
    if not value:
        return None

    # Try datetime formats first
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Fall back to date-only formats
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def parse_date(value):
    """
    Parse a date string into a datetime object.

    Args:
        value: Date string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    value = str(value).strip()
    if not value:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def parse_number(value):
    """
    Parse a value into a number (int or float).

    Args:
        value: Value to parse

    Returns:
        Number or None if parsing fails
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return value

    value = str(value).strip()
    if not value:
        return None

    try:
        # Try int first
        if '.' not in value:
            return int(value)
        return float(value)
    except ValueError:
        return None


# Date operators
def op_before(field_date, compare_date, value=None):
    """Field date is before compare date."""
    return field_date < compare_date


def op_after(field_date, compare_date, value=None):
    """Field date is after compare date."""
    return field_date > compare_date


def op_equals_date(field_date, compare_date, value=None):
    """Dates are equal (comparing date portion only)."""
    return field_date.date() == compare_date.date()


def op_within_days(field_date, compare_date, value):
    """Field date is within N days of compare date."""
    if value is None:
        return False
    days_diff = abs((field_date - compare_date).days)
    return days_diff <= value


def op_not_before(field_date, compare_date, value=None):
    """Field date is on or after compare date."""
    return field_date >= compare_date


def op_not_after(field_date, compare_date, value=None):
    """Field date is on or before compare date."""
    return field_date <= compare_date


# Time operators (for time-of-day checks)
def op_time_before(field_time, compare_time, value):
    """Field time is before the specified time value (e.g., "7:00 AM")."""
    if value is None:
        return False
    target_time = parse_time(value)
    if target_time is None:
        return False
    return field_time < target_time


def op_time_after(field_time, compare_time, value):
    """Field time is after the specified time value (e.g., "7:00 PM")."""
    if value is None:
        return False
    target_time = parse_time(value)
    if target_time is None:
        return False
    return field_time > target_time


def op_time_between(field_time, compare_time, value):
    """Field time is between start and end times (inclusive)."""
    if not isinstance(value, dict):
        return False
    start = parse_time(value.get('start'))
    end = parse_time(value.get('end'))
    if start is None or end is None:
        return False
    return start <= field_time <= end


def op_time_not_between(field_time, compare_time, value):
    """Field time is NOT between start and end times."""
    if not isinstance(value, dict):
        return False
    start = parse_time(value.get('start'))
    end = parse_time(value.get('end'))
    if start is None or end is None:
        return False
    return field_time < start or field_time > end


# DateTime operators (compare full datetime including time component)
def op_datetime_before(field_dt, compare_dt, value=None):
    """Field datetime is before compare datetime (includes time)."""
    return field_dt < compare_dt


def op_datetime_after(field_dt, compare_dt, value=None):
    """Field datetime is after compare datetime (includes time)."""
    return field_dt > compare_dt


def op_datetime_not_before(field_dt, compare_dt, value=None):
    """Field datetime is on or after compare datetime."""
    return field_dt >= compare_dt


def op_datetime_not_after(field_dt, compare_dt, value=None):
    """Field datetime is on or before compare datetime."""
    return field_dt <= compare_dt


def op_duration_lte(field_dt, compare_dt, value):
    """
    Duration between field and compare_to is less than or equal to value (in minutes).
    Calculates: compare_dt - field_dt <= value minutes
    Use for checking appointment length doesn't exceed a maximum.
    """
    if value is None or compare_dt is None:
        return False
    duration_minutes = (compare_dt - field_dt).total_seconds() / 60
    return duration_minutes <= value


def op_duration_gte(field_dt, compare_dt, value):
    """
    Duration between field and compare_to is greater than or equal to value (in minutes).
    Calculates: compare_dt - field_dt >= value minutes
    """
    if value is None or compare_dt is None:
        return False
    duration_minutes = (compare_dt - field_dt).total_seconds() / 60
    return duration_minutes >= value


def op_duration_between(field_dt, compare_dt, value):
    """
    Duration between field and compare_to is between min and max (in minutes).
    value should be {"min": X, "max": Y}
    """
    if not isinstance(value, dict) or compare_dt is None:
        return False
    min_val = value.get('min')
    max_val = value.get('max')
    if min_val is None or max_val is None:
        return False
    duration_minutes = (compare_dt - field_dt).total_seconds() / 60
    return min_val <= duration_minutes <= max_val


# String operators
def op_equals(field_value, compare_value, value):
    """Exact string match (case-sensitive)."""
    target = compare_value if compare_value is not None else value
    return str(field_value) == str(target)


def op_equals_ignore_case(field_value, compare_value, value):
    """Exact string match (case-insensitive)."""
    target = compare_value if compare_value is not None else value
    return str(field_value).lower() == str(target).lower()


def op_contains(field_value, compare_value, value):
    """Field contains the value substring."""
    target = compare_value if compare_value is not None else value
    return str(target) in str(field_value)


def op_starts_with(field_value, compare_value, value):
    """Field starts with value."""
    target = compare_value if compare_value is not None else value
    return str(field_value).startswith(str(target))


def op_ends_with(field_value, compare_value, value):
    """Field ends with value."""
    target = compare_value if compare_value is not None else value
    return str(field_value).endswith(str(target))


def op_in(field_value, compare_value, value):
    """Field value is in the provided list."""
    if not isinstance(value, list):
        return False
    return str(field_value) in [str(v) for v in value]


def op_starts_with_any(field_value, compare_value, value):
    """Field value starts with any of the provided list items."""
    if not isinstance(value, list):
        return False
    field_str = str(field_value)
    return any(field_str.startswith(str(v)) for v in value)


def op_not_equals(field_value, compare_value, value):
    """Field does not equal value."""
    target = compare_value if compare_value is not None else value
    return str(field_value) != str(target)


def op_matches_regex(field_value, compare_value, value):
    """Field matches regex pattern."""
    pattern = compare_value if compare_value is not None else value
    try:
        return bool(re.search(str(pattern), str(field_value)))
    except re.error:
        return False


# Numeric operators
def op_eq(field_num, compare_num, value):
    """Numeric equals."""
    target = compare_num if compare_num is not None else value
    return field_num == target


def op_ne(field_num, compare_num, value):
    """Numeric not equals."""
    target = compare_num if compare_num is not None else value
    return field_num != target


def op_gt(field_num, compare_num, value):
    """Greater than."""
    target = compare_num if compare_num is not None else value
    return field_num > target


def op_gte(field_num, compare_num, value):
    """Greater than or equal."""
    target = compare_num if compare_num is not None else value
    return field_num >= target


def op_lt(field_num, compare_num, value):
    """Less than."""
    target = compare_num if compare_num is not None else value
    return field_num < target


def op_lte(field_num, compare_num, value):
    """Less than or equal."""
    target = compare_num if compare_num is not None else value
    return field_num <= target


def op_between(field_num, compare_num, value):
    """Value is between min and max (inclusive)."""
    if not isinstance(value, dict):
        return False
    min_val = value.get('min')
    max_val = value.get('max')
    if min_val is None or max_val is None:
        return False
    return min_val <= field_num <= max_val


# Operator registry with type hints
DATE_OPERATORS = {
    'before': op_before,
    'after': op_after,
    'equals_date': op_equals_date,
    'within_days': op_within_days,
    'not_before': op_not_before,
    'not_after': op_not_after,
}

TIME_OPERATORS = {
    'time_before': op_time_before,
    'time_after': op_time_after,
    'time_between': op_time_between,
    'time_not_between': op_time_not_between,
}

DATETIME_OPERATORS = {
    'datetime_before': op_datetime_before,
    'datetime_after': op_datetime_after,
    'datetime_not_before': op_datetime_not_before,
    'datetime_not_after': op_datetime_not_after,
    'duration_lte': op_duration_lte,
    'duration_gte': op_duration_gte,
    'duration_between': op_duration_between,
}

STRING_OPERATORS = {
    'equals': op_equals,
    'equals_ignore_case': op_equals_ignore_case,
    'contains': op_contains,
    'starts_with': op_starts_with,
    'ends_with': op_ends_with,
    'in': op_in,
    'starts_with_any': op_starts_with_any,
    'not_equals': op_not_equals,
    'matches_regex': op_matches_regex,
}

NUMERIC_OPERATORS = {
    'eq': op_eq,
    'ne': op_ne,
    'gt': op_gt,
    'gte': op_gte,
    'lt': op_lt,
    'lte': op_lte,
    'between': op_between,
}


def get_field_value(fields, field_spec):
    """
    Get a field value, supporting fallback fields.

    Args:
        fields: Dict of field values
        field_spec: Either a string field name, or a list of field names to try in order

    Returns:
        tuple: (value, field_name_used) or (None, field_spec) if not found
    """
    if isinstance(field_spec, list):
        # Try each field in order, use first non-empty value
        for field_name in field_spec:
            value = fields.get(field_name)
            if value is not None and str(value).strip():
                return value, field_name
        return None, field_spec[0] if field_spec else None
    else:
        return fields.get(field_spec), field_spec


def evaluate_condition(condition, fields):
    """
    Evaluate a single condition against the extracted fields.

    Args:
        condition: Condition dict with field, operator, compare_to, value, description
            - field: Field name or list of field names (fallback order)
            - compare_to: Field name or list of field names (fallback order)
            - description: Human-readable description of what this condition checks
        fields: Dict of extracted field values

    Returns:
        tuple: (passed: bool, message: str, skip: bool)
    """
    field_spec = condition.get('field')
    operator = condition.get('operator')
    compare_to = condition.get('compare_to')
    value = condition.get('value')
    description = condition.get('description')

    # Get field value (supports fallback list)
    field_value, field_name = get_field_value(fields, field_spec)
    if field_value is None:
        return False, f"Required field '{field_name}' not found", True

    # Get comparison value from another field if specified
    compare_value = None
    if compare_to:
        compare_value, compare_field_name = get_field_value(fields, compare_to)
        if compare_value is None:
            return False, f"Comparison field '{compare_field_name}' not found", True
    else:
        compare_field_name = None

    # Determine operator type and evaluate
    if operator in DATE_OPERATORS:
        field_date = parse_date(field_value)
        if field_date is None:
            return False, f"Could not parse date from field '{field_name}': '{field_value}'", True

        compare_date = None
        if compare_to:
            compare_date = parse_date(compare_value)
            if compare_date is None:
                return False, f"Could not parse date from field '{compare_field_name}': '{compare_value}'", True

        op_func = DATE_OPERATORS[operator]
        try:
            result = op_func(field_date, compare_date, value)
            if result:
                msg = description if description else f"'{field_name}' ({field_value}) {operator}"
                if not description:
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_value})"
                    if value is not None:
                        msg += f" (value: {value})"
                return True, msg, False
            else:
                if description:
                    msg = f"{description}: '{field_name}'={field_value}"
                    if compare_to:
                        msg += f", '{compare_field_name}'={compare_value}"
                else:
                    msg = f"'{field_name}' ({field_value}) failed {operator}"
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_value})"
                    if value is not None:
                        msg += f" (value: {value})"
                return False, msg, False
        except Exception as e:
            return False, f"Error evaluating {operator}: {str(e)}", True

    elif operator in STRING_OPERATORS:
        op_func = STRING_OPERATORS[operator]
        try:
            result = op_func(field_value, compare_value, value)
            if result:
                msg = description if description else f"'{field_name}' ({field_value}) {operator}"
                if not description:
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_value})"
                    elif value is not None:
                        msg += f" '{value}'"
                return True, msg, False
            else:
                if description:
                    msg = f"{description}: '{field_name}'={field_value}"
                else:
                    msg = f"'{field_name}' ({field_value}) failed {operator}"
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_value})"
                    elif value is not None:
                        msg += f" '{value}'"
                return False, msg, False
        except Exception as e:
            return False, f"Error evaluating {operator}: {str(e)}", True

    elif operator in NUMERIC_OPERATORS:
        field_num = parse_number(field_value)
        if field_num is None:
            return False, f"Could not parse number from field '{field_name}': '{field_value}'", True

        compare_num = None
        if compare_to:
            compare_num = parse_number(compare_value)
            if compare_num is None:
                return False, f"Could not parse number from field '{compare_field_name}': '{compare_value}'", True

        op_func = NUMERIC_OPERATORS[operator]
        try:
            result = op_func(field_num, compare_num, value)
            if result:
                msg = description if description else f"'{field_name}' ({field_num}) {operator}"
                if not description:
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_num})"
                    elif value is not None:
                        msg += f" {value}"
                return True, msg, False
            else:
                if description:
                    msg = f"{description}: '{field_name}'={field_num}"
                    if compare_to:
                        msg += f", '{compare_field_name}'={compare_num}"
                else:
                    msg = f"'{field_name}' ({field_num}) failed {operator}"
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_num})"
                    elif value is not None:
                        msg += f" {value}"
                return False, msg, False
        except Exception as e:
            return False, f"Error evaluating {operator}: {str(e)}", True

    elif operator in TIME_OPERATORS:
        # Extract time from datetime field value
        field_dt = parse_datetime(field_value)
        if field_dt is None:
            field_time = parse_time(field_value)
            if field_time is None:
                return False, f"Could not parse time from field '{field_name}': '{field_value}'", True
        else:
            field_time = field_dt.time()

        op_func = TIME_OPERATORS[operator]
        try:
            result = op_func(field_time, None, value)
            if result:
                msg = description if description else f"'{field_name}' ({field_value}) {operator} {value}"
                return True, msg, False
            else:
                if description:
                    msg = f"{description}: '{field_name}'={field_value}"
                else:
                    msg = f"'{field_name}' ({field_value}) failed {operator} {value}"
                return False, msg, False
        except Exception as e:
            return False, f"Error evaluating {operator}: {str(e)}", True

    elif operator in DATETIME_OPERATORS:
        field_dt = parse_datetime(field_value)
        if field_dt is None:
            return False, f"Could not parse datetime from field '{field_name}': '{field_value}'", True

        compare_dt = None
        if compare_to:
            compare_dt = parse_datetime(compare_value)
            if compare_dt is None:
                return False, f"Could not parse datetime from field '{compare_field_name}': '{compare_value}'", True

        op_func = DATETIME_OPERATORS[operator]
        try:
            result = op_func(field_dt, compare_dt, value)
            if result:
                msg = description if description else f"'{field_name}' ({field_value}) {operator}"
                if not description and compare_to:
                    msg += f" '{compare_field_name}' ({compare_value})"
                return True, msg, False
            else:
                if description:
                    msg = f"{description}: '{field_name}'={field_value}"
                    if compare_to:
                        msg += f", '{compare_field_name}'={compare_value}"
                else:
                    msg = f"'{field_name}' ({field_value}) failed {operator}"
                    if compare_to:
                        msg += f" '{compare_field_name}' ({compare_value})"
                return False, msg, False
        except Exception as e:
            return False, f"Error evaluating {operator}: {str(e)}", True

    else:
        return False, f"Unknown operator: {operator}", True


def extract_csv_columns(csv_content):
    """
    Extract all columns from CSV content as a dictionary.

    Args:
        csv_content: Raw CSV string with headers

    Returns:
        dict: Column names mapped to values from the first data row,
              or empty dict if parsing fails
    """
    if not csv_content:
        return {}

    try:
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)

        if not rows:
            print("CSV has no data rows")
            return {}

        # Use first row for column values
        first_row = rows[0]
        columns = {}

        for col_name, value in first_row.items():
            if col_name is not None:
                columns[col_name] = value.strip() if value else None

        print(f"Extracted {len(columns)} columns from CSV: {list(columns.keys())[:10]}...")
        return columns

    except Exception as e:
        print(f"Error parsing CSV for deterministic rule: {e}")
        return {}


def extract_all_csv_rows(csv_content):
    """
    Extract all rows from CSV content as a list of dictionaries.

    Args:
        csv_content: Raw CSV string with headers

    Returns:
        list: List of dicts, each representing a row with column names as keys
    """
    if not csv_content:
        return []

    try:
        reader = csv.DictReader(StringIO(csv_content))
        rows = []
        for row in reader:
            cleaned_row = {}
            for col_name, value in row.items():
                if col_name is not None:
                    cleaned_row[col_name] = value.strip() if value else None
            rows.append(cleaned_row)

        print(f"Extracted {len(rows)} rows from CSV")
        return rows

    except Exception as e:
        print(f"Error parsing CSV rows: {e}")
        return []


def find_row_with_value(rows, column_name, value):
    """
    Find the first row where column_name equals value.

    Args:
        rows: List of row dicts
        column_name: Column to search in
        value: Value to match

    Returns:
        dict: The matching row, or None if not found
    """
    for row in rows:
        if row.get(column_name) == value:
            return row
    return None


def evaluate_row_match_condition(condition, all_rows):
    """
    Evaluate a row_match condition that searches across all CSV rows.

    This is used for multi-row CSVs where different data is on different lines.
    For example, finding a row where question_text="Insurance Type:" and checking
    if that row's answer="Medicare".

    Args:
        condition: Dict with:
            - question_field: Column name containing the question (e.g., "question_text")
            - question_value: Value to match in question_field (e.g., "Insurance Type:")
            - answer_field: Column name containing the answer (e.g., "answer")
            - answer_value: Expected value in answer_field (e.g., "Medicare")
        all_rows: List of all CSV row dicts

    Returns:
        tuple: (passed: bool, message: str, skip: bool)
    """
    question_field = condition.get('question_field')
    question_value = condition.get('question_value')
    answer_field = condition.get('answer_field')
    answer_value = condition.get('answer_value')

    if not all([question_field, question_value, answer_field, answer_value]):
        return False, 'row_match condition missing required fields', True

    # Search all rows for one where question_field matches question_value
    matching_row = find_row_with_value(all_rows, question_field, question_value)

    if matching_row is None:
        return False, f"No row found with '{question_field}'='{question_value}'", True

    # Check if the answer field in that row matches the expected value
    actual_answer = matching_row.get(answer_field)
    if actual_answer == answer_value:
        return True, f"Row match: '{question_field}'='{question_value}' has '{answer_field}'='{answer_value}'", False
    else:
        return False, f"Row match failed: '{question_field}'='{question_value}' has '{answer_field}'='{actual_answer}' (expected '{answer_value}')", False


def evaluate_deterministic_rule(rule_config, fields, data=None):
    """
    Evaluate a deterministic rule against CSV column values.

    Args:
        rule_config: Rule configuration dict with conditions and logic
        fields: Dict of pre-extracted field values (unused, kept for API compatibility)
        data: Dict with 'text' key containing raw CSV content

    Returns:
        tuple: (status: str, message: str)
            status is one of: 'PASS', 'FAIL', 'SKIP', 'ERROR'
    """
    conditions = rule_config.get('conditions', [])
    logic = rule_config.get('logic', 'all')  # 'all', 'any', or 'conditional'

    if not conditions and logic != 'conditional':
        return 'SKIP', 'No conditions defined for rule'

    # Extract columns directly from CSV
    if not data or not data.get('text'):
        return 'SKIP', 'No CSV data available for evaluation'

    csv_content = data.get('text', '')
    first_line = csv_content.split('\n')[0] if csv_content else ''
    if ',' not in first_line:
        return 'SKIP', 'Data does not appear to be CSV format'

    csv_columns = extract_csv_columns(csv_content)
    all_rows = extract_all_csv_rows(csv_content)

    if not csv_columns:
        return 'SKIP', 'No columns extracted from CSV'

    # Handle conditional logic: "if X then Y" rules
    # Format: { "logic": "conditional", "conditionals": [ { "if": [...], "then": [...] or "pass" }, ... ] }
    # If the "if" conditions match, the "then" conditions must also match (or auto-pass if "then": "pass")
    # If the "if" conditions don't match, continue to next conditional
    # If no conditionals match, the rule FAILS (diagnosis not in any acceptable category)
    #
    # Special condition type "row_match": searches ALL rows for a matching question/answer pair
    # Format: { "type": "row_match", "question_field": "question_text", "question_value": "Insurance Type:",
    #           "answer_field": "answer", "answer_value": "Medicare" }
    if logic == 'conditional':
        conditionals = rule_config.get('conditionals', [])
        if not conditionals:
            return 'SKIP', 'No conditionals defined for conditional rule'

        # Track the primary field value for better failure messages
        primary_field_value = None
        primary_field_name = None

        for conditional in conditionals:
            if_conditions = conditional.get('if', [])
            then_clause = conditional.get('then')
            # Support custom human-readable messages per conditional branch
            branch_pass_message = conditional.get('pass_message')
            branch_fail_message = conditional.get('fail_message')

            if not if_conditions:
                continue

            # Evaluate "if" conditions
            if_results = []
            if_messages = []
            if_skip = False
            for cond in if_conditions:
                # Capture the primary field being checked (first condition's field)
                if primary_field_name is None:
                    field_spec = cond.get('field')
                    field_val, field_nm = get_field_value(csv_columns, field_spec)
                    if field_val is not None:
                        primary_field_value = field_val
                        primary_field_name = field_nm

                passed, message, skip = evaluate_condition(cond, csv_columns)
                if_results.append(passed)
                if_messages.append(message)
                if skip:
                    if_skip = True

            # If any "if" condition had a skip, skip this conditional
            if if_skip:
                continue

            # Check if all "if" conditions are met
            if all(if_results):
                # "if" matched - check "then" clause
                # Support "then": "pass" as shorthand for auto-pass when IF matches
                if then_clause == 'pass':
                    if branch_pass_message:
                        return 'PASS', f"PASS - {branch_pass_message}"
                    return 'PASS', f"PASS - Condition met: {'; '.join(if_messages)}"

                # Otherwise evaluate "then" conditions as a list
                then_conditions = then_clause if isinstance(then_clause, list) else []
                if not then_conditions:
                    # Empty then list = auto-pass
                    if branch_pass_message:
                        return 'PASS', f"PASS - {branch_pass_message}"
                    return 'PASS', f"PASS - Condition met: {'; '.join(if_messages)}"

                then_results = []
                then_messages = []
                then_skip = False
                for cond in then_conditions:
                    # Check for special "row_match" condition type that searches all rows
                    if cond.get('type') == 'row_match':
                        passed, message, skip = evaluate_row_match_condition(cond, all_rows)
                    else:
                        passed, message, skip = evaluate_condition(cond, csv_columns)
                    then_results.append(passed)
                    then_messages.append(message)
                    if skip:
                        then_skip = True

                if then_skip:
                    skip_msgs = [m for i, m in enumerate(then_messages) if not then_results[i]]
                    return 'SKIP', f"Conditional matched but then-clause skipped: {'; '.join(skip_msgs)}"

                if all(then_results):
                    if branch_pass_message:
                        return 'PASS', f"PASS - {branch_pass_message}"
                    return 'PASS', f"PASS - Conditional met: IF ({'; '.join(if_messages)}) THEN ({'; '.join(then_messages)})"
                else:
                    failed = [m for i, m in enumerate(then_messages) if not then_results[i]]
                    if branch_fail_message:
                        return 'FAIL', f"FAIL - {branch_fail_message}"
                    return 'FAIL', f"FAIL - Conditional IF matched but THEN failed: IF ({'; '.join(if_messages)}) THEN FAILED ({'; '.join(failed)})"

        # No conditionals matched - rule fails (value not in any acceptable category)
        # Use custom fail_message if provided, otherwise build a descriptive message
        fail_message = rule_config.get('fail_message')
        if fail_message:
            if primary_field_value:
                return 'FAIL', f"FAIL - {fail_message}: '{primary_field_name}'={primary_field_value}"
            return 'FAIL', f"FAIL - {fail_message}"
        elif primary_field_value:
            return 'FAIL', f"FAIL - Value not acceptable: '{primary_field_name}'={primary_field_value}"
        return 'FAIL', 'FAIL - No conditional IF clauses matched (value not acceptable)'

    # Standard condition evaluation for 'all' and 'any' logic
    results = []
    messages = []
    has_skip = False

    for condition in conditions:
        passed, message, skip = evaluate_condition(condition, csv_columns)
        results.append(passed)
        messages.append(message)
        if skip:
            has_skip = True

    # If any condition resulted in a skip (missing field, parse error), skip the rule
    if has_skip:
        skip_messages = [m for i, m in enumerate(messages) if not results[i]]
        return 'SKIP', '; '.join(skip_messages)

    # Apply logic
    if logic == 'all':
        if all(results):
            return 'PASS', f"PASS - All conditions met: {'; '.join(messages)}"
        else:
            failed = [m for i, m in enumerate(messages) if not results[i]]
            return 'FAIL', f"FAIL - Conditions not met: {'; '.join(failed)}"
    elif logic == 'any':
        if any(results):
            passed_msgs = [m for i, m in enumerate(messages) if results[i]]
            return 'PASS', f"PASS - Condition met: {'; '.join(passed_msgs)}"
        else:
            return 'FAIL', f"FAIL - No conditions met: {'; '.join(messages)}"
    else:
        return 'ERROR', f"Unknown logic type: {logic}"
