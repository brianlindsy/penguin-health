"""
Deterministic Rule Evaluator for code-based validation without LLM calls.

Evaluates rules using declarative conditions that compare CSV column values.
Supports date, string, and numeric comparisons with AND/OR logic.

The 'field' in conditions refers directly to CSV column headers.
"""

import csv
import math
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO


_NARRATIVE_HASH_TABLE_NAME = os.environ.get(
    "NARRATIVE_HASH_TABLE", "penguin-health-narrative-hashes"
)
_NARRATIVE_HASH_TTL_DAYS = 7


def _narrative_hash_table():
    """Lazy DynamoDB resource so importing this module doesn't require AWS."""
    import boto3  # local import: tests can stub boto3 without loading it
    return boto3.resource("dynamodb").Table(_NARRATIVE_HASH_TABLE_NAME)


def evaluate_narrative_hash_unique(fields):
    """
    Cross-document duplicate-detection operator for the "narratives must be
    individualized" rule.

    Reads `narrative_hash`, `org_id`, and `source_record_id` from `fields`.
    Queries `penguin-health-narrative-hashes` for a prior write under
    (ORG#<org_id>, HASH#<narrative_hash>):
      - if no prior, writes the hash with a 7-day TTL and returns PASS
      - if a prior exists with a different source_record_id, returns FAIL
        with the prior note's id and capture time in the message
      - if a prior exists with the SAME source_record_id, returns PASS
        (re-evaluation of the same note is not a self-collision)

    Returns:
        tuple: (passed: bool, message: str, skip: bool)
    """
    narrative_hash_value = fields.get("narrative_hash")
    org_id = fields.get("org_id")
    source_record_id = fields.get("source_record_id")

    if not narrative_hash_value:
        return False, "Required field 'narrative_hash' not found", True
    if not org_id:
        return False, "Required field 'org_id' not found", True
    if not source_record_id:
        return False, "Required field 'source_record_id' not found", True

    table = _narrative_hash_table()
    pk = f"ORG#{org_id}"
    sk = f"HASH#{narrative_hash_value}"

    prior = table.get_item(Key={"pk": pk, "sk": sk}).get("Item")
    if prior:
        if prior.get("source_record_id") == source_record_id:
            return True, "Same note re-evaluated; not a duplicate", False
        return False, (
            f"Narrative is a duplicate of note "
            f"{prior.get('source_record_id')} from "
            f"{prior.get('captured_at')} within the last 7 days"
        ), False

    now = datetime.now(timezone.utc)
    table.put_item(Item={
        "pk": pk,
        "sk": sk,
        "source_record_id": source_record_id,
        "captured_at": now.isoformat().replace("+00:00", "Z"),
        "ttl": int((now + timedelta(days=_NARRATIVE_HASH_TTL_DAYS)).timestamp()),
    })
    return True, "Narrative is unique within 7-day window", False


def evaluate_sentence_count_meets_hourly_minimum(condition, fields):
    """
    Two-sentences-per-hour check for session narratives.

    The LLM extracts `sentence_count` upstream (a single scalar is a
    stable extraction task at temp 0.01); this operator does the
    deterministic math the LLM was previously flipping mid-response:
    required = 2 * ceil(minutes / 60); PASS when count >= required.

    `condition` uses the standard `field` / `compare_to` shape:
      - field: name of the count field (e.g. "sentence_count")
      - compare_to: name of the minutes field
        (e.g. "billing_list_time_worked_in_mins")

    Returns (passed, message, skip).
    """
    count_field, count_name = get_field_value(fields, condition.get('field'))
    minutes_field, minutes_name = get_field_value(
        fields, condition.get('compare_to'),
    )

    if count_field is None:
        return False, f"Required field '{count_name}' not found", True
    if minutes_field is None:
        return False, f"Required field '{minutes_name}' not found", True

    count = parse_number(count_field)
    minutes = parse_number(minutes_field)
    if count is None:
        return False, (
            f"Could not parse number from '{count_name}': '{count_field}'"
        ), True
    if minutes is None or minutes <= 0:
        return False, (
            f"Could not parse positive minutes from '{minutes_name}': "
            f"'{minutes_field}'"
        ), True

    hours_required = math.ceil(minutes / 60)
    required_minimum = 2 * hours_required
    passed = count >= required_minimum
    verdict = "PASS" if passed else "FAIL"
    msg = (
        f"{verdict} - {int(count)} sentences for {int(minutes)} min "
        f"(required {required_minimum} = 2 x ceil({int(minutes)}/60))"
    )
    return passed, msg, False


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


def _coerce_decimals(value):
    """Convert `Decimal` values to `int` or `float`, recursing into
    dicts and lists.

    Rule configs come out of DynamoDB, and boto3 deserializes every
    number as `decimal.Decimal`. Operators like
    `op_datetime_not_before_minus_minutes` then hit
    `timedelta(minutes=Decimal(5))`, which raises. Coercing once at
    the condition boundary means every operator gets a normal Python
    number, no per-operator guard needed.

    Integers come back as `int`; anything with a fractional part
    comes back as `float`.
    """
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: _coerce_decimals(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_decimals(v) for v in value]
    return value


def _try_fromisoformat(value):
    """Try `datetime.fromisoformat` and return None on failure.

    Kept as a helper so both `parse_datetime` and `parse_date` can
    try the ISO path first without duplicating the try/except. Python
    3.11+ handles the shapes centralreach's list endpoint returns:
    `T` separator, arbitrary fractional-second digits, and an
    optional trailing `Z`.

    Result is always tz-naive. `strptime` on the fallback path also
    produces naive datetimes, and mixing aware + naive within one
    operator (e.g. `signed_at` from the preview endpoint carries a
    `Z`, `billing_list_date_time_to` from the list endpoint does
    not) raises "can't compare offset-naive and offset-aware
    datetimes" at compare time. Since CR's timestamps are already
    in a consistent reference frame per session, stripping tzinfo
    here is safe and keeps every operator's compare uniform.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


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
    Tries ISO-8601 first (handles CR's `T`-separated form with arbitrary
    fractional-second precision), then the strptime format list.

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

    # ISO-8601 first — Python 3.11+ `fromisoformat` accepts the shapes
    # centralreach produces (e.g. `2026-07-01T03:00:07.1100000`, with
    # up to 7-digit fractional seconds and optional Z suffix). Covers
    # the JSON-record path without a per-vendor format entry.
    iso_dt = _try_fromisoformat(value)
    if iso_dt is not None:
        return iso_dt

    # Try datetime formats
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
    Tries ISO-8601 first (which handles `T`-separated datetimes by
    dropping the time component), then the strptime date-format list.

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

    # ISO-8601 first. For a datetime like `2026-07-01T03:00:07`, the
    # date operators only compare the .date() part downstream, so
    # returning the full datetime here is fine — matches the existing
    # behavior for `2024-01-15 14:30:00` on the strptime path below.
    iso_dt = _try_fromisoformat(value)
    if iso_dt is not None:
        return iso_dt

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


def op_datetime_not_before_minus_minutes(field_dt, compare_dt, value):
    """
    Field datetime is at or after `compare_dt - value minutes`.

    Used by the "signed no earlier than N minutes before billed end" rule:
      field=signed_at, compare_to=billed_end, value=5
    Passes when the field is within `value` minutes before compare_dt, OR
    at/after compare_dt. Fails when the field is more than `value` minutes
    before compare_dt.
    """
    if value is None or compare_dt is None:
        return False
    from datetime import timedelta
    threshold = compare_dt - timedelta(minutes=value)
    return field_dt >= threshold


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
    'datetime_not_before_minus_minutes': op_datetime_not_before_minus_minutes,
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


# ----- compare_expr: config-driven derived comparison values ---------------


def _parse_iso8601(value):
    """Parse a value as an ISO-8601 datetime.

    Accepts the arbitrary-fractional-second shape CR sends (e.g.
    `"2026-06-28T17:00:00.0000000"`). `datetime.fromisoformat`
    supports this on Python 3.11+ — the floor for the rules-engine
    Lambda and every ingest runner.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _expr_duration_minutes(expr, fields):
    """`compare_expr` op: whole-minute floor of `to - from`.

    Config shape:
        {"op": "duration_minutes", "from": "<field>", "to": "<field>"}

    Both `from` and `to` are field names looked up in `fields`
    (fallback lists supported, same as `field` / `compare_to`).

    Returns `(value, label, error)`:
      * `value` is an int (or None on failure)
      * `label` is a human-readable synthetic name for the error /
        result message, e.g. `"duration_minutes(billed_start, billed_end)"`
      * `error` is None on success, else a message the caller wraps
        into a SKIP outcome
    """
    from_spec = expr.get("from")
    to_spec = expr.get("to")
    if not from_spec or not to_spec:
        return None, "duration_minutes(?, ?)", (
            "compare_expr 'duration_minutes' requires 'from' and 'to'"
        )

    from_value, from_name = get_field_value(fields, from_spec)
    to_value, to_name = get_field_value(fields, to_spec)
    label = f"duration_minutes({from_name}, {to_name})"

    if from_value is None:
        return None, label, f"Comparison field '{from_name}' not found"
    if to_value is None:
        return None, label, f"Comparison field '{to_name}' not found"

    from_dt = _parse_iso8601(from_value)
    to_dt = _parse_iso8601(to_value)
    if from_dt is None:
        return None, label, (
            f"Could not parse datetime from field '{from_name}': '{from_value}'"
        )
    if to_dt is None:
        return None, label, (
            f"Could not parse datetime from field '{to_name}': '{to_value}'"
        )

    minutes = int((to_dt - from_dt).total_seconds() // 60)
    return minutes, label, None


COMPARE_EXPR_OPS = {
    "duration_minutes": _expr_duration_minutes,
}


def _resolve_compare_expr(expr, fields):
    """Dispatch a `compare_expr` dict against `COMPARE_EXPR_OPS`.

    Returns the same `(value, label, error)` triple the individual
    op functions do. Unknown `op` values fall through to an error so
    a typo in DynamoDB doesn't silently produce a passing rule.
    """
    op = expr.get("op") if isinstance(expr, dict) else None
    if not op:
        return None, "compare_expr(?)", "compare_expr missing 'op'"
    handler = COMPARE_EXPR_OPS.get(op)
    if handler is None:
        return None, f"compare_expr({op})", f"Unknown compare_expr op: '{op}'"
    return handler(expr, fields)


def evaluate_condition(condition, fields):
    """
    Evaluate a single condition against the extracted fields.

    Args:
        condition: Condition dict with field, operator, compare_to,
            compare_expr, value, description
            - field: Field name or list of field names (fallback order)
            - compare_to: Field name or list of field names (fallback order)
            - compare_expr: Inline expression producing the comparison
              value from other fields — see `COMPARE_EXPR_OPS`. Mutually
              exclusive with `compare_to`. The source field names for
              the expression are part of the condition config so an org
              can point at its own vendor fields without a code change.
            - description: Human-readable description of what this condition checks
        fields: Dict of extracted field values

    Returns:
        tuple: (passed: bool, message: str, skip: bool)
    """
    field_spec = condition.get('field')
    operator = condition.get('operator')
    compare_to = condition.get('compare_to')
    compare_expr = condition.get('compare_expr')
    # DynamoDB returns numbers as `Decimal`; operators like
    # `timedelta(minutes=value)` reject that type. Coerce once here so
    # every operator downstream sees plain ints/floats.
    value = _coerce_decimals(condition.get('value'))
    description = condition.get('description')

    # Cross-document operator: reads org_id + source_record_id from fields,
    # talks to DynamoDB. Doesn't fit the standard (field, compare, value) shape.
    if operator == 'narrative_hash_unique':
        return evaluate_narrative_hash_unique(fields)

    # LLM-extracted sentence_count + minutes field; the operator does the
    # ceil()-based hourly-minimum math. Split out because a single upstream
    # LLM call reliably returns the scalar count, then Python owns the
    # verdict — the LLM used to flip its own PASS/FAIL mid-reasoning here.
    if operator == 'sentence_count_meets_hourly_minimum':
        return evaluate_sentence_count_meets_hourly_minimum(condition, fields)

    # Get field value (supports fallback list)
    field_value, field_name = get_field_value(fields, field_spec)
    if field_value is None:
        return False, f"Required field '{field_name}' not found", True

    # Resolve the comparison value. Three shapes, in priority order:
    #   1. compare_expr — inline expression over other fields
    #   2. compare_to   — one field (or fallback list) looked up in fields
    #   3. neither      — the operator uses `value` directly
    compare_value = None
    compare_field_name = None
    if compare_expr is not None:
        if compare_to is not None:
            return False, (
                "Condition may set 'compare_to' OR 'compare_expr', not both"
            ), True
        compare_value, compare_field_name, expr_err = _resolve_compare_expr(
            compare_expr, fields,
        )
        if expr_err is not None:
            return False, expr_err, True
        # Downstream operator blocks branch on `compare_to` truthiness; keep
        # them going the "compared against a field" path.
        compare_to = compare_field_name
    elif compare_to:
        compare_value, compare_field_name = get_field_value(fields, compare_to)
        if compare_value is None:
            return False, f"Comparison field '{compare_field_name}' not found", True

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

        print(f"Extracted {len(columns)} columns from CSV")
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
    Evaluate a deterministic rule against extracted field values.

    Two input shapes are supported:

    1. CSV mode (legacy SFTP path): when `data['text']` looks like CSV, the
       evaluator parses it on the fly and uses CSV columns as the lookup
       dict. `fields` is ignored. `row_match` conditions are supported by
       searching all parsed rows.

    2. Pre-extracted fields mode (RPA JSON path, PDFs, anything else): when
       `data['text']` is not CSV — or when `data` is missing — the evaluator
       uses the `fields` dict directly. `row_match` is not available here
       because there is no multi-row source. `fields` for an RPA record is
       built by `field_extractor.extract_fields_from_json_record`.

    Args:
        rule_config: Rule configuration dict with conditions and logic
        fields: Dict of pre-extracted field values (used in JSON/text mode)
        data: Document data — either a CSV-bearing `{'text': ...}` or a
              parsed JSON record

    Returns:
        tuple: (status: str, message: str)
            status is one of: 'PASS', 'FAIL', 'SKIP', 'ERROR'
    """
    conditions = rule_config.get('conditions', [])
    logic = rule_config.get('logic', 'all')  # 'all', 'any', or 'conditional'

    if not conditions and logic != 'conditional':
        return 'SKIP', 'No conditions defined for rule'

    # JSON records (RPA / centralreach) always carry `extracted_fields`.
    # Never treat their `text` as CSV — the narrative is prose that
    # frequently contains commas, and the comma-in-first-line heuristic
    # would misfire and SKIP every rule with "No columns extracted".
    is_json_record = isinstance((data or {}).get('extracted_fields'), dict)
    csv_content = (data or {}).get('text', '')
    first_line = csv_content.split('\n')[0] if csv_content else ''
    looks_like_csv = (
        not is_json_record and bool(csv_content) and ',' in first_line
    )

    if looks_like_csv:
        csv_columns = extract_csv_columns(csv_content)
        all_rows = extract_all_csv_rows(csv_content)
        if not csv_columns:
            return 'SKIP', 'No columns extracted from CSV'
    else:
        if not fields:
            return 'SKIP', 'No extracted fields available for evaluation'
        csv_columns = fields
        all_rows = []

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
