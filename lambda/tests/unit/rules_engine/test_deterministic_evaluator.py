"""
Unit tests for deterministic_evaluator.py.

Tests the declarative rule evaluation system including:
- Date/time parsing
- All operator types (date, time, string, numeric)
- Condition evaluation
- Logic (all, any, conditional)
"""

import sys
import os
from datetime import datetime

# Add lambda directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'))


class TestDateParsing:
    """Test date parsing utilities."""

    def test_parse_date_iso_format(self):
        """Should parse ISO date format (YYYY-MM-DD)."""
        from deterministic_evaluator import parse_date

        result = parse_date('2024-01-15')

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_date_us_format(self):
        """Should parse US date format (MM/DD/YYYY)."""
        from deterministic_evaluator import parse_date

        result = parse_date('01/15/2024')

        assert result is not None
        assert result.month == 1
        assert result.day == 15
        assert result.year == 2024

    def test_parse_date_short_year(self):
        """Should parse short year format (MM/DD/YY)."""
        from deterministic_evaluator import parse_date

        result = parse_date('01/15/24')

        assert result is not None
        assert result.month == 1
        assert result.day == 15

    def test_parse_date_returns_none_for_invalid(self):
        """Should return None for invalid date strings."""
        from deterministic_evaluator import parse_date

        assert parse_date('not-a-date') is None
        assert parse_date('') is None
        assert parse_date(None) is None

    def test_parse_date_handles_existing_datetime(self):
        """Should handle datetime objects passed in."""
        from deterministic_evaluator import parse_date

        dt = datetime(2024, 1, 15)
        result = parse_date(dt)

        assert result == dt


class TestDatetimeParsing:
    """Test datetime parsing utilities."""

    def test_parse_datetime_with_time_am_pm(self):
        """Should parse datetime with AM/PM time."""
        from deterministic_evaluator import parse_datetime

        result = parse_datetime('01/15/2024 02:30:00 PM')

        assert result is not None
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_datetime_24_hour(self):
        """Should parse datetime with 24-hour time."""
        from deterministic_evaluator import parse_datetime

        result = parse_datetime('2024-01-15 14:30:00')

        assert result is not None
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_datetime_falls_back_to_date(self):
        """Should fall back to date-only parsing."""
        from deterministic_evaluator import parse_datetime

        result = parse_datetime('2024-01-15')

        assert result is not None
        assert result.year == 2024


class TestTimeParsing:
    """Test time parsing utilities."""

    def test_parse_time_am_pm(self):
        """Should parse time with AM/PM."""
        from deterministic_evaluator import parse_time

        result = parse_time('2:30 PM')

        assert result is not None
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_time_24_hour(self):
        """Should parse 24-hour time format."""
        from deterministic_evaluator import parse_time

        result = parse_time('14:30')

        assert result is not None
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_time_with_seconds(self):
        """Should parse time with seconds."""
        from deterministic_evaluator import parse_time

        result = parse_time('14:30:45')

        assert result is not None
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45

    def test_parse_time_returns_none_for_invalid(self):
        """Should return None for invalid time strings."""
        from deterministic_evaluator import parse_time

        assert parse_time('not-a-time') is None
        assert parse_time('') is None
        assert parse_time(None) is None


class TestNumberParsing:
    """Test number parsing utilities."""

    def test_parse_number_int(self):
        """Should parse integer strings."""
        from deterministic_evaluator import parse_number

        assert parse_number('42') == 42
        assert parse_number('0') == 0
        assert parse_number('-10') == -10

    def test_parse_number_float(self):
        """Should parse float strings."""
        from deterministic_evaluator import parse_number

        assert parse_number('3.14') == 3.14
        assert parse_number('0.5') == 0.5

    def test_parse_number_returns_none_for_invalid(self):
        """Should return None for invalid numbers."""
        from deterministic_evaluator import parse_number

        assert parse_number('abc') is None
        assert parse_number('') is None
        assert parse_number(None) is None

    def test_parse_number_handles_existing_numbers(self):
        """Should return existing numbers as-is."""
        from deterministic_evaluator import parse_number

        assert parse_number(42) == 42
        assert parse_number(3.14) == 3.14


class TestDateOperators:
    """Test date comparison operators."""

    def test_op_before(self):
        """before: field date is before compare date."""
        from deterministic_evaluator import op_before, parse_date

        date1 = parse_date('2024-01-10')
        date2 = parse_date('2024-01-15')

        assert op_before(date1, date2) is True
        assert op_before(date2, date1) is False
        assert op_before(date1, date1) is False

    def test_op_after(self):
        """after: field date is after compare date."""
        from deterministic_evaluator import op_after, parse_date

        date1 = parse_date('2024-01-10')
        date2 = parse_date('2024-01-15')

        assert op_after(date2, date1) is True
        assert op_after(date1, date2) is False

    def test_op_equals_date(self):
        """equals_date: dates are equal (date portion only)."""
        from deterministic_evaluator import op_equals_date, parse_date

        date1 = parse_date('2024-01-15')
        date2 = parse_date('2024-01-15')
        date3 = parse_date('2024-01-16')

        assert op_equals_date(date1, date2) is True
        assert op_equals_date(date1, date3) is False

    def test_op_within_days(self):
        """within_days: dates are within N days of each other."""
        from deterministic_evaluator import op_within_days, parse_date

        date1 = parse_date('2024-01-10')
        date2 = parse_date('2024-01-15')

        assert op_within_days(date1, date2, 10) is True
        assert op_within_days(date1, date2, 5) is True
        assert op_within_days(date1, date2, 3) is False

    def test_op_not_before(self):
        """not_before: field date is on or after compare date."""
        from deterministic_evaluator import op_not_before, parse_date

        date1 = parse_date('2024-01-15')
        date2 = parse_date('2024-01-15')
        date3 = parse_date('2024-01-10')

        assert op_not_before(date1, date2) is True  # Same date
        assert op_not_before(date1, date3) is True  # After
        assert op_not_before(date3, date1) is False  # Before

    def test_op_not_after(self):
        """not_after: field date is on or before compare date."""
        from deterministic_evaluator import op_not_after, parse_date

        date1 = parse_date('2024-01-15')
        date2 = parse_date('2024-01-15')
        date3 = parse_date('2024-01-20')

        assert op_not_after(date1, date2) is True  # Same date
        assert op_not_after(date1, date3) is True  # Before
        assert op_not_after(date3, date1) is False  # After


class TestDurationOperators:
    """Test duration (datetime difference) operators."""

    def test_op_duration_lte(self):
        """duration_lte: duration is less than or equal to N minutes."""
        from deterministic_evaluator import op_duration_lte, parse_datetime

        start = parse_datetime('01/15/2024 09:00 AM')
        end = parse_datetime('01/15/2024 09:45 AM')

        assert op_duration_lte(start, end, 60) is True   # 45 min <= 60
        assert op_duration_lte(start, end, 45) is True   # 45 min <= 45
        assert op_duration_lte(start, end, 30) is False  # 45 min > 30

    def test_op_duration_gte(self):
        """duration_gte: duration is greater than or equal to N minutes."""
        from deterministic_evaluator import op_duration_gte, parse_datetime

        start = parse_datetime('01/15/2024 09:00 AM')
        end = parse_datetime('01/15/2024 09:45 AM')

        assert op_duration_gte(start, end, 30) is True   # 45 min >= 30
        assert op_duration_gte(start, end, 45) is True   # 45 min >= 45
        assert op_duration_gte(start, end, 60) is False  # 45 min < 60

    def test_op_duration_between(self):
        """duration_between: duration is between min and max minutes."""
        from deterministic_evaluator import op_duration_between, parse_datetime

        start = parse_datetime('01/15/2024 09:00 AM')
        end = parse_datetime('01/15/2024 09:45 AM')

        assert op_duration_between(start, end, {'min': 30, 'max': 60}) is True
        assert op_duration_between(start, end, {'min': 50, 'max': 60}) is False


class TestStringOperators:
    """Test string comparison operators."""

    def test_op_equals(self):
        """equals: exact string match (case-sensitive)."""
        from deterministic_evaluator import op_equals

        assert op_equals('hello', None, 'hello') is True
        assert op_equals('Hello', None, 'hello') is False

    def test_op_equals_ignore_case(self):
        """equals_ignore_case: exact match (case-insensitive)."""
        from deterministic_evaluator import op_equals_ignore_case

        assert op_equals_ignore_case('Hello', None, 'hello') is True
        assert op_equals_ignore_case('HELLO', None, 'hello') is True

    def test_op_contains(self):
        """contains: field contains substring."""
        from deterministic_evaluator import op_contains

        assert op_contains('hello world', None, 'world') is True
        assert op_contains('hello world', None, 'foo') is False

    def test_op_starts_with(self):
        """starts_with: field starts with prefix."""
        from deterministic_evaluator import op_starts_with

        assert op_starts_with('hello world', None, 'hello') is True
        assert op_starts_with('hello world', None, 'world') is False

    def test_op_ends_with(self):
        """ends_with: field ends with suffix."""
        from deterministic_evaluator import op_ends_with

        assert op_ends_with('hello world', None, 'world') is True
        assert op_ends_with('hello world', None, 'hello') is False

    def test_op_in(self):
        """in: field value is in list."""
        from deterministic_evaluator import op_in

        assert op_in('apple', None, ['apple', 'banana', 'orange']) is True
        assert op_in('grape', None, ['apple', 'banana', 'orange']) is False

    def test_op_starts_with_any(self):
        """starts_with_any: field starts with any item in list."""
        from deterministic_evaluator import op_starts_with_any

        assert op_starts_with_any('hello world', None, ['hi', 'hello', 'hey']) is True
        assert op_starts_with_any('goodbye', None, ['hi', 'hello', 'hey']) is False

    def test_op_not_equals(self):
        """not_equals: field does not equal value."""
        from deterministic_evaluator import op_not_equals

        assert op_not_equals('hello', None, 'world') is True
        assert op_not_equals('hello', None, 'hello') is False

    def test_op_matches_regex(self):
        """matches_regex: field matches regex pattern."""
        from deterministic_evaluator import op_matches_regex

        assert op_matches_regex('abc123', None, r'\w+\d+') is True
        assert op_matches_regex('abc', None, r'\d+') is False


class TestNumericOperators:
    """Test numeric comparison operators."""

    def test_op_eq(self):
        """eq: numeric equals."""
        from deterministic_evaluator import op_eq

        assert op_eq(10, None, 10) is True
        assert op_eq(10, None, 20) is False

    def test_op_ne(self):
        """ne: numeric not equals."""
        from deterministic_evaluator import op_ne

        assert op_ne(10, None, 20) is True
        assert op_ne(10, None, 10) is False

    def test_op_gt(self):
        """gt: greater than."""
        from deterministic_evaluator import op_gt

        assert op_gt(10, None, 5) is True
        assert op_gt(10, None, 10) is False
        assert op_gt(10, None, 15) is False

    def test_op_gte(self):
        """gte: greater than or equal."""
        from deterministic_evaluator import op_gte

        assert op_gte(10, None, 5) is True
        assert op_gte(10, None, 10) is True
        assert op_gte(10, None, 15) is False

    def test_op_lt(self):
        """lt: less than."""
        from deterministic_evaluator import op_lt

        assert op_lt(5, None, 10) is True
        assert op_lt(10, None, 10) is False
        assert op_lt(15, None, 10) is False

    def test_op_lte(self):
        """lte: less than or equal."""
        from deterministic_evaluator import op_lte

        assert op_lte(5, None, 10) is True
        assert op_lte(10, None, 10) is True
        assert op_lte(15, None, 10) is False

    def test_op_between(self):
        """between: value is between min and max (inclusive)."""
        from deterministic_evaluator import op_between

        assert op_between(7, None, {'min': 5, 'max': 10}) is True
        assert op_between(5, None, {'min': 5, 'max': 10}) is True
        assert op_between(10, None, {'min': 5, 'max': 10}) is True
        assert op_between(15, None, {'min': 5, 'max': 10}) is False
        assert op_between(3, None, {'min': 5, 'max': 10}) is False


class TestGetFieldValue:
    """Test field value retrieval with fallback support."""

    def test_get_field_value_simple(self):
        """Should get value for simple field name."""
        from deterministic_evaluator import get_field_value

        fields = {'name': 'John', 'age': '30'}
        value, field_name = get_field_value(fields, 'name')

        assert value == 'John'
        assert field_name == 'name'

    def test_get_field_value_missing(self):
        """Should return None for missing field."""
        from deterministic_evaluator import get_field_value

        fields = {'name': 'John'}
        value, field_name = get_field_value(fields, 'missing')

        assert value is None
        assert field_name == 'missing'

    def test_get_field_value_fallback_list(self):
        """Should try fields in order and return first non-empty value."""
        from deterministic_evaluator import get_field_value

        fields = {'secondary': 'value2'}
        value, field_name = get_field_value(fields, ['primary', 'secondary'])

        assert value == 'value2'
        assert field_name == 'secondary'

    def test_get_field_value_fallback_first_available(self):
        """Should use first available in fallback list."""
        from deterministic_evaluator import get_field_value

        fields = {'primary': 'value1', 'secondary': 'value2'}
        value, field_name = get_field_value(fields, ['primary', 'secondary'])

        assert value == 'value1'
        assert field_name == 'primary'


class TestEvaluateCondition:
    """Test single condition evaluation."""

    def test_evaluate_condition_string_equals(self):
        """Should evaluate string equals condition."""
        from deterministic_evaluator import evaluate_condition

        fields = {'status': 'approved', 'program': 'Mental Health'}
        condition = {
            'field': 'status',
            'operator': 'equals',
            'value': 'approved',
        }

        passed, message, skip = evaluate_condition(condition, fields)

        assert passed is True
        assert skip is False

    def test_evaluate_condition_missing_field_returns_skip(self):
        """Should return skip=True when required field is missing."""
        from deterministic_evaluator import evaluate_condition

        fields = {'status': 'approved'}
        condition = {
            'field': 'missing_field',
            'operator': 'equals',
            'value': 'test',
        }

        passed, message, skip = evaluate_condition(condition, fields)

        assert skip is True
        assert 'not found' in message

    def test_evaluate_condition_with_compare_to_field(self):
        """Should compare two fields when compare_to is specified."""
        from deterministic_evaluator import evaluate_condition

        fields = {'start_date': '2024-01-10', 'end_date': '2024-01-15'}
        condition = {
            'field': 'start_date',
            'operator': 'before',
            'compare_to': 'end_date',
        }

        passed, message, skip = evaluate_condition(condition, fields)

        assert passed is True
        assert skip is False

    def test_evaluate_condition_with_fallback_fields(self):
        """Should use fallback when primary field is missing."""
        from deterministic_evaluator import evaluate_condition

        fields = {'alternate_status': 'approved'}
        condition = {
            'field': ['primary_status', 'alternate_status'],
            'operator': 'equals',
            'value': 'approved',
        }

        passed, message, skip = evaluate_condition(condition, fields)

        assert passed is True


class TestTimeOperators:
    """Test time-of-day operators."""

    def test_op_time_before(self):
        """time_before: field time is before specified time."""
        from deterministic_evaluator import op_time_before, parse_time

        field_time = parse_time('6:00 AM')

        assert op_time_before(field_time, None, '7:00 AM') is True
        assert op_time_before(field_time, None, '5:00 AM') is False

    def test_op_time_after(self):
        """time_after: field time is after specified time."""
        from deterministic_evaluator import op_time_after, parse_time

        field_time = parse_time('8:00 PM')

        assert op_time_after(field_time, None, '7:00 PM') is True
        assert op_time_after(field_time, None, '9:00 PM') is False

    def test_op_time_between(self):
        """time_between: field time is within range (inclusive)."""
        from deterministic_evaluator import op_time_between, parse_time

        field_time = parse_time('10:00 AM')

        assert op_time_between(field_time, None, {'start': '9:00 AM', 'end': '11:00 AM'}) is True
        assert op_time_between(field_time, None, {'start': '11:00 AM', 'end': '12:00 PM'}) is False

    def test_op_time_not_between(self):
        """time_not_between: field time is outside range."""
        from deterministic_evaluator import op_time_not_between, parse_time

        field_time = parse_time('8:00 AM')

        assert op_time_not_between(field_time, None, {'start': '9:00 AM', 'end': '5:00 PM'}) is True
        assert op_time_not_between(field_time, None, {'start': '7:00 AM', 'end': '9:00 AM'}) is False
