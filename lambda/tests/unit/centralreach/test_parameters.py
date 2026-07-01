"""Tests for centralreach.parameters — date-range resolution.

The runner ingests one date range per run. v1 default is "yesterday
in Eastern" for both ends (single-day cron); operator overrides via
env vars enable backfills. Three contracts pinned here:

  1. yesterday_eastern uses America/New_York wall clock, not UTC
  2. Env overrides take precedence over the default
  3. Malformed env values fail loud, not silent
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from centralreach.parameters import DateRange, resolve_date_range


_NY = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")


def _at(year, month, day, hour, minute, tz=_NY):
    def _now():
        return datetime(year, month, day, hour, minute, tzinfo=tz)
    return _now


def test_default_is_yesterday_eastern():
    out = resolve_date_range(env={}, now=_at(2026, 6, 30, 9, 0))
    assert out == DateRange(start_date="2026-06-29", end_date="2026-06-29")


def test_default_uses_eastern_not_utc_at_midnight_utc():
    """At 00:30 UTC on 2026-07-01 it's still 2026-06-30 in NY (EDT,
    UTC-4). The cron typically fires around that moment, so yesterday
    must be 2026-06-29."""
    utc_after_midnight = datetime(2026, 7, 1, 0, 30, tzinfo=_UTC)
    out = resolve_date_range(env={}, now=lambda: utc_after_midnight)
    assert out == DateRange(start_date="2026-06-29", end_date="2026-06-29")


def test_default_handles_dst_spring_forward():
    """2026-03-09 09:00 EDT — yesterday is 2026-03-08, the day DST
    skipped 02:00-03:00. Eastern wall clock still rolls cleanly."""
    out = resolve_date_range(env={}, now=_at(2026, 3, 9, 9, 0))
    assert out.start_date == "2026-03-08"


def test_env_override_both_dates_for_backfill():
    out = resolve_date_range(
        env={"CENTRALREACH_START_DATE": "2026-06-01",
             "CENTRALREACH_END_DATE": "2026-06-15"},
        now=_at(2026, 6, 30, 9, 0),
    )
    assert out == DateRange(start_date="2026-06-01", end_date="2026-06-15")


def test_env_override_only_start_keeps_default_end():
    out = resolve_date_range(
        env={"CENTRALREACH_START_DATE": "2026-06-01"},
        now=_at(2026, 6, 30, 9, 0),
    )
    assert out == DateRange(start_date="2026-06-01", end_date="2026-06-29")


def test_env_override_only_end_keeps_default_start():
    out = resolve_date_range(
        env={"CENTRALREACH_END_DATE": "2026-06-29"},
        now=_at(2026, 6, 30, 9, 0),
    )
    assert out == DateRange(start_date="2026-06-29", end_date="2026-06-29")


def test_malformed_env_date_raises():
    with pytest.raises(ValueError, match="ISO YYYY-MM-DD"):
        resolve_date_range(
            env={"CENTRALREACH_START_DATE": "06/01/2026"},
            now=_at(2026, 6, 30, 9, 0),
        )


def test_impossible_env_date_raises():
    """ISO-shape but not a real date."""
    with pytest.raises(ValueError, match="not a valid date"):
        resolve_date_range(
            env={"CENTRALREACH_START_DATE": "2026-13-01"},
            now=_at(2026, 6, 30, 9, 0),
        )


def test_empty_env_value_falls_through_to_default():
    """Operators clear an override by unsetting, not by emptying."""
    out = resolve_date_range(
        env={"CENTRALREACH_START_DATE": ""},
        now=_at(2026, 6, 30, 9, 0),
    )
    assert out.start_date == "2026-06-29"
