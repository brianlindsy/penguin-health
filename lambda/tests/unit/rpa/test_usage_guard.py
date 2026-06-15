"""Tests for rpa.usage_guard — allowed-hours window + blackout dates.

Pure tz math; no AWS, no fixtures beyond a built config dict.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from rpa.exceptions import RpaOutsideWindow
from rpa.usage_guard import check_or_raise


def _cfg(start="06:00", end="20:00", tz="America/Chicago", blackouts=None):
    return {"guardrails": {
        "timezone": tz,
        "allowed_hours": {"start": start, "end": end},
        "blackout_dates": blackouts or [],
    }}


def test_inside_window_passes():
    cfg = _cfg()
    # 13:00 CDT on a normal weekday
    now = datetime(2026, 6, 10, 18, 0, tzinfo=ZoneInfo("UTC"))  # 13:00 CDT
    check_or_raise(cfg, now)


def test_just_before_start_raises():
    cfg = _cfg(start="06:00", end="20:00")
    # 05:59 CDT
    now = datetime(2026, 6, 10, 10, 59, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(RpaOutsideWindow, match="outside_allowed_hours"):
        check_or_raise(cfg, now)


def test_just_after_end_raises():
    cfg = _cfg(start="06:00", end="20:00")
    # 20:01 CDT
    now = datetime(2026, 6, 11, 1, 1, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(RpaOutsideWindow, match="outside_allowed_hours"):
        check_or_raise(cfg, now)


def test_exactly_at_start_and_end_passes():
    cfg = _cfg(start="06:00", end="20:00")
    # Exactly 06:00 CDT
    check_or_raise(cfg, datetime(2026, 6, 10, 11, 0, tzinfo=ZoneInfo("UTC")))
    # Exactly 20:00 CDT
    check_or_raise(cfg, datetime(2026, 6, 11, 1, 0, tzinfo=ZoneInfo("UTC")))


def test_overnight_window_passes_in_first_half():
    # Allowed 22:00 to 06:00 local.
    cfg = _cfg(start="22:00", end="06:00")
    # 23:30 CDT
    now = datetime(2026, 6, 11, 4, 30, tzinfo=ZoneInfo("UTC"))
    check_or_raise(cfg, now)


def test_overnight_window_passes_in_second_half():
    cfg = _cfg(start="22:00", end="06:00")
    # 02:00 CDT
    now = datetime(2026, 6, 11, 7, 0, tzinfo=ZoneInfo("UTC"))
    check_or_raise(cfg, now)


def test_overnight_window_rejects_midday():
    cfg = _cfg(start="22:00", end="06:00")
    # 13:00 CDT
    now = datetime(2026, 6, 11, 18, 0, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(RpaOutsideWindow, match="outside_allowed_hours"):
        check_or_raise(cfg, now)


def test_blackout_date_raises_even_during_window():
    cfg = _cfg(blackouts=["2026-12-25"])
    # 13:00 CDT on Christmas
    now = datetime(2026, 12, 25, 19, 0, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(RpaOutsideWindow, match="blackout_date"):
        check_or_raise(cfg, now)


def test_blackout_date_uses_local_calendar_not_utc():
    # 23:30 UTC on 2026-12-26 == 17:30 CST on 2026-12-26 (NOT Christmas).
    # But 01:00 UTC on 2026-12-26 == 19:00 CST on 2026-12-25 → blackout.
    cfg = _cfg(blackouts=["2026-12-25"])
    utc_midnight_after = datetime(2026, 12, 26, 1, 0, tzinfo=ZoneInfo("UTC"))
    with pytest.raises(RpaOutsideWindow, match="blackout_date"):
        check_or_raise(cfg, utc_midnight_after)


def test_naive_datetime_treated_as_utc():
    cfg = _cfg()
    # 18:00 UTC = 13:00 CDT — inside window.
    naive = datetime(2026, 6, 10, 18, 0)
    check_or_raise(cfg, naive)


def test_dst_transition_window_respects_local_clock():
    # On 2026-03-08, US/Central springs forward at 02:00 → 03:00.
    # 14:00 UTC = 09:00 CST (before spring forward day's start time) OR
    # 14:00 UTC = 09:00 CDT after the transition. Either way 09:00 local is
    # inside the 06:00–20:00 window.
    cfg = _cfg()
    now = datetime(2026, 3, 8, 14, 0, tzinfo=ZoneInfo("UTC"))
    check_or_raise(cfg, now)


def test_different_timezones_evaluated_independently():
    # Same UTC instant; one tz inside, one outside.
    utc_instant = datetime(2026, 6, 10, 4, 0, tzinfo=ZoneInfo("UTC"))
    # In Chicago this is 23:00 prev day — outside 06:00–20:00.
    with pytest.raises(RpaOutsideWindow):
        check_or_raise(_cfg(tz="America/Chicago"), utc_instant)
    # In Tokyo this is 13:00 same day — inside 06:00–20:00.
    check_or_raise(_cfg(tz="Asia/Tokyo"), utc_instant)
