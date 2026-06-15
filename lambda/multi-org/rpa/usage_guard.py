"""Allowed-hours window + blackout date enforcement.

Pure tz/date math. No AWS calls. Called by the Fargate runner before
launching Playwright and again between top-level playbook steps so a
schedule that drifts past the allowed window halts cleanly mid-run.
"""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from .exceptions import RpaOutsideWindow


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def check_or_raise(cfg: dict, now: datetime) -> None:
    """Raise RpaOutsideWindow if `now` is outside the allowed window or
    falls on a blackout date in the org's local timezone.

    cfg["guardrails"] must contain:
        timezone       : IANA tz string, e.g. "America/Chicago"
        allowed_hours  : { start: "HH:MM", end: "HH:MM" } (local time)
        blackout_dates : [ "YYYY-MM-DD", ... ]            (local dates, optional)

    `now` should be timezone-aware. If naive, it's assumed UTC.
    """
    g = cfg["guardrails"]
    tz = ZoneInfo(g["timezone"])
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("UTC"))
    local = now.astimezone(tz)

    blackouts = set(g.get("blackout_dates") or [])
    if local.date().isoformat() in blackouts:
        raise RpaOutsideWindow(
            f"blackout_date: {local.date().isoformat()} (tz={g['timezone']})"
        )

    start = _parse_hhmm(g["allowed_hours"]["start"])
    end = _parse_hhmm(g["allowed_hours"]["end"])
    t = local.time()
    if start <= end:
        # Same-day window, e.g. 06:00 to 20:00.
        in_window = start <= t <= end
    else:
        # Overnight window, e.g. 22:00 to 06:00.
        in_window = t >= start or t <= end

    if not in_window:
        raise RpaOutsideWindow(
            "outside_allowed_hours: "
            f"now={t.isoformat(timespec='minutes')} "
            f"window={g['allowed_hours']['start']}-{g['allowed_hours']['end']} "
            f"tz={g['timezone']}"
        )
