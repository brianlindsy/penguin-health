"""Resolve the date range for one centralreach ingest run.

The Fargate runner needs two values to drive the list query:
`start_date` and `end_date`, both ISO `YYYY-MM-DD`. Default behavior
is a 14-day rolling window in Eastern time, ending yesterday
(inclusive). The ingest-cursor dedupe keeps this affordable —
entries already ingested on a prior run are skipped before any CR
API or Bedrock work. An operator running a manual backfill overrides
via `CENTRALREACH_START_DATE` / `CENTRALREACH_END_DATE`.

Why "Eastern": CR's billing entries are date-stamped in the org's
clinical day. A scheduled bot running just after midnight UTC needs
to query the prior US clinical day, not the prior UTC day. Eastern
is the default for v1 because the orgs onboarded so far are East
Coast — add `_central`/`_pacific` variants when a non-Eastern org
lands. Don't generalize early.

Why 14 days: catches late-arriving signatures / edits within two
weeks without paying the cost of re-fetching + re-Bedrocking entries
a prior run already ingested (the ingest-cursor dedupe handles that).

This module replaces the per-playbook `runtime_parameters` mechanism
from `rpa/parameters.py`. With no playbook JSON in the centralreach
path, the resolution is a direct function call from the runner —
no DynamoDB lookup, no template expansion, no engine context.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable
from zoneinfo import ZoneInfo


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_START_DATE_ENV = "CENTRALREACH_START_DATE"
_END_DATE_ENV = "CENTRALREACH_END_DATE"

# 14-day rolling window (inclusive) ending yesterday. If today is
# 2026-06-30 in Eastern, the default range is 2026-06-16..2026-06-29.
_DEFAULT_LOOKBACK_DAYS = 14


@dataclass(frozen=True)
class DateRange:
    """The resolved start + end dates for one ingest run.

    Both values are ISO `YYYY-MM-DD` strings. The runner passes them
    into the list query directly; the API call wrapper formats the
    additional UI-display fields (dateRange, startDateDisplay,
    endDateDisplay) the validator requires.
    """

    start_date: str
    end_date: str


def _today_eastern(now: Callable[[], datetime] | None = None) -> date:
    """Return today's date in America/New_York.

    `now` is injectable so tests can pin the clock without monkey-
    patching `datetime`. Default uses a real wall-clock call.
    """
    tz = ZoneInfo("America/New_York")
    return (now() if now else datetime.now(tz)).astimezone(tz).date()


def _default_start_eastern(now: Callable[[], datetime] | None = None) -> str:
    """Yesterday minus `_DEFAULT_LOOKBACK_DAYS - 1` days, so
    (start..end) inclusive spans exactly `_DEFAULT_LOOKBACK_DAYS`
    days ending yesterday-Eastern."""
    today = _today_eastern(now)
    return (today - timedelta(days=_DEFAULT_LOOKBACK_DAYS)).isoformat()


def _default_end_eastern(now: Callable[[], datetime] | None = None) -> str:
    """Yesterday in Eastern — same as before."""
    today = _today_eastern(now)
    return (today - timedelta(days=1)).isoformat()


def _validate_iso_date(env_var: str, value: str) -> str:
    """Reject anything that isn't a parseable ISO YYYY-MM-DD date.

    Re-raises as ValueError carrying the env var name so the operator
    sees which override was malformed.
    """
    if not _ISO_DATE.match(value):
        raise ValueError(
            f"{env_var}={value!r} is not ISO YYYY-MM-DD; "
            "operator override must be a valid ISO date"
        )
    try:
        date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(
            f"{env_var}={value!r} is not a valid date: {e}"
        ) from None
    return value


def resolve_date_range(
    *,
    env: dict | None = None,
    now: Callable[[], datetime] | None = None,
) -> DateRange:
    """Resolve the date range for this ingest run.

    Resolution order, applied independently to start and end:
      1. If the env override (`CENTRALREACH_START_DATE` or
         `CENTRALREACH_END_DATE`) is set to a valid ISO date, use it.
      2. Otherwise fall back to the default Eastern-wall-clock window:
         start = today - `_DEFAULT_LOOKBACK_DAYS` days,
         end   = yesterday.

    Either or both env vars can be set independently. Setting only
    `CENTRALREACH_START_DATE` gives a backfill from that date through
    yesterday; setting only `CENTRALREACH_END_DATE` shifts the window
    end while keeping the same 14-day span backward from that date.

    Raises ValueError on a malformed env override.
    """
    env = env if env is not None else os.environ

    start_raw = env.get(_START_DATE_ENV)
    end_raw = env.get(_END_DATE_ENV)

    start = (
        _validate_iso_date(_START_DATE_ENV, start_raw)
        if start_raw
        else _default_start_eastern(now=now)
    )
    end = (
        _validate_iso_date(_END_DATE_ENV, end_raw)
        if end_raw
        else _default_end_eastern(now=now)
    )

    return DateRange(start_date=start, end_date=end)
