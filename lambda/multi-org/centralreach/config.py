"""Load per-org CENTRALREACH_CONFIG from `penguin-health-org-config`.

The runner reads `CENTRALREACH_CONFIG` at task startup to discover the
org's timezone, allowed-hours window, blackout dates, and per-request
rate limit. Configuration lives in DynamoDB alongside the org's other
integration settings (FHIR_CONFIG, STEDI_CONFIG, RULES_CONFIG).

Item shape:
    pk = ORG#{org_id}
    sk = CENTRALREACH_CONFIG
    organization_id : str
    enabled         : bool — set false to pause an org without deleting
    display_name    : str — human-friendly name shown in the runs UI
    base_url        : str — usually https://members.centralreach.com
    bot_username    : str — informational; CR identifies the bot by
                            credentials, not username
    guardrails      :
        timezone                       : IANA tz string
        allowed_hours                  : {start: "HH:MM", end: "HH:MM"}
        rate_limit_ms_between_requests : int
        blackout_dates                 : [YYYY-MM-DD, ...]
    created_at      : ISO timestamp
    updated_at      : ISO timestamp
"""

from __future__ import annotations

from functools import lru_cache

import boto3

from .exceptions import CentralReachError


_TABLE_NAME = "penguin-health-org-config"


class CentralReachOrgNotConfigured(CentralReachError):
    """No CENTRALREACH_CONFIG row for this org, or the row exists with
    `enabled: false`."""


_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


@lru_cache(maxsize=100)
def load_centralreach_config(org_id: str) -> dict:
    """Return the CENTRALREACH_CONFIG item for `org_id`.

    Raises `CentralReachOrgNotConfigured` if missing or disabled.
    """
    response = _table.get_item(
        Key={"pk": f"ORG#{org_id}", "sk": "CENTRALREACH_CONFIG"},
    )
    item = response.get("Item")
    if not item:
        raise CentralReachOrgNotConfigured(
            f"no CENTRALREACH_CONFIG for org={org_id}",
        )
    if not item.get("enabled", False):
        raise CentralReachOrgNotConfigured(
            f"centralreach disabled for org={org_id}",
        )
    return item


def invalidate_cache() -> None:
    """Drop the lru_cache. Tests use this to seed fresh config between
    runs without restarting the interpreter."""
    load_centralreach_config.cache_clear()
