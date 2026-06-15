"""RPA integration: browser-based clinical-note ingestion via OAuth.

The bot has its own user account at each client's portal, completes consent
once, and runs scheduled scrape passes that extract notes and write flat
JSON records into the per-org bucket's `data/{date}/` prefix — where the
existing rules engine picks them up via the same path it uses for SFTP
charts. RPA is strictly read-only on the target portal (no `fill` op in
the playbook vocabulary).

Public surface:
    from rpa import load_rpa_config, load_playbook
    from rpa.record import RpaNoteRecord
    from rpa.usage_guard import check_or_raise
    from rpa.rate_limiter import RateLimiter

See lambda/multi-org/rpa/README.md for the operating manual.
"""

from .config import load_rpa_config, load_playbook, invalidate_cache
from .exceptions import (
    RpaError,
    RpaOrgNotConfigured,
    RpaPlaybookNotFound,
    RpaOutsideWindow,
    RpaAuthError,
    RpaUnsupportedVendor,
    RpaPlaybookError,
)

__all__ = [
    "load_rpa_config",
    "load_playbook",
    "invalidate_cache",
    "RpaError",
    "RpaOrgNotConfigured",
    "RpaPlaybookNotFound",
    "RpaOutsideWindow",
    "RpaAuthError",
    "RpaUnsupportedVendor",
    "RpaPlaybookError",
]
