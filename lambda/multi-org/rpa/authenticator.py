"""Vendor dispatch for the RPA bot's per-run authentication.

The Fargate runner calls `authenticate(org_id, org_cfg)` once at the
start of each run. This module looks up the right per-vendor
authenticator from the registry, loads credentials from Secrets Manager,
and returns an AuthSession dict the playbook engine consumes to set up
the Playwright BrowserContext.

Adding a new vendor:
  1. Write `lambda/multi-org/rpa/authenticators/<vendor>.py` exporting
     `AUTH_VENDOR` and `authenticate(...)`.
  2. Add the import + registry entry to `authenticators/__init__.py`.

No persistence between runs — JWT and cookie are in-memory only.
"""

from __future__ import annotations

from . import secrets
from .authenticators import REGISTRY
from .exceptions import RpaAuthError, RpaUnsupportedVendor


def authenticate(org_id: str, org_cfg: dict) -> dict:
    """Resolve the vendor strategy, load credentials, run the auth flow.

    org_cfg must contain:
        vendor                 : the registry key (e.g., "centralreach")
        vendor_settings.{vendor}: the per-vendor URL + scope subtree
    """
    vendor = org_cfg.get("vendor")
    if not vendor:
        raise RpaAuthError(f"RPA_CONFIG for org={org_id} has no vendor")

    fn = REGISTRY.get(vendor)
    if fn is None:
        raise RpaUnsupportedVendor(
            f"no authenticator registered for vendor={vendor!r}; "
            f"available: {sorted(REGISTRY)}"
        )

    vendor_cfg = (org_cfg.get("vendor_settings") or {}).get(vendor) or {}
    credentials = secrets.load_credentials(org_id)
    return fn(
        org_id=org_id,
        vendor_cfg=vendor_cfg,
        credentials=credentials,
    )
