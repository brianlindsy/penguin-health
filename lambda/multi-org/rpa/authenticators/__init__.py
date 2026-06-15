"""Per-vendor authenticators.

Each vendor module exports:

    AUTH_VENDOR = "<vendor key matching RPA_CONFIG.vendor>"

    def authenticate(*, org_id, vendor_cfg, credentials,
                     http_post=..., http_get=...) -> dict:
        '''Return an AuthSession dict the playbook runner consumes:
            {
              "cookies": [{"name": ..., "value": ..., "domain": ...,
                           "path": ...}, ...],
              "extra_http_headers": {...},   # optional
              "access_token": "<for diagnostics; never logged>",
            }
        '''

The dispatch entry point in `rpa.authenticator` routes to the right
module based on `RPA_CONFIG.vendor`. Adding a new vendor means a new
module here plus one line in the dispatch table — existing vendors are
untouched.
"""

from . import centralreach

REGISTRY = {
    centralreach.AUTH_VENDOR: centralreach.authenticate,
}

__all__ = ["REGISTRY"]
