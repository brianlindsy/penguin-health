"""CentralReach authenticator.

Implements the "Steps to Authenticate and Call Internal APIs / Use RPA"
flow from CR's "Authenticating a Request to CentralReach's APIs" doc
(rev. 2024-07-31):

  Step 1 (Example A): POST client_id + client_secret to CR's SSO
    `/connect/token` with grant_type=client_credentials, scope=cr-api.
    Receive a JWT (access_token).

  Step 2 (Example B): POST {"token": "<jwt>"} to CR's legacy auth
    service at `/api/?framework.authtoken`. Receive the two session
    cookies the system needs: crsd and crud. Both are required —
    one without the other is not enough to authenticate to internal
    APIs (per the glossary, both must be present).

  Step 3 happens in the playbook engine — set the cookies on the
    Playwright BrowserContext and the bot is logged in.

CSRF tokens (Example D in the doc) are NOT handled here because we
drive a real browser via Playwright; CR's docs explicitly note that
"RPA automatically handles this fetching and supplying of the CSRF
tokens." Direct internal-API scripting (also out of scope for v1) is
where CSRF would matter; if a future playbook adds that path it
should fetch the CSRF token itself via `/api/?framework.csrf`.

The URLs are hardcoded to the production endpoints CR documented.
Sandbox vs prod is a per-org config override under
`vendor_settings.centralreach.base_overrides` for the rare case CR
provisions a non-prod tenant.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Callable

from ..exceptions import RpaAuthError


AUTH_VENDOR = "centralreach"

_HTTP_TIMEOUT_SECONDS = 30

# Hardcoded per CR's documented endpoints. A per-org override is
# permitted (see _resolve_endpoints) but is not the common path —
# operators should leave the override unset unless CR has explicitly
# provisioned a non-prod tenant for the org.
_DEFAULT_SSO_TOKEN_URL = "https://login.centralreach.com/connect/token"
_DEFAULT_LEGACY_AUTH_URL = (
    "https://members.centralreach.com/api/?framework.authtoken"
)
_DEFAULT_SCOPE = "cr-api"

# Both cookies are required for internal-API / RPA traffic per the doc's
# glossary. If either is missing the run cannot proceed.
_REQUIRED_COOKIES = frozenset({"crsd", "crud"})


HttpPoster = Callable[[str, dict, dict], dict]
"""(url, form, headers) -> json dict"""

CookieFetcher = Callable[[str, dict, dict], list]
"""(url, body, headers) -> list of {name, value, domain, path, secure} cookies"""


# ----- default HTTP transports (injectable from tests) -------------------


def _default_http_post(url: str, form: dict, headers: dict | None = None) -> dict:
    body = urllib.parse.urlencode(form).encode("utf-8")
    req_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raise RpaAuthError(f"CR SSO returned HTTP {e.code}")
    except urllib.error.URLError as e:
        raise RpaAuthError(f"CR SSO unreachable: {type(e).__name__}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise RpaAuthError("CR SSO returned non-json")


def _default_fetch_cookies(url: str, body: dict, headers: dict) -> list:
    """POST a JSON body to the legacy auth service and harvest Set-Cookie.

    The request body shape is `{"token": "<jwt>"}` per CR's Example B —
    NOT `{"access_token": ...}` and NOT a `Bearer` Authorization header.

    urllib's CookieJar parses multiple Set-Cookie headers and standard
    attributes (Domain, Path, Secure) so we don't re-implement RFC 6265.
    """
    payload = json.dumps(body).encode("utf-8")
    req_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=payload, method="POST",
                                 headers=req_headers)
    jar = CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(jar)
    )
    try:
        opener.open(req, timeout=_HTTP_TIMEOUT_SECONDS).read()
    except urllib.error.HTTPError as e:
        raise RpaAuthError(f"CR legacy-auth returned HTTP {e.code}")
    except urllib.error.URLError as e:
        raise RpaAuthError(f"CR legacy-auth unreachable: {type(e).__name__}")

    out = []
    for c in jar:
        out.append({
            "name": c.name,
            "value": c.value,
            "domain": c.domain,
            "path": c.path,
            "secure": bool(c.secure),
        })
    return out


# ----- endpoint resolution -----------------------------------------------


def _resolve_endpoints(vendor_cfg: dict) -> tuple[str, str, str]:
    """Return (sso_token_url, legacy_auth_url, scope).

    Defaults match CR's documented production URLs. `vendor_cfg` may
    override them, but in practice only `scope` is sometimes customized
    per-org. URL overrides are reserved for CR-provisioned sandbox
    tenants and should be set deliberately, not by default.
    """
    overrides = (vendor_cfg or {}).get("base_overrides") or {}
    return (
        overrides.get("sso_token_url") or _DEFAULT_SSO_TOKEN_URL,
        overrides.get("legacy_auth_url") or _DEFAULT_LEGACY_AUTH_URL,
        (vendor_cfg or {}).get("scope") or _DEFAULT_SCOPE,
    )


# ----- public entry point ------------------------------------------------


def authenticate(
    *,
    org_id: str,
    vendor_cfg: dict,
    credentials: dict,
    http_post: HttpPoster = _default_http_post,
    fetch_cookies: CookieFetcher = _default_fetch_cookies,
) -> dict:
    """Run the documented Steps 1+2.

    `vendor_cfg` is the `RPA_CONFIG.vendor_settings.centralreach` subtree.
    For a typical prod org it is empty or just `{"scope": "..."}`. A
    sandbox override looks like:
        {"base_overrides": {"sso_token_url": "...", "legacy_auth_url": "..."}}

    `credentials` must contain client_id + client_secret (see rpa.secrets).

    Returns:
        {
          "cookies": [{name, value, domain, path, secure}, ...],
          "extra_http_headers": {},     # empty — CR uses cookies, not bearer
          "access_token": "<jwt>",      # for diagnostics; never logged
        }

    Raises RpaAuthError on any failure, including the case where the
    legacy auth endpoint returns successfully but is missing one of the
    two required session cookies (crsd, crud).
    """
    sso_token_url, legacy_auth_url, scope = _resolve_endpoints(vendor_cfg)

    # Step 1: client_credentials -> JWT (Example A in the CR doc).
    sso_form = {
        "grant_type": "client_credentials",
        "client_id": credentials["client_id"],
        "client_secret": credentials["client_secret"],
        "scope": scope,
    }
    sso_resp = http_post(sso_token_url, sso_form, {})
    access_token = sso_resp.get("access_token")
    if not access_token:
        raise RpaAuthError("CR SSO response missing access_token")

    # Step 2: JWT -> session cookies (Example B in the CR doc). The
    # request body shape is `{"token": <jwt>}`. No Authorization header.
    cookies = fetch_cookies(
        legacy_auth_url,
        {"token": access_token},
        {},
    )

    cookie_names = {c["name"] for c in cookies}
    missing = _REQUIRED_COOKIES - cookie_names
    if missing:
        raise RpaAuthError(
            f"CR legacy-auth response missing required cookie(s): "
            f"{sorted(missing)}; got {sorted(cookie_names)}. "
            "Both crsd and crud are required for internal-API / RPA "
            "traffic per CR's authentication doc."
        )

    return {
        "cookies": cookies,
        # CR's internal APIs / RPA use the session cookies; no bearer
        # header is needed on Playwright requests.
        "extra_http_headers": {},
        # Held only so the playbook engine can echo it if a future flow
        # needs the bearer alongside the cookies. Never logged.
        "access_token": access_token,
    }
