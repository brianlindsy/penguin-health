"""Authenticator interface + concrete implementations for centralreach.

The HTTP client takes any object implementing `Authenticator` —
typically a single concrete instance per Fargate task. Authenticators
return a `Session` describing the cookie jar seed values, the default
HTTP headers (Accept, CSRF token, etc.), and any diagnostic data the
runner needs (without ever logging the JWT/access token itself).

Implementations:
  * `OAuthAuthenticator`: CR's documented client_credentials flow —
    POST client_id + client_secret to the SSO `/connect/token`
    endpoint, exchange the JWT for session cookies via
    `framework.authtoken`, then fetch a CSRF token via
    `framework.csrf`. This is the documented strategy; see the
    design doc's Open Questions section for the known limitation
    (the `crud` cookie this produces is SSO-shaped, not user-shaped,
    which causes per-user resource endpoints to refuse access).
  * `PlaceholderAuthenticator`: a stub that raises fast — kept for
    tests that need an authenticator object without committing to a
    flow.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from .exceptions import CentralReachAuthError
from . import secrets as secrets_mod


# ----- Endpoint defaults ---------------------------------------------------

# Documented CR endpoints. A per-org sandbox override flips these to
# the sandbox-* hostnames via `vendor_settings.centralreach.base_overrides`
# on the org's CENTRALREACH_CONFIG row.
_DEFAULT_SSO_TOKEN_URL = "https://login.centralreach.com/connect/token"
_DEFAULT_LEGACY_AUTH_URL = (
    "https://members.centralreach.com/api/?framework.authtoken"
)
_DEFAULT_CSRF_URL = (
    "https://members.centralreach.com/api/?framework.csrf"
)
_DEFAULT_SCOPE = "cr-api"

_DEFAULT_REFERER = "https://members.centralreach.com/"

# Both cookies are required for internal-API traffic per CR's docs.
# If either is missing from the legacy-auth response we cannot
# proceed — the missing cookie is the operationally-actionable signal,
# not a generic "auth failed."
_REQUIRED_LEGACY_AUTH_COOKIES = frozenset({"crsd", "crud"})

_HTTP_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class Session:
    """The minimal session state the HTTP client needs.

    `cookies` seeds the client's cookie jar; the jar then absorbs
    `Set-Cookie` headers from every response, so the seed is a
    *starting state*, not a durable snapshot.

    `extra_headers` is applied to every outbound request. Includes
    `Accept`, `x-csrf-token`, `x-crapi-clientsource`, `Referer`, and
    `x-requested-with` per the design doc.

    `access_token` is the SSO JWT or session token, kept for
    diagnostic purposes. NEVER logged. The client does not send it as
    a Bearer header — CR's internal APIs use cookies, not bearer
    auth.
    """

    cookies: dict[str, str] = field(default_factory=dict)
    extra_headers: dict[str, str] = field(default_factory=dict)
    access_token: str | None = None


class Authenticator(ABC):
    """Abstract authenticator. Concrete impls live alongside this class.

    Implementations should:
      * Read credentials from Secrets Manager (or the test injection
        path), NOT from environment variables
      * Make any necessary HTTP calls to establish the session
      * Return a `Session` containing the cookie seeds and headers
        the HTTP client needs to drive subsequent requests
      * Raise `CentralReachAuthError` on any failure, never a generic
        Exception
    """

    @abstractmethod
    def authenticate(self, org_id: str) -> Session:
        """Mint a fresh session for `org_id`.

        Called once per Fargate task at startup. The session lives for
        the duration of the task; if it expires mid-run, the client's
        retry path re-authenticates by calling this method again.
        """


class PlaceholderAuthenticator(Authenticator):
    """Stub authenticator that fails fast.

    Kept for tests that need an Authenticator object without
    committing to a real flow.
    """

    def authenticate(self, org_id: str) -> Session:
        raise CentralReachAuthError(
            "PlaceholderAuthenticator is not a real auth flow. "
            "Use OAuthAuthenticator in production."
        )


# ----- HTTP shape (injectable for tests) -----------------------------------


HttpPostJson = Callable[[str, dict, dict], dict]
HttpPostForm = Callable[[str, dict, dict], dict]
HttpGetSetCookies = Callable[[str, dict], tuple[dict, dict[str, str]]]


def _http_post_form(url: str, form: dict, headers: dict) -> dict:
    """Default: POST a urlencoded form body, return parsed JSON.

    Used for the SSO `/connect/token` step. ServiceStack tolerates
    urlencoded form bodies for this endpoint; the response is
    `{"access_token": ..., "expires_in": ..., ...}`.
    """
    body = urllib.parse.urlencode(form).encode("ascii")
    req_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        **headers,
    }
    req = urllib.request.Request(url, data=body, method="POST",
                                  headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        raise CentralReachAuthError(
            f"CR SSO returned HTTP {e.code}"
        ) from e
    except urllib.error.URLError as e:
        raise CentralReachAuthError(
            f"CR SSO unreachable: {type(e).__name__}"
        ) from e
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise CentralReachAuthError(
            "CR SSO returned non-JSON body"
        ) from e


def _http_post_json(url: str, payload: dict, headers: dict) -> dict:
    """Default: POST a JSON body, return parsed JSON.

    Used for the legacy-auth step. The body is `{"token": "<jwt>"}`
    with no Authorization header — CR's docs are explicit about this.
    """
    body = json.dumps(payload).encode("utf-8")
    req_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        **headers,
    }
    req = urllib.request.Request(url, data=body, method="POST",
                                  headers=req_headers)
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as r:
            raw = r.read()
            set_cookie_headers = r.headers.get_all("Set-Cookie") or []
    except urllib.error.HTTPError as e:
        raise CentralReachAuthError(
            f"CR legacy-auth returned HTTP {e.code}"
        ) from e
    except urllib.error.URLError as e:
        raise CentralReachAuthError(
            f"CR legacy-auth unreachable: {type(e).__name__}"
        ) from e
    # legacy-auth's body is informational; the cookies are the
    # load-bearing return value. We still parse the body so a
    # malformed response surfaces, but we don't depend on its content.
    try:
        body_json = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        body_json = {}
    return {"body": body_json, "cookies": _parse_set_cookie_headers(set_cookie_headers)}


def _http_get_set_cookies(
    url: str, headers: dict,
) -> tuple[dict, dict[str, str]]:
    """Default: GET, return `(body_json, rotated_cookies)`.

    Used for the CSRF endpoint. Body is a status object
    (`{"success": true, ...}`); the CSRF token + rotated `crsd` come
    back via `Set-Cookie` headers, NOT in the body. Reading the body
    is purely a contract check — `success != true` indicates CR
    changed the endpoint shape.
    """
    req = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as r:
            raw = r.read()
            set_cookie_headers = r.headers.get_all("Set-Cookie") or []
    except urllib.error.HTTPError as e:
        raise CentralReachAuthError(
            f"CR csrf returned HTTP {e.code}"
        ) from e
    except urllib.error.URLError as e:
        raise CentralReachAuthError(
            f"CR csrf unreachable: {type(e).__name__}"
        ) from e
    try:
        body_json = json.loads(raw) if raw else {}
    except json.JSONDecodeError as e:
        raise CentralReachAuthError(
            "CR csrf returned non-JSON body"
        ) from e
    rotated = _parse_set_cookie_headers(set_cookie_headers)
    return body_json, rotated


def _parse_set_cookie_headers(headers: list[str]) -> dict[str, str]:
    """Extract `{name: value}` from a list of Set-Cookie header values.

    Drops attributes (`; path=...; secure; ...`) — only the name and
    value matter for forwarding into the HTTP client's cookie jar.
    """
    out: dict[str, str] = {}
    for header in headers:
        head = header.split(";", 1)[0].strip()
        if "=" not in head:
            continue
        name, value = head.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name:
            out[name] = value
    return out


# ----- OAuthAuthenticator -------------------------------------------------


def _resolve_endpoints(vendor_cfg: dict) -> tuple[str, str, str, str]:
    """Resolve (sso_token_url, legacy_auth_url, csrf_url, scope).

    Per-org `vendor_settings.centralreach.base_overrides` can swap the
    SSO + legacy-auth URLs to a sandbox tenant; everything else uses
    the documented prod endpoints. `scope` is overridable for tenants
    that need a non-`cr-api` scope, though we haven't seen one in
    practice.
    """
    overrides = (vendor_cfg.get("base_overrides") or {})
    sso = overrides.get("sso_token_url") or _DEFAULT_SSO_TOKEN_URL
    legacy = overrides.get("legacy_auth_url") or _DEFAULT_LEGACY_AUTH_URL
    csrf = overrides.get("csrf_url") or _DEFAULT_CSRF_URL
    scope = vendor_cfg.get("scope") or _DEFAULT_SCOPE
    return sso, legacy, csrf, scope


def _cookie_header(cookies: dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class OAuthAuthenticator(Authenticator):
    """CR's documented `client_credentials` -> session-cookie flow.

    Three-step auth:

      1. POST `client_id`/`client_secret` (form-encoded) to the SSO
         `/connect/token` endpoint. Receive a JWT (`access_token`).
      2. POST `{"token": "<jwt>"}` (JSON body, no Authorization header)
         to `/api/?framework.authtoken`. Receive `crsd` + `crud` as
         `Set-Cookie` response headers.
      3. GET `/api/?framework.csrf` with the cookies from step 2.
         Receive a `csrf-token` cookie (plus a rotated `crsd`) as
         `Set-Cookie` headers; the body is a status object only.

    The resulting `Session` carries:

      * `cookies`: `{crsd, crud, csrf-token, tzoffset}` — the rotated
        `crsd` from step 3, the original `crud` from step 2, the
        `csrf-token` from step 3, and `tzoffset` from the org's
        timezone.
      * `extra_headers`: the verbatim browser-equivalent default
        headers the HTTP client applies to every request (Accept,
        x-csrf-token, x-crapi-clientsource, x-requested-with, Referer).
      * `access_token`: the JWT, retained for diagnostic purposes
        only. NEVER logged.

    Known limitation:
      This flow produces an SSO-shaped `crud` cookie that some
      per-user resource endpoints (notably `resources.getresourceurl`)
      refuse to honor. See the design doc's Open Questions section.
      We ship this flow as the documented strategy; the operator-side
      workaround is to use a user-shaped session (different flow,
      pending verification).
    """

    def __init__(
        self,
        *,
        vendor_cfg: dict | None = None,
        tz_offset_minutes: int | None = None,
        load_credentials: Callable[[str], dict] = secrets_mod.load_credentials,
        http_post_form: HttpPostForm = _http_post_form,
        http_post_json: HttpPostJson = _http_post_json,
        http_get_set_cookies: HttpGetSetCookies = _http_get_set_cookies,
    ) -> None:
        """Construct an OAuthAuthenticator.

        `vendor_cfg` is the `CENTRALREACH_CONFIG.vendor_settings.centralreach`
        subtree if present. For a typical prod org it is empty or just
        carries a `scope` override. Sandbox tenants use a
        `base_overrides` dict pointing the SSO + legacy-auth URLs at
        sandbox hostnames. May be None.

        `tz_offset_minutes` is the org's UTC offset, used to set the
        `tzoffset` cookie on the session. Required for endpoints that
        check it. The Fargate runner resolves this from the org's
        configured timezone at task startup.

        The four `load_credentials` / `http_*` arguments are injection
        points for tests; production callers omit them.
        """
        self._vendor_cfg = vendor_cfg or {}
        self._tz_offset_minutes = tz_offset_minutes
        self._load_credentials = load_credentials
        self._http_post_form = http_post_form
        self._http_post_json = http_post_json
        self._http_get_set_cookies = http_get_set_cookies

    def authenticate(self, org_id: str) -> Session:
        sso_token_url, legacy_auth_url, csrf_url, scope = _resolve_endpoints(
            self._vendor_cfg,
        )

        # Load credentials from Secrets Manager (or test injection).
        # Raises CentralReachAuthError if the secret is missing or
        # malformed.
        credentials = self._load_credentials(org_id)
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        if not client_id or not client_secret:
            raise CentralReachAuthError(
                f"credentials for org={org_id} missing client_id or "
                "client_secret"
            )

        # Step 1: SSO -> JWT
        sso_form = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        }
        sso_resp = self._http_post_form(sso_token_url, sso_form, {})
        access_token = sso_resp.get("access_token")
        if not access_token:
            raise CentralReachAuthError(
                "CR SSO response missing access_token"
            )

        # Step 2: JWT -> session cookies
        legacy_resp = self._http_post_json(
            legacy_auth_url, {"token": access_token}, {},
        )
        legacy_cookies = legacy_resp.get("cookies") or {}
        missing = _REQUIRED_LEGACY_AUTH_COOKIES - set(legacy_cookies)
        if missing:
            raise CentralReachAuthError(
                f"CR legacy-auth response missing required cookie(s): "
                f"{sorted(missing)}; got {sorted(legacy_cookies)}"
            )

        # Step 3: GET CSRF token. Send the cookies we just got; the
        # response sets `csrf-token` and rotates `crsd`.
        csrf_request_headers = {
            "Accept": "application/json",
            "Cookie": _cookie_header(legacy_cookies),
            "Referer": _DEFAULT_REFERER,
        }
        csrf_body, rotated = self._http_get_set_cookies(
            csrf_url, csrf_request_headers,
        )
        if not isinstance(csrf_body, dict) or csrf_body.get("success") is not True:
            raise CentralReachAuthError(
                f"CR csrf endpoint returned success != true: {csrf_body!r}"
            )
        csrf_token = rotated.get("csrf-token")
        if not csrf_token:
            raise CentralReachAuthError(
                "CR csrf endpoint did not Set-Cookie csrf-token; "
                f"got cookies: {sorted(rotated)}"
            )

        # Merge: start from step 2's cookies, overlay rotated values
        # from step 3 (rotated `crsd` is the load-bearing one), append
        # `csrf-token` and `tzoffset`.
        session_cookies: dict[str, str] = dict(legacy_cookies)
        session_cookies.update(rotated)  # rotates crsd, adds csrf-token, etc.
        if self._tz_offset_minutes is not None:
            session_cookies["tzoffset"] = str(self._tz_offset_minutes)

        extra_headers = {
            "x-csrf-token": csrf_token,
            "x-crapi-clientsource": "members-spa",
            "x-requested-with": "XMLHttpRequest",
            "Referer": _DEFAULT_REFERER,
        }

        return Session(
            cookies=session_cookies,
            extra_headers=extra_headers,
            access_token=access_token,
        )
