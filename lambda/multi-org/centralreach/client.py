"""HTTP client for CR's internal API.

Single-session HTTP client backed by `http.cookiejar.CookieJar`. The
client is sync — the Fargate task drives requests one at a time so
there's no benefit to async — and uses stdlib `urllib.request` so we
don't grow the Lambda layer with `requests` for one consumer.

Why a real cookie jar matters:
  CR's endpoints rotate cookies on responses (verified empirically:
  the `framework.csrf` endpoint rotates `crsd` via Set-Cookie). The
  HTTP client must absorb every response's `Set-Cookie` headers
  before the next request reads from the jar, or sustained sessions
  fail intermittently. See the design doc's "HTTP client owns a live
  cookie jar" subsection.

Why explicit Content-Type checking matters:
  CR is a ServiceStack service. With the wrong `Accept` value
  ServiceStack returns its HTML metadata-snapshot page (status 200,
  content-type: text/html) instead of JSON. The client treats a
  text/html response from a JSON-expected endpoint as
  `CentralReachContentTypeError` — distinct from JSON parse errors —
  so the cause is obvious in audit and dashboard rollups.

Why ServiceStack `responseStatus` checking matters:
  CR returns HTTP 200 with a populated `responseStatus.errors`
  array when server-side validation fails. The transport succeeded;
  the API call did not. The client raises `CentralReachValidationError`
  in that case, carrying the offending `fieldName`.
"""

from __future__ import annotations

import http.cookiejar
import json
import urllib.error
import urllib.request
from typing import Any

from .auth import Authenticator, Session
from .exceptions import (
    CentralReachAPIError,
    CentralReachAuthError,
    CentralReachContentTypeError,
    CentralReachRateLimitError,
    CentralReachValidationError,
)
from .rate_limiter import RateLimiter


# Default base URL — overridable for sandbox tenants via the
# `centralreach_base_url` argument on the client constructor. Sandbox
# orgs land on `https://sandbox-members.centralreach.com`.
_DEFAULT_BASE_URL = "https://members.centralreach.com"

# Default `Accept` value — verbatim what the browser sends. Bare
# `application/json` was empirically rejected on some endpoints; the
# browser's value is what ServiceStack content-negotiates against.
# See the design doc's "ServiceStack content negotiation" subsection.
_DEFAULT_ACCEPT = "application/json, text/javascript, */*; q=0.01"


class CentralReachClient:
    """Authenticated HTTP client for CR's internal API.

    One instance per Fargate task. Holds the cookie jar, the
    rate-limit gate, and the default headers — all of which persist
    across every API call within the run. Concrete API call wrappers
    (`list_query`, `preview`, etc., landing in PR B) sit on top.
    """

    def __init__(
        self,
        org_id: str,
        authenticator: Authenticator,
        *,
        rate_limiter: RateLimiter,
        base_url: str = _DEFAULT_BASE_URL,
        timeout_seconds: int = 30,
        max_csrf_retries: int = 1,
    ) -> None:
        self._org_id = org_id
        self._authenticator = authenticator
        self._rate_limiter = rate_limiter
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._max_csrf_retries = max_csrf_retries

        # The cookie jar is the source of truth for session cookies.
        # We seed it with the authenticator's starting cookies and
        # then update from every response's Set-Cookie headers.
        self._cookie_jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cookie_jar)
        )

        # Headers applied to every outbound request. Built from the
        # session's `extra_headers` at authenticate time; the client
        # never sets these per-request.
        self._default_headers: dict[str, str] = {}

        # Set by `authenticate()`. None until first call.
        self._session: Session | None = None

    # ----- lifecycle -----------------------------------------------------

    def authenticate(self) -> None:
        """Establish the session by calling the authenticator.

        Seeds the cookie jar with the authenticator's starting cookies
        and copies the session's extra_headers into the default-header
        set. Idempotent — calling again replaces the session in-place.

        Raises `CentralReachAuthError` if the authenticator does.
        """
        session = self._authenticator.authenticate(self._org_id)

        # Replace the cookie jar wholesale rather than merging — on
        # re-auth we want to drop any stale cookies from the previous
        # session. The opener still points at the same jar object so
        # we mutate in place.
        self._cookie_jar.clear()
        for name, value in session.cookies.items():
            self._set_cookie(name, value)

        # Defaults are: Accept + everything the auth provided.
        # We don't trust the auth to set Accept correctly; the client
        # owns that header.
        self._default_headers = {
            "Accept": _DEFAULT_ACCEPT,
            **dict(session.extra_headers),
        }
        self._session = session

    def _ensure_authenticated(self) -> None:
        if self._session is None:
            self.authenticate()

    # ----- public request methods ---------------------------------------

    def post_json(
        self, path: str, body: dict | None = None,
    ) -> dict:
        """POST a JSON body to `{base_url}{path}` and return the parsed
        JSON response.

        Raises:
          * `CentralReachAuthError` if auth fails
          * `CentralReachContentTypeError` if the response is HTML
          * `CentralReachValidationError` if the response has a
            populated `responseStatus.errors`
          * `CentralReachRateLimitError` on HTTP 429
          * `CentralReachAPIError` on any other transport-level error
        """
        return self._request(
            "POST", path,
            body=json.dumps(body or {}).encode("utf-8"),
            content_type="application/json; charset=UTF-8",
        )

    def get_json(self, path: str) -> dict:
        """GET `{base_url}{path}` and return the parsed JSON response.

        Same exception surface as `post_json`.
        """
        return self._request("GET", path)

    def get_bytes(self, url: str) -> bytes:
        """GET an absolute URL and return the raw response body.

        Used for downloading presigned S3 URLs. Does NOT apply the
        client's default headers or cookie jar — S3 presigned URLs
        are self-authenticating and can be confused by extra
        headers.
        """
        self._rate_limiter.wait()
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=self._timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            raise CentralReachAPIError(
                f"GET {url} returned HTTP {e.code}",
                status_code=e.code,
            ) from e
        except urllib.error.URLError as e:
            raise CentralReachAPIError(
                f"GET {url} unreachable: {type(e).__name__}"
            ) from e

    # ----- internals -----------------------------------------------------

    def _request(
        self, method: str, path: str, *,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> dict:
        self._ensure_authenticated()
        self._rate_limiter.wait()

        url = self._base_url + path
        headers = dict(self._default_headers)
        if content_type:
            headers["Content-Type"] = content_type

        req = urllib.request.Request(
            url, data=body, method=method, headers=headers,
        )

        try:
            with self._opener.open(req, timeout=self._timeout) as r:
                response_body = r.read()
                response_content_type = r.headers.get("Content-Type") or ""
                # Cookie jar already absorbed Set-Cookie via the
                # HTTPCookieProcessor; nothing to do here.
        except urllib.error.HTTPError as e:
            if e.code == 429:
                raise CentralReachRateLimitError(
                    f"{method} {path} rate-limited (HTTP 429)"
                ) from e
            raise CentralReachAPIError(
                f"{method} {path} returned HTTP {e.code}",
                status_code=e.code,
            ) from e
        except urllib.error.URLError as e:
            raise CentralReachAPIError(
                f"{method} {path} unreachable: {type(e).__name__}"
            ) from e

        # Content-type check: ServiceStack returns HTML on content
        # negotiation failure. A 200 with text/html is not a JSON
        # parse error; it's a config-level mismatch.
        if "text/html" in response_content_type.lower():
            raise CentralReachContentTypeError(
                f"{method} {path} returned HTML "
                f"(Content-Type: {response_content_type!r}); "
                "expected JSON. Likely ServiceStack content negotiation "
                "fell back to the metadata snapshot page. Check Accept "
                "header and request shape."
            )

        try:
            payload = json.loads(response_body)
        except json.JSONDecodeError as e:
            raise CentralReachAPIError(
                f"{method} {path} returned non-JSON body "
                f"(Content-Type: {response_content_type!r}): "
                f"{type(e).__name__}"
            ) from e

        # ServiceStack validation check: 200 + populated
        # responseStatus.errors is a validation rejection, not
        # success.
        validation = _extract_validation_error(payload)
        if validation is not None:
            raise CentralReachValidationError(
                f"{method} {path} validation failed: {validation[1]}",
                field_name=validation[0],
            )

        return payload

    def _set_cookie(self, name: str, value: str) -> None:
        """Inject a cookie into the jar by name/value.

        Domain and path are scoped to the configured base URL's host.
        The opener uses the jar for both reading (outbound requests)
        and writing (Set-Cookie absorption from responses).
        """
        from urllib.parse import urlparse

        host = urlparse(self._base_url).hostname or ""
        cookie = http.cookiejar.Cookie(
            version=0, name=name, value=value,
            port=None, port_specified=False,
            domain=host, domain_specified=True, domain_initial_dot=False,
            path="/", path_specified=True,
            secure=True, expires=None,
            discard=False, comment=None, comment_url=None,
            rest={},
        )
        self._cookie_jar.set_cookie(cookie)


def _extract_validation_error(payload: Any) -> tuple[str | None, str] | None:
    """Pull the first `responseStatus.errors` entry out of a CR
    response if one exists.

    Returns `(field_name, message)` or None.

    The check is defensive — `payload` may be a non-dict (e.g. a list
    or scalar from a misrouted endpoint), and `responseStatus` may be
    absent, null, or have an empty `errors` array, all of which mean
    "no validation error."
    """
    if not isinstance(payload, dict):
        return None
    rs = payload.get("responseStatus")
    if not isinstance(rs, dict):
        return None
    errors = rs.get("errors")
    if not isinstance(errors, list) or not errors:
        return None
    first = errors[0]
    if not isinstance(first, dict):
        return None
    field = first.get("fieldName") if isinstance(first.get("fieldName"), str) else None
    msg = first.get("message") if isinstance(first.get("message"), str) else "validation failed"
    return field, msg
