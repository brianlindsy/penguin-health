"""Tests for centralreach.client — HTTP client behaviors.

The client owns five load-bearing behaviors that this file pins:

  1. Cookie jar absorbs Set-Cookie from every response
  2. Authenticator is called once at startup, seeds the cookie jar
  3. Default headers (Accept) are applied to every request
  4. ServiceStack content-type negotiation failures raise
     CentralReachContentTypeError, not parse errors
  5. ServiceStack validation failures (200 + responseStatus.errors)
     raise CentralReachValidationError carrying the fieldName

We use urllib's stdlib stack and patch `_opener.open` directly. Tests
never hit the network.
"""

from __future__ import annotations

import io
import json
from typing import Any
from unittest.mock import patch

import pytest

from centralreach.auth import Authenticator, Session
from centralreach.client import CentralReachClient
from centralreach.exceptions import (
    CentralReachAPIError,
    CentralReachAuthError,
    CentralReachContentTypeError,
    CentralReachRateLimitError,
    CentralReachValidationError,
)
from centralreach.rate_limiter import RateLimiter


# ----- helpers ---------------------------------------------------------------


class _NoSleepLimiter(RateLimiter):
    """Rate limiter that never sleeps — keeps tests fast."""

    def __init__(self):
        super().__init__(0)


class FakeAuthenticator(Authenticator):
    """Returns a canned Session. Tracks calls so tests can assert
    auth is invoked exactly once."""

    def __init__(self, session: Session):
        self._session = session
        self.call_count = 0
        self.last_org_id: str | None = None

    def authenticate(self, org_id: str) -> Session:
        self.call_count += 1
        self.last_org_id = org_id
        return self._session


class RaisingAuthenticator(Authenticator):
    """Always raises CentralReachAuthError. Tests the auth-failure
    propagation path."""

    def authenticate(self, org_id: str) -> Session:
        raise CentralReachAuthError("nope")


class FakeHTTPResponse:
    """Stand-in for urllib's HTTPResponse context manager. Supports
    `with` syntax, `.read()`, and `.headers.get(...)`.
    """

    def __init__(
        self, body: bytes, *,
        content_type: str = "application/json",
        set_cookies: list[str] | None = None,
    ):
        self._body = body
        self.headers = _FakeHeaders(content_type, set_cookies or [])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._body


class _FakeHeaders:
    def __init__(self, content_type: str, set_cookies: list[str]):
        self._ct = content_type
        self._set_cookies = set_cookies

    def get(self, name: str, default: str | None = None) -> str | None:
        if name.lower() == "content-type":
            return self._ct
        return default

    def get_all(self, name: str) -> list[str] | None:
        if name.lower() == "set-cookie":
            return list(self._set_cookies) or None
        return None


def _make_client(
    *,
    authenticator: Authenticator | None = None,
    base_url: str = "https://members.centralreach.com",
) -> tuple[CentralReachClient, FakeAuthenticator]:
    """Build a client with a default fake authenticator. Returns
    (client, auth) so tests can assert on the auth call count."""
    if authenticator is None:
        authenticator = FakeAuthenticator(Session(
            cookies={"crsd": "AUTH_CRSD", "crud": "AUTH_CRUD",
                     "csrf-token": "AUTH_TOKEN", "tzoffset": "300"},
            extra_headers={"x-csrf-token": "AUTH_TOKEN",
                           "x-crapi-clientsource": "members-spa",
                           "Referer": base_url + "/"},
        ))
    client = CentralReachClient(
        org_id="demo",
        authenticator=authenticator,
        rate_limiter=_NoSleepLimiter(),
        base_url=base_url,
    )
    return client, authenticator  # type: ignore[return-value]


# ----- authenticate ----------------------------------------------------------


def test_authenticate_calls_authenticator_once():
    client, auth = _make_client()
    client.authenticate()
    assert auth.call_count == 1
    assert auth.last_org_id == "demo"


def test_authenticate_seeds_cookie_jar():
    """After authenticate(), the cookie jar contains every cookie the
    Session returned, scoped to the base URL's host."""
    client, _ = _make_client()
    client.authenticate()
    names = {c.name for c in client._cookie_jar}
    assert {"crsd", "crud", "csrf-token", "tzoffset"} <= names


def test_authenticate_replaces_session_on_reauth():
    """Calling authenticate() again drops the previous cookies and
    seeds fresh ones — supports the retry-on-auth-failure path."""
    first = FakeAuthenticator(Session(cookies={"old": "X"}))
    client = CentralReachClient(
        org_id="demo", authenticator=first,
        rate_limiter=_NoSleepLimiter(),
    )
    client.authenticate()
    assert {c.name for c in client._cookie_jar} == {"old"}

    # Swap authenticator and re-auth
    client._authenticator = FakeAuthenticator(Session(cookies={"new": "Y"}))
    client.authenticate()
    names = {c.name for c in client._cookie_jar}
    assert "old" not in names
    assert "new" in names


def test_auth_failure_propagates():
    client = CentralReachClient(
        org_id="demo",
        authenticator=RaisingAuthenticator(),
        rate_limiter=_NoSleepLimiter(),
    )
    with pytest.raises(CentralReachAuthError):
        client.authenticate()


def test_first_request_auto_authenticates():
    """If the caller forgets to call authenticate(), the first
    request triggers it automatically."""
    client, auth = _make_client()
    with patch.object(client, "_opener") as mock_opener:
        mock_opener.open.return_value = FakeHTTPResponse(b'{"ok": true}')
        client.get_json("/some/path")
    assert auth.call_count == 1


# ----- default headers -------------------------------------------------------


def test_request_includes_accept_header():
    """Pinned: every request must carry the verbatim browser Accept
    value — bare `application/json` is empirically rejected on some
    CR endpoints."""
    client, _ = _make_client()
    client.authenticate()
    captured: dict[str, Any] = {}

    def capture_open(req, timeout=None):
        captured["headers"] = dict(req.headers)
        return FakeHTTPResponse(b'{"ok": true}')

    with patch.object(client._opener, "open", side_effect=capture_open):
        client.get_json("/anything")

    # Header names get title-cased by urllib.request.Request — check
    # case-insensitively.
    headers_ci = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers_ci.get("accept") == "application/json, text/javascript, */*; q=0.01"


def test_request_includes_authenticator_extra_headers():
    """Headers from the authenticator's Session.extra_headers must
    flow through to every request — that's how the CSRF token and
    Referer reach the server."""
    client, _ = _make_client()
    client.authenticate()
    captured: dict[str, Any] = {}

    def capture_open(req, timeout=None):
        captured["headers"] = dict(req.headers)
        return FakeHTTPResponse(b'{"ok": true}')

    with patch.object(client._opener, "open", side_effect=capture_open):
        client.get_json("/anything")

    headers_ci = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers_ci.get("x-csrf-token") == "AUTH_TOKEN"
    assert headers_ci.get("x-crapi-clientsource") == "members-spa"
    assert "referer" in headers_ci


# ----- response handling -----------------------------------------------------


def test_get_json_returns_parsed_body():
    client, _ = _make_client()
    client.authenticate()
    body = json.dumps({"result": "OK", "items": [1, 2, 3]}).encode("utf-8")
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(body)):
        out = client.get_json("/anything")
    assert out == {"result": "OK", "items": [1, 2, 3]}


def test_post_json_sends_json_body_with_content_type():
    client, _ = _make_client()
    client.authenticate()
    captured: dict[str, Any] = {}

    def capture_open(req, timeout=None):
        captured["body"] = req.data
        captured["headers"] = dict(req.headers)
        return FakeHTTPResponse(b'{"ok": true}')

    with patch.object(client._opener, "open", side_effect=capture_open):
        client.post_json("/x", body={"a": 1})

    assert json.loads(captured["body"].decode()) == {"a": 1}
    headers_ci = {k.lower(): v for k, v in captured["headers"].items()}
    assert "application/json" in headers_ci.get("content-type", "")


# ----- content-type negotiation failure --------------------------------------


def test_html_response_raises_content_type_error():
    """The ServiceStack metadata-snapshot fallback returns HTML at
    status 200. Treating that as JSON parse failure would obscure the
    real cause."""
    client, _ = _make_client()
    client.authenticate()
    html = b"<h1>Snapshot of GetThing generated by ServiceStack</h1>"
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(html, content_type="text/html; charset=utf-8")):
        with pytest.raises(CentralReachContentTypeError) as excinfo:
            client.get_json("/anything")
    assert "ServiceStack" in str(excinfo.value)


def test_unparseable_json_raises_api_error_not_content_type_error():
    """If content-type is json but the body is broken, that's an API
    error (something failed server-side), not a content-type
    negotiation failure."""
    client, _ = _make_client()
    client.authenticate()
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(b"not json")):
        with pytest.raises(CentralReachAPIError):
            client.get_json("/anything")


# ----- ServiceStack validation errors ----------------------------------------


def test_validation_error_raises_centralreach_validation_error():
    """200 + responseStatus.errors with a fieldName must raise
    CentralReachValidationError carrying that fieldName."""
    client, _ = _make_client()
    client.authenticate()
    body = json.dumps({
        "result": "OK",
        "failed": False,
        "responseStatus": {
            "errorCode": "NotEmpty",
            "message": "'DateRange' must not be empty.",
            "errors": [{
                "errorCode": "NotEmpty",
                "fieldName": "DateRange",
                "message": "'DateRange' must not be empty.",
            }],
        },
    }).encode("utf-8")
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(body)):
        with pytest.raises(CentralReachValidationError) as excinfo:
            client.post_json("/x", body={})
    assert excinfo.value.field_name == "DateRange"


def test_empty_response_status_is_not_validation_error():
    """A `responseStatus` present but with no errors array is success
    (some endpoints include responseStatus on every response)."""
    client, _ = _make_client()
    client.authenticate()
    body = json.dumps({
        "result": "OK",
        "responseStatus": {},
    }).encode("utf-8")
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(body)):
        out = client.get_json("/x")
    assert out["result"] == "OK"


def test_no_response_status_field_is_not_validation_error():
    """Endpoints without responseStatus at all should not be flagged."""
    client, _ = _make_client()
    client.authenticate()
    body = json.dumps({"result": "ok", "success": True}).encode("utf-8")
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(body)):
        out = client.get_json("/x")
    assert out["success"] is True


# ----- transport errors ------------------------------------------------------


def test_429_raises_rate_limit_error():
    import urllib.error
    client, _ = _make_client()
    client.authenticate()

    def raise_429(req, timeout=None):
        raise urllib.error.HTTPError(
            req.full_url, 429, "Too Many Requests", {}, None,
        )

    with patch.object(client._opener, "open", side_effect=raise_429):
        with pytest.raises(CentralReachRateLimitError):
            client.get_json("/x")


def test_5xx_raises_api_error_with_status_code():
    import urllib.error
    client, _ = _make_client()
    client.authenticate()

    def raise_503(req, timeout=None):
        raise urllib.error.HTTPError(
            req.full_url, 503, "Service Unavailable", {}, None,
        )

    with patch.object(client._opener, "open", side_effect=raise_503):
        with pytest.raises(CentralReachAPIError) as excinfo:
            client.get_json("/x")
    assert excinfo.value.status_code == 503


def test_url_error_raises_api_error():
    import urllib.error
    client, _ = _make_client()
    client.authenticate()

    def raise_url_error(req, timeout=None):
        raise urllib.error.URLError("connection refused")

    with patch.object(client._opener, "open", side_effect=raise_url_error):
        with pytest.raises(CentralReachAPIError):
            client.get_json("/x")


# ----- get_bytes (S3 presigned downloads) ------------------------------------


def test_get_bytes_returns_raw_body():
    """Used for downloading PDFs from S3 presigned URLs. Returns
    raw bytes; does NOT JSON-parse or check content-type (PDFs
    are application/pdf, not application/json)."""
    client, _ = _make_client()
    pdf_bytes = b"%PDF-1.4\nfake pdf content\n%%EOF"
    with patch("urllib.request.urlopen",
               return_value=FakeHTTPResponse(pdf_bytes,
                                              content_type="application/pdf")):
        out = client.get_bytes("https://s3.example/file.pdf")
    assert out == pdf_bytes


# ----- rate limiter integration ---------------------------------------------


def test_rate_limiter_wait_called_before_each_request():
    calls = []

    class CountingLimiter(RateLimiter):
        def __init__(self):
            super().__init__(0)

        def wait(self):
            calls.append(1)

    client = CentralReachClient(
        org_id="demo",
        authenticator=FakeAuthenticator(Session()),
        rate_limiter=CountingLimiter(),
    )
    client.authenticate()
    with patch.object(client._opener, "open",
                      return_value=FakeHTTPResponse(b'{"ok": true}')):
        client.get_json("/a")
        client.get_json("/b")
        client.post_json("/c", body={})
    # 3 requests -> 3 limiter.wait() calls (auth itself doesn't gate)
    assert sum(calls) == 3
