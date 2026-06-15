"""Tests for the CentralReach authenticator.

Each test pins the request shape against CR's documented examples
(Authenticating a Request to CentralReach's APIs, rev. 2024-07-31):

  Example A: client_credentials POST to /connect/token
  Example B: {"token": "<jwt>"} POST to /api/?framework.authtoken,
             returns crsd + crud cookies

HTTP transports are injected so we don't hit the network.
"""

import pytest

from rpa.authenticators import centralreach
from rpa.exceptions import RpaAuthError


PROD_SSO = "https://login.centralreach.com/connect/token"
PROD_LEGACY = "https://members.centralreach.com/api/?framework.authtoken"

CREDS = {"client_id": "demo-bot-cid", "client_secret": "very-secret"}


def _both_cookies():
    return [
        {"name": "crsd", "value": "abc", "domain": ".centralreach.com",
         "path": "/", "secure": True},
        {"name": "crud", "value": "xyz", "domain": ".centralreach.com",
         "path": "/", "secure": True},
    ]


def test_happy_path_uses_documented_urls_and_request_shapes():
    """Pins Example A + Example B request shapes exactly."""
    sso_calls = []
    cookie_calls = []

    def fake_post(url, form, headers):
        sso_calls.append((url, form, headers))
        return {"access_token": "JWT-123", "expires_in": 3600}

    def fake_fetch(url, body, headers):
        cookie_calls.append((url, body, headers))
        return _both_cookies()

    session = centralreach.authenticate(
        org_id="demo",
        vendor_cfg={},  # Defaults to documented prod URLs.
        credentials=CREDS,
        http_post=fake_post,
        fetch_cookies=fake_fetch,
    )

    assert session["access_token"] == "JWT-123"
    assert {c["name"] for c in session["cookies"]} == {"crsd", "crud"}
    # CR uses cookies, not bearer headers, so extra_http_headers stays empty.
    assert session["extra_http_headers"] == {}

    # Example A: documented prod URL + client_credentials + scope=cr-api.
    assert len(sso_calls) == 1
    url, form, _ = sso_calls[0]
    assert url == PROD_SSO
    assert form == {
        "grant_type": "client_credentials",
        "client_id": "demo-bot-cid",
        "client_secret": "very-secret",
        "scope": "cr-api",
    }

    # Example B: documented prod URL, JSON body {"token": <jwt>},
    # NO Authorization header.
    assert len(cookie_calls) == 1
    url, body, headers = cookie_calls[0]
    assert url == PROD_LEGACY
    assert body == {"token": "JWT-123"}
    assert "Authorization" not in headers


def test_scope_can_be_overridden_per_org():
    captured_form = {}

    def fake_post(url, form, headers):
        captured_form.update(form)
        return {"access_token": "JWT"}

    centralreach.authenticate(
        org_id="demo",
        vendor_cfg={"scope": "cr-api custom-scope"},
        credentials=CREDS,
        http_post=fake_post,
        fetch_cookies=lambda u, b, h: _both_cookies(),
    )
    assert captured_form["scope"] == "cr-api custom-scope"


def test_default_scope_is_cr_api():
    captured_form = {}

    def fake_post(url, form, headers):
        captured_form.update(form)
        return {"access_token": "JWT"}

    centralreach.authenticate(
        org_id="demo",
        vendor_cfg={},
        credentials=CREDS,
        http_post=fake_post,
        fetch_cookies=lambda u, b, h: _both_cookies(),
    )
    assert captured_form["scope"] == "cr-api"


def test_base_overrides_redirect_to_sandbox_urls():
    """Per-org URL overrides exist for CR-provisioned sandbox tenants,
    but are off by default."""
    sso_calls = []
    cookie_calls = []

    def fake_post(url, form, headers):
        sso_calls.append(url)
        return {"access_token": "JWT"}

    def fake_fetch(url, body, headers):
        cookie_calls.append(url)
        return _both_cookies()

    centralreach.authenticate(
        org_id="demo",
        vendor_cfg={"base_overrides": {
            "sso_token_url": "https://sandbox-login.centralreach.com/connect/token",
            "legacy_auth_url": "https://sandbox-members.centralreach.com/api/?framework.authtoken",
        }},
        credentials=CREDS,
        http_post=fake_post,
        fetch_cookies=fake_fetch,
    )
    assert sso_calls == ["https://sandbox-login.centralreach.com/connect/token"]
    assert cookie_calls == [
        "https://sandbox-members.centralreach.com/api/?framework.authtoken"
    ]


def test_sso_response_missing_token_raises():
    with pytest.raises(RpaAuthError, match="SSO response missing access_token"):
        centralreach.authenticate(
            org_id="demo",
            vendor_cfg={},
            credentials=CREDS,
            http_post=lambda url, form, headers: {"error": "invalid_client"},
            fetch_cookies=lambda u, b, h: _both_cookies(),
        )


def test_missing_crsd_cookie_raises():
    only_crud = [c for c in _both_cookies() if c["name"] == "crud"]
    with pytest.raises(RpaAuthError, match="missing required cookie.*crsd"):
        centralreach.authenticate(
            org_id="demo",
            vendor_cfg={},
            credentials=CREDS,
            http_post=lambda u, f, h: {"access_token": "JWT"},
            fetch_cookies=lambda u, b, h: only_crud,
        )


def test_missing_crud_cookie_raises():
    only_crsd = [c for c in _both_cookies() if c["name"] == "crsd"]
    with pytest.raises(RpaAuthError, match="missing required cookie.*crud"):
        centralreach.authenticate(
            org_id="demo",
            vendor_cfg={},
            credentials=CREDS,
            http_post=lambda u, f, h: {"access_token": "JWT"},
            fetch_cookies=lambda u, b, h: only_crsd,
        )


def test_empty_cookies_raises():
    with pytest.raises(RpaAuthError, match="missing required cookie"):
        centralreach.authenticate(
            org_id="demo",
            vendor_cfg={},
            credentials=CREDS,
            http_post=lambda url, form, headers: {"access_token": "JWT"},
            fetch_cookies=lambda u, b, h: [],
        )


def test_vendor_key_matches_registry():
    assert centralreach.AUTH_VENDOR == "centralreach"
