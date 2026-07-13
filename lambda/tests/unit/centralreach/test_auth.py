"""Tests for centralreach.auth — interface contract + concrete impls.

Pins seven contracts:
  1. Session/Authenticator interface shape
  2. PlaceholderAuthenticator raises fast (kept for tests)
  3. OAuthAuthenticator's three-step flow produces a populated Session
  4. Step 1 (SSO) failure surfaces as CentralReachAuthError
  5. Step 2 (legacy-auth) missing required cookies surfaces with the
     missing cookie names
  6. Step 3 (CSRF) failure modes — body success!=true and no token
     cookie
  7. Per-org sandbox override flips the SSO + legacy-auth URLs
"""

from __future__ import annotations

import pytest

from centralreach.auth import (
    Authenticator,
    OAuthAuthenticator,
    PlaceholderAuthenticator,
    Session,
    _DEFAULT_LEGACY_AUTH_URL,
    _DEFAULT_REFERER,
    _DEFAULT_SCOPE,
    _DEFAULT_SSO_TOKEN_URL,
    _parse_set_cookie_headers,
)
from centralreach.exceptions import CentralReachAuthError


# ----- Session / Authenticator -------------------------------------------


def test_session_defaults_are_empty():
    s = Session()
    assert s.cookies == {}
    assert s.extra_headers == {}
    assert s.access_token is None


def test_session_carries_values():
    s = Session(
        cookies={"crsd": "abc"},
        extra_headers={"x-csrf-token": "xyz"},
        access_token="jwt",
    )
    assert s.cookies == {"crsd": "abc"}
    assert s.extra_headers == {"x-csrf-token": "xyz"}
    assert s.access_token == "jwt"


def test_authenticator_is_abstract():
    """Pinned: instantiating Authenticator directly must raise — every
    concrete impl has to override `authenticate`."""
    with pytest.raises(TypeError):
        Authenticator()


def test_placeholder_authenticator_raises_fast():
    auth = PlaceholderAuthenticator()
    with pytest.raises(CentralReachAuthError) as excinfo:
        auth.authenticate(org_id="demo")
    assert "PlaceholderAuthenticator" in str(excinfo.value)


# ----- _parse_set_cookie_headers ------------------------------------------


def test_parse_set_cookie_extracts_name_value_only():
    """Strips attributes; only `name=value` survives. Matches the CR
    Set-Cookie shapes we captured in the design phase."""
    headers = [
        "crsd=NEW_CRSD; path=/; secure; HttpOnly; SameSite=None",
        "csrf-token=THE_TOKEN; Priority=High; path=/; secure; SameSite=None",
        "uiver=26.6.2; path=/; secure; SameSite=None",
    ]
    out = _parse_set_cookie_headers(headers)
    assert out == {
        "crsd": "NEW_CRSD",
        "csrf-token": "THE_TOKEN",
        "uiver": "26.6.2",
    }


def test_parse_set_cookie_empty_input_returns_empty_dict():
    assert _parse_set_cookie_headers([]) == {}


def test_parse_set_cookie_skips_malformed_entries():
    headers = ["this-has-no-equals; whatever", "valid=ok"]
    assert _parse_set_cookie_headers(headers) == {"valid": "ok"}


# ----- OAuthAuthenticator: helpers --------------------------------


def _build_authenticator(
    *,
    vendor_cfg: dict | None = None,
    tz_offset_minutes: int | None = -300,  # Eastern
    credentials: dict | None = None,
    sso_response: dict | None = None,
    legacy_response: dict | None = None,
    legacy_responses: list[dict] | None = None,
    csrf_response: tuple[dict, dict[str, str]] | None = None,
    sso_raises: Exception | None = None,
    legacy_raises: Exception | None = None,
    csrf_raises: Exception | None = None,
    retry_backoff_seconds: tuple[float, ...] = (),
):
    """Build an OAuthAuthenticator with stubbed HTTP + credentials.

    Tests use this to drive the three steps without hitting the network.
    Each stub records the call so the test can assert URL + payload.
    """
    if credentials is None:
        credentials = {"client_id": "client-xyz", "client_secret": "shh"}

    if sso_response is None:
        sso_response = {"access_token": "JWT.ABC.DEF", "expires_in": 3600}

    if legacy_response is None:
        legacy_response = {
            "body": {},
            "cookies": {"crsd": "ORIG_CRSD", "crud": "CRUD_VALUE"},
        }

    if csrf_response is None:
        csrf_response = (
            {"success": True, "result": "ok"},
            {"crsd": "ROTATED_CRSD", "csrf-token": "CSRF_TOKEN_VAL"},
        )

    calls = {
        "load_credentials": [],
        "sso": [],
        "legacy": [],
        "csrf": [],
        "sleep": [],
    }

    def _load(org_id):
        calls["load_credentials"].append(org_id)
        return credentials

    def _post_form(url, form, headers):
        calls["sso"].append({"url": url, "form": form, "headers": headers})
        if sso_raises:
            raise sso_raises
        return sso_response

    def _post_json(url, payload, headers):
        calls["legacy"].append({
            "url": url, "payload": payload, "headers": headers,
        })
        if legacy_raises:
            raise legacy_raises
        if legacy_responses is not None:
            # Return the response indexed by call count so a test can
            # drive different behavior across retry attempts.
            idx = len(calls["legacy"]) - 1
            return legacy_responses[min(idx, len(legacy_responses) - 1)]
        return legacy_response

    def _get_csrf(url, headers):
        calls["csrf"].append({"url": url, "headers": headers})
        if csrf_raises:
            raise csrf_raises
        return csrf_response

    auth = OAuthAuthenticator(
        vendor_cfg=vendor_cfg,
        tz_offset_minutes=tz_offset_minutes,
        load_credentials=_load,
        http_post_form=_post_form,
        http_post_json=_post_json,
        http_get_set_cookies=_get_csrf,
        retry_backoff_seconds=retry_backoff_seconds,
        sleep=lambda s: calls["sleep"].append(s),
    )
    return auth, calls


# ----- OAuthAuthenticator: happy path -------------------------------------


def test_oauth_happy_path_produces_populated_session():
    auth, calls = _build_authenticator()
    session = auth.authenticate("demo")

    # Cookies: rotated crsd, original crud, csrf-token, tzoffset
    assert session.cookies == {
        "crsd": "ROTATED_CRSD",
        "crud": "CRUD_VALUE",
        "csrf-token": "CSRF_TOKEN_VAL",
        "tzoffset": "-300",
    }
    # Extra headers: csrf token, ServiceStack-friendly defaults
    assert session.extra_headers == {
        "x-csrf-token": "CSRF_TOKEN_VAL",
        "x-crapi-clientsource": "members-spa",
        "x-requested-with": "XMLHttpRequest",
        "Referer": _DEFAULT_REFERER,
    }
    # JWT retained for diagnostic purposes
    assert session.access_token == "JWT.ABC.DEF"


def test_oauth_step1_posts_correct_sso_form_to_default_url():
    auth, calls = _build_authenticator()
    auth.authenticate("demo")
    assert len(calls["sso"]) == 1
    call = calls["sso"][0]
    assert call["url"] == _DEFAULT_SSO_TOKEN_URL
    assert call["form"] == {
        "grant_type": "client_credentials",
        "client_id": "client-xyz",
        "client_secret": "shh",
        "scope": _DEFAULT_SCOPE,
    }


def test_oauth_step2_posts_jwt_to_legacy_auth_url():
    """No Authorization header — CR's docs explicitly note JWT goes in
    the body, not as a Bearer."""
    auth, calls = _build_authenticator()
    auth.authenticate("demo")
    assert len(calls["legacy"]) == 1
    call = calls["legacy"][0]
    assert call["url"] == _DEFAULT_LEGACY_AUTH_URL
    assert call["payload"] == {"token": "JWT.ABC.DEF"}
    # No Authorization header in the call
    assert "Authorization" not in call["headers"]
    assert "authorization" not in {k.lower() for k in call["headers"]}


def test_oauth_step3_sends_step2_cookies_to_csrf():
    """The CSRF endpoint needs `crsd` + `crud` from step 2 to issue
    the token (and rotate crsd in the process)."""
    auth, calls = _build_authenticator()
    auth.authenticate("demo")
    assert len(calls["csrf"]) == 1
    cookie_header = calls["csrf"][0]["headers"]["Cookie"]
    assert "crsd=ORIG_CRSD" in cookie_header
    assert "crud=CRUD_VALUE" in cookie_header


def test_oauth_credentials_loaded_via_secrets_module():
    auth, calls = _build_authenticator()
    auth.authenticate("demo")
    assert calls["load_credentials"] == ["demo"]


def test_oauth_omits_tzoffset_cookie_when_not_provided():
    """If the runner didn't pass tz_offset_minutes, the cookie is
    simply absent rather than set to a default. Endpoints that
    require it fail downstream — better surface than guessing."""
    auth, _ = _build_authenticator(tz_offset_minutes=None)
    session = auth.authenticate("demo")
    assert "tzoffset" not in session.cookies


# ----- OAuthAuthenticator: failure modes ----------------------------------


def test_credentials_missing_client_id_raises():
    auth, _ = _build_authenticator(credentials={"client_secret": "shh"})
    with pytest.raises(CentralReachAuthError, match="missing client_id"):
        auth.authenticate("demo")


def test_credentials_missing_client_secret_raises():
    auth, _ = _build_authenticator(credentials={"client_id": "abc"})
    with pytest.raises(CentralReachAuthError, match="missing client_id"):
        auth.authenticate("demo")


def test_sso_response_missing_access_token_raises():
    auth, _ = _build_authenticator(sso_response={"expires_in": 3600})
    with pytest.raises(CentralReachAuthError, match="missing access_token"):
        auth.authenticate("demo")


def test_legacy_auth_missing_required_cookies_raises_with_names():
    """Pinned: the error message names which cookie(s) were missing,
    so an operator can diagnose without re-running."""
    auth, _ = _build_authenticator(legacy_response={
        "body": {},
        "cookies": {"crsd": "X"},  # crud missing
    })
    with pytest.raises(CentralReachAuthError) as excinfo:
        auth.authenticate("demo")
    msg = str(excinfo.value)
    assert "crud" in msg


def test_legacy_auth_no_cookies_at_all_raises_with_both_names():
    auth, _ = _build_authenticator(legacy_response={
        "body": {},
        "cookies": {},
    })
    with pytest.raises(CentralReachAuthError) as excinfo:
        auth.authenticate("demo")
    msg = str(excinfo.value)
    assert "crsd" in msg
    assert "crud" in msg


def test_csrf_body_success_not_true_raises():
    auth, _ = _build_authenticator(csrf_response=(
        {"success": False, "result": "ok"},
        {"csrf-token": "X"},  # token present but body says fail
    ))
    with pytest.raises(CentralReachAuthError, match="success != true"):
        auth.authenticate("demo")


def test_csrf_endpoint_did_not_set_csrf_token_cookie():
    """CR set some cookies but not `csrf-token`. Indicates the endpoint
    shape changed; surface specifically rather than as a generic auth
    error."""
    auth, _ = _build_authenticator(csrf_response=(
        {"success": True, "result": "ok"},
        {"crsd": "ROTATED"},  # no csrf-token
    ))
    with pytest.raises(CentralReachAuthError, match="csrf-token"):
        auth.authenticate("demo")


def test_sso_http_error_propagates_as_auth_error():
    auth, _ = _build_authenticator(
        sso_raises=CentralReachAuthError("CR SSO returned HTTP 401"),
    )
    with pytest.raises(CentralReachAuthError, match="HTTP 401"):
        auth.authenticate("demo")


# ----- OAuthAuthenticator: sandbox overrides ------------------------------


def test_sandbox_override_swaps_sso_and_legacy_urls():
    """A per-org `vendor_settings.centralreach.base_overrides` flips
    the SSO + legacy-auth URLs to sandbox tenant hostnames. Other URLs
    (CSRF + Referer) stay on the documented prod endpoints unless
    explicitly overridden."""
    vendor_cfg = {
        "base_overrides": {
            "sso_token_url":
                "https://sandbox-login.centralreach.com/connect/token",
            "legacy_auth_url":
                "https://sandbox-members.centralreach.com/api/?framework.authtoken",
        },
    }
    auth, calls = _build_authenticator(vendor_cfg=vendor_cfg)
    auth.authenticate("demo")
    assert calls["sso"][0]["url"] == \
        "https://sandbox-login.centralreach.com/connect/token"
    assert calls["legacy"][0]["url"] == \
        "https://sandbox-members.centralreach.com/api/?framework.authtoken"


def test_scope_override_passes_through_to_sso():
    """A per-org scope override flips the OAuth scope sent in step 1."""
    auth, calls = _build_authenticator(
        vendor_cfg={"scope": "custom-scope"},
    )
    auth.authenticate("demo")
    assert calls["sso"][0]["form"]["scope"] == "custom-scope"


def test_default_scope_is_cr_api_when_no_override():
    auth, calls = _build_authenticator()
    auth.authenticate("demo")
    assert calls["sso"][0]["form"]["scope"] == "cr-api"


# ----- OAuthAuthenticator: retry on transient auth failure ---------------


def test_retry_recovers_from_transient_missing_cookies():
    """Legacy-auth's first response has no cookies (the failure mode
    from the production incident). The second attempt succeeds and the
    caller sees a normal session."""
    good_legacy = {
        "body": {},
        "cookies": {"crsd": "ORIG_CRSD", "crud": "CRUD_VALUE"},
    }
    auth, calls = _build_authenticator(
        legacy_responses=[{"body": {}, "cookies": {}}, good_legacy],
        retry_backoff_seconds=(0.5,),
    )
    session = auth.authenticate("demo")
    assert session.access_token == "JWT.ABC.DEF"
    assert len(calls["legacy"]) == 2
    assert calls["sleep"] == [0.5]


def test_retry_exhausted_raises_last_error():
    """Every attempt fails the missing-cookie check; the final error
    surfaces after all retries, and we slept once per retry."""
    auth, calls = _build_authenticator(
        legacy_response={"body": {}, "cookies": {}},
        retry_backoff_seconds=(0.5, 1.0),
    )
    with pytest.raises(CentralReachAuthError, match="missing required cookie"):
        auth.authenticate("demo")
    assert len(calls["legacy"]) == 3
    assert calls["sleep"] == [0.5, 1.0]


def test_happy_path_does_not_sleep():
    """A successful first attempt must not delay the caller."""
    auth, calls = _build_authenticator(retry_backoff_seconds=(60.0, 60.0))
    auth.authenticate("demo")
    assert calls["sleep"] == []
