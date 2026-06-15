"""Tests for the vendor-dispatch entry point `rpa.authenticator.authenticate`."""

import json

import boto3
import pytest
from moto import mock_aws

from rpa import authenticator, secrets
from rpa.authenticators import REGISTRY
from rpa.exceptions import RpaAuthError, RpaUnsupportedVendor


@pytest.fixture
def sm():
    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secrets._reset_for_tests(client=client)
        client.create_secret(
            Name="penguin-health/rpa/demo/credentials",
            SecretString=json.dumps(
                {"client_id": "cid-1", "client_secret": "secret-1"}
            ),
        )
        yield client
        secrets._reset_for_tests()


def test_dispatches_to_centralreach(sm, monkeypatch):
    seen = {}

    def fake_cr_authenticate(*, org_id, vendor_cfg, credentials):
        seen.update({
            "org_id": org_id,
            "vendor_cfg": vendor_cfg,
            "credentials": credentials,
        })
        return {"cookies": [{"name": "X"}], "extra_http_headers": {},
                "access_token": "JWT"}

    monkeypatch.setitem(REGISTRY, "centralreach", fake_cr_authenticate)

    # The CR-specific shape only has overrides (rare) and scope (optional).
    # Production orgs typically pass an empty dict; this test exercises the
    # override path so the plumbing is verified.
    org_cfg = {
        "vendor": "centralreach",
        "vendor_settings": {
            "centralreach": {
                "scope": "cr-api custom-scope",
                "base_overrides": {
                    "sso_token_url": "https://sandbox-login.centralreach.com/connect/token",
                },
            },
        },
    }
    out = authenticator.authenticate("demo", org_cfg)

    assert out["access_token"] == "JWT"
    assert seen["org_id"] == "demo"
    assert seen["vendor_cfg"]["scope"] == "cr-api custom-scope"
    assert seen["vendor_cfg"]["base_overrides"]["sso_token_url"] == (
        "https://sandbox-login.centralreach.com/connect/token"
    )
    assert seen["credentials"] == {"client_id": "cid-1",
                                   "client_secret": "secret-1"}


def test_unknown_vendor_raises(sm):
    with pytest.raises(RpaUnsupportedVendor, match="no authenticator"):
        authenticator.authenticate("demo", {"vendor": "myavatar"})


def test_no_vendor_in_config_raises(sm):
    with pytest.raises(RpaAuthError, match="no vendor"):
        authenticator.authenticate("demo", {"vendor_settings": {}})


def test_missing_vendor_settings_passes_empty_dict(sm, monkeypatch):
    """When vendor_settings is absent, the per-vendor authenticator still
    gets called with an empty dict — vendor module is responsible for
    raising on required keys, not the dispatcher.
    """
    captured = {}

    def fake(*, org_id, vendor_cfg, credentials):
        captured["vendor_cfg"] = vendor_cfg
        return {"cookies": [], "extra_http_headers": {}, "access_token": "X"}

    monkeypatch.setitem(REGISTRY, "centralreach", fake)
    authenticator.authenticate("demo", {"vendor": "centralreach"})
    assert captured["vendor_cfg"] == {}
