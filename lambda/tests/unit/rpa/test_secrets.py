"""Tests for rpa.secrets — per-org client_id + client_secret loader."""

import json

import boto3
import pytest
from moto import mock_aws

from rpa import secrets as secrets_mod
from rpa.exceptions import RpaAuthError


@pytest.fixture
def sm():
    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secrets_mod._reset_for_tests(client=client)
        yield client
        secrets_mod._reset_for_tests()


def _put(sm, org_id, payload):
    sm.create_secret(
        Name=f"penguin-health/rpa/{org_id}/credentials",
        SecretString=json.dumps(payload),
    )


def test_load_credentials_returns_payload(sm):
    _put(sm, "demo", {"client_id": "cid-1", "client_secret": "secret-1"})
    out = secrets_mod.load_credentials("demo")
    assert out == {"client_id": "cid-1", "client_secret": "secret-1"}


def test_missing_secret_raises(sm):
    with pytest.raises(RpaAuthError, match="failed to read credentials"):
        secrets_mod.load_credentials("no-such-org")


def test_malformed_json_raises(sm):
    sm.create_secret(
        Name="penguin-health/rpa/demo/credentials",
        SecretString="this is not json",
    )
    with pytest.raises(RpaAuthError, match="not valid JSON"):
        secrets_mod.load_credentials("demo")


def test_missing_client_id_raises(sm):
    _put(sm, "demo", {"client_secret": "s"})
    with pytest.raises(RpaAuthError, match="missing client_id"):
        secrets_mod.load_credentials("demo")


def test_missing_client_secret_raises(sm):
    _put(sm, "demo", {"client_id": "cid"})
    with pytest.raises(RpaAuthError, match="missing client_id or client_secret"):
        secrets_mod.load_credentials("demo")


def test_empty_values_rejected(sm):
    _put(sm, "demo", {"client_id": "", "client_secret": "s"})
    with pytest.raises(RpaAuthError, match="missing client_id"):
        secrets_mod.load_credentials("demo")
