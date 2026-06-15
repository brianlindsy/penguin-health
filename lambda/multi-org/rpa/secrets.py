"""Per-org RPA bot credentials loader.

Each RPA-enabled org has a Secrets Manager secret containing the
client_id + client_secret issued by the vendor for the bot user account.
The runner reads this once per task invocation; no caching across runs
since Fargate tasks are ephemeral anyway.

The secret name convention `penguin-health/rpa/{org_id}/credentials` keeps
each org's credentials isolated in the wildcard IAM grant
(`secretsmanager:GetSecretValue` on `penguin-health/rpa/*`). Rotation is
operator-driven — a config edit at the vendor's developer portal followed
by a `PutSecretValue` call. No refresh tokens, no token-rotation logic.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .exceptions import RpaAuthError


_secrets_client = boto3.client("secretsmanager")


def _reset_for_tests(client: Any | None = None) -> None:
    """Test hook to rebind the module-level Secrets Manager client to a
    moto-backed one.
    """
    global _secrets_client
    if client is None:
        _secrets_client = boto3.client("secretsmanager")
    else:
        _secrets_client = client


def _secret_name(org_id: str) -> str:
    return f"penguin-health/rpa/{org_id}/credentials"


def load_credentials(org_id: str) -> dict:
    """Return `{"client_id": str, "client_secret": str}` for the given org.

    Raises RpaAuthError if the secret is missing, malformed, or doesn't
    contain both required keys. Never logs the secret payload.
    """
    name = _secret_name(org_id)
    try:
        resp = _secrets_client.get_secret_value(SecretId=name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        raise RpaAuthError(
            f"failed to read credentials for org={org_id}: {code}"
        )

    try:
        payload = json.loads(resp["SecretString"])
    except (KeyError, json.JSONDecodeError):
        raise RpaAuthError(
            f"credentials secret for org={org_id} is not valid JSON"
        )

    client_id = payload.get("client_id")
    client_secret = payload.get("client_secret")
    if not client_id or not client_secret:
        raise RpaAuthError(
            f"credentials secret for org={org_id} missing client_id "
            "or client_secret"
        )
    return {"client_id": client_id, "client_secret": client_secret}
