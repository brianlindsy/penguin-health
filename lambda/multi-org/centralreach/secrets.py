"""Per-org centralreach bot credentials loader.

Each centralreach-enabled org has a Secrets Manager secret containing
the credentials the bot's authenticator uses. The runner reads this
once per task invocation; no caching across runs since Fargate tasks
are ephemeral anyway.

Credential payload shape depends on the auth flow the runner ends up
using. The OAuth `client_credentials` shape is `{client_id, client_secret}`;
a ServiceStack login flow would carry `{username, password}` instead.
Both shapes are accepted — the authenticator inspects which keys are
present. See the design doc's Open Questions section on the auth gap.

Secret name convention `penguin-health/centralreach/{org_id}/credentials`
keeps each org's credentials isolated in the wildcard IAM grant
(`secretsmanager:GetSecretValue` on `penguin-health/centralreach/*`).
Rotation is operator-driven — a config edit at the vendor's developer
portal followed by a `PutSecretValue` call.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .exceptions import CentralReachAuthError


_secrets_client = boto3.client("secretsmanager")


def _reset_for_tests(client: Any | None = None) -> None:
    """Test hook to rebind the module-level Secrets Manager client to
    a moto-backed one."""
    global _secrets_client
    if client is None:
        _secrets_client = boto3.client("secretsmanager")
    else:
        _secrets_client = client


def _secret_name(org_id: str) -> str:
    return f"penguin-health/centralreach/{org_id}/credentials"


def load_credentials(org_id: str) -> dict:
    """Return the credentials payload for `org_id`.

    Returned dict is the parsed Secrets Manager `SecretString` —
    the authenticator inspects which keys are present to choose
    its auth flow. Raises `CentralReachAuthError` if the secret is
    missing or unparseable. Never logs the payload.
    """
    name = _secret_name(org_id)
    try:
        resp = _secrets_client.get_secret_value(SecretId=name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        raise CentralReachAuthError(
            f"failed to read credentials for org={org_id}: {code}",
        )

    try:
        payload = json.loads(resp["SecretString"])
    except (KeyError, json.JSONDecodeError):
        raise CentralReachAuthError(
            f"credentials secret for org={org_id} is not valid JSON",
        )

    if not isinstance(payload, dict) or not payload:
        raise CentralReachAuthError(
            f"credentials secret for org={org_id} must be a non-empty object",
        )
    return payload
