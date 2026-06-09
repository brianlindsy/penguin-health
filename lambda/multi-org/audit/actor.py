"""Actor identity for audit events.

Two flavors:
  * `from_event(event)` — extracts a human agent from an API GW v2 JWT
    request. Mirrors admin_api.py:get_user_claims for the email/groups
    parsing so the audit record's agent matches the authorization check.
  * `SystemPrincipal(name)` — synthesizes a system agent for event-driven
    Lambdas (S3 / SNS / EventBridge / cron) where there is no user.

Both return the same dict shape so emit() doesn't branch on actor type.
"""

from __future__ import annotations


def from_event(event: dict | None) -> dict:
    """Pull agent identity from an API Gateway HTTP API v2 event.

    Returns the canonical actor dict even if fields are missing — the
    audit row will then carry nulls, and the caller can decide whether
    to fall back to a SystemPrincipal.
    """
    event = event or {}
    request_context = event.get("requestContext") or {}
    authorizer = request_context.get("authorizer") or {}
    jwt_claims = (authorizer.get("jwt") or {}).get("claims") or {}

    # cognito:groups comes back as either "[Admins, Users]" (string) or as
    # a list, depending on which Cognito version emitted the token. This
    # matches the parsing in admin_api.py:get_user_claims line 76-80.
    groups_raw = jwt_claims.get("cognito:groups", [])
    if isinstance(groups_raw, str):
        groups = [
            g.strip() for g in groups_raw.strip("[]").split(",") if g.strip()
        ]
    else:
        groups = list(groups_raw or [])

    http = request_context.get("http") or {}

    return {
        "agent_type": "human",
        # sub is always present in a valid JWT; email may be absent on
        # service-to-service tokens. Prefer email for human-readable
        # audit rows; fall back to sub so we never emit a null agent_id.
        "agent_id": jwt_claims.get("email") or jwt_claims.get("sub"),
        "agent_email": jwt_claims.get("email"),
        "agent_groups": groups,
        "client_ip": http.get("sourceIp"),
        "user_agent": http.get("userAgent"),
    }


class SystemPrincipal:
    """Synthetic agent for system-triggered Lambdas.

    Construct once at module load to avoid per-invocation allocations:

        principal = SystemPrincipal("fhir-eligibility-poller")

    Then pass `principal.as_actor()` to `emit(...)`.
    """

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("SystemPrincipal requires a non-empty name")
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def as_actor(self) -> dict:
        return {
            "agent_type": "system",
            "agent_id": self._name,
            "agent_email": None,
            "agent_groups": [],
            "client_ip": None,
            "user_agent": None,
        }
