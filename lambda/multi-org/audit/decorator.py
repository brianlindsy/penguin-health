"""`@audited` — decorator for HTTP handlers that touch PHI.

Wraps the function call with:
  * actor extraction from JWT claims (via actor.from_event)
  * resource_id resolution from path / body
  * patient extraction from request body (opt-in)
  * outcome computation from HTTP status code or exception
  * timing

Use on the destination handler, not on the wrapping dispatch lambdas in
admin_api.py:177-204. The dispatch lambdas pass `event=`, `path_params=`,
`body=` as kwargs, so decorating the destination keeps the contract clean.

Exceptions from the wrapped handler are re-raised AFTER the audit row
is written — failures must always be logged, even ones that interrupt
the response path. Audit emission itself never raises (see emitter.py).
"""

from __future__ import annotations

import functools
import time

from .actor import from_event
from .emitter import emit
from .schema import (
    OUTCOME_MAJOR_FAILURE,
    outcome_for_status,
)


def audited(action: str,
            resource_type: str,
            *,
            resource_from_path: str | None = None,
            resource_from_body: str | None = None,
            purpose_of_use: str = "OPERATIONS",
            call_type: str | None = None,
            patient_from_body: bool = False):
    """Decorate an HTTP handler with audit-event emission.

    Args:
        action: One of audit.schema.ACTION_*.
        resource_type: e.g. "Coverage", "ValidationResult", "Encounter".
        resource_from_path: Path-parameter key to use as resource_id
            (e.g. "encounterId").
        resource_from_body: Body-key fallback if the resource id lives
            in the request body instead of the path.
        purpose_of_use: One of audit.schema.PURPOSE_*.
        call_type: Free-form sub-categorization (e.g. "eligibility").
        patient_from_body: When True, pull first_name/last_name/dob from
            the request body to populate the patient_hash + initials.

    The wrapped function must accept `event`, `path_params`, and `body`
    as kwargs — which is how admin_api.py:lambda_handler dispatches.
    """

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(event=None, path_params=None, body=None, **kw):
            path_params = path_params or {}
            actor = from_event(event)
            org_id = path_params.get("orgId") or ""
            resource_id = None
            if resource_from_path:
                resource_id = path_params.get(resource_from_path)
            elif resource_from_body and isinstance(body, dict):
                resource_id = body.get(resource_from_body)

            patient = None
            if patient_from_body and isinstance(body, dict):
                # Tolerate missing keys — the resulting hash is over
                # empty strings, which is fine for an audit row even
                # when the upstream handler is going to 400.
                patient = {
                    "first_name": body.get("first_name"),
                    "last_name": body.get("last_name"),
                    "dob": body.get("dob"),
                }

            started = time.monotonic()
            try:
                response = fn(event=event, path_params=path_params,
                              body=body, **kw)
            except Exception as e:
                duration_ms = int((time.monotonic() - started) * 1000)
                emit(
                    action=action,
                    resource={"type": resource_type, "id": resource_id,
                              "org": org_id},
                    actor=actor,
                    org_id=org_id,
                    outcome=OUTCOME_MAJOR_FAILURE,
                    purpose_of_use=purpose_of_use,
                    patient=patient,
                    call_type=call_type,
                    error_class=type(e).__name__,
                    duration_ms=duration_ms,
                )
                raise

            duration_ms = int((time.monotonic() - started) * 1000)
            status = (response.get("statusCode", 200)
                      if isinstance(response, dict) else 200)
            emit(
                action=action,
                resource={"type": resource_type, "id": resource_id,
                          "org": org_id},
                actor=actor,
                org_id=org_id,
                outcome=outcome_for_status(status),
                purpose_of_use=purpose_of_use,
                patient=patient,
                call_type=call_type,
                http_status=status,
                duration_ms=duration_ms,
            )
            return response

        return wrapper

    return deco
