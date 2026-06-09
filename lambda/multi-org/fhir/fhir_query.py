import os
import threading
import time

from audit import SystemPrincipal, emit as _audit_emit

from .config import load_fhir_config
from .credible_client import CredibleFhirClient
from .exceptions import FhirAuthError, FhirOrgNotConfigured
from .kms_resolver import resolve_alias

# Module-level principal — every FHIR fetch from this Lambda is attributed
# to the Lambda function name. The poller and materializer can override
# the actor by calling audit.emit themselves at a higher layer if they
# want richer attribution (e.g. linking a fetch to the validation_run_id
# that triggered it). The point of the emit here is that no FHIR fetch
# can happen without leaving an audit trail.
_PRINCIPAL = SystemPrincipal(
    os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "fhir-query")
)


_VENDORS = {
    'credible': CredibleFhirClient,
}

_clients = {}
_clients_lock = threading.Lock()


def get_client(org_id):
    with _clients_lock:
        client = _clients.get(org_id)
        if client is not None:
            return client
    fhir_config = load_fhir_config(org_id)
    vendor = fhir_config.get('vendor')
    if vendor not in _VENDORS:
        raise FhirOrgNotConfigured(f"unknown FHIR vendor '{vendor}' for org={org_id}")

    client_id = fhir_config.get('client_id')
    kms_alias = fhir_config.get('kms_alias')
    if not client_id or not kms_alias:
        raise FhirAuthError(
            f"FHIR_CONFIG for org={org_id} missing client_id and/or kms_alias"
        )

    resolved = resolve_alias(kms_alias)
    credentials = {
        'client_id': client_id,
        'kid': resolved['kid'],
        'kms_key_arn': resolved['kms_key_arn'],
    }
    client = _VENDORS[vendor](org_id, fhir_config, credentials)
    with _clients_lock:
        _clients.setdefault(org_id, client)
        return _clients[org_id]


def get_resource(org_id, resource_type, resource_id):
    """Fetch a single FHIR resource. Emits one audit event per call,
    success or failure."""
    started = time.monotonic()
    actor = _PRINCIPAL.as_actor()
    try:
        resource = get_client(org_id).get_resource(resource_type, resource_id)
        _audit_emit(
            action="read",
            resource={"type": resource_type, "id": resource_id, "org": org_id},
            actor=actor,
            org_id=org_id,
            purpose_of_use="OPERATIONS",
            call_type="fhir_fetch",
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        return resource
    except Exception as e:
        _audit_emit(
            action="read",
            resource={"type": resource_type, "id": resource_id, "org": org_id},
            actor=actor,
            org_id=org_id,
            outcome="major-failure",
            purpose_of_use="OPERATIONS",
            call_type="fhir_fetch",
            error_class=type(e).__name__,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        raise


def search(org_id, resource_type, params, *, max_results=None, max_pages=None):
    """Bulk FHIR search. Emits one audit event per yielded resource so an
    Athena query for "every patient touched by this search" stays accurate.

    Emitting at the wrapper (not per-page inside `_request_json_url`) keeps
    the audit trail aligned with what the caller actually consumed —
    a max_results cap or early `break` correctly truncates the trail too.
    """
    actor = _PRINCIPAL.as_actor()
    for resource in get_client(org_id).search(
        resource_type, params, max_results=max_results, max_pages=max_pages
    ):
        _audit_emit(
            action="read",
            resource={"type": resource_type, "id": resource.get("id"),
                      "org": org_id},
            actor=actor,
            org_id=org_id,
            purpose_of_use="OPERATIONS",
            call_type="fhir_search",
        )
        yield resource


def reset_clients_for_tests():
    with _clients_lock:
        _clients.clear()
