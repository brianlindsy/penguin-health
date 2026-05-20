import threading

from .config import load_fhir_config
from .credible_client import CredibleFhirClient
from .exceptions import FhirAuthError, FhirOrgNotConfigured
from .kms_resolver import resolve_alias


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
    return get_client(org_id).get_resource(resource_type, resource_id)


def search(org_id, resource_type, params, *, max_results=None, max_pages=None):
    yield from get_client(org_id).search(
        resource_type, params, max_results=max_results, max_pages=max_pages
    )


def reset_clients_for_tests():
    with _clients_lock:
        _clients.clear()
