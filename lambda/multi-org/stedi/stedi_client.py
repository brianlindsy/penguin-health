"""HTTP client for the Stedi healthcare APIs.

Three endpoints:
  POST /change/medicalnetwork/eligibility/v3   — synchronous 270/271 check
  POST /insurance-discovery/check/v1            — demographic search across payers
  POST /coordination-of-benefits/check/v1       — primacy ranking for ≥2 active coverages

Auth is a static API key in the Authorization header (one per Stedi account).
We always forward X-Forwarded-For from the caller's IP — CMS requires this
for payer requests after 2025-11-08.

Logging redacts SSN before printing the request body. We never log Stedi
response bodies in CloudWatch (they contain PHI); only status, duration,
and the Stedi controlNumber (their trace id).
"""

import copy
import json
import logging
import time
import urllib.error
import urllib.request

from .exceptions import (
    StediAuthError,
    StediBadRequest,
    StediRateLimited,
    StediUpstreamError,
)


logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = 'https://healthcare.us.stedi.com'
_DEFAULT_CONNECT_TIMEOUT = 10
_DEFAULT_READ_TIMEOUT = 30
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 0.5


class StediClient:
    def __init__(self, api_key, *, base_url=None, client_ip=None):
        self.api_key = api_key
        self.base_url = (base_url or _DEFAULT_BASE_URL).rstrip('/')
        self.client_ip = client_ip

    def check_eligibility(self, payload):
        return self._post('/2024-04-01/change/medicalnetwork/eligibility/v3', payload)

    def check_insurance_discovery(self, payload):
        return self._post('/2024-04-01/insurance-discovery/check/v1', payload)

    def check_coordination_of_benefits(self, payload):
        return self._post('/2024-04-01/coordination-of-benefits/check/v1', payload)

    def _post(self, path, payload):
        url = f"{self.base_url}{path}"
        body_bytes = json.dumps(payload).encode('utf-8')
        headers = {
            # Stedi auth header format: "Key <api_key>" — confirmed against
            # the curl examples in their healthcare API reference.
            'Authorization': f'Key {self.api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        if self.client_ip:
            headers['X-Forwarded-For'] = self.client_ip

        attempt = 0
        start = time.monotonic()
        while True:
            attempt += 1
            request = urllib.request.Request(url, data=body_bytes, method='POST', headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=_DEFAULT_READ_TIMEOUT) as response:
                    response_body = response.read().decode('utf-8')
                    parsed = json.loads(response_body) if response_body else {}
                    self._log_success(path, response.status, time.monotonic() - start, parsed)
                    return parsed
            except urllib.error.HTTPError as e:
                status = e.code
                error_body = e.read().decode('utf-8', errors='replace') if hasattr(e, 'read') else ''
                if status in (400, 422):
                    raise StediBadRequest(f"POST {path} -> {status}: {error_body[:500]}") from e
                if status in (401, 403):
                    raise StediAuthError(f"POST {path} -> {status}") from e
                if status in (429, 500, 502, 503, 504):
                    if attempt >= _MAX_RETRIES:
                        if status == 429:
                            raise StediRateLimited(f"POST {path} -> 429 after {attempt} attempts") from e
                        raise StediUpstreamError(f"POST {path} -> {status} after {attempt} attempts") from e
                    time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                    continue
                raise StediUpstreamError(f"POST {path} -> {status}") from e
            except urllib.error.URLError as e:
                if attempt >= _MAX_RETRIES:
                    raise StediUpstreamError(f"POST {path} -> {e.reason}") from e
                time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue

    def _log_success(self, path, status, duration_s, response):
        control_number = (
            response.get('controlNumber')
            or response.get('meta', {}).get('controlNumber')
            or response.get('discoveryId')
            or response.get('cobId')
        )
        logger.info(
            "stedi_call path=%s status=%s duration_ms=%d control=%s",
            path, status, int(duration_s * 1000), control_number,
        )


def redact_for_logging(payload):
    """Return a deep copy with SSN and full member IDs scrubbed. Used by
    audit + error reporting paths that need to surface request context
    without leaking PHI into CloudWatch."""
    safe = copy.deepcopy(payload)
    _scrub(safe)
    return safe


def _scrub(node):
    if isinstance(node, dict):
        for key in list(node.keys()):
            if key.lower() in ('ssn', 'socialsecuritynumber'):
                node[key] = '***REDACTED***'
            elif key == 'memberId' and isinstance(node[key], str) and len(node[key]) > 4:
                node[key] = '*' * (len(node[key]) - 4) + node[key][-4:]
            else:
                _scrub(node[key])
    elif isinstance(node, list):
        for item in node:
            _scrub(item)
