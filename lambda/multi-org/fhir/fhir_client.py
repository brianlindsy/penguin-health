import json
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

from .exceptions import (
    FhirAuthError,
    FhirNotFound,
    FhirRateLimited,
    FhirUpstreamError,
)


_TOKEN_SAFETY_MARGIN_SECONDS = 60
_DEFAULT_CONNECT_TIMEOUT = 10
_DEFAULT_READ_TIMEOUT = 30
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 0.5

_org_semaphores = {}
_semaphores_lock = threading.Lock()


def _get_org_semaphore(org_id, concurrency):
    with _semaphores_lock:
        sem = _org_semaphores.get(org_id)
        if sem is None:
            sem = threading.BoundedSemaphore(concurrency)
            _org_semaphores[org_id] = sem
        return sem


class FhirClient:
    def __init__(self, org_id, fhir_config, credentials):
        self.org_id = org_id
        self.config = fhir_config
        self.credentials = credentials
        self.base_url = fhir_config['base_url'].rstrip('/')
        self.page_size = int(fhir_config.get('page_size', 100))
        self.concurrency = int(fhir_config.get('concurrency', 4))
        self._token = None
        self._token_expires_at = 0
        self._token_lock = threading.Lock()
        self._semaphore = _get_org_semaphore(org_id, self.concurrency)

    def authenticate(self):
        raise NotImplementedError

    def _ensure_token(self):
        now = time.time()
        if self._token and now < self._token_expires_at - _TOKEN_SAFETY_MARGIN_SECONDS:
            return self._token
        with self._token_lock:
            now = time.time()
            if self._token and now < self._token_expires_at - _TOKEN_SAFETY_MARGIN_SECONDS:
                return self._token
            token, expires_in = self.authenticate()
            self._token = token
            self._token_expires_at = time.time() + expires_in
            return token

    def _invalidate_token(self):
        with self._token_lock:
            self._token = None
            self._token_expires_at = 0

    def get_resource(self, resource_type, resource_id):
        path = f"/{resource_type}/{urllib.parse.quote(resource_id, safe='')}"
        return self._request_json('GET', path)

    def search(self, resource_type, params, *, max_results=None, max_pages=None):
        params = dict(params or {})
        params.setdefault('_count', self.page_size)
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{self.base_url}/{resource_type}?{query}"

        seen = 0
        page = 0
        while url is not None:
            bundle = self._request_json_url('GET', url)
            page += 1
            for entry in bundle.get('entry') or []:
                resource = entry.get('resource')
                if resource is None:
                    continue
                yield resource
                seen += 1
                if max_results is not None and seen >= max_results:
                    return
            if max_pages is not None and page >= max_pages:
                return
            url = _next_link(bundle)

    def _request_json(self, method, path):
        return self._request_json_url(method, f"{self.base_url}{path}")

    def _request_json_url(self, method, url):
        body, status = self._request_with_retry(method, url)
        return json.loads(body) if body else {}

    def _request_with_retry(self, method, url, _retried_401=False):
        token = self._ensure_token()
        request = urllib.request.Request(
            url,
            method=method,
            headers={
                'Authorization': f'Bearer {token}',
                'Accept': 'application/fhir+json',
            },
        )

        attempt = 0
        while True:
            attempt += 1
            with self._semaphore:
                try:
                    with urllib.request.urlopen(request, timeout=_DEFAULT_READ_TIMEOUT) as response:
                        return response.read().decode('utf-8'), response.status
                except urllib.error.HTTPError as e:
                    status = e.code
                    if status == 404:
                        raise FhirNotFound(f"{method} {url} -> 404") from e
                    if status == 401 and not _retried_401:
                        self._invalidate_token()
                        return self._request_with_retry(method, url, _retried_401=True)
                    if status == 401:
                        raise FhirAuthError(f"{method} {url} -> 401 after token refresh") from e
                    if status in (429, 500, 502, 503, 504):
                        if attempt >= _MAX_RETRIES:
                            if status == 429:
                                raise FhirRateLimited(f"{method} {url} -> 429 after {attempt} attempts") from e
                            raise FhirUpstreamError(f"{method} {url} -> {status} after {attempt} attempts") from e
                        time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                        continue
                    raise FhirUpstreamError(f"{method} {url} -> {status}") from e
                except urllib.error.URLError as e:
                    if attempt >= _MAX_RETRIES:
                        raise FhirUpstreamError(f"{method} {url} -> {e.reason}") from e
                    time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                    continue


def _next_link(bundle):
    for link in bundle.get('link') or []:
        if link.get('relation') == 'next' and link.get('url'):
            return link['url']
    return None
