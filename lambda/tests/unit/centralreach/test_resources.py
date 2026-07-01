"""Tests for centralreach.resources.

Pins three contracts:
  1. The request body sends `resourceId` + `_utcOffsetMinutes`
  2. `success: true` + `url` parses to ResourceUrlResponse
  3. `success: false` raises PdfNotAvailable (not a generic API error)
"""

from __future__ import annotations

from typing import Any

import pytest

from centralreach.resources import (
    PdfNotAvailable,
    ResourceUrlResponse,
    download_pdf,
    fetch_pdf_bytes,
    get_resource_url,
)


# ----- fixtures -------------------------------------------------------------


_SUCCESS_RESPONSE = {
    "fileName": "2026/6/28/some-id.pdf",
    "querytime": 0,
    "cacheDate": "06/28/2026 23:22:47",
    "preventSeek": False,
    "result": "ok",
    "cacheKey": "",
    "url": "https://s3.amazonaws.com/docs.centralreach.com/2026/6/28/id.pdf?AWSAccessKeyId=KEY&Expires=12345&Signature=SIG",
    "requesttime": 9,
    "EncodingFormats": "",
    "success": True,
    "latestTimeViewed": 0,
    "cacheExpires": "06/29/2026 02:22:47",
    "name": "[REDACTED]",
}


_FAILURE_RESPONSE = {
    "cacheExpires": "06/30/2026 02:36:08",
    "success": False,
    "requesttime": 15,
    "cacheKey": "",
    "cacheDate": "06/29/2026 23:49:54",
    "querytime": 0,
    "result": "ok",
    # Note: result is still "ok" — the success signal is `success`,
    # not `result`, on this endpoint.
}


class _StubClient:
    def __init__(self, post_payload=None, bytes_payload=None):
        self._post_payload = post_payload
        self._bytes_payload = bytes_payload
        self.last_post_path: str | None = None
        self.last_post_body: dict[str, Any] | None = None
        self.last_get_bytes_url: str | None = None

    def post_json(self, path, body=None):
        self.last_post_path = path
        self.last_post_body = body
        return self._post_payload

    def get_bytes(self, url):
        self.last_get_bytes_url = url
        return self._bytes_payload


# ----- request shape -------------------------------------------------------


def test_request_uses_correct_path_and_body():
    client = _StubClient(post_payload=_SUCCESS_RESPONSE)
    get_resource_url(client, resource_id=42, utc_offset_minutes=300)
    assert client.last_post_path == "/api/?resources.getresourceurl"
    assert client.last_post_body == {
        "resourceId": 42,
        "_utcOffsetMinutes": 300,
    }


# ----- success path --------------------------------------------------------


def test_success_response_parses_url_and_metadata():
    client = _StubClient(post_payload=_SUCCESS_RESPONSE)
    response = get_resource_url(client, resource_id=42, utc_offset_minutes=300)
    assert isinstance(response, ResourceUrlResponse)
    assert response.url.startswith("https://s3.amazonaws.com/")
    assert response.file_name == "2026/6/28/some-id.pdf"
    assert response.cache_expires == "06/29/2026 02:22:47"


# ----- failure path --------------------------------------------------------


def test_failure_response_raises_pdf_not_available():
    """`success: false` with no URL is a real production case — the
    bot session does not have permission to access the requested
    file. Distinct from transport errors; pipeline skips with
    no_pdf_available."""
    client = _StubClient(post_payload=_FAILURE_RESPONSE)
    with pytest.raises(PdfNotAvailable) as excinfo:
        get_resource_url(client, resource_id=42, utc_offset_minutes=300)
    assert "42" in str(excinfo.value)


def test_failure_when_success_true_but_url_empty():
    """Defense in depth: `success: true` plus empty URL is also a
    failure. Should never happen but we don't want to silently return
    an empty URL the runner then tries to GET."""
    payload = {**_SUCCESS_RESPONSE, "url": ""}
    client = _StubClient(post_payload=payload)
    with pytest.raises(PdfNotAvailable):
        get_resource_url(client, resource_id=42, utc_offset_minutes=300)


# ----- pdf download --------------------------------------------------------


def test_download_pdf_delegates_to_client_get_bytes():
    pdf_bytes = b"%PDF-1.4\nfake\n%%EOF"
    client = _StubClient(bytes_payload=pdf_bytes)
    out = download_pdf(client, "https://s3.amazonaws.com/x.pdf")
    assert out == pdf_bytes
    assert client.last_get_bytes_url == "https://s3.amazonaws.com/x.pdf"


# ----- combined two-step fetch ---------------------------------------------


def test_fetch_pdf_bytes_does_both_steps():
    pdf_bytes = b"%PDF-1.4\nfake\n%%EOF"
    client = _StubClient(post_payload=_SUCCESS_RESPONSE, bytes_payload=pdf_bytes)
    resource, body = fetch_pdf_bytes(
        client, resource_id=42, utc_offset_minutes=300,
    )
    assert isinstance(resource, ResourceUrlResponse)
    assert resource.file_name == "2026/6/28/some-id.pdf"
    assert body == pdf_bytes
    # Both API calls fired
    assert client.last_post_path == "/api/?resources.getresourceurl"
    assert client.last_get_bytes_url.startswith("https://s3.amazonaws.com/")


def test_fetch_pdf_bytes_does_not_download_when_url_unavailable():
    """If the resourceurl call fails, no download attempt should fire.
    Otherwise we'd waste a GET on an invalid URL and possibly mask the
    real cause of the failure."""
    client = _StubClient(post_payload=_FAILURE_RESPONSE)
    with pytest.raises(PdfNotAvailable):
        fetch_pdf_bytes(client, resource_id=42, utc_offset_minutes=300)
    assert client.last_get_bytes_url is None
