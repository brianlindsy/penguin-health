"""Resource URL endpoint — `POST /api/?resources.getresourceurl`.

Two-step PDF acquisition:

  1. POST `{resourceId, _utcOffsetMinutes}` to the CR endpoint;
     receive a short-lived presigned S3 URL pointing at the rendered
     PDF on `docs.centralreach.com`.
  2. GET the presigned URL; receive raw PDF bytes.

The `resourceId` is the file id from `preview.files[0].id`, NOT the
billing entry id. (Empirically established — see the design doc's
per-entry pipeline section.)

The endpoint has been observed returning HTTP 200 with `success: false`
and no diagnostic info — most commonly when the session is not
authorized to access the requested file. We wrap that case in
`PdfNotAvailable` so the pipeline can skip the entry with a
structured reason rather than treat it as a transport error.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .client import CentralReachClient
from .exceptions import CentralReachError


_RESOURCE_URL_PATH = "/api/?resources.getresourceurl"


class PdfNotAvailable(CentralReachError):
    """The resourceurl endpoint returned `success: false` with no URL.

    Distinguishable from `CentralReachAPIError` (transport failures)
    and `CentralReachValidationError` (bad request shape). Indicates
    the request reached the endpoint and parsed correctly, but CR
    declined to issue a presigned URL for the requested resource.
    Most common when the bot session does not have permission to
    access the file.
    """


@dataclass(frozen=True)
class ResourceUrlResponse:
    """Parsed response from the resourceurl endpoint."""

    url: str
    file_name: str
    cache_expires: str | None  # CR's `cacheExpires` field; informational

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "ResourceUrlResponse":
        url = str(raw.get("url") or "")
        return cls(
            url=url,
            file_name=str(raw.get("fileName") or ""),
            cache_expires=str(raw.get("cacheExpires")) if raw.get("cacheExpires") else None,
        )


def _is_success(raw: dict[str, Any]) -> bool:
    """The resourceurl endpoint uses lowercase `result: "ok"` plus
    `success: true`. This differs from the list-query endpoint's
    `result: "OK"` + `failed: false`. CR is inconsistent across
    endpoints, so we check each endpoint's documented success signal
    explicitly rather than assuming."""
    return bool(raw.get("success")) and bool(raw.get("url"))


def get_resource_url(
    client: CentralReachClient, resource_id: int, *,
    utc_offset_minutes: int,
) -> ResourceUrlResponse:
    """Fetch the presigned S3 URL for one file resource.

    `resource_id` is `preview.files[0].id` — the file's own id, NOT
    the billing entry id.

    Raises `PdfNotAvailable` if CR returns `success: false` without a
    URL (most commonly an access-control issue at the resource level).
    """
    raw = client.post_json(
        _RESOURCE_URL_PATH,
        body={
            "resourceId": resource_id,
            "_utcOffsetMinutes": utc_offset_minutes,
        },
    )
    if not _is_success(raw):
        raise PdfNotAvailable(
            f"CR declined to issue a presigned URL for resource "
            f"{resource_id}: success={raw.get('success')!r}, "
            f"result={raw.get('result')!r}"
        )
    return ResourceUrlResponse.from_json(raw)


def download_pdf(client: CentralReachClient, presigned_url: str) -> bytes:
    """Download the raw PDF bytes from a presigned S3 URL.

    Thin wrapper around `client.get_bytes()` — kept here for symmetry
    with `get_resource_url` so the pipeline imports both from the
    same module.
    """
    return client.get_bytes(presigned_url)


def fetch_pdf_bytes(
    client: CentralReachClient, resource_id: int, *,
    utc_offset_minutes: int,
) -> tuple[ResourceUrlResponse, bytes]:
    """Combined two-step fetch: resourceurl POST then S3 GET.

    Returns the `(response_metadata, pdf_bytes)` tuple. The pipeline
    uses both: response metadata for the file name (used for
    diagnostics) and the bytes for the S3 write.
    """
    resource = get_resource_url(
        client, resource_id, utc_offset_minutes=utc_offset_minutes,
    )
    pdf_bytes = download_pdf(client, resource.url)
    return resource, pdf_bytes
