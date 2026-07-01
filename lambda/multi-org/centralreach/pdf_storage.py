"""Write PDF bytes to the per-org S3 bucket.

Symmetric with the JSON record path (`data/{date}/{ts}__{id}.json`):

  Bucket: penguin-health-{org_id}
  Key:    pdfs/{ingest_date}/{captured_at_compact}__{source_record_id}.pdf

Side-by-side prefixes on the same bucket — JSON records and PDFs for
the same entry share basename and date partition, which makes
operational queries against S3 inventory simpler.

Object metadata:
  x-amz-meta-source-record-id : the record's source_record_id
  x-amz-meta-template-id       : the CR templateId from preview

Both are bytewise non-PHI (ids, not values). We deliberately do NOT
attach patient identity to S3 object metadata: CloudTrail data events
log metadata in plaintext when enabled, and bucket-level KMS encryption
applies only to object bodies.

See the design doc's "PDF storage" section for the full bucket, key,
IAM, and lifecycle spec.
"""

from __future__ import annotations

from typing import Any

import boto3


_PDF_PREFIX = "pdfs"


def _bucket_for_org(org_id: str) -> str:
    """Mirror of `rpa/result_writer.py::_bucket_for_org` and
    `lambda/api/nl_agent_tools.py::org_data_bucket`. Per-org bucket
    convention.
    """
    return f"penguin-health-{org_id}"


def pdf_s3_key(
    *, ingest_date: str, captured_at_compact: str, source_record_id: str,
) -> str:
    """Build the S3 key for a PDF.

    `ingest_date` is the YYYY-MM-DD partition; `captured_at_compact`
    is the per-object timestamp (YYYYMMDDTHHMMSSZ); `source_record_id`
    is CR's billing entry id with no prefix.

    Mirrors the JSON record key shape from `rpa/result_writer.py::s3_key_for`:
        records: data/{ingest_date}/{captured_at_compact}__{source_record_id}.json
        pdfs:    pdfs/{ingest_date}/{captured_at_compact}__{source_record_id}.pdf
    """
    return (
        f"{_PDF_PREFIX}/{ingest_date}/"
        f"{captured_at_compact}__{source_record_id}.pdf"
    )


def write_pdf(
    *,
    org_id: str,
    source_record_id: str,
    template_id: int | None,
    ingest_date: str,
    captured_at_compact: str,
    pdf_bytes: bytes,
    s3_client: Any | None = None,
) -> str:
    """Write PDF bytes to the per-org bucket and return the full S3 key.

    Bucket-level KMS encryption applies automatically; no per-object
    override is needed. Object metadata records the source record id
    and template id for ops queries against S3 inventory.

    The S3 client is injectable so tests can supply a moto-backed
    client. Production callers omit it and the module-level client is
    used.
    """
    bucket = _bucket_for_org(org_id)
    key = pdf_s3_key(
        ingest_date=ingest_date,
        captured_at_compact=captured_at_compact,
        source_record_id=source_record_id,
    )

    metadata: dict[str, str] = {
        "source-record-id": str(source_record_id),
    }
    if template_id is not None:
        metadata["template-id"] = str(template_id)

    client = s3_client if s3_client is not None else boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        Metadata=metadata,
    )
    return key
