"""Tests for centralreach.pdf_storage — writes PDF bytes to per-org S3.

Uses moto to mock S3. Pins:
  1. Key shape matches the design (`pdfs/{date}/{ts}__{id}.pdf`)
  2. Object body and Content-Type land in S3 unchanged
  3. Object metadata carries source-record-id and template-id; NOT
     patient identity
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from centralreach.pdf_storage import pdf_s3_key, write_pdf


@pytest.fixture
def s3_client():
    """moto-backed S3 client with the per-org bucket pre-created."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


# ----- key shape -----------------------------------------------------------


def test_pdf_s3_key_shape_matches_record_key_shape():
    """Pin: PDF key must mirror the JSON record key shape so PDFs
    and records for the same entry sit side-by-side under matching
    basenames."""
    key = pdf_s3_key(
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        source_record_id="1234",
    )
    assert key == "pdfs/2026-06-28/20260628T220000Z__1234.pdf"


def test_pdf_s3_key_uses_pdfs_prefix_not_data_prefix():
    """The records prefix is `data/`; PDFs must use `pdfs/` so the
    rules engine's `data/` reads don't accidentally pick up PDFs."""
    key = pdf_s3_key(
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        source_record_id="1234",
    )
    assert key.startswith("pdfs/")
    assert not key.startswith("data/")


# ----- write_pdf -----------------------------------------------------------


def test_write_pdf_uploads_to_per_org_bucket(s3_client):
    pdf_bytes = b"%PDF-1.4\nhello world\n%%EOF"
    key = write_pdf(
        org_id="demo",
        source_record_id="1234",
        template_id=113875,
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        pdf_bytes=pdf_bytes,
        s3_client=s3_client,
    )
    assert key == "pdfs/2026-06-28/20260628T220000Z__1234.pdf"
    response = s3_client.get_object(Bucket="penguin-health-demo", Key=key)
    assert response["Body"].read() == pdf_bytes


def test_write_pdf_sets_content_type_application_pdf(s3_client):
    write_pdf(
        org_id="demo",
        source_record_id="1234",
        template_id=113875,
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        pdf_bytes=b"%PDF-1.4\n",
        s3_client=s3_client,
    )
    response = s3_client.get_object(
        Bucket="penguin-health-demo",
        Key="pdfs/2026-06-28/20260628T220000Z__1234.pdf",
    )
    assert response["ContentType"] == "application/pdf"


def test_write_pdf_attaches_metadata(s3_client):
    """Object metadata carries source-record-id and template-id. Both
    are bytewise non-PHI."""
    write_pdf(
        org_id="demo",
        source_record_id="1234",
        template_id=113875,
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        pdf_bytes=b"%PDF-1.4\n",
        s3_client=s3_client,
    )
    response = s3_client.head_object(
        Bucket="penguin-health-demo",
        Key="pdfs/2026-06-28/20260628T220000Z__1234.pdf",
    )
    metadata = response.get("Metadata") or {}
    assert metadata["source-record-id"] == "1234"
    assert metadata["template-id"] == "113875"


def test_write_pdf_metadata_does_not_include_patient_identity(s3_client):
    """Defensive pin: ensure no PHI-shaped keys ever leak into S3
    object metadata. CloudTrail data events log metadata in plaintext
    when enabled, and bucket-level KMS only covers object bodies."""
    write_pdf(
        org_id="demo",
        source_record_id="1234",
        template_id=113875,
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        pdf_bytes=b"%PDF-1.4\n",
        s3_client=s3_client,
    )
    response = s3_client.head_object(
        Bucket="penguin-health-demo",
        Key="pdfs/2026-06-28/20260628T220000Z__1234.pdf",
    )
    metadata = response.get("Metadata") or {}
    assert set(metadata.keys()) <= {"source-record-id", "template-id"}


def test_write_pdf_omits_template_metadata_when_template_id_is_none(s3_client):
    """If a future template lacks an id, the metadata should simply
    omit the field rather than write an empty value or a sentinel."""
    write_pdf(
        org_id="demo",
        source_record_id="1234",
        template_id=None,
        ingest_date="2026-06-28",
        captured_at_compact="20260628T220000Z",
        pdf_bytes=b"%PDF-1.4\n",
        s3_client=s3_client,
    )
    response = s3_client.head_object(
        Bucket="penguin-health-demo",
        Key="pdfs/2026-06-28/20260628T220000Z__1234.pdf",
    )
    metadata = response.get("Metadata") or {}
    assert "template-id" not in metadata
    assert metadata["source-record-id"] == "1234"


def test_write_pdf_uses_per_org_bucket_name():
    """The bucket name follows the existing per-org convention. Verify
    against a different org_id."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-some-other-org")

        write_pdf(
            org_id="some-other-org",
            source_record_id="42",
            template_id=None,
            ingest_date="2026-06-28",
            captured_at_compact="20260628T220000Z",
            pdf_bytes=b"%PDF-1.4\n",
            s3_client=client,
        )

        listing = client.list_objects_v2(Bucket="penguin-health-some-other-org")
        assert listing["KeyCount"] == 1
