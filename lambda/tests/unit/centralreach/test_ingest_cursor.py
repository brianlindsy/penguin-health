"""Tests for centralreach.ingest_cursor.

Pins six contracts:
  1. Feature flag env var gates `is_enabled`
  2. Force-reingest env var gates `is_force_reingest`
  3. `has_ingested` returns True/False by presence, uses correct
     pk/sk shape, and fails closed to False on ClientError
  4. `mark_ingested` writes the presence row with the expected shape
  5. `mark_ingested` is a silent no-op when a row already exists
     (ConditionalCheckFailedException path)
  6. Other DDB errors on `mark_ingested` are swallowed, not raised
"""

from __future__ import annotations

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from centralreach import ingest_cursor


_TABLE_NAME = "penguin-health-centralreach-ingest-cursor-test"


@pytest.fixture
def cursor_table():
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        table = ddb.create_table(
            TableName=_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


# ----- feature flag --------------------------------------------------------


def test_is_enabled_defaults_false():
    assert ingest_cursor.is_enabled(env={}) is False


def test_is_enabled_true_when_flag_true():
    assert ingest_cursor.is_enabled(
        env={"CENTRALREACH_INGEST_DEDUPE_ENABLED": "true"},
    ) is True


def test_is_enabled_case_insensitive():
    assert ingest_cursor.is_enabled(
        env={"CENTRALREACH_INGEST_DEDUPE_ENABLED": "TRUE"},
    ) is True


def test_is_force_reingest_defaults_false():
    assert ingest_cursor.is_force_reingest(env={}) is False


def test_is_force_reingest_true_when_flag_true():
    assert ingest_cursor.is_force_reingest(
        env={"CENTRALREACH_FORCE_REINGEST": "true"},
    ) is True


# ----- has_ingested --------------------------------------------------------


def test_has_ingested_returns_false_when_row_absent(cursor_table):
    assert ingest_cursor.has_ingested(
        "demo", "12345", table=cursor_table,
    ) is False


def test_has_ingested_returns_true_when_row_present(cursor_table):
    cursor_table.put_item(Item={
        "pk": "ORG#demo",
        "sk": "ENTRY#12345",
        "first_ingested_at": "2026-07-19T12:00:00Z",
    })
    assert ingest_cursor.has_ingested(
        "demo", "12345", table=cursor_table,
    ) is True


def test_has_ingested_uses_org_scoped_pk(cursor_table):
    """Pinned: an entry ingested for one org must not shadow the same
    source_record_id in another org."""
    cursor_table.put_item(Item={
        "pk": "ORG#other-org",
        "sk": "ENTRY#12345",
        "first_ingested_at": "2026-07-19T12:00:00Z",
    })
    assert ingest_cursor.has_ingested(
        "demo", "12345", table=cursor_table,
    ) is False


def test_has_ingested_fails_closed_to_false_on_client_error():
    """Fail-closed: if we can't read the cursor, re-ingest (safer than
    silently skipping) — the mark_ingested conditional put will still
    protect against a genuine race."""

    class BrokenTable:
        def get_item(self, **_):
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "boom"}},
                "GetItem",
            )

    assert ingest_cursor.has_ingested(
        "demo", "12345", table=BrokenTable(),
    ) is False


# ----- mark_ingested -------------------------------------------------------


def test_mark_ingested_writes_row_with_expected_shape(cursor_table):
    wrote = ingest_cursor.mark_ingested(
        "demo", "12345",
        ingest_run_id="run-abc",
        pdf_s3_key="pdfs/2026-07-19/x.pdf",
        record_s3_key="data/2026-07-19/x.json",
        now_iso="2026-07-19T12:00:00Z",
        table=cursor_table,
    )
    assert wrote is True

    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#12345"},
    )["Item"]
    assert item == {
        "pk": "ORG#demo",
        "sk": "ENTRY#12345",
        "first_ingested_at": "2026-07-19T12:00:00Z",
        "first_ingest_run_id": "run-abc",
        "pdf_s3_key": "pdfs/2026-07-19/x.pdf",
        "record_s3_key": "data/2026-07-19/x.json",
    }


def test_mark_ingested_is_noop_when_row_already_exists(cursor_table):
    """Pinned: a second mark_ingested for the same key returns False
    and does NOT clobber the first-ingest metadata. This is the
    concurrent-run race we care about."""
    ingest_cursor.mark_ingested(
        "demo", "12345",
        ingest_run_id="run-first",
        pdf_s3_key="pdfs/first.pdf",
        record_s3_key="data/first.json",
        now_iso="2026-07-19T12:00:00Z",
        table=cursor_table,
    )
    wrote = ingest_cursor.mark_ingested(
        "demo", "12345",
        ingest_run_id="run-second",
        pdf_s3_key="pdfs/second.pdf",
        record_s3_key="data/second.json",
        now_iso="2026-07-19T13:00:00Z",
        table=cursor_table,
    )
    assert wrote is False

    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#12345"},
    )["Item"]
    # First-ingest metadata preserved verbatim.
    assert item["first_ingest_run_id"] == "run-first"
    assert item["first_ingested_at"] == "2026-07-19T12:00:00Z"
    assert item["pdf_s3_key"] == "pdfs/first.pdf"


def test_mark_ingested_swallows_other_ddb_errors():
    """Non-conditional-check errors are logged and swallowed — the
    ingest itself already succeeded; losing the cursor row only costs
    one reprocess on the next run."""

    class BrokenTable:
        def put_item(self, **_):
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "boom"}},
                "PutItem",
            )

    wrote = ingest_cursor.mark_ingested(
        "demo", "12345",
        ingest_run_id="run-abc",
        pdf_s3_key="pdfs/x.pdf",
        record_s3_key="data/x.json",
        now_iso="2026-07-19T12:00:00Z",
        table=BrokenTable(),
    )
    assert wrote is False  # write did not land; nothing else raised
