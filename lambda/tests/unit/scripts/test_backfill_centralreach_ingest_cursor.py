"""Tests for scripts/backfill_centralreach_ingest_cursor.py.

Pins seven contracts:
  1. Key regex parses the exact `data/{date}/{compact}__{id}.json`
     shape and rejects other keys
  2. `_iso_from_compact` inverts the runner's compact timestamp
  3. Multiple ingests of the same source_record_id collapse to the
     EARLIEST captured_at_compact
  4. Backfill writes one cursor row per unique source_record_id with
     the expected item shape
  5. Re-running the backfill is a silent no-op — no duplicates, no
     clobbering of first-ingest metadata
  6. Dry-run mode writes nothing
  7. Discovery scans org-config for enabled CENTRALREACH_CONFIG rows
"""

from __future__ import annotations

import os
import sys

import boto3
import pytest
from moto import mock_aws


SCRIPTS_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "scripts",
))
sys.path.insert(0, SCRIPTS_DIR)

import backfill_centralreach_ingest_cursor as bf  # noqa: E402


_CURSOR_TABLE = "penguin-health-centralreach-ingest-cursor"
_ORG_CONFIG_TABLE = "penguin-health-org-config"


# ----- fixtures ------------------------------------------------------------


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def s3(aws):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="penguin-health-demo")
    yield client


@pytest.fixture
def cursor_table(aws):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName=_CURSOR_TABLE,
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
    yield ddb.Table(_CURSOR_TABLE)


@pytest.fixture
def org_config_table(aws):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName=_ORG_CONFIG_TABLE,
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
    yield ddb.Table(_ORG_CONFIG_TABLE)


# ----- helpers -------------------------------------------------------------


def _put_record(s3, org_id: str, date: str, compact: str, src_id: str):
    key = f"data/{date}/{compact}__{src_id}.json"
    s3.put_object(
        Bucket=f"penguin-health-{org_id}",
        Key=key,
        Body=b'{"placeholder": true}',
    )
    return key


# ----- key parsing ---------------------------------------------------------


def test_key_regex_matches_expected_shape():
    key = "data/2026-06-28/20260628T220000Z__502614593.json"
    m = bf._KEY_RE.match(key)
    assert m is not None
    assert m.group("date") == "2026-06-28"
    assert m.group("compact") == "20260628T220000Z"
    assert m.group("id") == "502614593"


def test_key_regex_rejects_pdf_prefix():
    """Only records/JSON at `data/`, not `pdfs/`."""
    key = "pdfs/2026-06-28/20260628T220000Z__502614593.pdf"
    assert bf._KEY_RE.match(key) is None


def test_key_regex_rejects_missing_date_folder():
    key = "data/20260628T220000Z__502614593.json"
    assert bf._KEY_RE.match(key) is None


def test_key_regex_accepts_non_numeric_source_record_id():
    """Source record ids are stringly-typed on the way out."""
    key = "data/2026-06-28/20260628T220000Z__abc-123.json"
    m = bf._KEY_RE.match(key)
    assert m is not None
    assert m.group("id") == "abc-123"


# ----- compact -> iso ------------------------------------------------------


def test_iso_from_compact_round_trips():
    assert bf._iso_from_compact("20260628T220000Z") == "2026-06-28T22:00:00Z"


# ----- earliest fold -------------------------------------------------------


def test_earliest_collapse_picks_lowest_compact():
    """Multiple JSONs for the same source_record_id collapse to the
    earliest compact timestamp — matches live semantics of
    `first_ingested_at`."""
    keys = iter([
        "data/2026-06-28/20260628T220000Z__502.json",  # later
        "data/2026-06-27/20260627T100000Z__502.json",  # earliest — wins
        "data/2026-06-28/20260628T230000Z__502.json",  # later
        "data/2026-06-28/20260628T100000Z__999.json",  # different id
    ])
    out = sorted(bf._earliest_by_record_id(keys))
    assert out == [
        ("502", "20260627T100000Z",
         "data/2026-06-27/20260627T100000Z__502.json"),
        ("999", "20260628T100000Z",
         "data/2026-06-28/20260628T100000Z__999.json"),
    ]


def test_earliest_fold_skips_unmatched_keys():
    keys = iter([
        "data/2026-06-28/20260628T220000Z__502.json",
        "data/2026-06-28/malformed.json",         # no compact/id
        "pdfs/2026-06-28/20260628T220000Z__502.pdf",  # wrong prefix
    ])
    out = list(bf._earliest_by_record_id(keys))
    assert len(out) == 1
    assert out[0][0] == "502"


# ----- end-to-end backfill -------------------------------------------------


def test_backfill_writes_one_cursor_row_per_unique_record(s3, cursor_table):
    _put_record(s3, "demo", "2026-06-28", "20260628T220000Z", "502")
    _put_record(s3, "demo", "2026-06-28", "20260628T230000Z", "502")  # dup
    _put_record(s3, "demo", "2026-06-27", "20260627T100000Z", "999")

    totals = bf._backfill_org(
        org_id="demo", s3=s3, cursor_table=cursor_table,
        ingest_run_id="backfill-run-1", dry_run=False,
    )
    assert totals["unique_records"] == 2
    assert totals["written"] == 2
    assert totals["duplicate"] == 0
    assert totals["errored"] == 0

    # Verify item shape for one row.
    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#502"},
    )["Item"]
    assert item["first_ingested_at"] == "2026-06-28T22:00:00Z"
    assert item["first_ingest_run_id"] == "backfill-run-1"
    assert item["record_s3_key"] == "data/2026-06-28/20260628T220000Z__502.json"
    assert item["pdf_s3_key"] == ""


def test_backfill_earliest_wins_on_duplicate_ids(s3, cursor_table):
    """The 'first_ingested_at' must reflect the true first ingest, not
    an arbitrary order of S3 List results."""
    # Insert in reverse-chronological order so the folder isn't lucky.
    _put_record(s3, "demo", "2026-06-30", "20260630T120000Z", "502")
    _put_record(s3, "demo", "2026-06-28", "20260628T090000Z", "502")

    bf._backfill_org(
        org_id="demo", s3=s3, cursor_table=cursor_table,
        ingest_run_id="run-1", dry_run=False,
    )
    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#502"},
    )["Item"]
    assert item["first_ingested_at"] == "2026-06-28T09:00:00Z"
    assert item["record_s3_key"] == "data/2026-06-28/20260628T090000Z__502.json"


def test_backfill_is_idempotent(s3, cursor_table):
    """Re-running the backfill must not clobber `first_ingest_run_id`
    or `first_ingested_at`. ConditionalCheckFailedException is the
    intended path."""
    _put_record(s3, "demo", "2026-06-28", "20260628T220000Z", "502")

    bf._backfill_org(
        org_id="demo", s3=s3, cursor_table=cursor_table,
        ingest_run_id="run-first", dry_run=False,
    )
    second = bf._backfill_org(
        org_id="demo", s3=s3, cursor_table=cursor_table,
        ingest_run_id="run-second", dry_run=False,
    )
    assert second["written"] == 0
    assert second["duplicate"] == 1
    assert second["errored"] == 0

    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#502"},
    )["Item"]
    # First run's provenance preserved.
    assert item["first_ingest_run_id"] == "run-first"


def test_backfill_dry_run_writes_nothing(s3, cursor_table):
    _put_record(s3, "demo", "2026-06-28", "20260628T220000Z", "502")

    totals = bf._backfill_org(
        org_id="demo", s3=s3, cursor_table=cursor_table,
        ingest_run_id="run-1", dry_run=True,
    )
    assert totals["unique_records"] == 1
    assert totals["written"] == 1  # what we WOULD write

    item = cursor_table.get_item(
        Key={"pk": "ORG#demo", "sk": "ENTRY#502"},
    ).get("Item")
    assert item is None  # nothing actually written


def test_backfill_skips_org_with_no_bucket(s3, cursor_table, capsys):
    """A configured org whose bucket doesn't exist yet (or was
    deleted) is skipped with a warning rather than blowing up the
    run — some prod orgs may not have written any records yet."""
    totals = bf._backfill_org(
        org_id="nonexistent-org", s3=s3, cursor_table=cursor_table,
        ingest_run_id="run-1", dry_run=False,
    )
    assert totals == {
        "keys_scanned": 0,
        "unmatched_keys": 0,
        "unique_records": 0,
        "written": 0,
        "duplicate": 0,
        "errored": 0,
    }
    assert "no bucket" in capsys.readouterr().err


# ----- org discovery -------------------------------------------------------


def test_list_cr_org_ids_returns_only_enabled_configs(org_config_table):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    # Enabled CR orgs.
    org_config_table.put_item(Item={
        "pk": "ORG#alpha", "sk": "CENTRALREACH_CONFIG",
        "organization_id": "alpha", "enabled": True,
    })
    org_config_table.put_item(Item={
        "pk": "ORG#bravo", "sk": "CENTRALREACH_CONFIG",
        "organization_id": "bravo", "enabled": True,
    })
    # Disabled — must NOT appear.
    org_config_table.put_item(Item={
        "pk": "ORG#charlie", "sk": "CENTRALREACH_CONFIG",
        "organization_id": "charlie", "enabled": False,
    })
    # Different sk — must NOT appear.
    org_config_table.put_item(Item={
        "pk": "ORG#delta", "sk": "FHIR_CONFIG",
        "organization_id": "delta", "enabled": True,
    })

    assert bf._list_cr_org_ids(ddb) == ["alpha", "bravo"]
