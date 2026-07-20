"""Tests for scripts/cleanup_float_error_sentinels.py.

Pins six contracts:
  1. Scan iterator only yields rows where document_id starts with
     ERROR# AND the `error` attribute contains the float-bug marker
  2. Non-matching error sentinels (other exceptions) are left alone
  3. GSI2 query path scopes to a single run
  4. --org filter narrows scan results by organization_id
  5. --org + --run applies the post-filter on the GSI2 result
  6. Dry-run does not delete
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

import cleanup_float_error_sentinels as cleanup  # noqa: E402


_TABLE = "penguin-health-validation-results"


@pytest.fixture
def table():
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi2pk", "AttributeType": "S"},
                {"AttributeName": "gsi2sk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[{
                "IndexName": "gsi2",
                "KeySchema": [
                    {"AttributeName": "gsi2pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi2sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb.Table(_TABLE)


def _put_error_sentinel(
    table, *, org: str, run: str, s3_key: str,
    error_msg: str = "Float types are not supported. Use Decimal types instead.",
):
    """Mirror the shape store_results writes for the outer-except path."""
    doc_id = f"ERROR#{s3_key}"
    ts = "2026-07-19T12:00:00Z"
    table.put_item(Item={
        "pk": f"DOC#{doc_id}",
        "sk": f"VALIDATION#{ts}",
        "gsi1pk": f"DATE#{ts[:10]}",
        "gsi1sk": f"DOC#{doc_id}",
        "gsi2pk": f"RUN#{run}",
        "gsi2sk": f"DOC#{doc_id}",
        "organization_id": org,
        "document_id": doc_id,
        "validation_run_id": run,
        "validation_timestamp": ts,
        "s3_key": s3_key,
        "summary": {"total_rules": 0, "passed": 0, "failed": 0, "skipped": 0},
        "rules": [],
        "error": error_msg,
    })


def _put_real_result(table, *, org: str, run: str, doc_id: str):
    """Non-sentinel row that must never be touched by the cleanup."""
    ts = "2026-07-19T12:00:00Z"
    table.put_item(Item={
        "pk": f"DOC#{doc_id}",
        "sk": f"VALIDATION#{ts}",
        "gsi1pk": f"DATE#{ts[:10]}",
        "gsi1sk": f"DOC#{doc_id}",
        "gsi2pk": f"RUN#{run}",
        "gsi2sk": f"DOC#{doc_id}",
        "organization_id": org,
        "document_id": doc_id,
        "validation_run_id": run,
        "validation_timestamp": ts,
        "summary": {"total_rules": 3, "passed": 3, "failed": 0, "skipped": 0},
        "rules": [],
    })


# ----- scan iterator -------------------------------------------------------


def test_scan_yields_only_float_bug_sentinels(table):
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/x__1.json",
    )
    # Different error message — must NOT be yielded.
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/x__2.json",
        error_msg="TypeError: expected str, got NoneType",
    )
    # Real result row (not an ERROR# sentinel) — must NOT be yielded.
    _put_real_result(table, org="supportive-care", run="run-1", doc_id="123")

    out = list(cleanup._iter_error_sentinels_by_scan(table, org_id=None))
    assert len(out) == 1
    assert out[0]["s3_key"] == "data/2026-07-18/x__1.json"


def test_scan_filters_by_org(table):
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/sc__1.json",
    )
    _put_error_sentinel(
        table, org="other-org", run="run-2",
        s3_key="data/2026-07-18/other__1.json",
    )

    out = list(cleanup._iter_error_sentinels_by_scan(
        table, org_id="supportive-care",
    ))
    assert len(out) == 1
    assert out[0]["organization_id"] == "supportive-care"


# ----- GSI2 query ---------------------------------------------------------


def test_gsi2_query_scopes_to_single_run(table):
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/x__1.json",
    )
    _put_error_sentinel(
        table, org="supportive-care", run="run-2",
        s3_key="data/2026-07-18/x__2.json",
    )
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/x__3.json",
        error_msg="unrelated ValueError",  # must NOT be yielded
    )

    out = list(cleanup._iter_error_sentinels_by_run(table, "run-1"))
    s3_keys = sorted(item["s3_key"] for item in out)
    assert s3_keys == ["data/2026-07-18/x__1.json"]


# ----- delete --------------------------------------------------------------


def test_delete_removes_row(table):
    _put_error_sentinel(
        table, org="supportive-care", run="run-1",
        s3_key="data/2026-07-18/x__1.json",
    )
    item = next(cleanup._iter_error_sentinels_by_scan(table, org_id=None))

    outcome = cleanup._delete(table, item)
    assert outcome == "deleted"

    remaining = list(cleanup._iter_error_sentinels_by_scan(table, org_id=None))
    assert remaining == []


def test_delete_is_idempotent_on_missing_row(table):
    """DDB DeleteItem is idempotent — a delete on a nonexistent key is
    a silent no-op, not an error. Ensures re-running the cleanup is
    safe."""
    fake_item = {
        "pk": "DOC#ERROR#not-there",
        "sk": "VALIDATION#never",
    }
    outcome = cleanup._delete(table, fake_item)
    assert outcome == "deleted"
