"""Tests for scripts/reset_centralreach_org.py.

Pins seven contracts:
  1. Cursor delete only touches rows for the target org, not other orgs
  2. Narrative-hash delete only touches rows for the target org
  3. Document-queue delete removes BOTH pointer rows and version rows
     for the target org, leaves other orgs' rows alone
  4. Validation-results delete removes rows filtered by organization_id
     regardless of pk shape (results table doesn't key on org)
  5. Dry-run does not delete anything
  6. Idempotent: re-running after a delete is a silent no-op
  7. `--org` is required — no accidental empty-org run
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws


SCRIPTS_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "scripts",
))
sys.path.insert(0, SCRIPTS_DIR)

import reset_centralreach_org as reset  # noqa: E402


_CURSOR = "penguin-health-centralreach-ingest-cursor"
_QUEUE = "penguin-health-document-queue"
_RESULTS = "penguin-health-validation-results"
_NARRATIVE = "penguin-health-narrative-hashes"


def _make_pk_sk_table(name: str) -> None:
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName=name,
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


@pytest.fixture
def tables():
    with mock_aws():
        for name in (_CURSOR, _QUEUE, _RESULTS, _NARRATIVE):
            _make_pk_sk_table(name)
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        yield {
            "cursor": ddb.Table(_CURSOR),
            "queue": ddb.Table(_QUEUE),
            "results": ddb.Table(_RESULTS),
            "narrative": ddb.Table(_NARRATIVE),
        }


def _table_all_keys(table) -> set[tuple[str, str]]:
    """Return the set of (pk, sk) currently in the table."""
    out: set[tuple[str, str]] = set()
    kwargs: dict = {"ProjectionExpression": "pk, sk"}
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            out.add((item["pk"], item["sk"]))
        token = resp.get("LastEvaluatedKey")
        if not token:
            break
        kwargs["ExclusiveStartKey"] = token
    return out


# ----- ingest cursor -------------------------------------------------------


def test_cursor_reset_removes_target_org_and_spares_other_orgs(tables):
    tables["cursor"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "ENTRY#111",
    })
    tables["cursor"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "ENTRY#222",
    })
    tables["cursor"].put_item(Item={
        "pk": "ORG#other", "sk": "ENTRY#333",
    })

    totals = reset._batch_delete(
        tables["cursor"],
        reset._iter_cursor_keys(tables["cursor"], "supportive-care"),
        dry_run=False,
    )
    assert totals["found"] == 2
    assert totals["deleted"] == 2

    remaining = _table_all_keys(tables["cursor"])
    assert remaining == {("ORG#other", "ENTRY#333")}


def test_cursor_dry_run_does_not_delete(tables):
    tables["cursor"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "ENTRY#111",
    })

    totals = reset._batch_delete(
        tables["cursor"],
        reset._iter_cursor_keys(tables["cursor"], "supportive-care"),
        dry_run=True,
    )
    assert totals["found"] == 1
    assert totals["deleted"] == 0
    assert _table_all_keys(tables["cursor"]) == {
        ("ORG#supportive-care", "ENTRY#111"),
    }


def test_cursor_reset_is_idempotent(tables):
    tables["cursor"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "ENTRY#111",
    })
    reset._batch_delete(
        tables["cursor"],
        reset._iter_cursor_keys(tables["cursor"], "supportive-care"),
        dry_run=False,
    )
    # Second run finds nothing to delete, does not raise.
    totals = reset._batch_delete(
        tables["cursor"],
        reset._iter_cursor_keys(tables["cursor"], "supportive-care"),
        dry_run=False,
    )
    assert totals == {"found": 0, "deleted": 0, "errored": 0}


# ----- narrative hashes ---------------------------------------------------


def test_narrative_hash_reset_removes_only_target_org(tables):
    tables["narrative"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "HASH#aaa",
    })
    tables["narrative"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "HASH#bbb",
    })
    tables["narrative"].put_item(Item={
        "pk": "ORG#other", "sk": "HASH#ccc",
    })

    reset._batch_delete(
        tables["narrative"],
        reset._iter_narrative_hash_keys(tables["narrative"], "supportive-care"),
        dry_run=False,
    )
    assert _table_all_keys(tables["narrative"]) == {
        ("ORG#other", "HASH#ccc"),
    }


# ----- document queue ------------------------------------------------------


def test_queue_reset_removes_pointers_and_versions(tables):
    """Pinned: both pointer rows (pk=ORG#{org}) and version rows
    (pk=ORG#{org}#DOC#{doc_id}) for the target org must be deleted.
    Other orgs' rows must survive."""
    # supportive-care: two documents, each with two version rows.
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "DOC#111",
        "document_id": "111",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care#DOC#111",
        "sk": "VERSION#2026-07-19T12:00:00Z",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care#DOC#111",
        "sk": "VERSION#2026-07-20T12:00:00Z",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "DOC#222",
        "document_id": "222",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care#DOC#222",
        "sk": "VERSION#2026-07-19T12:00:00Z",
    })
    # Other org — must survive.
    tables["queue"].put_item(Item={
        "pk": "ORG#other", "sk": "DOC#999",
        "document_id": "999",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#other#DOC#999",
        "sk": "VERSION#2026-07-19T12:00:00Z",
    })

    totals = reset._reset_queue(
        tables["queue"], "supportive-care", dry_run=False,
    )
    assert totals["pointers_found"] == 2
    assert totals["pointers_deleted"] == 2
    assert totals["versions_found"] == 3
    assert totals["versions_deleted"] == 3

    remaining = _table_all_keys(tables["queue"])
    assert remaining == {
        ("ORG#other", "DOC#999"),
        ("ORG#other#DOC#999", "VERSION#2026-07-19T12:00:00Z"),
    }


def test_queue_reset_dry_run_deletes_nothing(tables):
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "DOC#111",
        "document_id": "111",
    })
    tables["queue"].put_item(Item={
        "pk": "ORG#supportive-care#DOC#111",
        "sk": "VERSION#2026-07-19T12:00:00Z",
    })

    totals = reset._reset_queue(
        tables["queue"], "supportive-care", dry_run=True,
    )
    assert totals["pointers_found"] == 1
    assert totals["versions_found"] == 1
    assert totals["pointers_deleted"] == 0
    assert totals["versions_deleted"] == 0
    assert len(_table_all_keys(tables["queue"])) == 2


# ----- validation results --------------------------------------------------


def test_validation_results_reset_filters_by_organization_id(tables):
    """Pinned: validation-results has no org in its pk, so the delete
    has to filter on the `organization_id` attribute. Other orgs'
    rows must not be touched regardless of their pk shape."""
    tables["results"].put_item(Item={
        "pk": "DOC#111", "sk": "VALIDATION#2026-07-19T12:00:00Z",
        "organization_id": "supportive-care",
    })
    tables["results"].put_item(Item={
        "pk": "DOC#ERROR#data/2026-07-18/x.json",
        "sk": "VALIDATION#2026-07-19T12:00:00Z",
        "organization_id": "supportive-care",
    })
    tables["results"].put_item(Item={
        "pk": "DOC#999", "sk": "VALIDATION#2026-07-19T12:00:00Z",
        "organization_id": "other",
    })

    reset._batch_delete(
        tables["results"],
        reset._iter_validation_results_keys(tables["results"], "supportive-care"),
        dry_run=False,
    )
    assert _table_all_keys(tables["results"]) == {
        ("DOC#999", "VALIDATION#2026-07-19T12:00:00Z"),
    }


# ----- projections -------------------------------------------------------


def test_iter_cursor_keys_projects_only_pk_sk(tables):
    """Pinned: the iterator must not pull full items into memory —
    memory rule about PHI-holding tables. Only pk + sk come back
    from DDB."""
    tables["cursor"].put_item(Item={
        "pk": "ORG#supportive-care", "sk": "ENTRY#111",
        "first_ingested_at": "2026-07-19T12:00:00Z",
        "first_ingest_run_id": "run-abc",
        "pdf_s3_key": "pdfs/2026-07-19/x.pdf",
        "record_s3_key": "data/2026-07-19/x.json",
    })
    items = list(reset._iter_cursor_keys(tables["cursor"], "supportive-care"))
    assert items == [{"pk": "ORG#supportive-care", "sk": "ENTRY#111"}]
    # No stray attributes leaked.
    assert set(items[0].keys()) == {"pk", "sk"}


def test_iter_validation_results_keys_projects_only_pk_sk(tables):
    tables["results"].put_item(Item={
        "pk": "DOC#111", "sk": "VALIDATION#2026-07-19T12:00:00Z",
        "organization_id": "supportive-care",
        "field_values": {
            "mileage": Decimal("12.5"),
            "supervisor_name": "Dr. Doe",
        },
        "rules": [{"status": "PASS"}],
    })
    items = list(reset._iter_validation_results_keys(
        tables["results"], "supportive-care",
    ))
    assert items == [{"pk": "DOC#111", "sk": "VALIDATION#2026-07-19T12:00:00Z"}]
    assert set(items[0].keys()) == {"pk", "sk"}
