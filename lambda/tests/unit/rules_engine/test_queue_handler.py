"""
Regression tests for the document-queue write path.

Locks down the two contracts that everything else depends on:

  * `compute_content_hash` is deterministic across key orderings, nested
    dict orderings, and Decimal↔str round-trips. A change to the
    canonicalizer silently invalidates every hash already in DynamoDB —
    these tests make that change loud.
  * The dedup fork in `document_validator.validate_document` short-circuits
    before `extract_fields` when the content hash matches the latest queue
    pointer, so byte-identical resends never pay Bedrock/rule-eval cost.
"""

import os
import sys
from decimal import Decimal
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine',
    ),
)


class TestCanonicalHash:
    def test_key_order_independent(self):
        import queue_handler
        h1 = queue_handler.compute_content_hash({'a': 1, 'b': 2, 'c': 3})
        h2 = queue_handler.compute_content_hash({'c': 3, 'a': 1, 'b': 2})
        assert h1 == h2

    def test_nested_key_order_independent(self):
        import queue_handler
        left = {'outer': {'x': 1, 'y': 2}, 'note': 'hi'}
        right = {'note': 'hi', 'outer': {'y': 2, 'x': 1}}
        assert (
            queue_handler.compute_content_hash(left)
            == queue_handler.compute_content_hash(right)
        )

    def test_decimal_and_str_equivalent(self):
        import queue_handler
        # DynamoDB round-trips numeric fields as Decimal. A canonicalizer
        # that hashes the raw type would break every hash the moment a
        # record survives a DDB read.
        as_str = queue_handler.compute_content_hash({'rate': '1.50'})
        as_decimal = queue_handler.compute_content_hash({'rate': Decimal('1.50')})
        assert as_str == as_decimal

    def test_value_change_flips_hash(self):
        import queue_handler
        base = queue_handler.compute_content_hash({'text': 'session went well'})
        edited = queue_handler.compute_content_hash({'text': 'session went well.'})
        assert base != edited

    def test_hash_is_hex_and_deterministic(self):
        import queue_handler
        h = queue_handler.compute_content_hash({'a': 1})
        assert isinstance(h, str)
        assert len(h) == 64  # sha256 hex
        assert int(h, 16) >= 0
        # Same input on a fresh invocation must produce the exact same
        # digest — no wallclock, no random salt.
        assert h == queue_handler.compute_content_hash({'a': 1})


class TestDedupFork:
    def _config(self):
        return {
            'field_mappings': {},
            'csv_column_mappings': {},
            'rules': [{'rule_id': 'r1', 'name': 'r1', 'enabled': True,
                       'category': 'x', 'evaluator': 'llm'}],
        }

    def test_skips_when_hash_matches_pointer(self, monkeypatch):
        """Same document_id + same content_hash → validate_document
        returns a `skipped_duplicate` sentinel and never invokes field
        extraction or rule evaluation."""
        monkeypatch.setenv('QUEUE_WRITE_ENABLED', 'true')
        monkeypatch.setenv('DOCUMENT_QUEUE_TABLE', 'penguin-health-document-queue')

        import document_validator
        import queue_handler

        data = {'source_record_id': 'DOC-42', 'text': 'unchanged'}
        matching_hash = queue_handler.compute_content_hash(data)
        pointer = {
            'pk': 'ORG#org-a', 'sk': 'DOC#DOC-42',
            'document_id': 'DOC-42',
            'organization_id': 'org-a',
            'content_hash': matching_hash,
            'latest_version_sk': 'VERSION#2026-07-15T10:00:00',
        }
        monkeypatch.setattr(queue_handler, 'lookup_pointer',
                            lambda org, doc: pointer)

        extract_calls = []
        monkeypatch.setattr(
            document_validator, 'extract_fields',
            lambda *a, **kw: extract_calls.append(a) or {},
        )
        # If evaluate_rule ever runs on a skipped-duplicate we've already
        # regressed — spy on it via ThreadPoolExecutor is overkill, just
        # trip on any call.
        monkeypatch.setattr(
            document_validator, 'evaluate_rule',
            lambda *a, **kw: pytest.fail('evaluate_rule must not run for duplicates'),
        )

        result = document_validator.validate_document(
            data, 'file.json', self._config(), 'org-a', 'run-1',
        )

        assert result == {
            'skipped_duplicate': True,
            'document_id': 'DOC-42',
            'organization_id': 'org-a',
            'filename': 'file.json',
            'content_hash': matching_hash,
            'duplicate_of_version_sk': 'VERSION#2026-07-15T10:00:00',
            'validation_run_id': 'run-1',
        }
        assert extract_calls == []

    def test_processes_when_hash_differs(self, monkeypatch):
        """Different content hash → the fork lets validation proceed and
        the caller-facing result carries the new hash for persistence."""
        monkeypatch.setenv('QUEUE_WRITE_ENABLED', 'true')
        monkeypatch.setenv('DOCUMENT_QUEUE_TABLE', 'penguin-health-document-queue')

        import document_validator
        import queue_handler

        data = {'source_record_id': 'DOC-42', 'text': 'revised content'}
        new_hash = queue_handler.compute_content_hash(data)
        stale_pointer = {
            'pk': 'ORG#org-a', 'sk': 'DOC#DOC-42',
            'document_id': 'DOC-42',
            'organization_id': 'org-a',
            'content_hash': 'some-old-hash',
            'latest_version_sk': 'VERSION#2026-07-14T10:00:00',
        }
        monkeypatch.setattr(queue_handler, 'lookup_pointer',
                            lambda org, doc: stale_pointer)
        monkeypatch.setattr(document_validator, 'extract_fields',
                            lambda *a, **kw: {'source_record_id': 'DOC-42'})

        # Force zero rules so no ThreadPoolExecutor / Bedrock work runs.
        config = self._config()
        config['rules'] = []

        result = document_validator.validate_document(
            data, 'file.json', config, 'org-a', 'run-2',
        )

        assert not result.get('skipped_duplicate')
        assert result['document_id'] == 'DOC-42'
        assert result['content_hash'] == new_hash

    def test_disabled_flag_bypasses_lookup(self, monkeypatch):
        """QUEUE_WRITE_ENABLED='false' turns the whole fork into a no-op —
        no queue lookup, no hash computation cost, back to legacy path."""
        monkeypatch.setenv('QUEUE_WRITE_ENABLED', 'false')

        import document_validator
        import queue_handler

        def _boom(*a, **kw):
            pytest.fail('lookup_pointer must not run when flag is off')

        monkeypatch.setattr(queue_handler, 'lookup_pointer', _boom)
        monkeypatch.setattr(document_validator, 'extract_fields',
                            lambda *a, **kw: {})

        config = self._config()
        config['rules'] = []
        result = document_validator.validate_document(
            {'source_record_id': 'DOC-42', 'text': 'x'},
            'file.json', config, 'org-a', 'run-3',
        )
        assert not result.get('skipped_duplicate')


# ----- Queue-write tolerates floats in the results payload ----------------
#
# Regression: CR-ingested records carry raw Python floats in `field_values`
# (mileage, rate_client, client_charges, ...). A naked `put_item` blew up
# with "Float types are not supported. Use Decimal types instead." — the
# validation-results write already ran through `parse_float=Decimal`, but
# the queue-write path did not. That left every CR doc out of the queue
# table entirely, and the outer `except` in `process_file` wrote an error
# sentinel row that prevented reprocess. Cover both put sites so the
# regression can't return.

_QUEUE_TABLE = "penguin-health-document-queue-test"


@pytest.fixture
def queue_table(monkeypatch):
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        table = ddb.create_table(
            TableName=_QUEUE_TABLE,
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
        monkeypatch.setenv("DOCUMENT_QUEUE_TABLE", _QUEUE_TABLE)
        yield table


def _results_with_floats():
    """CR-shaped results payload with raw floats where they land in prod
    (billing rate/charge/mileage on `field_values`, plus a summary
    percentage). If any of these reach `put_item` un-converted, boto3
    raises 'Float types are not supported.'."""
    return {
        "organization_id": "supportive-care",
        "document_id": "508056438",
        "content_hash": "a" * 64,
        "validation_run_id": "run-abc",
        "validation_timestamp": "2026-07-19T12:00:00Z",
        "field_values": {
            "mileage": 12.5,
            "rate_client": 165.0,
            "client_charges": 412.5,
            "amount_owed": 0.0,
            "supervisor_signature_names": ["Dr. Doe"],
        },
        "summary": {
            "total_rules": 3,
            "passed": 2,
            "failed": 1,
            "skipped": 0,
            "pass_rate": 0.6667,
        },
        "rules": [
            {"status": "PASS"},
            {"status": "PASS"},
            {"status": "FAIL", "fixed": False, "finding_confirmed": False},
        ],
    }


class TestUpsertTolerates_Floats:
    def test_create_path_writes_when_field_values_contain_floats(self, queue_table):
        """Pinned: `upsert_new_or_version(create)` must accept a
        results payload with raw floats and land the pointer + version
        rows. Regression for CR-ingested docs being invisible in the
        queue."""
        import queue_handler
        branch = queue_handler.upsert_new_or_version(
            _results_with_floats(), pointer=None,
        )
        assert branch == "queue_create"

        # Pointer: field_values_snapshot has the floats-turned-Decimals.
        pointer = queue_table.get_item(
            Key={"pk": "ORG#supportive-care", "sk": "DOC#508056438"},
        )["Item"]
        assert pointer["field_values_snapshot"]["mileage"] == Decimal("12.5")
        assert pointer["failed_findings"] == 1
        assert pointer["open_findings"] == 1

        # Version row: summary carries the float pass_rate — also a
        # put_item site that must accept the conversion.
        version = queue_table.get_item(
            Key={
                "pk": "ORG#supportive-care#DOC#508056438",
                "sk": "VERSION#2026-07-19T12:00:00Z",
            },
        )["Item"]
        assert version["summary"]["pass_rate"] == Decimal("0.6667")
        assert version["field_values_snapshot"]["mileage"] == Decimal("12.5")

    def test_new_version_path_writes_when_field_values_contain_floats(
        self, queue_table,
    ):
        """Same regression, second branch. First call creates; second
        call with a different hash lands the new-version write which
        goes through a separate `put_item` at the pointer write site."""
        import queue_handler
        results = _results_with_floats()
        queue_handler.upsert_new_or_version(results, pointer=None)

        # Simulate a re-eval producing a different content_hash.
        results2 = _results_with_floats()
        results2["content_hash"] = "b" * 64
        results2["validation_timestamp"] = "2026-07-19T13:00:00Z"
        results2["field_values"]["mileage"] = 14.75  # also a float
        stale_pointer = queue_table.get_item(
            Key={"pk": "ORG#supportive-care", "sk": "DOC#508056438"},
        )["Item"]

        branch = queue_handler.upsert_new_or_version(results2, stale_pointer)
        assert branch == "queue_new_version"

        pointer = queue_table.get_item(
            Key={"pk": "ORG#supportive-care", "sk": "DOC#508056438"},
        )["Item"]
        assert pointer["content_hash"] == "b" * 64
        assert pointer["field_values_snapshot"]["mileage"] == Decimal("14.75")


def test_to_ddb_item_converts_nested_floats():
    """The helper handles floats nested in lists and dicts, not just
    top-level. Pointer rows have both shapes (`field_values_snapshot`
    is a dict, `rules` was a list on legacy shapes)."""
    import queue_handler
    out = queue_handler._to_ddb_item({
        "top": 1.5,
        "nested": {"inner": 2.5, "list": [3.5, {"deep": 4.5}]},
    })
    assert out["top"] == Decimal("1.5")
    assert out["nested"]["inner"] == Decimal("2.5")
    assert out["nested"]["list"][0] == Decimal("3.5")
    assert out["nested"]["list"][1]["deep"] == Decimal("4.5")
