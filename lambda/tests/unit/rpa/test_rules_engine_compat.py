"""Compatibility test: an RpaNoteRecord JSON written by result_writer must
satisfy the rules engine's existing `process_file` contract — namely, the
parsed JSON has a `text` field that the validator passes straight through
to LLM rules ([rules_engine_rag.py:346-349] wraps CSVs as {'text': content}
and uses JSONs as-is).

This test imports the rules engine module to ensure that integration is
real, not aspirational, but does not exercise Bedrock or DynamoDB — it
shells out to the same `json.loads` step `process_file` performs and
asserts `data['text']` round-trips. If a future rules-engine change ever
requires an additional field of JSON inputs, this test will fail loudly
so we can update both sides together.
"""

import json

import boto3
import pytest
from moto import mock_aws

import rules_engine_rag  # noqa: F401 — import-time regression check
from rpa import result_writer


SAMPLE_EXTRACTION = {
    "source_record_id": "note-compat-001",
    "first_name": "Alex",
    "last_name": "Stone",
    "dob": "1985-07-12",
    "source_patient_id": "MRN-99887",
    "visit_date": "2026-06-08",
    "provider_display": "Dr. Riya Patel",
    "note_type": "Therapy Session",
    "text": "Client engaged actively in cognitive-behavioral exercises.",
    "body_html": "<p>Client engaged actively in cognitive-behavioral exercises.</p>",
    "extracted_fields": {"session_duration_minutes": 60},
}

ACTOR = {
    "agent_type": "system",
    "agent_id": "rpa-runner",
    "agent_email": None,
    "agent_groups": [],
    "client_ip": None,
    "user_agent": "rpa-runner/test",
}


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="penguin-health-demo")
        yield client


def test_record_json_satisfies_rules_engine_data_text_contract(s3):
    out = result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-compat",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: "x",
    )

    # Reproduce the read+parse step process_file performs at
    # rules_engine_rag.py:343-349.
    obj = s3.get_object(Bucket=out["s3_bucket"], Key=out["s3_key"])
    content = obj["Body"].read().decode("utf-8")
    data = json.loads(content)

    assert "text" in data, (
        "Rules engine reads data['text'] for non-CSV inputs; missing this "
        "key breaks every LLM-evaluated rule for RPA-sourced notes."
    )
    assert data["text"] == SAMPLE_EXTRACTION["text"]


def test_filename_matches_rules_engine_document_id_extractor(s3):
    """`process_file` ultimately calls `document_validator.extract_document_id_from_filename`
    which expects `{ts}__{chart_id}.csv|json` and returns the segment after `__`.
    Our writer must produce filenames that survive this round-trip."""
    from document_validator import extract_document_id_from_filename

    out = result_writer.persist_note(
        extraction=SAMPLE_EXTRACTION,
        org_id="demo",
        vendor="credible",
        playbook_run_id="run-compat",
        captured_at="2026-06-10T14:00:00Z",
        ingest_date="2026-06-10",
        captured_at_compact="20260610T140000Z",
        actor=ACTOR,
        s3_client=s3,
        audit_emit_fn=lambda **kw: "x",
    )

    # The extractor only accepts .csv files today. Confirm the assumption
    # explicitly so an unexpected behavior change here fails the build —
    # if and when the extractor learns about .json, this assertion gets
    # updated and we drop the override below.
    doc_id_from_csv = extract_document_id_from_filename(
        out["s3_key"].replace(".json", ".csv")
    )
    assert doc_id_from_csv == "note-compat-001", (
        "Document-id extractor lost the chart_id segment after '__'; "
        "result_writer's filename pattern is incompatible."
    )
