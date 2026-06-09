"""Tests for audit.emitter — DDB hot mirror write + Firehose put.

Firehose calls are intercepted by replacing the module's client with a
MagicMock; moto's Firehose mock requires extra setup that isn't worth the
complexity for unit-level assertions.
"""

from unittest.mock import MagicMock

import pytest

from audit import SystemPrincipal, emit, from_event, patient_hash


@pytest.fixture
def emitter(mock_dynamodb, monkeypatch):
    """Rebind the emitter module to use the moto-backed audit table and
    swap in mock Firehose / CloudWatch clients so tests stay hermetic."""
    from audit import emitter as emitter_mod

    table = mock_dynamodb.Table("penguin-health-audit")
    monkeypatch.setattr(emitter_mod, "_table", table)

    firehose_mock = MagicMock()
    monkeypatch.setattr(emitter_mod, "_firehose", firehose_mock)

    cloudwatch_mock = MagicMock()
    monkeypatch.setattr(emitter_mod, "_cloudwatch", cloudwatch_mock)

    return {
        "module": emitter_mod,
        "table": table,
        "firehose": firehose_mock,
        "cloudwatch": cloudwatch_mock,
    }


def test_emit_writes_ddb_and_firehose(emitter):
    actor = SystemPrincipal("test-poller").as_actor()
    event_id = emit(
        action="read",
        resource={"type": "Coverage", "id": "enc-1", "org": "org-1"},
        actor=actor,
        org_id="org-1",
        purpose_of_use="ELIGIBILITY",
        call_type="eligibility",
        patient={"first_name": "Jane", "last_name": "Doe", "dob": "19800101"},
        member_id="ABCDEFGH12345",
        payer={"id": "AETNA", "name": "Aetna"},
        external_control_number="CTRL-1",
        duration_ms=123,
        result={"status": "active", "active": True,
                "plan": {"name": "Aetna PPO"}},
    )
    assert event_id

    # DDB row landed with the canonical key layout.
    rows = emitter["table"].query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={
            ":p": f"PATIENT#org-1#{patient_hash('Jane', 'Doe', '19800101')}",
        },
    )["Items"]
    assert len(rows) == 1
    row = rows[0]
    assert row["org_id"] == "org-1"
    assert row["agent_id"] == "test-poller"
    assert row["resource_type"] == "Coverage"
    assert row["resource_id"] == "enc-1"
    assert row["member_id_last4"] == "2345"
    assert row["payer_name"] == "Aetna"
    assert row["patient_first_initial"] == "J"
    assert row["patient_last_initial"] == "D"
    # Forbidden raw fields must not leak.
    assert "member_id" not in row
    assert "ssn" not in row
    assert "first_name" not in row

    # Firehose got exactly one put with a newline-terminated JSON record.
    emitter["firehose"].put_record.assert_called_once()
    call_kwargs = emitter["firehose"].put_record.call_args.kwargs
    assert call_kwargs["DeliveryStreamName"] == "penguin-health-audit"
    data = call_kwargs["Record"]["Data"]
    assert data.endswith(b"\n")
    assert b'"event_id"' in data
    assert b'"member_id_last4"' in data
    # full member id never appears in the Firehose payload either
    assert b"ABCDEFGH12345" not in data


def test_emit_with_no_patient_omits_gsi1(emitter):
    actor = SystemPrincipal("system").as_actor()
    emit(
        action="execute",
        resource={"type": "BedrockPrompt", "id": "rule-1"},
        actor=actor,
        org_id="org-1",
        purpose_of_use="DOC_PROCESSING",
        call_type="bedrock_invoke",
    )
    # No patient → no gsi1 key, so the patient GSI query returns nothing.
    rows = emitter["table"].query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={":p": "PATIENT#org-1#anything"},
    )["Items"]
    assert rows == []
    # But the row is on the main table.
    main = emitter["table"].query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(main) == 1
    assert main[0]["resource_type"] == "BedrockPrompt"


def test_emit_swallows_firehose_failures(emitter):
    """A flaky Firehose must never bubble up into the request path."""
    emitter["firehose"].put_record.side_effect = Exception("kaboom")
    actor = SystemPrincipal("system").as_actor()
    # Must not raise even though Firehose blows up every try.
    event_id = emit(
        action="read",
        resource={"type": "Coverage"},
        actor=actor,
        org_id="org-1",
    )
    assert event_id
    # DDB write still happened (this is the durability guarantee).
    rows = emitter["table"].query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(rows) == 1
    # CloudWatch metric was emitted for the Firehose failure.
    emitter["cloudwatch"].put_metric_data.assert_called_once()
    assert emitter["cloudwatch"].put_metric_data.call_args.kwargs["Namespace"] \
        == "PenguinHealth/Audit"
    metric = emitter["cloudwatch"].put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["MetricName"] == "FirehosePutFailure"


def test_emit_swallows_ddb_failures(emitter):
    """A DDB hiccup must not bubble up either — the request path stays clean."""
    emitter["module"]._table = MagicMock()
    emitter["module"]._table.put_item.side_effect = Exception("kaboom")
    actor = SystemPrincipal("system").as_actor()
    event_id = emit(
        action="read",
        resource={"type": "Coverage"},
        actor=actor,
        org_id="org-1",
    )
    assert event_id
    # AuditEmitFailure metric was emitted.
    emitter["cloudwatch"].put_metric_data.assert_called_once()
    metric = emitter["cloudwatch"].put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["MetricName"] == "AuditEmitFailure"


def test_emit_with_http_actor_captures_client_ip(emitter):
    event = {
        "requestContext": {
            "authorizer": {"jwt": {"claims": {"email": "u@x.com", "sub": "s"}}},
            "http": {"sourceIp": "10.0.0.99", "userAgent": "ua"},
        },
    }
    emit(
        action="read",
        resource={"type": "Coverage"},
        actor=from_event(event),
        org_id="org-1",
    )
    rows = emitter["table"].query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert rows[0]["event"]["client_ip"] == "10.0.0.99"
    assert rows[0]["event"]["user_agent"] == "ua"
    assert rows[0]["event"]["agent_email"] == "u@x.com"
