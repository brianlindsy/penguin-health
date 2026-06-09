"""Tests for the @audited decorator.

Validates that the decorator captures actor + outcome + timing and never
swallows handler exceptions. The DDB write is observed against the moto
table; Firehose is mocked away.
"""

from unittest.mock import MagicMock

import pytest

from audit import audited


@pytest.fixture
def emitter_stub(mock_dynamodb, monkeypatch):
    """Same setup as test_emitter — moto DDB, mocked Firehose / CloudWatch."""
    from audit import emitter as emitter_mod
    monkeypatch.setattr(
        emitter_mod, "_table", mock_dynamodb.Table("penguin-health-audit"))
    monkeypatch.setattr(emitter_mod, "_firehose", MagicMock())
    monkeypatch.setattr(emitter_mod, "_cloudwatch", MagicMock())
    return emitter_mod


def _event(email="u@x.com", source_ip="10.0.0.1"):
    return {
        "requestContext": {
            "authorizer": {"jwt": {"claims": {"email": email, "sub": "sub-1"}}},
            "http": {"sourceIp": source_ip, "userAgent": "curl/8"},
        },
    }


def test_decorator_emits_on_success(emitter_stub, mock_dynamodb):
    @audited(action="read", resource_type="Coverage",
             resource_from_path="encounterId",
             purpose_of_use="ELIGIBILITY",
             call_type="eligibility")
    def handler(event, path_params, body, **_):
        return {"statusCode": 200, "body": "{}"}

    response = handler(
        event=_event(),
        path_params={"orgId": "org-1", "encounterId": "enc-42"},
        body=None,
    )
    assert response["statusCode"] == 200

    rows = mock_dynamodb.Table("penguin-health-audit").query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(rows) == 1
    row = rows[0]
    assert row["resource_type"] == "Coverage"
    assert row["resource_id"] == "enc-42"
    assert row["outcome"] == "success"
    assert row["agent_email"] == "u@x.com"
    assert row["event"]["client_ip"] == "10.0.0.1"
    assert row["event"]["http_status"] == 200
    assert row["event"]["duration_ms"] >= 0


def test_decorator_maps_4xx_to_minor_failure(emitter_stub, mock_dynamodb):
    @audited(action="read", resource_type="Coverage",
             purpose_of_use="ELIGIBILITY")
    def handler(event, path_params, body, **_):
        return {"statusCode": 403, "body": "{}"}

    handler(event=_event(), path_params={"orgId": "org-1"}, body=None)
    rows = mock_dynamodb.Table("penguin-health-audit").query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert rows[0]["outcome"] == "minor-failure"


def test_decorator_maps_5xx_to_serious_failure(emitter_stub, mock_dynamodb):
    @audited(action="read", resource_type="Coverage",
             purpose_of_use="ELIGIBILITY")
    def handler(event, path_params, body, **_):
        return {"statusCode": 502, "body": "{}"}

    handler(event=_event(), path_params={"orgId": "org-1"}, body=None)
    rows = mock_dynamodb.Table("penguin-health-audit").query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert rows[0]["outcome"] == "serious-failure"


def test_decorator_emits_major_failure_on_exception_and_re_raises(
        emitter_stub, mock_dynamodb):
    @audited(action="read", resource_type="Coverage",
             purpose_of_use="ELIGIBILITY")
    def handler(event, path_params, body, **_):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        handler(event=_event(), path_params={"orgId": "org-1"}, body=None)

    rows = mock_dynamodb.Table("penguin-health-audit").query(
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": "ORG#org-1"},
    )["Items"]
    assert len(rows) == 1
    row = rows[0]
    assert row["outcome"] == "major-failure"
    # error_class captures the type name only — never the message
    # (RuntimeError.boom would be PHI-bearing in real handlers).
    assert row["event"]["error_class"] == "RuntimeError"
    assert "boom" not in str(row["event"])


def test_decorator_hashes_patient_from_body_when_opted_in(
        emitter_stub, mock_dynamodb):
    @audited(action="read", resource_type="Coverage",
             purpose_of_use="ELIGIBILITY",
             call_type="eligibility",
             patient_from_body=True)
    def handler(event, path_params, body, **_):
        return {"statusCode": 200, "body": "{}"}

    handler(
        event=_event(),
        path_params={"orgId": "org-1"},
        body={"first_name": "Jane", "last_name": "Doe", "dob": "19800101"},
    )

    from audit import patient_hash
    expected = patient_hash("Jane", "Doe", "19800101")
    rows = mock_dynamodb.Table("penguin-health-audit").query(
        IndexName="gsi1",
        KeyConditionExpression="gsi1pk = :p",
        ExpressionAttributeValues={":p": f"PATIENT#org-1#{expected}"},
    )["Items"]
    assert len(rows) == 1
    # Initials are stored; raw first/last name are not.
    row = rows[0]
    assert row["patient_first_initial"] == "J"
    assert "first_name" not in row
    assert "last_name" not in row


def test_decorator_passes_kwargs_to_destination(emitter_stub, mock_dynamodb):
    """The dispatch lambdas in admin_api.py pass extras like authorize_fn —
    the decorator must forward them transparently."""
    seen_kwargs = {}

    @audited(action="read", resource_type="Coverage",
             purpose_of_use="ELIGIBILITY")
    def handler(event, path_params, body, authorize_fn=None, **kw):
        seen_kwargs["authorize_fn"] = authorize_fn
        seen_kwargs["extra"] = kw
        return {"statusCode": 200}

    sentinel = object()
    handler(
        event=_event(),
        path_params={"orgId": "org-1"},
        body=None,
        authorize_fn=sentinel,
        random_thing=42,
    )
    assert seen_kwargs["authorize_fn"] is sentinel
    assert seen_kwargs["extra"]["random_thing"] == 42
