"""Tests for audit.actor — pure functions, no AWS."""

import pytest

from audit import SystemPrincipal, from_event


def test_from_event_extracts_human_agent():
    event = {
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": {
                        "email": "u@example.com",
                        "sub": "sub-123",
                        "cognito:groups": "[Admins, Auditors]",
                    },
                },
            },
            "http": {"sourceIp": "10.0.0.1", "userAgent": "curl/8"},
        },
    }
    a = from_event(event)
    assert a["agent_type"] == "human"
    assert a["agent_id"] == "u@example.com"
    assert a["agent_email"] == "u@example.com"
    assert a["agent_groups"] == ["Admins", "Auditors"]
    assert a["client_ip"] == "10.0.0.1"
    assert a["user_agent"] == "curl/8"


def test_from_event_handles_list_groups():
    """Some Cognito flows emit cognito:groups as a list, not a string."""
    event = {
        "requestContext": {
            "authorizer": {
                "jwt": {
                    "claims": {
                        "sub": "sub-1",
                        "cognito:groups": ["Admins"],
                    },
                },
            },
        },
    }
    a = from_event(event)
    assert a["agent_groups"] == ["Admins"]
    # No email → falls back to sub.
    assert a["agent_id"] == "sub-1"
    assert a["agent_email"] is None


def test_from_event_with_no_event_returns_nulls():
    a = from_event(None)
    assert a["agent_type"] == "human"
    assert a["agent_id"] is None
    assert a["agent_email"] is None
    assert a["agent_groups"] == []
    assert a["client_ip"] is None


def test_system_principal_shape():
    p = SystemPrincipal("fhir-eligibility-poller")
    a = p.as_actor()
    assert a["agent_type"] == "system"
    assert a["agent_id"] == "fhir-eligibility-poller"
    assert a["agent_email"] is None
    assert a["client_ip"] is None
    assert a["user_agent"] is None
    assert p.name == "fhir-eligibility-poller"


def test_system_principal_rejects_empty_name():
    with pytest.raises(ValueError):
        SystemPrincipal("")
