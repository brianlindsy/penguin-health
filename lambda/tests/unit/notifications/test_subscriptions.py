"""Tests for the notifications.subscriptions helper module."""

import pytest


@pytest.fixture
def subscriptions(mock_dynamodb, monkeypatch):
    import importlib
    import notifications.subscriptions as subs_mod
    importlib.reload(subs_mod)
    monkeypatch.setattr(
        subs_mod, "_table",
        mock_dynamodb.Table("penguin-health-org-config"),
    )
    return subs_mod


def test_set_subscription_creates_then_updates(subscriptions):
    item = subscriptions.set_subscription(
        email="alice@example.com",
        org_id="test-org",
        event_type="validation_run_complete",
        enabled=True,
    )
    assert item["enabled"] is True
    assert item["created_at"] == item["updated_at"]
    created_at = item["created_at"]

    updated = subscriptions.set_subscription(
        email="alice@example.com",
        org_id="test-org",
        event_type="validation_run_complete",
        enabled=False,
    )
    assert updated["enabled"] is False
    # created_at preserved, updated_at advances.
    assert updated["created_at"] == created_at
    assert updated["updated_at"] >= created_at


def test_get_subscribers_returns_only_enabled(subscriptions):
    subscriptions.set_subscription(
        email="alice@example.com", org_id="test-org",
        event_type="validation_run_complete", enabled=True,
    )
    subscriptions.set_subscription(
        email="bob@example.com", org_id="test-org",
        event_type="validation_run_complete", enabled=False,
    )
    subscriptions.set_subscription(
        email="carol@example.com", org_id="test-org",
        event_type="eligibility_issue", enabled=True,
    )
    # Other org — must not leak in.
    subscriptions.set_subscription(
        email="alice@example.com", org_id="other-org",
        event_type="validation_run_complete", enabled=True,
    )

    subs = subscriptions.get_subscribers("test-org", "validation_run_complete")
    assert subs == ["alice@example.com"]
    elig_subs = subscriptions.get_subscribers("test-org", "eligibility_issue")
    assert elig_subs == ["carol@example.com"]


def test_get_subscribers_empty_when_unknown(subscriptions):
    assert subscriptions.get_subscribers("nobody", "validation_run_complete") == []
    assert subscriptions.get_subscribers("", "validation_run_complete") == []
    assert subscriptions.get_subscribers("test-org", "") == []


def test_list_my_subscriptions_scoped_to_user_and_org(subscriptions):
    subscriptions.set_subscription(
        email="alice@example.com", org_id="test-org",
        event_type="validation_run_complete", enabled=True,
    )
    subscriptions.set_subscription(
        email="alice@example.com", org_id="test-org",
        event_type="eligibility_issue", enabled=False,
    )
    subscriptions.set_subscription(
        email="alice@example.com", org_id="other-org",
        event_type="validation_run_complete", enabled=True,
    )
    subscriptions.set_subscription(
        email="bob@example.com", org_id="test-org",
        event_type="validation_run_complete", enabled=True,
    )

    rows = subscriptions.list_my_subscriptions("alice@example.com", "test-org")
    assert {(r["event_type"], r["enabled"]) for r in rows} == {
        ("validation_run_complete", True),
        ("eligibility_issue", False),
    }


def test_set_subscription_requires_all_fields(subscriptions):
    with pytest.raises(ValueError):
        subscriptions.set_subscription(
            email="", org_id="x", event_type="validation_run_complete", enabled=True,
        )
    with pytest.raises(ValueError):
        subscriptions.set_subscription(
            email="a@b", org_id="", event_type="validation_run_complete", enabled=True,
        )
    with pytest.raises(ValueError):
        subscriptions.set_subscription(
            email="a@b", org_id="x", event_type="", enabled=True,
        )
