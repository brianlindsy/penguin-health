"""Tests for rpa.config — RPA_CONFIG + RPA_PLAYBOOK loaders.

Pattern mirrors test_audit.py: rebind the module-level boto3 table to a
moto-backed table for the duration of the test.
"""

import pytest

from rpa import config as config_module
from rpa.exceptions import RpaOrgNotConfigured, RpaPlaybookNotFound


@pytest.fixture
def cfg(mock_dynamodb):
    """Rebind rpa.config._table to a moto-backed penguin-health-org-config table.

    Also clears the lru_cache between tests so item edits in a previous test
    don't bleed through.
    """
    config_module.invalidate_cache()
    config_module._table = mock_dynamodb.Table("penguin-health-org-config")
    yield config_module
    config_module.invalidate_cache()


def _put(cfg, item):
    cfg._table.put_item(Item=item)


def test_load_rpa_config_returns_enabled_item(cfg):
    _put(cfg, {
        "pk": "ORG#demo",
        "sk": "RPA_CONFIG",
        "enabled": True,
        "vendor": "credible",
        "display_name": "Demo Credible bot",
    })
    item = cfg.load_rpa_config("demo")
    assert item["vendor"] == "credible"
    assert item["display_name"] == "Demo Credible bot"


def test_load_rpa_config_missing_raises(cfg):
    with pytest.raises(RpaOrgNotConfigured, match="no RPA_CONFIG"):
        cfg.load_rpa_config("nonexistent-org")


def test_load_rpa_config_disabled_raises(cfg):
    _put(cfg, {
        "pk": "ORG#demo",
        "sk": "RPA_CONFIG",
        "enabled": False,
        "vendor": "credible",
    })
    with pytest.raises(RpaOrgNotConfigured, match="disabled"):
        cfg.load_rpa_config("demo")


def test_load_playbook_prefers_org_specific_over_shared(cfg):
    _put(cfg, {
        "pk": "ORG#shared",
        "sk": "RPA_PLAYBOOK#credible-notes-v3",
        "version": 3,
        "vendor": "credible",
        "source": "shared",
    })
    _put(cfg, {
        "pk": "ORG#demo",
        "sk": "RPA_PLAYBOOK#credible-notes-v3",
        "version": 3,
        "vendor": "credible",
        "source": "demo-override",
    })
    pb = cfg.load_playbook("demo", "credible-notes-v3")
    assert pb["source"] == "demo-override"


def test_load_playbook_falls_back_to_shared(cfg):
    _put(cfg, {
        "pk": "ORG#shared",
        "sk": "RPA_PLAYBOOK#credible-notes-v3",
        "version": 3,
        "vendor": "credible",
        "source": "shared",
    })
    pb = cfg.load_playbook("demo", "credible-notes-v3")
    assert pb["source"] == "shared"


def test_load_playbook_missing_raises(cfg):
    with pytest.raises(RpaPlaybookNotFound):
        cfg.load_playbook("demo", "no-such-playbook")
