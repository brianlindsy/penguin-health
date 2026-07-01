"""Tests for scripts/multi-org/add_centralreach_config.py.

Smoke-tests the argument parsing + DynamoDB item build. The actual
DDB PUT is exercised via moto; the dry-run path is tested too since
operators use it to preview onboarding changes.
"""

import importlib.util
import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT_PATH = (
    _REPO_ROOT / "scripts" / "multi-org" / "add_centralreach_config.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "add_centralreach_config", str(_SCRIPT_PATH),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_MIN_ARGS = [
    "--org-id", "demo",
    "--display-name", "Demo CR ingest",
    "--bot-username", "centralreach-bot+demo",
]


# ----- argument parsing + build_item ---------------------------------------


def test_min_args_build_item_has_required_fields():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS)
    item = script.build_item(args)

    assert item["pk"] == "ORG#demo"
    assert item["sk"] == "CENTRALREACH_CONFIG"
    assert item["organization_id"] == "demo"
    assert item["enabled"] is True
    assert item["display_name"] == "Demo CR ingest"
    assert item["bot_username"] == "centralreach-bot+demo"
    assert item["base_url"] == "https://members.centralreach.com"
    # Default guardrails
    g = item["guardrails"]
    assert g["timezone"] == "America/Chicago"
    assert g["allowed_hours"] == {"start": "06:00", "end": "20:00"}
    assert g["rate_limit_ms_between_requests"] == 1500
    assert g["blackout_dates"] == []


def test_disabled_flag_sets_enabled_false():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + ["--disabled"])
    item = script.build_item(args)
    assert item["enabled"] is False


def test_custom_timezone_and_window():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + [
        "--timezone", "America/New_York",
        "--allowed-hours-start", "08:00",
        "--allowed-hours-end", "18:00",
        "--rate-limit-ms", "2000",
    ])
    item = script.build_item(args)
    g = item["guardrails"]
    assert g["timezone"] == "America/New_York"
    assert g["allowed_hours"] == {"start": "08:00", "end": "18:00"}
    assert g["rate_limit_ms_between_requests"] == 2000


def test_blackout_dates_parsed():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + [
        "--blackout-dates", "2026-12-25,2027-01-01",
    ])
    item = script.build_item(args)
    assert item["guardrails"]["blackout_dates"] == [
        "2026-12-25", "2027-01-01",
    ]


def test_blank_blackout_dates_yields_empty_list():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + ["--blackout-dates", ""])
    item = script.build_item(args)
    assert item["guardrails"]["blackout_dates"] == []


# ----- input validation ----------------------------------------------------


@pytest.mark.parametrize("bad_value", [
    "25:00",   # impossible hour
    "06:60",   # impossible minute
    "0600",    # missing colon
    "garbage",
])
def test_invalid_hhmm_rejected(bad_value):
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + [
        "--allowed-hours-start", bad_value,
    ])
    with pytest.raises(SystemExit):
        script.build_item(args)


def test_invalid_blackout_date_rejected():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + [
        "--blackout-dates", "not-a-date",
    ])
    with pytest.raises(SystemExit):
        script.build_item(args)


def test_negative_rate_limit_rejected():
    script = _load_script()
    args = script.parse_args(_MIN_ARGS + ["--rate-limit-ms", "-100"])
    with pytest.raises(SystemExit):
        script.build_item(args)


# ----- dry-run path --------------------------------------------------------


def test_dry_run_prints_json_and_does_not_write_to_ddb(capsys):
    script = _load_script()
    rc = script.main(_MIN_ARGS + ["--dry-run"])
    assert rc == 0
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["pk"] == "ORG#demo"
    assert parsed["sk"] == "CENTRALREACH_CONFIG"
