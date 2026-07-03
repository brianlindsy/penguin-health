"""Tests for scripts/multi-org/seed_ui_display_fields.py.

Smoke-tests arg parsing + build_item. We don't exercise the DDB PUT here
(the script is a straight put_item call after build_item — trivial); the
projection behavior it enables is covered by test_document_validator_*.
"""

import importlib.util
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT_PATH = (
    _REPO_ROOT / "scripts" / "multi-org" / "seed_ui_display_fields.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "seed_ui_display_fields", str(_SCRIPT_PATH),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_supportive_care_preset_seeds_expected_mapping():
    """The supportive-care preset must cover every canonical field the
    UI reads for that org — regressions here mean documents render with
    missing columns after a re-seed."""
    script = _load_script()
    args = script.parse_args([
        "--org-id", "supportive-care",
        "--preset", "supportive-care",
    ])
    item = script.build_item(args)

    assert item["pk"] == "ORG#supportive-care"
    assert item["sk"] == "UI_DISPLAY_FIELDS"
    assert item["organization_id"] == "supportive-care"
    m = item["mappings"]
    # The canonical UI names the detail page renders (see
    # ValidationRunDetailPage.jsx FIELD_LABELS + MULTI_FILTER_FIELDS).
    assert m["service_id"] == "source_record_id"
    assert m["employee_name"] == "provider_display"
    assert m["date"] == "visit_date"
    assert m["program"] == "billing_list_procedure_code_string"
    assert m["service_type"] == "note_type"
    assert m["cpt_code"] == "billing_list_procedure_code_id"
    assert m["rate"] == "billing_list_rate_client"
    assert m["payer_description"] == "billing_list_payor_name"


def test_explicit_map_entries_override_preset():
    """--map on top of --preset lets an operator tweak one entry without
    forking the preset."""
    script = _load_script()
    args = script.parse_args([
        "--org-id", "supportive-care",
        "--preset", "supportive-care",
        "--map", "employee_name=note_provider_signature_name",
    ])
    item = script.build_item(args)
    assert item["mappings"]["employee_name"] == "note_provider_signature_name"
    # Other preset entries survive.
    assert item["mappings"]["date"] == "visit_date"


def test_maps_only_no_preset():
    script = _load_script()
    args = script.parse_args([
        "--org-id", "my-org",
        "--map", "employee_name=staff_name",
        "--map", "date=svc_date",
    ])
    item = script.build_item(args)
    assert item["mappings"] == {
        "employee_name": "staff_name",
        "date": "svc_date",
    }


def test_clear_writes_empty_mappings():
    """--clear lets an operator disable projection for an org without
    deleting the DDB item — the rules-engine then falls through to raw
    field_values."""
    script = _load_script()
    args = script.parse_args(["--org-id", "some-org", "--clear"])
    item = script.build_item(args)
    assert item["mappings"] == {}


def test_no_mappings_and_no_clear_raises():
    """Silent no-op writes are the wrong default; force an explicit
    signal (--clear) if that's really what the operator wants."""
    script = _load_script()
    args = script.parse_args(["--org-id", "some-org"])
    with pytest.raises(SystemExit):
        script.build_item(args)


def test_malformed_map_entry_raises():
    script = _load_script()
    args = script.parse_args([
        "--org-id", "some-org",
        "--map", "no-equals-sign",
    ])
    with pytest.raises(SystemExit):
        script.build_item(args)


def test_empty_side_of_map_entry_raises():
    script = _load_script()
    args = script.parse_args([
        "--org-id", "some-org",
        "--map", "employee_name=",
    ])
    with pytest.raises(SystemExit):
        script.build_item(args)


def test_unknown_canonical_name_still_writes_but_warns(capsys):
    """Custom canonical names aren't rejected (orgs can define new UI
    fields), but the operator gets a visible warning."""
    script = _load_script()
    args = script.parse_args([
        "--org-id", "some-org",
        "--map", "custom_thing=source_key",
    ])
    item = script.build_item(args)
    assert item["mappings"]["custom_thing"] == "source_key"
    out = capsys.readouterr().out
    assert "custom_thing" in out
