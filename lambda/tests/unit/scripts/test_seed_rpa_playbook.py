"""Tests for scripts/multi-org/seed_rpa_playbook.py.

The script duplicates the runtime allowlist + CSS-only guard deliberately
(it runs from the repo root, not the Lambda asset). Pin both invariants
here so the duplication stays in lockstep with the runtime — the
`_ALLOWED_OPS` pin in test_playbook_engine.py covers the other side.
"""

import importlib.util
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT = _REPO_ROOT / "scripts" / "multi-org" / "seed_rpa_playbook.py"


def _load():
    spec = importlib.util.spec_from_file_location("seed_rpa_playbook",
                                                  str(_SCRIPT))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_allowed_ops_matches_runtime():
    """If the runtime allowlist changes (e.g. `fill` is added later),
    update the script too — this test will fail until both are in sync."""
    from rpa.playbook_engine import _ALLOWED_OPS as runtime
    seed = _load()
    assert seed.ALLOWED_OPS == runtime


def test_validates_happy_path():
    seed = _load()
    doc = {"steps": [
        {"op": "navigate", "url": "https://x/"},
        {"op": "wait_for_selector", "selector": "div.ready"},
        {"op": "loop_over_list", "selector": "tr", "body": [
            {"op": "extract", "fields": {
                "id": {"selector": "td.id"},
                "name": {"selector": "td.n", "attr": "innerText"},
            }},
            {"op": "emit_note"},
        ]},
        {"op": "if_exists", "selector": ".empty", "then": [
            {"op": "log", "message": "no more"},
            {"op": "stop"},
        ]},
    ]}
    assert seed.validate_playbook(doc) == []


def test_rejects_fill_op():
    seed = _load()
    doc = {"steps": [
        {"op": "navigate", "url": "https://x/"},
        {"op": "fill", "selector": "input", "value": "y"},
    ]}
    errs = seed.validate_playbook(doc)
    assert len(errs) == 1
    assert "fill" in errs[0]
    assert "not in allowlist" in errs[0]


@pytest.mark.parametrize("bad_selector", [
    "text=Sign in",
    "xpath=//div",
    "role=button",
    "css=div.x",
    "/html/body",
    "//div[@id='x']",
])
def test_rejects_non_css_selectors_top_level(bad_selector):
    seed = _load()
    doc = {"steps": [
        {"op": "click", "selector": bad_selector},
    ]}
    errs = seed.validate_playbook(doc)
    assert errs == [
        f"$.steps[0].selector={bad_selector!r} uses non-CSS prefix; "
        "v1 playbooks must use plain CSS selectors only"
    ]


def test_rejects_non_css_selector_in_extract_fields():
    seed = _load()
    doc = {"steps": [
        {"op": "extract", "fields": {
            "x": {"selector": "xpath=//div"},
        }},
    ]}
    errs = seed.validate_playbook(doc)
    assert any("fields.x.selector" in e for e in errs)


def test_walks_nested_loop_bodies():
    seed = _load()
    doc = {"steps": [
        {"op": "loop_over_list", "selector": "tr", "body": [
            {"op": "loop_over_list", "selector": "td", "body": [
                {"op": "click", "selector": "text=bad"},
            ]},
        ]},
    ]}
    errs = seed.validate_playbook(doc)
    # Path-tracking proves the walker descended into both nested loops.
    assert errs == [
        "$.steps[0].body[0].body[0].selector='text=bad' uses non-CSS prefix;"
        " v1 playbooks must use plain CSS selectors only"
    ]


def test_walks_if_exists_branches():
    seed = _load()
    doc = {"steps": [{
        "op": "if_exists", "selector": ".banner",
        "then": [{"op": "navigate", "url": "/then/"}],
        "else": [{"op": "click", "selector": "xpath=//div"}],
    }]}
    errs = seed.validate_playbook(doc)
    assert any("else[0].selector" in e for e in errs)


def test_rejects_non_dict_root():
    seed = _load()
    assert seed.validate_playbook(["not a dict"]) == [
        "$: expected object, got list"
    ]


def test_rejects_non_list_steps():
    seed = _load()
    errs = seed.validate_playbook({"steps": "oops"})
    assert errs == ["$.steps: expected list, got str"]
