"""Validate the supportive-care rule seed file.

Catches regressions in scripts/multi-org/rule-seeds/supportive-care-aba.json
and the seed script's item shape — both are downstream from rule wording
that comes out of stakeholder conversations, so a typo here breaks the
whole org.
"""

import importlib.util
import json
import os
import sys


_REPO_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')
)
_SEED_PATH = os.path.join(
    _REPO_ROOT, 'scripts', 'multi-org', 'rule-seeds', 'supportive-care-aba.json'
)
_SCRIPT_PATH = os.path.join(
    _REPO_ROOT, 'scripts', 'multi-org', 'seed_supportive_care_rules.py'
)


def _load_seed_script_module():
    spec = importlib.util.spec_from_file_location('seed_supportive_care_rules', _SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_seed():
    with open(_SEED_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def test_seed_file_parses():
    seed = _load_seed()
    assert seed['org_id'] == 'supportive-care'
    assert seed['category'] == 'Compliance Audit'


def test_seed_has_12_rules():
    """User input listed 'third person' twice (3 and 8); collapsed to one rule."""
    seed = _load_seed()
    assert len(seed['rules']) == 12


def test_seed_rule_id_8_is_absent():
    """Sanity check on the rule-3/rule-8 dedup decision."""
    seed = _load_seed()
    ids = {str(r['id']) for r in seed['rules']}
    assert '8' not in ids
    # All other expected ids must be present.
    assert ids == {'1', '2', '3', '4', '5', '6', '7', '9', '10', '11', '12', '13'}


def test_seed_rule_types_match_plan():
    """7 deterministic, 5 LLM. Rule 2 (sentence count) moved from LLM to
    deterministic — the LLM extracts sentence_count as a scalar, Python
    owns the 2-per-hour math so the model can't flip its verdict mid-response.
    """
    seed = _load_seed()
    by_id = {str(r['id']): r for r in seed['rules']}
    deterministic_ids = {'1', '2', '4', '5', '6', '10', '12'}
    llm_ids = {'3', '7', '9', '11', '13'}
    for rule_id in deterministic_ids:
        assert by_id[rule_id]['type'] == 'deterministic', f"rule {rule_id} should be deterministic"
    for rule_id in llm_ids:
        assert by_id[rule_id]['type'] == 'llm', f"rule {rule_id} should be llm"


def test_rule_11_is_disabled_by_default():
    """Pending stakeholder reply on tracked_data_summary field availability."""
    seed = _load_seed()
    by_id = {str(r['id']): r for r in seed['rules']}
    assert by_id['11']['enabled'] is False


def test_llm_rules_have_rule_text():
    seed = _load_seed()
    for rule in seed['rules']:
        if rule['type'] == 'llm':
            assert rule.get('rule_text'), f"LLM rule {rule['id']} missing rule_text"


def test_deterministic_rules_have_conditions():
    seed = _load_seed()
    for rule in seed['rules']:
        if rule['type'] == 'deterministic':
            assert rule.get('conditions'), f"Deterministic rule {rule['id']} missing conditions"


def test_rule_1_uses_narrative_hash_unique_operator():
    seed = _load_seed()
    rule = next(r for r in seed['rules'] if str(r['id']) == '1')
    assert rule['conditions'][0]['operator'] == 'narrative_hash_unique'
    assert rule['conditions'][0]['field'] == 'narrative_hash'


def test_rule_2_uses_sentence_count_operator():
    """Rule 2 pairs an LLM extraction (sentence_count) with a deterministic
    2-per-hour compare against billing_list_time_worked_in_mins."""
    seed = _load_seed()
    rule = next(r for r in seed['rules'] if str(r['id']) == '2')
    assert rule['type'] == 'deterministic'
    extract_names = [f['name'] for f in rule['fields_to_extract']]
    assert extract_names == ['sentence_count']
    cond = rule['conditions'][0]
    assert cond['operator'] == 'sentence_count_meets_hourly_minimum'
    assert cond['field'] == 'sentence_count'
    assert cond['compare_to'] == 'billing_list_time_worked_in_mins'


def test_rule_4_uses_minus_minutes_operator():
    seed = _load_seed()
    rule = next(r for r in seed['rules'] if str(r['id']) == '4')
    cond = rule['conditions'][0]
    assert cond['operator'] == 'datetime_not_before_minus_minutes'
    assert cond['field'] == 'signed_at'
    assert cond['compare_to'] == 'billing_list_date_time_to'
    assert cond['value'] == 5


def test_rule_10_caps_at_4_hours():
    seed = _load_seed()
    rule = next(r for r in seed['rules'] if str(r['id']) == '10')
    cond = rule['conditions'][0]
    assert cond['operator'] == 'lte'
    assert cond['value'] == 240  # minutes


def test_seed_script_dry_run_produces_one_item_per_rule(capsys):
    """End-to-end smoke: --dry-run echoes each rule and a final count line."""
    mod = _load_seed_script_module()
    rc = mod.main(['--dry-run'])
    assert rc == 0
    out = capsys.readouterr().out
    seed = _load_seed()
    for rule in seed['rules']:
        assert f"rule {rule['id']}" in out, f"rule {rule['id']} missing from dry-run output"
    assert f"{len(seed['rules'])} rule(s) would be upserted" in out


def test_seed_script_rule_item_shape():
    """The DynamoDB item shape must match what admin_api.py:413-434 produces."""
    mod = _load_seed_script_module()
    seed = _load_seed()
    rule = seed['rules'][0]
    item = mod._rule_item(
        org_id='supportive-care',
        rule=rule,
        category=seed['category'],
        version=seed['version'],
    )

    # The keys admin_api.py writes — must all be present
    expected = {
        'pk', 'sk', 'gsi1pk', 'gsi1sk',
        'rule_id', 'name', 'category', 'description', 'enabled', 'type',
        'version', 'rule_text', 'fields_to_extract', 'notes',
        'conditions', 'conditionals', 'logic',
        'created_at', 'updated_at',
    }
    assert expected.issubset(item.keys())

    assert item['pk'] == 'ORG#supportive-care'
    assert item['sk'].startswith('RULE#')
    assert item['gsi1pk'] == 'RULE'
    assert item['gsi1sk'].startswith('ORG#supportive-care#RULE#')
