from __future__ import annotations

import json
from pathlib import Path

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.sync import apply_custom_code_tree, find_component_id, slugify_component_name


def test_slugify_component_name():
    assert slugify_component_name('add datalayer') == 'add-datalayer'
    assert slugify_component_name('recommendation-areaを挿入') == 'recommendation-area'
    assert slugify_component_name('getOffer') == 'getoffer'


def test_find_component_id_from_flat_export(tmp_path: Path):
    rule_dir = tmp_path / 'rules' / 'my-rule'
    rule_dir.mkdir(parents=True)
    code_file = rule_dir / 'rcabc_test.custom-code.js'
    code_file.write_text('console.log(1)', encoding='utf-8')
    (rule_dir / 'rcabc_test.json').write_text(json.dumps({'id': 'RC123'}), encoding='utf-8')

    assert find_component_id(code_file) == 'RC123'


def test_find_component_id_from_actions_layout(tmp_path: Path):
    actions_dir = tmp_path / 'rules' / 'my-rule' / 'actions'
    actions_dir.mkdir(parents=True)
    code_file = actions_dir / 'recommendation-area.custom-code.js'
    code_file.write_text('console.log(1)', encoding='utf-8')
    (actions_dir.parent / 'rule-components.json').write_text(
        json.dumps([
            {
                'id': 'RC456',
                'attributes': {
                    'delegate_descriptor_id': 'core::actions::custom-code',
                    'name': 'recommendation-areaを挿入',
                },
            }
        ]),
        encoding='utf-8',
    )

    assert find_component_id(code_file) == 'RC456'


def test_apply_custom_code_tree_calls_apply(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    rule_dir = root / 'rules' / 'my-rule'
    rule_dir.mkdir(parents=True)
    code_file = rule_dir / 'rcabc_test.custom-code.js'
    code_file.write_text('console.log(1)', encoding='utf-8')
    (rule_dir / 'rcabc_test.json').write_text(json.dumps({'id': 'RC123'}), encoding='utf-8')

    calls = []

    def fake_apply(config, component_id, new_code, *, dry_run=True):
        calls.append({'component_id': component_id, 'new_code': new_code, 'dry_run': dry_run})
        return {'component_id': component_id, 'changed': True, 'applied': not dry_run}

    monkeypatch.setattr('megaton_lib.audit.providers.tag_config.sync.apply_custom_code', fake_apply)

    config = AdobeTagsConfig(property_id='PR123')
    results = apply_custom_code_tree(config, root, dry_run=True)

    assert calls == [{'component_id': 'RC123', 'new_code': 'console.log(1)', 'dry_run': True}]
    assert results[0]['path'] == 'rules/my-rule/rcabc_test.custom-code.js'
