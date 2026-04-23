from __future__ import annotations

import json
from pathlib import Path

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.baseline import (
    APPLY_BASELINE_FILENAME,
    hash_normalized_text,
    hash_settings_object,
)
from megaton_lib.audit.providers.tag_config.sync import (
    StaleBaseConflictError,
    apply_custom_code_tree,
    apply_data_element_settings_tree,
    apply_exported_changes_tree,
    find_component_id,
    find_data_element_id,
    format_stale_base_conflict_message,
    raise_for_stale_base_conflicts,
    slugify_component_name,
)


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


def test_find_data_element_id_from_settings_sidecar(tmp_path: Path):
    data_elements_dir = tmp_path / 'data-elements'
    data_elements_dir.mkdir(parents=True)
    settings_file = data_elements_dir / 'deabc_test.settings.json'
    settings_file.write_text('{"source":"window.x"}', encoding='utf-8')
    (data_elements_dir / 'deabc_test.json').write_text(json.dumps({'id': 'DE123'}), encoding='utf-8')

    assert find_data_element_id(settings_file) == 'DE123'


def test_apply_data_element_settings_tree_calls_apply(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    data_elements_dir = root / 'data-elements'
    data_elements_dir.mkdir(parents=True)
    settings_file = data_elements_dir / 'deabc_test.settings.json'
    settings_file.write_text('{"storageDuration": "pageview", "source": "window.ctx"}', encoding='utf-8')
    (data_elements_dir / 'deabc_test.json').write_text(json.dumps({'id': 'DE123'}), encoding='utf-8')

    calls = []

    def fake_apply(config, component_id, new_settings, *, dry_run=True):
        calls.append({'component_id': component_id, 'new_settings': new_settings, 'dry_run': dry_run})
        return {'component_id': component_id, 'changed': True, 'applied': not dry_run}

    monkeypatch.setattr('megaton_lib.audit.providers.tag_config.sync.apply_data_element_settings', fake_apply)

    config = AdobeTagsConfig(property_id='PR123')
    results = apply_data_element_settings_tree(config, root, dry_run=False)

    assert calls == [{
        'component_id': 'DE123',
        'new_settings': {'storageDuration': 'pageview', 'source': 'window.ctx'},
        'dry_run': False,
    }]
    assert results[0]['path'] == 'data-elements/deabc_test.settings.json'


def test_apply_exported_changes_tree_runs_custom_code_then_settings(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    (root / 'rules').mkdir(parents=True)
    (root / 'data-elements').mkdir(parents=True)

    monkeypatch.setattr(
        'megaton_lib.audit.providers.tag_config.sync.apply_custom_code_tree',
        lambda config, path, *, dry_run=True: [{'path': 'rules/r1/custom.custom-code.js', 'component_id': 'RC1'}],
    )
    monkeypatch.setattr(
        'megaton_lib.audit.providers.tag_config.sync.apply_data_element_settings_tree',
        lambda config, path, *, dry_run=True: [{'path': 'data-elements/de1.settings.json', 'component_id': 'DE1'}],
    )

    config = AdobeTagsConfig(property_id='PR123')
    results = apply_exported_changes_tree(config, root, dry_run=True)

    assert results == [
        {'path': 'rules/r1/custom.custom-code.js', 'component_id': 'RC1'},
        {'path': 'data-elements/de1.settings.json', 'component_id': 'DE1'},
    ]


def test_apply_custom_code_tree_marks_remote_only_drift(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    rule_dir = root / 'rules' / 'my-rule'
    rule_dir.mkdir(parents=True)
    code_file = rule_dir / 'rcabc_test.custom-code.js'
    code_file.write_text('console.log(1)', encoding='utf-8')
    (rule_dir / 'rcabc_test.json').write_text(
        json.dumps({'id': 'RC123', 'attributes': {'updated_at': 't1'}, 'meta': {'latest_revision_number': 1}}),
        encoding='utf-8',
    )
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps({
            'schema_version': 1,
            'resources': {
                'rules/my-rule/rcabc_test.custom-code.js': {
                    'kind': 'custom_code',
                    'component_id': 'RC123',
                    'source_hash': hash_normalized_text('console.log(1)'),
                }
            },
        }),
        encoding='utf-8',
    )

    monkeypatch.setattr(
        'megaton_lib.audit.providers.tag_config.sync.get_component_settings',
        lambda config, component_id: {'settings': {'source': 'console.log(2)'}, 'resource_type': 'rule_components'},
    )

    called = {'apply': False}

    def _fake_apply(*args, **kwargs):
        called['apply'] = True
        return {}

    monkeypatch.setattr('megaton_lib.audit.providers.tag_config.sync.apply_custom_code', _fake_apply)

    results = apply_custom_code_tree(AdobeTagsConfig(property_id='PR123'), root, dry_run=True)

    assert called['apply'] is False
    assert results[0]['stale_status'] == 'remote_only'


def test_apply_custom_code_tree_marks_conflict(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    rule_dir = root / 'rules' / 'my-rule'
    rule_dir.mkdir(parents=True)
    code_file = rule_dir / 'rcabc_test.custom-code.js'
    code_file.write_text('console.log(local)', encoding='utf-8')
    (rule_dir / 'rcabc_test.json').write_text(json.dumps({'id': 'RC123'}), encoding='utf-8')
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps({
            'schema_version': 1,
            'resources': {
                'rules/my-rule/rcabc_test.custom-code.js': {
                    'kind': 'custom_code',
                    'component_id': 'RC123',
                    'source_hash': hash_normalized_text('console.log(base)'),
                }
            },
        }),
        encoding='utf-8',
    )

    monkeypatch.setattr(
        'megaton_lib.audit.providers.tag_config.sync.get_component_settings',
        lambda config, component_id: {'settings': {'source': 'console.log(remote)'}, 'resource_type': 'rule_components'},
    )

    results = apply_custom_code_tree(AdobeTagsConfig(property_id='PR123'), root, dry_run=True)

    assert results[0]['stale_status'] == 'conflict'


def test_apply_data_element_settings_tree_marks_conflict(monkeypatch, tmp_path: Path):
    root = tmp_path / 'property'
    data_elements_dir = root / 'data-elements'
    data_elements_dir.mkdir(parents=True)
    settings_file = data_elements_dir / 'deabc_test.settings.json'
    settings_file.write_text(json.dumps({'storageDuration': 'pageview', 'source': 'window.base'}), encoding='utf-8')
    (data_elements_dir / 'deabc_test.json').write_text(json.dumps({'id': 'DE123'}), encoding='utf-8')
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps({
            'schema_version': 1,
            'resources': {
                'data-elements/deabc_test.settings.json': {
                    'kind': 'settings',
                    'component_id': 'DE123',
                    'settings_hash': hash_settings_object({'storageDuration': 'pageview', 'source': 'window.base'}),
                }
            },
        }),
        encoding='utf-8',
    )
    settings_file.write_text(json.dumps({'storageDuration': 'pageview', 'source': 'window.local'}), encoding='utf-8')

    monkeypatch.setattr(
        'megaton_lib.audit.providers.tag_config.sync.get_component_settings',
        lambda config, component_id: {'settings': {'storageDuration': 'pageview', 'source': 'window.remote'}, 'resource_type': 'data_elements'},
    )

    results = apply_data_element_settings_tree(AdobeTagsConfig(property_id='PR123'), root, dry_run=True)

    assert results[0]['stale_status'] == 'conflict'


def test_raise_for_stale_base_conflicts_reports_paths():
    results = [
        {
            'path': 'rules/my-rule/rcabc_test.custom-code.js',
            'component_id': 'RC123',
            'stale_status': 'conflict',
            'stale_detail': 'local and remote custom code both changed since export',
        },
        {
            'path': 'data-elements/deabc_test.settings.json',
            'component_id': 'DE123',
            'stale_status': 'remote_only',
            'stale_detail': 'remote data-element settings changed since export; local file still matches baseline',
        },
    ]

    message = format_stale_base_conflict_message(results)
    assert 'rules/my-rule/rcabc_test.custom-code.js' in message
    assert 'Remote-only drift' in message

    try:
        raise_for_stale_base_conflicts(results, allow_stale_base=False)
    except StaleBaseConflictError as exc:
        assert 'stale-base conflicts' in str(exc)
    else:
        raise AssertionError('expected StaleBaseConflictError')
