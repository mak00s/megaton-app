from __future__ import annotations

from megaton_lib.validation.contracts import check_rule, resolve_path, validate_contract


def test_resolve_path_success():
    exists, value = resolve_path({'a': {'b': 'x'}}, 'a.b')
    assert exists is True
    assert value == 'x'


def test_check_rule_type_and_non_empty():
    result = check_rule({'page': {'id': 'abc'}}, {'path': 'page.id', 'type': 'string', 'nonEmpty': True})
    assert result['ok'] is True
    assert result['reason'] == 'ok'


def test_check_rule_missing_path():
    result = check_rule({'page': {}}, {'path': 'page.id'})
    assert result['ok'] is False
    assert result['reason'] == 'missing path'


def test_validate_contract_aggregates_checks():
    contract = {
        'name': 'digitalData contract',
        'required': [
            {'path': 'page.id', 'type': 'string', 'nonEmpty': True},
            {'path': 'items', 'type': 'array', 'minItems': 1},
        ],
    }
    report = validate_contract({'page': {'id': 'abc'}, 'items': [1]}, contract)
    assert report['ok'] is True
    assert len(report['checks']) == 2
