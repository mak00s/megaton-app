"""Validation helpers for JSON-like path contracts."""

from __future__ import annotations

from typing import Any

TYPE_MAP = {
    'string': str,
    'array': list,
    'object': dict,
    'number': (int, float),
    'boolean': bool,
}


def resolve_path(obj: Any, path: str) -> tuple[bool, Any]:
    current = obj
    for part in path.split('.'):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def check_rule(data: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    path = str(rule.get('path', ''))
    exists, value = resolve_path(data, path)
    result = {
        'path': path,
        'exists': exists,
        'ok': True,
        'reason': '',
        'usedBy': rule.get('usedBy', ''),
    }

    if not exists:
        result['ok'] = False
        result['reason'] = 'missing path'
        return result

    expected_type = rule.get('type')
    if expected_type:
        py_type = TYPE_MAP.get(str(expected_type))
        if py_type is None:
            result['ok'] = False
            result['reason'] = f'unsupported contract type: {expected_type}'
            return result
        if not isinstance(value, py_type):
            result['ok'] = False
            result['reason'] = f'type mismatch: expected {expected_type}, got {type(value).__name__}'
            return result

    if rule.get('nonEmpty') is True and isinstance(value, str) and value.strip() == '':
        result['ok'] = False
        result['reason'] = 'empty string'
        return result

    min_items = rule.get('minItems')
    if min_items is not None:
        if not isinstance(value, list):
            result['ok'] = False
            result['reason'] = 'minItems set but value is not an array'
            return result
        if len(value) < int(min_items):
            result['ok'] = False
            result['reason'] = f'array length {len(value)} < minItems {min_items}'
            return result

    result['reason'] = 'ok'
    return result


def validate_contract(data: dict[str, Any] | None, contract: dict[str, Any]) -> dict[str, Any]:
    report = {
        'contract': contract.get('name', ''),
        'hasData': data is not None,
        'checks': [],
        'ok': True,
    }

    if data is None:
        report['ok'] = False
        report['checks'].append({
            'path': 'data',
            'exists': False,
            'ok': False,
            'reason': 'data is undefined',
            'usedBy': 'all contract checks',
        })
        return report

    for rule in contract.get('required', []):
        check = check_rule(data, rule)
        report['checks'].append(check)
        if not check['ok']:
            report['ok'] = False

    return report
