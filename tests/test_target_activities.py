from __future__ import annotations

import json

import pytest

from megaton_lib.audit.config import AdobeOAuthConfig, AdobeTargetConfig
from megaton_lib.audit.providers.target.activities import export_activities, resolve_activity_ids
from megaton_lib.audit.providers.target.client import AdobeTargetClient


class _Resp:
    def __init__(self, status_code: int, payload=None, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses: list[_Resp]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def request(self, method, url, headers=None, params=None, json=None, timeout=None, **kw):
        self.calls.append({
            'method': method,
            'url': url,
            'headers': headers,
            'params': params,
            'json': json,
        })
        if not self._responses:
            raise RuntimeError('No more responses')
        return self._responses.pop(0)


def _token_response(*a, **kw):
    return _Resp(200, {'access_token': 'tok', 'expires_in': 3600})


@pytest.fixture
def target_env(monkeypatch, tmp_path):
    monkeypatch.setenv('ADOBE_CLIENT_ID', 'cid')
    monkeypatch.setenv('ADOBE_CLIENT_SECRET', 'csec')
    monkeypatch.setenv('ADOBE_ORG_ID', 'ORG@AdobeOrg')
    monkeypatch.setattr(
        'megaton_lib.audit.providers.adobe_auth.requests.post',
        _token_response,
    )
    return tmp_path


def _make_client(tmp_path) -> AdobeTargetClient:
    cfg = AdobeTargetConfig(
        tenant_id='testtenant',
        oauth=AdobeOAuthConfig(token_cache_file=str(tmp_path / '.tok.json')),
    )
    return AdobeTargetClient(cfg)


def test_resolve_activity_ids_from_raw_ids(tmp_path):
    assert resolve_activity_ids(tmp_path / 'missing.json', '123,456') == [123, 456]


def test_resolve_activity_ids_from_index(tmp_path):
    index_path = tmp_path / 'index.json'
    index_path.write_text(json.dumps({'activities': [{'id': 111}, {'id': 222}]}), encoding='utf-8')
    assert resolve_activity_ids(index_path) == [111, 222]


def test_export_activities_writes_json_and_index(target_env, tmp_path):
    client = _make_client(target_env)
    client.session = _Session([
        _Resp(200, {'id': 111, 'name': 'Activity One'}),
        _Resp(200, {'id': 222, 'name': 'Activity Two'}),
    ])

    output_root = tmp_path / 'activities'
    index_payload = export_activities(client, 'testtenant', output_root, [111, 222])

    assert (output_root / '111.json').exists()
    assert (output_root / '222.json').exists()
    assert index_payload['activities'][0]['name'] == 'Activity One'
    written_index = json.loads((output_root / 'index.json').read_text(encoding='utf-8'))
    assert [item['id'] for item in written_index['activities']] == [111, 222]
