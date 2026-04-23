from __future__ import annotations

import json
import pytest

from megaton_lib.audit.config import AdobeOAuthConfig, AdobeTargetConfig
from megaton_lib.audit.providers.target.activities import export_activities, fetch_activity, resolve_activity_ids
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


def _make_client(tmp_path, *, accept_header: str = "application/vnd.adobe.target.v1+json") -> AdobeTargetClient:
    cfg = AdobeTargetConfig(
        tenant_id='testtenant',
        oauth=AdobeOAuthConfig(token_cache_file=str(tmp_path / '.tok.json')),
        accept_header=accept_header,
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
    v3_client = _make_client(target_env, accept_header='application/vnd.adobe.target.v3+json')
    v3_client.session = _Session([
        _Resp(200, {'id': 111, 'name': 'Activity One'}),
        _Resp(200, {'id': 222, 'name': 'Activity Two'}),
    ])
    from megaton_lib.audit.providers.target import activities as activities_mod
    original = activities_mod._v3_activity_client
    activities_mod._v3_activity_client = lambda _client: v3_client
    try:
        output_root = tmp_path / 'activities'
        index_payload = export_activities(client, 'testtenant', output_root, [111, 222])
    finally:
        activities_mod._v3_activity_client = original

    assert (output_root / '111.json').exists()
    assert (output_root / '222.json').exists()
    assert index_payload['activities'][0]['name'] == 'Activity One'
    written_index = json.loads((output_root / 'index.json').read_text(encoding='utf-8'))
    assert [item['id'] for item in written_index['activities']] == [111, 222]


def test_fetch_activity_uses_v3_and_falls_back_to_xt(target_env, tmp_path):
    client = _make_client(target_env)
    v3_client = _make_client(target_env, accept_header='application/vnd.adobe.target.v3+json')
    v3_client.session = _Session([
        _Resp(404, {"error": "not found"}),
        _Resp(200, {'id': 812437, 'name': 'XT Activity', 'state': 'approved'}),
    ])
    from megaton_lib.audit.providers.target import activities as activities_mod
    original = activities_mod._v3_activity_client
    activities_mod._v3_activity_client = lambda _client: v3_client
    try:
        result = fetch_activity(client, 'testtenant', 812437)
    finally:
        activities_mod._v3_activity_client = original

    assert result['name'] == 'XT Activity'
    assert len(v3_client.session.calls) == 2
    assert v3_client.session.calls[0]['headers']['Accept'] == 'application/vnd.adobe.target.v3+json'
    assert v3_client.session.calls[0]['url'].endswith('/target/activities/ab/812437')
    assert v3_client.session.calls[1]['url'].endswith('/target/activities/xt/812437')


def test_fetch_activity_propagates_non_404_errors_without_xt_retry(target_env, tmp_path):
    client = _make_client(target_env)
    v3_client = _make_client(target_env, accept_header='application/vnd.adobe.target.v3+json')
    v3_client.session = _Session([
        _Resp(403, {"error": "forbidden"}),
    ])
    from megaton_lib.audit.providers.target import activities as activities_mod
    original = activities_mod._v3_activity_client
    activities_mod._v3_activity_client = lambda _client: v3_client
    try:
        with pytest.raises(RuntimeError, match="HTTP 403"):
            fetch_activity(client, 'testtenant', 812437)
    finally:
        activities_mod._v3_activity_client = original

    assert len(v3_client.session.calls) == 1
    assert v3_client.session.calls[0]['url'].endswith('/target/activities/ab/812437')


def test_export_activities_supports_options_xt_activity(target_env, tmp_path):
    client = _make_client(target_env)
    v3_client = _make_client(target_env)
    v3_client.session = _Session([
        _Resp(404, {"error": "not found"}),
        _Resp(200, {'id': 812437, 'name': 'XT Activity', 'state': 'approved'}),
    ])
    output_root = tmp_path / 'activities'

    from megaton_lib.audit.providers.target import activities as activities_mod

    original = activities_mod._v3_activity_client
    activities_mod._v3_activity_client = lambda _client: v3_client
    try:
        index_payload = export_activities(client, 'testtenant', output_root, [812437])
    finally:
        activities_mod._v3_activity_client = original

    assert (output_root / '812437.json').exists()
    written = json.loads((output_root / '812437.json').read_text(encoding='utf-8'))
    assert written['name'] == 'XT Activity'
    assert index_payload['activities'][0]['id'] == 812437


def test_with_accept_header_preserves_runtime_http_settings(target_env, tmp_path):
    cfg = AdobeTargetConfig(
        tenant_id='testtenant',
        oauth=AdobeOAuthConfig(token_cache_file=str(tmp_path / '.tok.json')),
    )
    client = AdobeTargetClient(
        cfg,
        max_retries=2,
        backoff_factor=0.25,
        jitter=0.05,
        timeout_sec=7.0,
    )
    session = _Session([])
    client.session = session

    v3_client = client.with_accept_header('application/vnd.adobe.target.v3+json')

    assert v3_client is not client
    assert v3_client.config.accept_header == 'application/vnd.adobe.target.v3+json'
    assert v3_client.max_retries == 2
    assert v3_client.backoff_factor == 0.25
    assert v3_client.jitter == 0.05
    assert v3_client.timeout_sec == 7.0
    assert v3_client.session is session
    assert v3_client._auth is client._auth
