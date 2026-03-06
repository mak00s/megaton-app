"""Tests for the Adobe Target API client."""

from __future__ import annotations

import json
import time

import pytest

from megaton_lib.audit.config import AdobeOAuthConfig, AdobeTargetConfig
from megaton_lib.audit.providers.target.client import AdobeTargetClient


class _Resp:
    """Minimal requests.Response stub."""

    def __init__(self, status_code: int, payload=None, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        return self._payload


class _Session:
    """Minimal requests.Session stub for the Target client."""

    def __init__(self, responses: list[_Resp]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def request(self, method, url, headers=None, params=None, json=None, timeout=None, **kw):
        self.calls.append({
            "method": method, "url": url, "headers": headers,
            "params": params, "json": json,
        })
        if not self._responses:
            raise RuntimeError("No more responses")
        return self._responses.pop(0)


def _token_response(*a, **kw):
    return _Resp(200, {"access_token": "tok", "expires_in": 3600})


@pytest.fixture
def target_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ADOBE_CLIENT_ID", "cid")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "csec")
    monkeypatch.setenv("ADOBE_ORG_ID", "ORG@AdobeOrg")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.requests.post",
        _token_response,
    )
    return tmp_path


def _make_client(tmp_path) -> tuple[AdobeTargetConfig, AdobeTargetClient]:
    cfg = AdobeTargetConfig(
        tenant_id="testtenant",
        oauth=AdobeOAuthConfig(token_cache_file=str(tmp_path / ".tok.json")),
    )
    return cfg, AdobeTargetClient(cfg)


def test_client_base_url(target_env):
    cfg, client = _make_client(target_env)
    assert client.base_url == "https://mc.adobe.io/testtenant/target/recs"


def test_get_sends_correct_headers(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([_Resp(200, {"id": 1, "name": "test"})])
    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.client.requests.Session",
        lambda: session,
    )
    # Re-init session
    client.session = session

    result = client.get("/criteria/123")
    assert result == {"id": 1, "name": "test"}
    call = session.calls[0]
    assert call["method"] == "GET"
    assert "/criteria/123" in call["url"]
    assert call["headers"]["Authorization"] == "Bearer tok"


def test_get_all_pagination(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([
        _Resp(200, [{"id": 1}, {"id": 2}]),
        _Resp(200, [{"id": 3}]),
    ])
    client.session = session

    items = client.get_all("/criteria", limit=2)
    assert len(items) == 3
    assert items[0]["id"] == 1
    assert items[2]["id"] == 3


def test_get_all_max_items(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([
        _Resp(200, [{"id": 1}, {"id": 2}, {"id": 3}]),
    ])
    client.session = session

    items = client.get_all("/criteria", limit=100, max_items=2)
    assert len(items) == 2


def test_retry_on_429(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([
        _Resp(429, {"error": "rate limit"}, {"Retry-After": "0"}),
        _Resp(200, {"id": 1}),
    ])
    client.session = session

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.client.time.sleep",
        lambda _: None,
    )

    result = client.get("/criteria/1")
    assert result == {"id": 1}
    assert len(session.calls) == 2


def test_401_triggers_refresh(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([
        _Resp(401, {"error": "unauthorized"}),
        _Resp(200, {"id": 1}),
    ])
    client.session = session

    result = client.get("/criteria/1")
    assert result == {"id": 1}
    # Second call should have refreshed headers
    assert len(session.calls) == 2


def test_patch_sends_json(target_env, monkeypatch):
    _, client = _make_client(target_env)

    session = _Session([_Resp(200, {"id": 1, "updated": True})])
    client.session = session

    result = client.patch("/criteria/1", {"name": "updated"})
    assert result["updated"] is True
    assert session.calls[0]["method"] == "PATCH"
    assert session.calls[0]["json"] == {"name": "updated"}
