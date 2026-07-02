from __future__ import annotations

import pytest
import requests

from megaton_lib import http_fetch
from megaton_lib.http_fetch import _with_retry, fetch_json, fetch_text, safe_fetch_json

pytestmark = pytest.mark.unit


class _Resp:
    def __init__(self, *, text="", json_data=None, status=200):
        self.text = text
        self._json = json_data
        self.status_code = status
        self.content = text.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


def test_with_retry_retries_then_succeeds():
    calls = {"n": 0}
    sleeps: list[float] = []

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.ConnectionError("boom")
        return "ok"

    assert _with_retry(flaky, sleep=sleeps.append) == "ok"
    assert calls["n"] == 3
    assert sleeps == [1.5, 3.0]  # expo backoff, clamped to [1, 8]


def test_with_retry_reraises_after_max_attempts():
    sleeps: list[float] = []

    def always_fails():
        raise requests.ConnectionError("down")

    with pytest.raises(requests.ConnectionError):
        _with_retry(always_fails, sleep=sleeps.append)
    assert len(sleeps) == 2  # 3 attempts → 2 waits


def test_with_retry_does_not_retry_non_request_errors():
    calls = {"n": 0}

    def bug():
        calls["n"] += 1
        raise ValueError("logic bug")

    with pytest.raises(ValueError):
        _with_retry(bug, sleep=lambda _s: None)
    assert calls["n"] == 1


def test_fetch_text_sends_ua_and_returns_body(monkeypatch):
    captured = {}

    def fake_get(url, headers=None, timeout=None):
        captured.update(url=url, headers=headers, timeout=timeout)
        return _Resp(text="hello")

    monkeypatch.setattr(http_fetch.requests, "get", fake_get)
    assert fetch_text("https://example.com/x") == "hello"
    assert "Mozilla" in captured["headers"]["User-Agent"]
    assert captured["timeout"] == http_fetch.DEFAULT_TIMEOUT


def test_fetch_json_post_sends_payload(monkeypatch):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json)
        return _Resp(json_data={"ok": True})

    monkeypatch.setattr(http_fetch.requests, "post", fake_post)
    assert fetch_json("https://example.com/api", method="POST", payload={"a": 1}) == {"ok": True}
    assert captured["json"] == {"a": 1}


def test_safe_fetch_json_returns_none_on_error(monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        return _Resp(status=500)

    monkeypatch.setattr(http_fetch.requests, "get", fake_get)
    monkeypatch.setattr(http_fetch.time, "sleep", lambda _s: None)
    assert safe_fetch_json("https://example.com/api") is None
