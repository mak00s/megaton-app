from __future__ import annotations

import json
import urllib.error

import pytest

from megaton_lib import notify
from megaton_lib.notify import post_webhook

pytestmark = pytest.mark.unit


class _FakeResp:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_post_webhook_success_sends_json(monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["body"] = json.loads(req.data.decode("utf-8"))
        captured["content_type"] = req.get_header("Content-type")
        captured["timeout"] = timeout
        return _FakeResp(200)

    monkeypatch.setattr(notify.urllib.request, "urlopen", fake_urlopen)
    assert post_webhook("https://hook.example/x", {"text": "こんにちは", "n": 1}) is True
    assert captured["body"] == {"text": "こんにちは", "n": 1}
    assert captured["content_type"] == "application/json"
    assert captured["timeout"] == 10


def test_post_webhook_non_2xx_returns_false(monkeypatch):
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda req, timeout=None: _FakeResp(500))
    assert post_webhook("https://hook.example/x", {}) is False


def test_post_webhook_never_raises_on_network_error(monkeypatch):
    def boom(req, timeout=None):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(notify.urllib.request, "urlopen", boom)
    assert post_webhook("https://hook.example/x", {"a": 1}) is False


def test_post_webhook_empty_url_is_noop():
    assert post_webhook("", {"a": 1}) is False


def test_post_webhook_never_raises_on_unserializable_payload(monkeypatch):
    def must_not_be_called(req, timeout=None):  # pragma: no cover
        raise AssertionError("urlopen must not run when serialization fails")

    monkeypatch.setattr(notify.urllib.request, "urlopen", must_not_be_called)
    circular: dict = {}
    circular["self"] = circular
    assert post_webhook("https://hook.example/x", circular) is False  # ValueError
    assert post_webhook("https://hook.example/x", {("t", "k"): 1}) is False  # TypeError
