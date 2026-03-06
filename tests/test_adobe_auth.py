"""Tests for the shared Adobe OAuth client."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient, IMS_TOKEN_URL


class _TokenResponse:
    """Minimal requests.Response stub for token endpoint."""

    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    """Set up required env vars and tmp cache path."""
    monkeypatch.setenv("ADOBE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("ADOBE_ORG_ID", "TEST@AdobeOrg")
    return tmp_path


def _make_client(tmp_path: Path, **overrides) -> AdobeOAuthClient:
    """Create client with token cache in tmp_path."""
    kwargs = {"token_cache_file": str(tmp_path / ".token.json")}
    kwargs.update(overrides)
    return AdobeOAuthClient(**kwargs)


# ---- init / credential resolution ----


def test_missing_client_id_raises(monkeypatch, tmp_path):
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "s")
    monkeypatch.setenv("ADOBE_ORG_ID", "o")
    with pytest.raises(RuntimeError, match="client_id"):
        _make_client(tmp_path)


def test_missing_client_secret_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("ADOBE_CLIENT_ID", "c")
    monkeypatch.delenv("ADOBE_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("ADOBE_ORG_ID", "o")
    with pytest.raises(RuntimeError, match="client_secret"):
        _make_client(tmp_path)


def test_missing_org_id_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("ADOBE_CLIENT_ID", "c")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "s")
    monkeypatch.delenv("ADOBE_ORG_ID", raising=False)
    with pytest.raises(RuntimeError, match="org_id"):
        _make_client(tmp_path)


def test_explicit_values_override_env(auth_env, monkeypatch):
    """Explicit constructor values take precedence over env vars."""
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.requests.post",
        lambda *a, **kw: _TokenResponse(200, {"access_token": "tok", "expires_in": 3600}),
    )
    client = _make_client(
        auth_env,
        client_id="explicit-id",
        client_secret="explicit-secret",
        org_id="EXPLICIT@AdobeOrg",
    )
    assert client.client_id == "explicit-id"
    assert client.client_secret == "explicit-secret"
    assert client.org_id == "EXPLICIT@AdobeOrg"


# ---- token request ----


def test_token_request_on_init(auth_env, monkeypatch):
    """On init without cache, a token request is made."""
    mock_post = MagicMock(
        return_value=_TokenResponse(200, {"access_token": "fresh-token", "expires_in": 7200})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)

    assert client.access_token == "fresh-token"
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == IMS_TOKEN_URL
    assert call_kwargs[1]["data"]["grant_type"] == "client_credentials"
    assert call_kwargs[1]["data"]["client_id"] == "test-client-id"


def test_token_request_failure_raises(auth_env, monkeypatch):
    """Failed token request raises RuntimeError."""
    mock_post = MagicMock(
        return_value=_TokenResponse(401, {"error": "invalid_client"})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    with pytest.raises(RuntimeError, match="token request failed"):
        _make_client(auth_env)


# ---- token caching ----


def test_cached_token_used_on_init(auth_env, monkeypatch):
    """If a valid cached token exists, no network request is made."""
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "cached-tok", "expires_at": time.time() + 3600}),
        encoding="utf-8",
    )

    mock_post = MagicMock()
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)

    assert client.access_token == "cached-tok"
    mock_post.assert_not_called()


def test_expired_cache_triggers_request(auth_env, monkeypatch):
    """Expired cached token triggers a new token request."""
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "old-tok", "expires_at": time.time() - 100}),
        encoding="utf-8",
    )

    mock_post = MagicMock(
        return_value=_TokenResponse(200, {"access_token": "new-tok", "expires_in": 3600})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)

    assert client.access_token == "new-tok"
    mock_post.assert_called_once()


def test_cache_within_60s_buffer_triggers_refresh(auth_env, monkeypatch):
    """Token expiring within 60s buffer is treated as expired."""
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "almost-expired", "expires_at": time.time() + 30}),
        encoding="utf-8",
    )

    mock_post = MagicMock(
        return_value=_TokenResponse(200, {"access_token": "refreshed", "expires_in": 3600})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)

    assert client.access_token == "refreshed"


def test_corrupted_cache_triggers_request(auth_env, monkeypatch):
    """Corrupted cache file triggers a new token request."""
    cache_path = auth_env / ".token.json"
    cache_path.write_text("not-json", encoding="utf-8")

    mock_post = MagicMock(
        return_value=_TokenResponse(200, {"access_token": "fallback", "expires_in": 3600})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)

    assert client.access_token == "fallback"


# ---- refresh ----


def test_refresh_access_token(auth_env, monkeypatch):
    """refresh_access_token() force-refreshes even with valid cache."""
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "initial", "expires_at": time.time() + 3600}),
        encoding="utf-8",
    )

    call_count = 0

    def mock_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return _TokenResponse(200, {"access_token": f"refreshed-{call_count}", "expires_in": 3600})

    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    client = _make_client(auth_env)
    assert client.access_token == "initial"  # used cache
    assert call_count == 0

    new_token = client.refresh_access_token()
    assert new_token == "refreshed-1"
    assert client.access_token == "refreshed-1"
    assert call_count == 1


# ---- get_headers ----


def test_get_headers_base(auth_env, monkeypatch):
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "tok123", "expires_at": time.time() + 3600}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.requests.post",
        lambda *a, **kw: None,  # should not be called
    )

    client = _make_client(auth_env)
    headers = client.get_headers()

    assert headers["Authorization"] == "Bearer tok123"
    assert headers["x-api-key"] == "test-client-id"
    assert headers["x-gw-ims-org-id"] == "TEST@AdobeOrg"


def test_get_headers_with_extra(auth_env, monkeypatch):
    cache_path = auth_env / ".token.json"
    cache_path.write_text(
        json.dumps({"access_token": "tok", "expires_at": time.time() + 3600}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.requests.post",
        lambda *a, **kw: None,
    )

    client = _make_client(auth_env)
    headers = client.get_headers(extra={"Accept": "application/json", "X-Custom": "val"})

    assert headers["Accept"] == "application/json"
    assert headers["X-Custom"] == "val"
    assert "Authorization" in headers


# ---- token save persists to disk ----


def test_token_saved_to_disk(auth_env, monkeypatch):
    mock_post = MagicMock(
        return_value=_TokenResponse(200, {"access_token": "disk-tok", "expires_in": 7200})
    )
    monkeypatch.setattr("megaton_lib.audit.providers.adobe_auth.requests.post", mock_post)

    cache_path = auth_env / ".token.json"
    client = _make_client(auth_env)

    assert cache_path.exists()
    saved = json.loads(cache_path.read_text(encoding="utf-8"))
    assert saved["access_token"] == "disk-tok"
    assert saved["expires_at"] > time.time()
