"""Tests for megaton_lib.audit.providers.tag_config.bootstrap."""

from __future__ import annotations

import json

import pytest

from megaton_lib.audit.providers.tag_config.bootstrap import (
    build_tags_config,
    seed_adobe_oauth_env,
)


def test_seed_adobe_oauth_env_explicit_args(monkeypatch):
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_SECRET", raising=False)
    cid, secret, org = seed_adobe_oauth_env(
        client_id="id1", client_secret="secret1", org_id="org1",
    )
    assert cid == "id1"
    assert secret == "secret1"
    assert org == "org1"


def test_seed_adobe_oauth_env_creds_file(monkeypatch, tmp_path):
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ADOBE_ORG_ID", raising=False)

    creds = {"client_id": "file-id", "client_secret": "file-secret", "org_id": "file-org"}
    creds_path = tmp_path / "creds.json"
    creds_path.write_text(json.dumps(creds))

    cid, secret, org = seed_adobe_oauth_env(creds_file=creds_path)
    assert cid == "file-id"
    assert secret == "file-secret"
    assert org == "file-org"


def test_seed_adobe_oauth_env_explicit_overrides_creds_file(monkeypatch, tmp_path):
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_SECRET", raising=False)

    creds = {"client_id": "file-id", "client_secret": "file-secret", "org_id": "file-org"}
    creds_path = tmp_path / "creds.json"
    creds_path.write_text(json.dumps(creds))

    cid, secret, org = seed_adobe_oauth_env(
        creds_file=creds_path,
        client_id="explicit-id",
    )
    assert cid == "explicit-id"
    assert secret == "file-secret"


def test_seed_adobe_oauth_env_missing_creds_file_ignored(monkeypatch, tmp_path):
    monkeypatch.setenv("ADOBE_CLIENT_ID", "env-id")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "env-secret")

    cid, secret, _ = seed_adobe_oauth_env(
        creds_file=tmp_path / "nonexistent.json",
    )
    assert cid == "env-id"
    assert secret == "env-secret"


def test_build_tags_config_with_creds_file(monkeypatch, tmp_path):
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_SECRET", raising=False)

    creds = {"client_id": "file-id", "client_secret": "file-secret", "org_id": "file-org"}
    creds_path = tmp_path / "creds.json"
    creds_path.write_text(json.dumps(creds))

    config = build_tags_config(
        property_id="PR123",
        creds_file=creds_path,
        token_cache_file=tmp_path / ".cache.json",
    )
    assert config.property_id == "PR123"
