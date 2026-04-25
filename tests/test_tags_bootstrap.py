"""Tests for megaton_lib.audit.providers.tag_config.bootstrap."""

from __future__ import annotations

import json
import os

import pytest

from megaton_lib.audit.providers.tag_config.bootstrap import (
    bootstrap_account_env,
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


def test_bootstrap_account_env_explicit_account_loads_env(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    (tmp_path / ".env.wws").write_text("ADOBE_CLIENT_ID=wws-id\n", encoding="utf-8")

    account = bootstrap_account_env("wws", project_root=tmp_path)

    assert account == "wws"
    assert os.environ["ACCOUNT"] == "wws"
    assert os.environ["ADOBE_CLIENT_ID"] == "wws-id"


def test_bootstrap_account_env_explicit_account_overrides_stale_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ACCOUNT", "wws")
    monkeypatch.setenv("TAGS_PROPERTY_ID", "PR-WWS")
    monkeypatch.setenv("ADOBE_CLIENT_ID", "wws-id")
    (tmp_path / ".env.csk").write_text(
        "TAGS_PROPERTY_ID=PR-CSK\nADOBE_CLIENT_ID=csk-id\n",
        encoding="utf-8",
    )

    account = bootstrap_account_env("csk", project_root=tmp_path)

    assert account == "csk"
    assert os.environ["ACCOUNT"] == "csk"
    assert os.environ["TAGS_PROPERTY_ID"] == "PR-CSK"
    assert os.environ["ADOBE_CLIENT_ID"] == "csk-id"


def test_bootstrap_account_env_reads_pyproject_default(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    (tmp_path / "pyproject.toml").write_text(
        '[tool.megaton]\ndefault_account = "dms"\n',
        encoding="utf-8",
    )
    (tmp_path / ".env.dms").write_text("ADOBE_CLIENT_ID=dms-id\n", encoding="utf-8")

    account = bootstrap_account_env(project_root=tmp_path)

    assert account == "dms"
    assert os.environ["ADOBE_CLIENT_ID"] == "dms-id"


def test_bootstrap_account_env_uses_single_matching_env_file(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    (tmp_path / ".env.csk").write_text("ADOBE_CLIENT_ID=csk-id\n", encoding="utf-8")

    account = bootstrap_account_env(project_root=tmp_path)

    assert account == "csk"
    assert os.environ["ADOBE_CLIENT_ID"] == "csk-id"


def test_bootstrap_account_env_resolves_from_property_hint(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    (tmp_path / ".env.csk").write_text("ADOBE_CLIENT_ID=csk-id\n", encoding="utf-8")
    (tmp_path / ".env.wws").write_text("ADOBE_CLIENT_ID=wws-id\n", encoding="utf-8")

    account = bootstrap_account_env(
        project_root=tmp_path,
        property_id="PR-CSK",
        account_hints={
            "csk": {"property_ids": ["PR-CSK"]},
            "wws": {"property_ids": ["PR-WWS"]},
        },
    )

    assert account == "csk"
    assert os.environ["ADOBE_CLIENT_ID"] == "csk-id"


def test_bootstrap_account_env_resolves_from_library_hint(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    (tmp_path / ".env.dms").write_text("ADOBE_CLIENT_ID=dms-id\n", encoding="utf-8")
    (tmp_path / ".env.wws").write_text("ADOBE_CLIENT_ID=wws-id\n", encoding="utf-8")

    account = bootstrap_account_env(
        project_root=tmp_path,
        library_id="LB-DMS",
        account_hints={
            "dms": {"library_ids": ["LB-DMS"]},
            "wws": {"library_ids": ["LB-WWS"]},
        },
    )

    assert account == "dms"
    assert os.environ["ADOBE_CLIENT_ID"] == "dms-id"


def test_bootstrap_account_env_resolves_from_remote_and_path_hints(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    monkeypatch.delenv("ADOBE_CLIENT_ID", raising=False)
    project = tmp_path / "analysis-wws"
    project.mkdir()
    (project / ".env.wws").write_text("ADOBE_CLIENT_ID=wws-id\n", encoding="utf-8")
    (project / ".env.csk").write_text("ADOBE_CLIENT_ID=csk-id\n", encoding="utf-8")

    account = bootstrap_account_env(
        project_root=project,
        git_remote_url="git@github.com:example/analysis-wws.git",
        account_hints={
            "wws": {"remote_contains": ["analysis-wws"]},
            "csk": {"path_contains": ["analysis-csk"]},
        },
    )

    assert account == "wws"
    assert os.environ["ADOBE_CLIENT_ID"] == "wws-id"


def test_bootstrap_account_env_rejects_ambiguous_hints(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    (tmp_path / ".env.csk").write_text("", encoding="utf-8")
    (tmp_path / ".env.wws").write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Account hints are ambiguous"):
        bootstrap_account_env(
            project_root=tmp_path,
            property_id="PR1",
            account_hints={
                "csk": {"property_ids": ["PR1"]},
                "wws": {"property_ids": ["PR1"]},
            },
        )


def test_bootstrap_account_env_requires_account_when_ambiguous(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    (tmp_path / ".env.csk").write_text("", encoding="utf-8")
    (tmp_path / ".env.wws").write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ACCOUNT is required"):
        bootstrap_account_env(project_root=tmp_path)


def test_bootstrap_account_env_rejects_unknown_account(monkeypatch, tmp_path):
    monkeypatch.delenv("ACCOUNT", raising=False)
    (tmp_path / ".env.other").write_text("", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Unknown ACCOUNT=other"):
        bootstrap_account_env("other", project_root=tmp_path)


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
