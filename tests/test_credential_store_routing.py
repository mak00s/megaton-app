"""App credential routing: megaton credential-store fallback and store tips."""

from __future__ import annotations

import json

import pytest

from megaton_lib import credentials, megaton_client as mc


def sa_key(name: str) -> dict:
    return {
        "type": "service_account", "project_id": "proj",
        "client_email": f"{name}@proj.iam.gserviceaccount.com",
        "private_key_id": "k", "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token",
    }


@pytest.fixture
def store(tmp_path, monkeypatch):
    pytest.importorskip("megaton.auth.store")
    home = tmp_path / "store"
    (home / "sa").mkdir(parents=True)
    (home / "sa" / "corp.json").write_text(json.dumps(sa_key("corp")), encoding="utf-8")
    monkeypatch.setenv("MEGATON_HOME", str(home))
    monkeypatch.delenv("MEGATON_CREDS_PATH", raising=False)
    return home


def no_repo_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(credentials, "_resolve_default_dir", lambda default_dir: tmp_path / "absent")


def test_falls_back_to_store_when_no_credentials_dir(store, tmp_path, monkeypatch):
    no_repo_dir(monkeypatch, tmp_path)
    assert credentials.list_service_account_paths() == [str(store / "sa" / "corp.json")]


def test_repo_credentials_dir_still_wins_over_store(store, tmp_path, monkeypatch):
    repo = tmp_path / "credentials"
    repo.mkdir()
    (repo / "local.json").write_text(json.dumps(sa_key("local")), encoding="utf-8")
    monkeypatch.setattr(credentials, "_resolve_default_dir", lambda default_dir: repo)
    assert credentials.list_service_account_paths() == [str(repo / "local.json")]


def test_existing_empty_credentials_dir_does_not_fall_back(store, tmp_path, monkeypatch):
    repo = tmp_path / "credentials"
    repo.mkdir()
    monkeypatch.setattr(credentials, "_resolve_default_dir", lambda default_dir: repo)
    assert credentials.list_service_account_paths() == []


def test_env_var_disables_store_fallback(store, tmp_path, monkeypatch):
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("MEGATON_CREDS_PATH", str(empty))
    assert credentials.list_service_account_paths() == []


def test_explicit_default_dir_disables_store_fallback(store, tmp_path):
    assert credentials.list_service_account_paths(default_dir=tmp_path / "unused") == []


def test_repo_keys_missing_from_store_get_one_tip(store, tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("MEGATON_HINTS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    repo = tmp_path / "credentials"
    repo.mkdir()
    (repo / "local.json").write_text(json.dumps(sa_key("local")), encoding="utf-8")
    monkeypatch.setattr(credentials, "_resolve_default_dir", lambda default_dir: repo)
    credentials.list_service_account_paths()
    credentials.list_service_account_paths()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.count("[megaton-app] hint:") == 1
    assert f"megaton auth import --sa {repo / 'local.json'} --name <name>" in captured.err


def test_router_suppresses_core_store_tips(monkeypatch):
    hints = pytest.importorskip("megaton.auth.hints")
    if not hasattr(hints, "suppressed"):
        pytest.skip("core without tip suppression")
    seen = []

    class FakeMegaton:
        def __init__(self, path, headless):
            seen.append(hints.hints_enabled())

    monkeypatch.delenv("MEGATON_HINTS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setattr(mc.start, "Megaton", FakeMegaton)
    mc.reset_registry()
    mc.get_megaton("/keys/picked-by-router.json")
    assert seen == [False]
    assert hints.hints_enabled() is True
    mc.reset_registry()


@pytest.mark.parametrize("evidence,expected", [
    ({"status": "truncated", "fetched_rows": 1000, "row_limit": 1000, "total_rows": 4321},
     "Fetched 1000 of 4321 rows; set limit to at least 4321."),
    ({"status": "truncated", "fetched_rows": 1000, "row_limit": 1000, "total_rows": 250000},
     "exceeds the 100000 row maximum; split the date range"),
    ({"status": "unknown", "fetched_rows": 1000, "row_limit": 1000, "total_rows": None},
     "exactly the limit 1000; the source may have more. Raise limit (for example to 10000)"),
    ({"status": "unknown", "fetched_rows": 100000, "row_limit": 100000, "total_rows": None},
     "the 100000 row maximum; the source may have more. Split the date range"),
    ({"status": "unknown", "fetched_rows": None, "row_limit": None, "total_rows": None},
     "raw_rows/v1 completeness contract"),
])
def test_incomplete_error_gives_concrete_next_step(evidence, expected):
    error = mc.QueryIncompleteError(evidence)
    assert expected in error.suggestion
    assert expected in str(error)
    assert "allow_incomplete=True" in str(error)


def test_repo_key_tip_quotes_paths_with_spaces(store, tmp_path, monkeypatch, capsys):
    import shlex

    monkeypatch.delenv("MEGATON_HINTS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    repo = tmp_path / "my repo" / "credentials"
    repo.mkdir(parents=True)
    key = repo / "local key.json"
    key.write_text(json.dumps(sa_key("local")), encoding="utf-8")
    monkeypatch.setattr(credentials, "_resolve_default_dir", lambda default_dir: repo)
    credentials.list_service_account_paths()
    err = capsys.readouterr().err
    command = err.split("`megaton auth import", 1)[1].split("`", 1)[0]
    assert str(key) in shlex.split(command)
