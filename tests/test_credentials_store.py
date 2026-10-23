"""Credential-store integration: --account resolution for Docs/Gmail/GTM CLIs and SA lookup."""

import json
from pathlib import Path

import pytest

from megaton_lib import credentials


@pytest.fixture
def store(tmp_path, monkeypatch):
    home = tmp_path / "store"
    monkeypatch.setenv("MEGATON_HOME", str(home))
    (home / "users").mkdir(parents=True)
    (home / "sa").mkdir()
    (home / "credentials.toml").write_text(
        '[users]\nsim = "sim@example.com"\n[sa]\nsheets = "sa_proj_sheets.json"\n', encoding="utf-8"
    )
    (home / "users" / "sim@example.com.json").write_text(json.dumps({
        "refresh_token": "r", "client_id": "c", "client_secret": "s", "token_uri": "https://oauth2.googleapis.com/token",
        "scopes": ["https://www.googleapis.com/auth/documents"], "account": "sim@example.com",
    }), encoding="utf-8")
    (home / "sa" / "sa_proj_sheets.json").write_text(json.dumps({
        "type": "service_account", "project_id": "proj", "client_email": "sheets@proj.iam.gserviceaccount.com",
        "private_key": "k", "token_uri": "https://oauth2.googleapis.com/token",
    }), encoding="utf-8")
    return home


def test_store_user_token_resolves_alias_to_store_file(store):
    path, email = credentials.store_user_token("sim")
    assert Path(path) == store / "users" / "sim@example.com.json"
    assert email == "sim@example.com"
    with pytest.raises(FileNotFoundError, match="megaton auth login --account other@example.com"):
        credentials.store_user_token("other@example.com")


def test_resolve_account_args_prefers_explicit_values(store):
    assert credentials.resolve_account_args("sim", None, None) == (
        str(store / "users" / "sim@example.com.json"), "sim@example.com")
    assert credentials.resolve_account_args("sim", "/explicit.json", None) == ("/explicit.json", "sim@example.com")
    assert credentials.resolve_account_args(None, "/t.json", "x@example.com") == ("/t.json", "x@example.com")
    with pytest.raises(ValueError, match="--account"):
        credentials.resolve_account_args(None, "/t.json", None)


def test_service_account_store_prefix_and_alias(store, monkeypatch):
    assert credentials.store_service_account_path("sheets") == str(store / "sa" / "sa_proj_sheets.json")
    monkeypatch.setenv("MEGATON_CREDS_PATH", "store:sheets")
    assert credentials.resolve_service_account_path() == str(store / "sa" / "sa_proj_sheets.json")
    monkeypatch.setenv("MEGATON_CREDS_PATH", "store:missing")
    with pytest.raises(Exception, match="missing"):
        credentials.resolve_service_account_path()


def test_docs_edit_account_fills_token_and_email(store, monkeypatch, capsys):
    from megaton_lib import docs_edit

    captured = {}

    class FakeClient:
        @classmethod
        def from_oauth_file(cls, token, *, expected_email):
            captured.update(token=token, expected_email=expected_email)
            return cls()

        def get(self, document_id):
            return {"documentId": document_id}

    monkeypatch.setattr(docs_edit, "DocsClient", FakeClient)
    assert docs_edit.main(["--account", "sim", "get", "doc1"]) == 0
    assert captured == {"token": str(store / "users" / "sim@example.com.json"), "expected_email": "sim@example.com"}
    assert json.loads(capsys.readouterr().out)["document"]["documentId"] == "doc1"


def test_docs_edit_without_account_or_token_fails(store, capsys):
    from megaton_lib import docs_edit

    assert docs_edit.main(["get", "doc1"]) != 0


def test_gtm_review_account_resolves_before_dispatch(store, monkeypatch, tmp_path):
    from megaton_lib import gtm_review

    seen = {}

    def fake_authorize(**kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(gtm_review, "authorize_gtm_user", fake_authorize)
    client = tmp_path / "client.json"
    client.write_text("{}")
    assert gtm_review.main(["auth", "--account", "sim", "--client-secrets", str(client)]) == 0
    assert seen["token_path"] == store / "users" / "sim@example.com.json"
    assert seen["expected_email"] == "sim@example.com"


def test_list_service_account_paths_accepts_store_prefix(store, monkeypatch):
    monkeypatch.setenv("MEGATON_CREDS_PATH", "store:sheets")
    assert credentials.list_service_account_paths() == [str(store / "sa" / "sa_proj_sheets.json")]


def test_store_helpers_explain_missing_megaton(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "megaton.auth.store":
            raise ImportError("old megaton")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="megaton>=2.6.0"):
        credentials.store_user_token("sim")
