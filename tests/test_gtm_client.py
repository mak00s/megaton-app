from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from megaton_lib import gtm_client, gtm_review


def token_file(tmp_path, **extra):
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"type": "authorized_user", "scopes": gtm_client.SCOPES_READ,
                                "refresh_token": "fixture-only", **extra}))
    return path


def install_credentials(monkeypatch, *, email="test@example.com", verified=True, granted=None):
    from google.oauth2.credentials import Credentials

    creds = SimpleNamespace(granted_scopes=granted)
    load = MagicMock(return_value=creds)
    refresh = MagicMock(side_effect=lambda c: c)
    identity = MagicMock()
    identity.userinfo().get().execute.return_value = {"email": email, "verified_email": verified}
    service = MagicMock()
    build = MagicMock(side_effect=lambda api, version, **kwargs: identity if api == "oauth2" else service)
    monkeypatch.setattr(Credentials, "from_authorized_user_info", load)
    monkeypatch.setattr(gtm_client, "refresh_user_credentials", refresh)
    monkeypatch.setattr(gtm_client, "build_service", build)
    return creds, load, refresh, build, service


def test_oauth_load_preserves_grant_and_verifies_account(tmp_path, monkeypatch):
    path = token_file(tmp_path)
    creds, load, _, build, service = install_credentials(monkeypatch)
    original = path.read_bytes()
    client = gtm_client.GtmClient.from_oauth_file(path, expected_email="TEST@example.com")
    assert client._service is service
    assert load.call_args.args[0]["scopes"] == gtm_client.SCOPES_READ
    assert load.call_args.kwargs == {}
    assert path.read_bytes() == original
    assert build.call_args_list[-1].args == ("tagmanager", "v2")


@pytest.mark.parametrize("extra", [
    {"type": "service_account", "private_key": "fixture-only"},
    {"scopes": None}, {"scopes": []},
    {"scopes": gtm_client.SCOPES_READ + ["https://www.googleapis.com/auth/tagmanager.publish"]},
    {"scopes": gtm_client.SCOPES_READ + ["https://www.googleapis.com/auth/gmail.readonly"]},
])
def test_invalid_tokens_stop_before_refresh(tmp_path, monkeypatch, extra):
    _, load, refresh, build, _ = install_credentials(monkeypatch)
    with pytest.raises(gtm_client.GtmAuthError):
        gtm_client.GtmClient.from_oauth_file(token_file(tmp_path, **extra), expected_email="test@example.com")
    load.assert_not_called()
    refresh.assert_not_called()
    build.assert_not_called()


@pytest.mark.parametrize("email,verified", [("other@example.com", True), ("test@example.com", False)])
def test_account_mismatch_never_builds_gtm(tmp_path, monkeypatch, email, verified):
    _, _, _, build, _ = install_credentials(monkeypatch, email=email, verified=verified)
    with pytest.raises(gtm_client.GtmAuthError, match="expected verified email"):
        gtm_client.GtmClient.from_oauth_file(token_file(tmp_path), expected_email="test@example.com")
    assert [c.args[0] for c in build.call_args_list] == ["oauth2"]


def test_granted_scopes_checked_after_refresh(tmp_path, monkeypatch):
    _, _, _, build, _ = install_credentials(monkeypatch, granted=[gtm_client.SCOPES_READ[1]])
    with pytest.raises(gtm_client.GtmAuthError, match="Missing"):
        gtm_client.GtmClient.from_oauth_file(token_file(tmp_path), expected_email="test@example.com")
    build.assert_not_called()


def test_missing_account_fails_before_token_read(monkeypatch):
    with pytest.raises(gtm_client.GtmAuthError, match="expected_email"):
        gtm_client.GtmClient.from_oauth_file("missing.json", expected_email="")


def install_flow(tmp_path, monkeypatch):
    from google_auth_oauthlib.flow import InstalledAppFlow

    client = tmp_path / "client.json"
    client.write_text(json.dumps({"installed": {"client_id": "fixture-only"}}))
    creds = SimpleNamespace(scopes=gtm_client.SCOPES_READ, granted_scopes=gtm_client.SCOPES_READ,
                            refresh_token="fixture-only", to_json=lambda: json.dumps({
                                "scopes": gtm_client.SCOPES_READ, "refresh_token": "fixture-only"}))
    flow = MagicMock()
    flow.run_local_server.return_value = creds
    factory = MagicMock(return_value=flow)
    monkeypatch.setattr(InstalledAppFlow, "from_client_secrets_file", factory)
    verify = MagicMock()
    monkeypatch.setattr(gtm_client, "_verify_identity", verify)
    return client, creds, flow, factory, verify


def test_initial_auth_verifies_before_private_save(tmp_path, monkeypatch):
    client, _, flow, factory, verify = install_flow(tmp_path, monkeypatch)
    token = tmp_path / "separate" / "token.json"
    verify.side_effect = lambda *_: pytest.fail("saved before verification") if token.exists() else None
    gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token, expected_email="test@example.com")
    assert token.exists()
    assert token.stat().st_mode & 0o777 == 0o600
    assert factory.call_args.args == (str(client), gtm_client.SCOPES_READ)
    assert flow.run_local_server.call_args.kwargs["login_hint"] == "test@example.com"
    assert list(token.parent.glob(".gtm-token-*")) == []


def test_initial_auth_wrong_account_does_not_save(tmp_path, monkeypatch):
    client, _, _, _, verify = install_flow(tmp_path, monkeypatch)
    verify.side_effect = gtm_client.GtmAuthError("wrong account")
    token = tmp_path / "token.json"
    with pytest.raises(gtm_client.GtmAuthError):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token, expected_email="test@example.com")
    assert not token.exists()


def test_auth_reuses_existing_token_without_consent(tmp_path, monkeypatch):
    client, _, _, factory, _ = install_flow(tmp_path, monkeypatch)
    install_credentials(monkeypatch)
    token = token_file(tmp_path)
    before = token.read_bytes()
    gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token, expected_email="test@example.com")
    factory.assert_not_called()
    assert token.read_bytes() == before


def test_auth_invalid_existing_token_never_reauthorizes(tmp_path, monkeypatch):
    client, _, _, factory, _ = install_flow(tmp_path, monkeypatch)
    token = token_file(tmp_path, scopes=[])
    before = token.read_bytes()
    with pytest.raises(gtm_client.GtmAuthError):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token, expected_email="test@example.com")
    factory.assert_not_called()
    assert token.read_bytes() == before


def test_auth_no_refresh_token_does_not_save(tmp_path, monkeypatch):
    client, creds, _, _, _ = install_flow(tmp_path, monkeypatch)
    creds.refresh_token = None
    with pytest.raises(gtm_client.GtmAuthError, match="refresh token"):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "token.json",
                                      expected_email="test@example.com")
    assert not (tmp_path / "token.json").exists()


def test_initial_auth_broad_grant_does_not_save(tmp_path, monkeypatch):
    client, creds, _, _, verify = install_flow(tmp_path, monkeypatch)
    creds.granted_scopes = gtm_client.SCOPES_READ + ["https://www.googleapis.com/auth/tagmanager.publish"]
    with pytest.raises(gtm_client.GtmAuthError, match="non-publishing"):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "token.json",
                                      expected_email="test@example.com")
    verify.assert_not_called()
    assert not (tmp_path / "token.json").exists()


def test_edit_authorization_requests_no_publish(tmp_path, monkeypatch):
    client, creds, _, factory, _ = install_flow(tmp_path, monkeypatch)
    creds.scopes = creds.granted_scopes = gtm_client.SCOPES_EDIT
    gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "edit.json",
                                  expected_email="test@example.com", access="edit")
    assert factory.call_args.args[1] == gtm_client.SCOPES_EDIT
    assert not any(s.endswith("/tagmanager.publish") for s in factory.call_args.args[1])


def test_review_accepts_non_publishing_edit_token(tmp_path, monkeypatch):
    install_credentials(monkeypatch, granted=gtm_client.SCOPES_EDIT)
    gtm_client.GtmClient.from_oauth_file(token_file(tmp_path, scopes=gtm_client.SCOPES_EDIT),
                                        expected_email="test@example.com")


def test_edit_auth_never_upgrades_read_token(tmp_path, monkeypatch):
    client, _, _, factory, _ = install_flow(tmp_path, monkeypatch)
    install_credentials(monkeypatch)
    token = token_file(tmp_path)
    before = token.read_bytes()
    with pytest.raises(gtm_client.GtmAuthError, match="Missing GTM edit"):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token,
                                      expected_email="test@example.com", access="edit")
    factory.assert_not_called()
    assert token.read_bytes() == before


@pytest.mark.parametrize("suffix", ["publish", "delete.containers", "manage.users", "manage.accounts"])
def test_edit_access_rejects_forbidden_scopes(suffix):
    with pytest.raises(gtm_client.GtmAuthError, match="non-publishing"):
        gtm_client._require_scopes(gtm_client.SCOPES_EDIT + [
            f"https://www.googleapis.com/auth/tagmanager.{suffix}"], access="edit")


def test_oauthlib_added_openid_is_checked_before_recovery(tmp_path, monkeypatch):
    client, creds, flow, _, _ = install_flow(tmp_path, monkeypatch)
    actual = gtm_client.SCOPES_EDIT + ["openid"]
    warning = Warning("fixture scope change")
    warning.new_scope = actual
    warning.token = {"scope": " ".join(actual), "access_token": "fixture-only"}
    flow.run_local_server.side_effect = warning
    creds.scopes = creds.granted_scopes = actual
    flow.credentials = creds
    gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "edit.json",
                                  expected_email="test@example.com", access="edit")
    assert flow.oauth2session.token["scope"].split() == actual


def test_scope_change_with_publish_is_not_recovered(tmp_path, monkeypatch):
    client, _, flow, _, verify = install_flow(tmp_path, monkeypatch)
    warning = Warning("fixture scope change")
    warning.new_scope = gtm_client.SCOPES_EDIT + ["https://www.googleapis.com/auth/tagmanager.publish"]
    warning.token = {"scope": " ".join(warning.new_scope), "access_token": "fixture-only"}
    flow.run_local_server.side_effect = warning
    with pytest.raises(gtm_client.GtmAuthError, match="non-publishing"):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "edit.json",
                                      expected_email="test@example.com", access="edit")
    verify.assert_not_called()
    assert not (tmp_path / "edit.json").exists()


def test_concurrent_token_creation_is_not_overwritten(tmp_path, monkeypatch):
    client, creds, flow, _, _ = install_flow(tmp_path, monkeypatch)
    token = tmp_path / "token.json"

    def authorize(**kwargs):
        token.write_text("other authorization")
        return creds

    flow.run_local_server.side_effect = authorize
    with pytest.raises(FileExistsError):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=token,
                                      expected_email="test@example.com")
    assert token.read_text() == "other authorization"
    assert list(tmp_path.glob(".gtm-token-*")) == []


def test_auth_rejects_service_account_client(tmp_path):
    client = tmp_path / "client.json"
    client.write_text('{"type":"service_account"}')
    with pytest.raises(gtm_client.GtmAuthError, match="Desktop"):
        gtm_client.authorize_gtm_user(client_secrets_path=client, token_path=tmp_path / "token.json",
                                      expected_email="test@example.com")


def test_container_inventory_paginates_accounts_and_containers():
    service = MagicMock()
    service.accounts().list().execute.side_effect = [
        {"account": [{"path": "accounts/1"}], "nextPageToken": "a"},
        {"account": [{"path": "accounts/2"}]},
    ]
    service.accounts().containers().list().execute.side_effect = [
        {"container": [{"publicId": "GTM-A"}], "nextPageToken": "b"},
        {"container": [{"publicId": "GTM-B"}]},
        {"container": [{"publicId": "GTM-C"}]},
    ]
    assert [c["publicId"] for c in gtm_client.GtmClient(service).containers()] == ["GTM-A", "GTM-B", "GTM-C"]


def test_review_reads_every_workspace_and_live_without_mutations():
    service = MagicMock()
    containers = service.accounts().containers()
    containers.get().execute.return_value = {"publicId": "GTM-A"}
    containers.versions().live().execute.return_value = {"containerVersionId": "12"}
    workspaces = containers.workspaces()
    workspaces.list().execute.return_value = {"workspace": [
        {"path": "accounts/1/containers/2/workspaces/3"},
        {"path": "accounts/1/containers/2/workspaces/4"},
    ]}
    for plural, singular in (("tags", "tag"), ("triggers", "trigger"), ("variables", "variable"),
                             ("folders", "folder"), ("templates", "template"),
                             ("built_in_variables", "builtInVariable")):
        getattr(workspaces, plural)().list().execute.return_value = {singular: [{"name": plural}]}
    workspaces.getStatus().execute.return_value = {"workspaceChange": [{"changeStatus": "updated"}]}
    service.reset_mock()
    result = gtm_client.GtmClient(service).review("accounts/1/containers/2")
    assert len(result["workspaces"]) == 2
    assert result["live_version"]["containerVersionId"] == "12"
    assert result["workspaces"][0]["resources"]["templates"] == [{"name": "templates"}]
    assert not result["snapshot_atomic"]
    for call in service.mock_calls:
        assert not any(f".{name}(" in str(call) for name in
                       ("create", "update", "delete", "sync", "publish", "quick_preview", "resolve_conflict"))


def test_repeated_pagination_token_fails_instead_of_partial_success():
    resource = MagicMock()
    resource.list().execute.return_value = {"tag": [], "nextPageToken": "same"}
    with pytest.raises(RuntimeError, match="repeated page token"):
        gtm_client.GtmClient._list(resource, "tag", parent="fixture")


def test_review_invalid_path_makes_no_request():
    service = MagicMock()
    with pytest.raises(ValueError):
        gtm_client.GtmClient(service).review("https://tagmanager.google.com/")
    assert not service.mock_calls


def test_cli_help_does_not_authenticate(monkeypatch, capsys):
    load = MagicMock()
    monkeypatch.setattr(gtm_review.GtmClient, "from_oauth_file", load)
    with pytest.raises(SystemExit) as exc:
        gtm_review.main(["--help"])
    assert exc.value.code == 0
    assert "No GTM edits or publication" in capsys.readouterr().out
    load.assert_not_called()


def test_cli_redacts_api_error_and_preserves_output(tmp_path, monkeypatch, capsys):
    output = tmp_path / "result.json"
    output.write_text("previous snapshot")
    load = MagicMock(side_effect=RuntimeError("SECRET_TOKEN=fixture"))
    monkeypatch.setattr(gtm_review.GtmClient, "from_oauth_file", load)
    assert gtm_review.main(["containers", "--token", "missing.json", "--expected-email",
                           "test@example.com", "--output", str(output)]) == 1
    assert "SECRET_TOKEN" not in capsys.readouterr().err
    assert output.read_text() == "previous snapshot"


def test_cli_does_not_overwrite_token(tmp_path, monkeypatch):
    load = MagicMock()
    monkeypatch.setattr(gtm_review.GtmClient, "from_oauth_file", load)
    token = token_file(tmp_path)
    assert gtm_review.main(["containers", "--token", str(token), "--expected-email",
                           "test@example.com", "--output", str(token)]) == 1
    load.assert_not_called()


def test_cli_writes_private_snapshot(tmp_path, monkeypatch):
    client = MagicMock()
    client.containers.return_value = [{"publicId": "GTM-A"}]
    monkeypatch.setattr(gtm_review.GtmClient, "from_oauth_file", lambda *a, **k: client)
    output = tmp_path / "result.json"
    assert gtm_review.main(["containers", "--token", "fixture.json", "--expected-email",
                           "test@example.com", "--output", str(output)]) == 0
    assert json.loads(output.read_text())["containers"] == [{"publicId": "GTM-A"}]
    assert output.stat().st_mode & 0o777 == 0o600
