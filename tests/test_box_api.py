import hashlib
import json
import time
from unittest.mock import Mock

import pytest
import requests

from megaton_lib.box_api import (
    BoxAPIError,
    BoxArtifactClient,
    OAuthTokenFile,
    upload_files_to_box_folder_via_api_sync,
)


def response(data, status=200):
    return Mock(status_code=status, json=lambda: data)


def client(*responses, write=True):
    session = Mock()
    session.request.side_effect = [
        response({"login": "owner@example.test"}),
        *responses,
    ]
    api = BoxArtifactClient(
        "private-token",
        expected_login="owner@example.test",
        session=session,
        allow_write=write,
    )
    return api, session


def artifact(tmp_path):
    path = tmp_path / "report.xlsx"
    path.write_bytes(b"report")
    return path, hashlib.sha1(b"report").hexdigest()


def test_actor_mismatch():
    session = Mock()
    session.request.return_value = response({"login": "other@example.test"})
    with pytest.raises(BoxAPIError, match="box_actor_mismatch"):
        BoxArtifactClient("token", expected_login="owner@example.test", session=session)


def test_new_upload_attributes_first_and_readback(tmp_path):
    path, digest = artifact(tmp_path)
    verified = {"id": "2", "name": path.name, "sha1": digest, "parent": {"id": "1"}}
    api, session = client(
        response({"entries": [], "total_count": 0}),
        response({"entries": [{"id": "2"}]}),
        response(verified),
    )
    assert api.upload("1", path)["upload_status"] == "created"
    call = session.request.call_args_list[2]
    assert call.args[1] == "https://upload.box.com/api/2.0/files/content"
    assert [part[0] for part in call.kwargs["files"]] == ["attributes", "file"]
    assert call.kwargs["allow_redirects"] is False


def test_identical_skip_and_changed_requires_policy(tmp_path):
    path, digest = artifact(tmp_path)
    api, session = client(
        response(
            {
                "entries": [
                    {"id": "2", "type": "file", "name": path.name, "sha1": digest}
                ],
                "total_count": 1,
            }
        )
    )
    assert api.upload("1", path)["upload_status"] == "skipped_identical"
    assert session.request.call_count == 2
    api, _ = client(
        response(
            {
                "entries": [
                    {"id": "2", "type": "file", "name": path.name, "sha1": "different"}
                ],
                "total_count": 1,
            }
        )
    )
    with pytest.raises(BoxAPIError, match="existing_file_requires_version_policy"):
        api.upload("1", path)


def test_version_uses_etag_and_existing_id(tmp_path):
    path, digest = artifact(tmp_path)
    api, session = client(
        response(
            {
                "entries": [
                    {
                        "id": "2",
                        "type": "file",
                        "name": path.name,
                        "sha1": "old",
                        "etag": "3",
                    }
                ],
                "total_count": 1,
            }
        ),
        response({"entries": [{"id": "2"}]}),
        response({"id": "2", "name": path.name, "sha1": digest, "parent": {"id": "1"}}),
    )
    assert (
        api.upload("1", path, existing_policy="version")["upload_status"] == "versioned"
    )
    call = session.request.call_args_list[2]
    assert call.args[1].endswith("/files/2/content")
    assert call.kwargs["headers"]["If-Match"] == "3"


def test_unknown_write_never_retries_and_no_secret():
    api, session = client(requests.Timeout("private-token upstream-details"))
    with pytest.raises(BoxAPIError, match="^write_outcome_unknown$"):
        api.request("POST", "/folders", json={})
    assert session.request.call_count == 2


def test_write_disabled():
    api, session = client(write=False)
    with pytest.raises(BoxAPIError, match="write_not_enabled"):
        api.request("POST", "/folders", json={})
    assert session.request.call_count == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://app.box.com/file/1",
        "https://evil.test/file/1",
        "https://user:password@app.box.com/file/1",
        "https://app.box.com/file/no",
    ],
)
def test_url_allowlist(url):
    api, session = client()
    with pytest.raises(BoxAPIError):
        api.resolve(url)
    assert session.request.call_count == 1


def test_shared_link_cannot_broaden():
    api, session = client(response({"shared_link": None}))
    with pytest.raises(BoxAPIError, match="shared_access_requires_separate_approval"):
        api.shared_link("file", "1", "open")
    assert session.request.call_count == 2


def test_duplicate_batch_fails_before_auth(tmp_path):
    path, _ = artifact(tmp_path)
    with pytest.raises(BoxAPIError, match="duplicate_batch_file_names"):
        upload_files_to_box_folder_via_api_sync(
            parent_folder_url="https://app.box.com/folder/1", file_paths=[path, path]
        )


def test_download_blocks_untrusted_redirect(tmp_path):
    api, session = client()
    session.get.return_value = Mock(
        status_code=302, headers={"Location": "https://evil.test/download"}
    )
    with pytest.raises(BoxAPIError, match="unsafe_download_redirect"):
        api.download({"id": "1", "name": "report.xlsx", "size": 1}, tmp_path)
    assert not (tmp_path / "report.xlsx").exists()


def test_download_hash_failure_removes_temp(tmp_path):
    api, session = client()
    session.get.return_value = Mock(status_code=200, iter_content=lambda n: [b"data"])
    with pytest.raises(BoxAPIError, match="download_integrity_failed"):
        api.download(
            {"id": "1", "name": "report.xlsx", "size": 4, "sha1": "wrong"}, tmp_path
        )
    assert list(tmp_path.iterdir()) == []


def test_refresh_rotation_and_uncertain_refresh(tmp_path, monkeypatch):
    path = tmp_path / "token.json"
    path.write_text(
        json.dumps(
            dict(
                client_id="id",
                client_secret="secret",
                refresh_token="old",
                access_token="expired",
                expires_at=0,
            )
        )
    )
    path.chmod(0o600)
    post = Mock(
        return_value=response(
            dict(access_token="new", refresh_token="rotated", expires_in=3600)
        )
    )
    monkeypatch.setattr(requests, "post", post)
    assert OAuthTokenFile(path)() == "new"
    assert json.loads(path.read_text())["refresh_token"] == "rotated"
    assert path.stat().st_mode & 0o077 == 0
    assert OAuthTokenFile(path)() == "new"
    assert post.call_count == 1
    data = json.loads(path.read_text())
    data["expires_at"] = time.time() - 1
    path.write_text(json.dumps(data))
    post.side_effect = requests.Timeout("secret")
    with pytest.raises(BoxAPIError, match="reauthentication_required"):
        OAuthTokenFile(path)()
    assert json.loads(path.read_text())["refresh_pending"] is True
    with pytest.raises(BoxAPIError, match="reauthentication_required"):
        OAuthTokenFile(path)()
    assert post.call_count == 2


def test_download_cdn_has_no_bearer_and_verified_bytes(tmp_path, monkeypatch):
    api, session = client()
    session.get.return_value = Mock(
        status_code=302, headers={"Location": "https://dl.boxcloud.com/download/signed"}
    )
    cdn = Mock(return_value=Mock(status_code=200, iter_content=lambda n: [b"data"]))
    monkeypatch.setattr(requests, "get", cdn)
    destination = api.download(
        {
            "id": "1",
            "name": "report.xlsx",
            "size": 4,
            "sha1": hashlib.sha1(b"data").hexdigest(),
        },
        tmp_path,
    )
    assert destination.read_bytes() == b"data"
    assert "headers" not in cdn.call_args.kwargs
    assert cdn.call_args.kwargs["allow_redirects"] is False


def test_folder_batch_creates_then_verifies_and_returns_ui_contract(tmp_path):
    path, digest = artifact(tmp_path)
    api, session = client(
        response({"id": "1", "type": "folder"}),
        response({"entries": [], "total_count": 0}),
        response({"id": "3"}),
        response({"name": "202609", "parent": {"id": "1"}}),
        response({"entries": [], "total_count": 0}),
        response({"entries": [{"id": "2"}]}),
        response({"id": "2", "name": path.name, "sha1": digest, "parent": {"id": "3"}}),
    )
    result = upload_files_to_box_folder_via_api_sync(
        parent_folder_url="https://app.box.com/folder/1",
        target_subfolder_name="202609",
        file_paths=[path],
        client=api,
    )[0]
    assert result["folder_created"] is True
    assert result["upload_mode"] == "box-api"
    assert result["web_url"] == "https://app.box.com/file/2"
    assert result["sha1"] == digest


def test_shared_context_propagates_only_to_box_api():
    api, session = client(
        response({"id": "1", "type": "folder"}),
        response({"entries": [], "total_count": 0}),
    )
    api.resolve("https://app.box.com/s/sharedkey")
    api.children("1")
    assert (
        session.request.call_args.kwargs["headers"]["BoxApi"]
        == "shared_link=https://app.box.com/s/sharedkey"
    )


def test_insecure_token_file_fails_without_request(tmp_path):
    path = tmp_path / "token.json"
    path.write_text("{}")
    path.chmod(0o644)
    with pytest.raises(BoxAPIError, match="token_file_permissions_require_0600"):
        OAuthTokenFile(path)()


@pytest.mark.parametrize("host", ["example-brand.box.com", "example-brand.app.box.com"])
def test_enterprise_shared_link_header_has_no_query_or_fragment(host):
    api, session = client(response({"id": "1", "type": "folder"}))
    api.resolve(f"https://{host}/s/key123?utm_source=email&unexpected=1#fragment")
    assert session.request.call_args.args[1] == "https://api.box.com/2.0/shared_items"
    assert (
        session.request.call_args.kwargs["headers"]["BoxApi"]
        == f"shared_link=https://{host}/s/key123"
    )


@pytest.mark.parametrize("host", ["box.com.evil.test", "evilbox.com", "a.b.box.com"])
def test_shared_link_host_lookalike_rejected(host):
    api, session = client()
    with pytest.raises(BoxAPIError, match="invalid_box_url"):
        api.resolve(f"https://{host}/s/key123")
    assert session.request.call_count == 1


@pytest.mark.parametrize(
    "level", [["2026-09", "September"], ("2026-09",), {"name": "2026-09"}, 202609, None]
)
def test_non_string_folder_rejected_before_auth(tmp_path, level):
    path, _ = artifact(tmp_path)
    with pytest.raises(BoxAPIError, match="subfolder_name_must_be_string"):
        upload_files_to_box_folder_via_api_sync(
            parent_folder_url="https://app.box.com/folder/1",
            target_subfolder_name=level,
            file_paths=[path],
        )


@pytest.mark.parametrize("target", ["file", "folder"])
@pytest.mark.parametrize("access", ["company", "open", "invited"])
def test_link_failure_retains_all_upload_results(tmp_path, target, access):
    path, digest = artifact(tmp_path)
    api = Mock()
    api.resolve.return_value = {"id": "1", "type": "folder"}
    api.upload.return_value = {"id": "2", "sha1": digest, "upload_status": "created"}
    api.shared_link.side_effect = BoxAPIError(
        "shared_access_requires_separate_approval"
    )
    results = upload_files_to_box_folder_via_api_sync(
        parent_folder_url="https://app.box.com/folder/1",
        file_paths=[path],
        client=api,
        create_shared_link=True,
        shared_link_target=target,
        shared_link_access=access,
    )
    assert results[0]["file_id"] == "2"
    assert results[0]["upload_status"] == "created"
    prefix = "folder_" if target == "folder" else ""
    assert results[0][prefix + "shared_link_status"] == "failed"
    assert (
        results[0][prefix + "shared_link_error"]
        == "shared_access_requires_separate_approval"
    )


def test_existing_zip_prevents_folder_content_download(tmp_path):
    import asyncio
    from megaton_lib.box_api import download_from_box_via_api

    (tmp_path / "box-download.zip").write_bytes(b"existing")
    api = Mock()
    api.resolve.return_value = {"id": "1", "type": "folder"}
    with pytest.raises(BoxAPIError, match="download_destination_exists"):
        asyncio.run(
            download_from_box_via_api(
                url="https://app.box.com/folder/1",
                download_dir=tmp_path,
                expected_folder_file_names=["report.xlsx"],
                client=api,
            )
        )
    api.children.assert_not_called()
    api.download.assert_not_called()
    assert (tmp_path / "box-download.zip").read_bytes() == b"existing"


def test_connect_timeout_unlocks_for_later_explicit_refresh(tmp_path, monkeypatch):
    path = tmp_path / "token.json"
    path.write_text(
        json.dumps(
            dict(
                client_id="id",
                client_secret="secret",
                refresh_token="old",
                access_token="expired",
                expires_at=0,
            )
        )
    )
    path.chmod(0o600)
    post = Mock(
        side_effect=[
            requests.ConnectTimeout("secret"),
            response(
                dict(access_token="new", refresh_token="rotated", expires_in=3600)
            ),
        ]
    )
    monkeypatch.setattr(requests, "post", post)
    with pytest.raises(BoxAPIError, match="refresh_connection_timeout"):
        OAuthTokenFile(path)()
    assert json.loads(path.read_text())["refresh_pending"] is False
    assert OAuthTokenFile(path)() == "new"
    assert post.call_count == 2


@pytest.mark.parametrize(
    "failure", [requests.ConnectionError("secret"), response({}, 503)]
)
def test_ambiguous_refresh_still_requires_reauthentication(
    tmp_path, monkeypatch, failure
):
    path = tmp_path / "token.json"
    path.write_text(
        json.dumps(
            dict(
                client_id="id",
                client_secret="secret",
                refresh_token="old",
                access_token="expired",
                expires_at=0,
            )
        )
    )
    path.chmod(0o600)
    post = (
        Mock(side_effect=failure)
        if isinstance(failure, Exception)
        else Mock(return_value=failure)
    )
    monkeypatch.setattr(requests, "post", post)
    with pytest.raises(BoxAPIError, match="reauthentication_required"):
        OAuthTokenFile(path)()
    with pytest.raises(BoxAPIError, match="reauthentication_required"):
        OAuthTokenFile(path)()
    assert post.call_count == 1
