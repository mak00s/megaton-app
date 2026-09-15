from copy import deepcopy
from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from megaton_lib.docs_client import DocsClient, DocsEditError, plan_edit


def document(text="Hello world\n", revision="r1"):
    return {"documentId": "doc1", "revisionId": revision, "title": "Report", "tabs": [{
        "tabProperties": {"tabId": "t.0", "title": "Results"},
        "documentTab": {"body": {"content": [{"startIndex": 1, "paragraph": {
            "elements": [{"startIndex": 1, "textRun": {"content": text}}],
        }}]}},
    }]}


def client_with(*snapshots):
    service = Mock()
    service.documents.return_value.get.return_value.execute.side_effect = snapshots
    return DocsClient(service), service.documents.return_value


def test_preview_never_writes():
    client, api = client_with(document())
    plan = client.plan("doc1", match="world", text="team")
    assert plan.after == "Hello team\n"
    assert "-Hello world" in plan.preview()["diff"]
    assert client.apply(plan)["mode"] == "dry_run"
    api.batchUpdate.assert_not_called()
    api.get.assert_called_once_with(documentId="doc1", includeTabsContent=True,
                                    suggestionsViewMode="SUGGESTIONS_INLINE")


@pytest.mark.parametrize("operation,text,after,start,end,count", [
    ("replace", "team", "Hello team\n", 7, 12, 2),
    ("replace", "", "Hello \n", 7, 12, 1),
    ("insert_after", "!", "Hello world!\n", 12, 12, 1),
])
def test_apply_and_verify(operation, text, after, start, end, count):
    original = document()
    plan = plan_edit(original, match="world", text=text, operation=operation)
    client, api = client_with(original, document(after, "r2"))
    result = client.apply(plan, apply=True)
    assert result["ok"] and result["verified"]
    assert result["write_status"] == "acknowledged"
    body = api.batchUpdate.call_args.kwargs["body"]
    assert body["writeControl"] == {"requiredRevisionId": "r1"}
    assert len(body["requests"]) == count
    assert plan.start_index == start and plan.end_index == end
    for request in body["requests"]:
        target = next(iter(request.values()))
        assert target.get("range", target.get("location"))["tabId"] == "t.0"
    api.batchUpdate.return_value.execute.assert_called_once_with(num_retries=0)


def test_utf16_non_bmp_index():
    plan = plan_edit(document("😀 world\n"), match="world", text="日本語")
    assert (plan.start_index, plan.end_index) == (4, 9)


@pytest.mark.parametrize("tab_id", ["t.0", "t.wgbcka9y4har"])
def test_real_tab_id_plan_apply_verify(tab_id):
    original = document()
    original["tabs"][0]["tabProperties"]["tabId"] = tab_id
    updated = document("Hello team\n", "r2")
    updated["tabs"][0]["tabProperties"]["tabId"] = tab_id
    client, api = client_with(original, original, updated)
    plan = client.plan("doc1", match="world", text="team", tab_id=tab_id)
    assert plan.tab_id == tab_id
    assert client.apply(plan, apply=True)["verified"]
    requests = api.batchUpdate.call_args.kwargs["body"]["requests"]
    assert requests[0]["deleteContentRange"]["range"]["tabId"] == tab_id
    assert requests[1]["insertText"]["location"]["tabId"] == tab_id


@pytest.mark.parametrize("bad_id", ["doc.with.dot", "https://docs.google.com/document/d/doc1/edit"])
def test_document_id_validation_remains_strict(bad_id):
    client, api = client_with()
    with pytest.raises(DocsEditError, match="document ID"):
        client.get(bad_id)
    api.get.assert_not_called()


@pytest.mark.parametrize("bad_id", ["", "t.0/path", "https://example.com", "t.0?x=y"])
def test_reject_invalid_api_tab_id(bad_id):
    doc = document()
    doc["tabs"][0]["tabProperties"]["tabId"] = bad_id
    with pytest.raises(DocsEditError, match="tab ID"):
        plan_edit(doc, match="world", text="team")


@pytest.mark.parametrize("text,match", [("world world\n", "world"), ("aaa\n", "aa"), ("hello\n", "world")])
def test_missing_or_ambiguous_match(text, match):
    with pytest.raises(DocsEditError, match="Expected one"):
        plan_edit(document(text), match=match, text="X")


@pytest.mark.parametrize("text", ["a\nb", "a\x00b", "\ue000", "\ud800", "a\u2029b"])
def test_reject_unsupported_insert_text(text):
    with pytest.raises(DocsEditError, match="single-line"):
        plan_edit(document(), match="world", text=text)


def test_nested_tabs_require_selection():
    doc = document()
    child = deepcopy(doc["tabs"][0])
    child["tabProperties"]["tabId"] = "t.wgbcka9y4har"
    doc["tabs"][0]["childTabs"] = [child]
    with pytest.raises(DocsEditError, match="tab_id"):
        plan_edit(doc, match="world", text="team")
    assert plan_edit(doc, match="world", text="team", tab_id="t.wgbcka9y4har").tab_id == "t.wgbcka9y4har"


@pytest.mark.parametrize("kind", ["suggestions", "runs", "table", "revision", "noop"])
def test_unsupported_structures_and_noop(kind):
    doc = document()
    tab = doc["tabs"][0]
    block = tab["documentTab"]["body"]["content"][0]
    if kind == "suggestions":
        block["paragraph"]["elements"][0]["suggestedInsertionIds"] = ["s1"]
    elif kind == "runs":
        block["paragraph"]["elements"].append({"textRun": {"content": "extra"}})
    elif kind == "table":
        tab["documentTab"]["body"]["content"] = [{"table": {"tableRows": [{"content": block}]}}]
    elif kind == "revision":
        del doc["revisionId"]
    with pytest.raises(DocsEditError):
        plan_edit(doc, match="world", text="world" if kind == "noop" else "team")


def test_changed_revision_or_tampered_plan_prevents_write():
    plan = plan_edit(document(), match="world", text="team")
    for snapshot, supplied in [(document(revision="r2"), plan), (document(), replace(plan, start_index=1))]:
        client, api = client_with(snapshot)
        with pytest.raises(DocsEditError, match="changed"):
            client.apply(supplied, apply=True)
        api.batchUpdate.assert_not_called()


@pytest.mark.parametrize("status,expected", [(None, "unknown"), (400, "rejected"), (503, "unknown")])
def test_write_errors_never_retry_or_leak(status, expected):
    client, api = client_with(document())
    error = RuntimeError("SECRET document body")
    error.resp = SimpleNamespace(status=status)
    api.batchUpdate.return_value.execute.side_effect = error
    result = client.apply(plan_edit(document(), match="world", text="team"), apply=True)
    assert result["write_status"] == expected
    assert result["document_id"] == "doc1"
    assert "SECRET" not in str(result)
    assert api.get.call_count == 1
    api.batchUpdate.return_value.execute.assert_called_once_with(num_retries=0)


@pytest.mark.parametrize("readback,error", [(RuntimeError("secret"), "readback_failed"), (document(), "verification_mismatch")])
def test_readback_failure_retains_acknowledged_write(readback, error):
    client, api = client_with(document(), readback)
    result = client.apply(plan_edit(document(), match="world", text="team"), apply=True)
    assert not result["ok"] and not result["verified"]
    assert result["write_status"] == "acknowledged"
    assert result["error"] == error
    assert api.batchUpdate.call_count == 1


def test_get_preserves_structured_tables():
    doc = document()
    doc["tabs"][0]["documentTab"]["body"]["content"].append({"table": {"rows": 2}})
    client, _ = client_with(doc)
    assert client.get("doc1") == doc


@pytest.mark.parametrize("token_type", [None, "authorized_user"])
def test_oauth_existing_token_and_verified_identity(token_type, tmp_path, monkeypatch):
    from google.oauth2.credentials import Credentials
    from megaton_lib import google_workspace

    payload = {"refresh_token": "test", "client_id": "test", "client_secret": "test"}
    if token_type:
        payload["type"] = token_type
    token = tmp_path / "token.json"
    original = json.dumps(payload)
    token.write_text(original)
    creds = Mock()
    loader = Mock(return_value=creds)
    monkeypatch.setattr(Credentials, "from_authorized_user_info", loader)
    monkeypatch.setattr(google_workspace, "refresh_user_credentials", lambda c: c)
    identity, service = Mock(), Mock()
    identity.userinfo.return_value.get.return_value.execute.return_value = {
        "email": "user@example.com", "verified_email": True,
    }
    builder = Mock(side_effect=[identity, service])
    monkeypatch.setattr(google_workspace, "build_service", builder)
    client = DocsClient.from_oauth_file(token, expected_email="User@Example.com")
    assert client._service is service
    assert builder.call_args.args == ("docs", "v1")
    assert token.read_text() == original


@pytest.mark.parametrize("identity", [{"email": "other@example.com", "verified_email": True},
                                       {"email": "user@example.com", "verified_email": False}])
def test_oauth_wrong_identity_stops_before_docs(identity, tmp_path, monkeypatch):
    from google.oauth2.credentials import Credentials
    from megaton_lib import google_workspace

    token = tmp_path / "token.json"
    token.write_text("{}")
    monkeypatch.setattr(Credentials, "from_authorized_user_info", Mock())
    monkeypatch.setattr(google_workspace, "refresh_user_credentials", lambda c: c)
    service = Mock()
    service.userinfo.return_value.get.return_value.execute.return_value = identity
    builder = Mock(return_value=service)
    monkeypatch.setattr(google_workspace, "build_service", builder)
    with pytest.raises(DocsEditError, match="verified email"):
        DocsClient.from_oauth_file(token, expected_email="user@example.com")
    assert builder.call_count == 1
    assert builder.call_args.args == ("oauth2", "v2")


def test_service_account_rejected_before_refresh(tmp_path, monkeypatch):
    from megaton_lib import google_workspace

    token = tmp_path / "token.json"
    token.write_text(json.dumps({"type": "service_account", "private_key": "secret"}))
    refresh = Mock()
    monkeypatch.setattr(google_workspace, "refresh_user_credentials", refresh)
    with pytest.raises(DocsEditError, match="service account"):
        DocsClient.from_oauth_file(token, expected_email="user@example.com")
    refresh.assert_not_called()
