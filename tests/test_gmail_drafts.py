from __future__ import annotations

import base64
import json
import sys
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from types import SimpleNamespace

import pytest

from megaton_lib import gmail_client as module
from megaton_lib import gmail_draft as cli
from megaton_lib.gmail_client import GmailClient, GmailDraftContent


def raw(message):
    return base64.urlsafe_b64encode(message.as_bytes(policy=policy.SMTP)).decode()


def parsed(value):
    return BytesParser(policy=policy.SMTP).parsebytes(base64.urlsafe_b64decode(value))


class Request:
    def __init__(self, callback):
        self.callback = callback

    def execute(self):
        return self.callback()


class Resource:
    def __init__(self, service, kind):
        self.service, self.kind = service, kind

    def get(self, **kwargs):
        assert kwargs["userId"] == "me"
        assert kwargs["format"] == "raw"

        def run():
            if self.kind == "messages":
                return {"id": kwargs["id"], "threadId": "thread-1", "raw": raw(self.service.source)}
            if self.service.fail_read:
                raise RuntimeError("private response content")
            return self.service.drafts_data[kwargs["id"]]
        return Request(run)

    def create(self, **kwargs):
        return Request(lambda: self.service.write("create", "draft-1", kwargs))

    def update(self, **kwargs):
        return Request(lambda: self.service.write("update", kwargs["id"], kwargs))


class Service:
    def __init__(self):
        self.source = EmailMessage()
        self.source["From"] = '"External, Person" <external@example.com>'
        self.source["To"] = 'sender@example.com, team@example.com'
        self.source["Cc"] = 'TEAM@example.com, other@example.com, alias@example.com'
        self.source["Bcc"] = 'hidden@example.com'
        self.source["Subject"] = "日本語の報告"
        self.source["Message-ID"] = "<parent@example.com>"
        self.source["References"] = "<ancestor@example.com>"
        self.source.set_content("Original private body")
        self.writes = []
        self.drafts_data = {}
        self.fail_write = False
        self.fail_read = False
        self.corrupt = False
        self.account = "sender@example.com"

    def users(self):
        return self

    def getProfile(self, **kwargs):
        return Request(lambda: {"emailAddress": self.account})

    def messages(self):
        return Resource(self, "messages")

    def drafts(self):
        return Resource(self, "drafts")

    def write(self, action, draft_id, kwargs):
        assert kwargs["userId"] == "me"
        self.writes.append((action, kwargs))
        if self.fail_write:
            raise TimeoutError("private response content")
        value = dict(kwargs["body"]["message"])
        if self.corrupt:
            message = parsed(value["raw"])
            message.replace_header("To", "wrong@example.com")
            value["raw"] = raw(message)
        value.setdefault("threadId", "new-thread")
        value["id"] = f"message-{len(self.writes)}"
        value["labelIds"] = ["DRAFT"]
        self.drafts_data[draft_id] = {"id": draft_id, "message": value}
        return {"id": draft_id, "message": {"id": value["id"]}}


@pytest.fixture
def setup():
    service = Service()
    client = object.__new__(GmailClient)
    client._service = service
    return client, service


def new_content():
    return GmailDraftContent.new(sender="sender@example.com", to=["other@example.com"],
                                 subject="Report", body_text="Private body",
                                 attachments=[("old.txt", b"old", "text/plain")])


def save(client, content=None):
    return client.save_draft(content or new_content(), expected_email="sender@example.com")


def test_reply_all_threads_and_deduplicates(setup):
    client, service = setup
    service.source["Reply-To"] = "reply@example.com"
    service.source.add_attachment(b"secret", maintype="application", subtype="pdf", filename="original.pdf")
    content = client.prepare_reply("source-1", expected_email=service.account, body_text="Reply",
                                   self_aliases=["alias@example.com"])
    summary = content.summary()
    assert summary["to"] == ["reply@example.com", "team@example.com"]
    assert summary["cc"] == ["other@example.com"]
    assert summary["bcc"] == []
    assert summary["attachments"] == []
    assert summary["in_reply_to"] == "<parent@example.com>"
    assert summary["references"] == "<ancestor@example.com> <parent@example.com>"
    assert summary["subject"] == "日本語の報告"
    assert not service.writes
    result = save(client, content)
    assert result["verified"] and result["applied"]
    assert service.writes[0][1]["body"]["message"]["threadId"] == "thread-1"


def test_single_reply_does_not_copy_cc(setup):
    client, service = setup
    result = client.create_reply_draft("source-1", expected_email=service.account, body_text="Reply", reply_all=False)
    assert result["to"] == ["external@example.com"]
    assert result["cc"] == []
    assert result["verified"]


@pytest.mark.parametrize("header,value", [
    ("Message-ID", None), ("Message-ID", "gmail-internal-id"),
    ("References", "broken-reference"), ("From", "sender@example.com"),
    ("Reply-To", "sender@example.com"),
])
def test_unsafe_source_stops_before_write(setup, header, value):
    client, service = setup
    del service.source[header]
    if value is not None:
        service.source[header] = value
    with pytest.raises(ValueError):
        client.create_reply_draft("source-1", expected_email=service.account, body_text="Reply", reply_all=False)
    assert service.writes == []


def test_account_mismatch_stops_all_writes(setup):
    client, service = setup
    with pytest.raises(ValueError, match="does not match"):
        client.save_draft(new_content(), expected_email="wrong@example.com")
    with pytest.raises(ValueError, match="From"):
        content = new_content()
        content.message.replace_header("From", "wrong@example.com")
        save(client, content)
    assert service.writes == []


def test_create_read_update_verify_attachment_and_body(setup):
    client, service = setup
    result = save(client)
    assert result["ok"] and result["verified"]
    assert result["draft_id"] == "draft-1"
    before = client.get_draft("draft-1")
    changed = client.update_draft("draft-1", expected_email=service.account,
                                  expected_fingerprint=before.fingerprint,
                                  attachments=[("new.pdf", b"new bytes", "application/pdf")])
    assert changed["verified"]
    assert changed["message_id"] != result["message_id"]
    assert changed["draft_id"] == result["draft_id"]
    assert changed["body_parts"] == result["body_parts"]
    assert [item["filename"] for item in changed["attachments"]] == ["new.pdf"]
    after = client.get_draft("draft-1")
    cleared = client.update_draft("draft-1", expected_email=service.account,
                                  expected_fingerprint=after.fingerprint, attachments=[], body_text="New body")
    assert cleared["attachments"] == []
    assert cleared["body_parts"] != changed["body_parts"]


def test_update_rejects_stale_or_missing_fingerprint(setup):
    client, service = setup
    save(client)
    with pytest.raises(ValueError, match="changed"):
        client.update_draft("draft-1", expected_email=service.account, expected_fingerprint="stale", body_text="Changed")
    with pytest.raises(ValueError, match="expected_fingerprint"):
        client.save_draft(new_content(), expected_email=service.account, draft_id="draft-1")
    assert len(service.writes) == 1


@pytest.mark.parametrize("labels", [["SENT"], ["TRASH"], [], None,
                                    ["DRAFT", "SENT"], ["DRAFT", "TRASH"]])
def test_inactive_draft_rejected_before_read_verify_or_update(setup, labels):
    client, service = setup
    save(client)
    before = client.get_draft("draft-1")
    message = service.drafts_data["draft-1"]["message"]
    message["id"] = "returned-message"
    if labels is None:
        del message["labelIds"]
    else:
        message["labelIds"] = labels
    # Valid-looking MIME and an unchanged fingerprint must not override labels.
    operations = [
        lambda: client.get_draft("draft-1"),
        lambda: client.verify_draft("draft-1", expected=before.content),
        lambda: client.update_draft("draft-1", expected_email=service.account,
                                    expected_fingerprint=before.fingerprint, body_text="Changed"),
        lambda: client.save_draft(before.content, expected_email=service.account,
                                  draft_id="draft-1", expected_fingerprint=before.fingerprint),
    ]
    for operation in operations:
        with pytest.raises(module.GmailDraftStateError) as caught:
            operation()
        assert caught.value.result()["message_id"] == "returned-message"
        assert caught.value.result()["errors"] == ["draft_not_active"]
    assert len(service.writes) == 1


def test_sent_during_readback_is_not_verified_or_recreated(setup, monkeypatch):
    client, service = setup
    original = service.write

    def write(*args):
        response = original(*args)
        service.drafts_data[response["id"]]["message"]["labelIds"] = ["SENT"]
        return response

    monkeypatch.setattr(service, "write", write)
    result = save(client)
    assert not result["ok"] and not result["verified"]
    assert result["applied"] is True
    assert result["draft_id"] == "draft-1"
    assert result["label_ids"] == ["SENT"]
    assert result["errors"] == ["draft_not_active"]
    assert len(service.writes) == 1


@pytest.mark.parametrize("command", ["get", "verify", "update"])
def test_cli_rejects_sent_message_without_export_or_write(setup, monkeypatch, tmp_path, capsys, command):
    client, service = setup
    save(client)
    before = client.get_draft("draft-1")
    service.drafts_data["draft-1"]["message"]["labelIds"] = ["SENT"]
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    monkeypatch.setattr(module, "load_draft_credentials_from_env", lambda **kwargs: None)
    output = tmp_path / "private.txt"
    options = {"get": ["--body-output", str(output)],
               "verify": ["--expected-json", str(tmp_path / "unused.json")],
               "update": ["--expected-fingerprint", before.fingerprint, "--apply"]}
    assert cli.main([command, "--draft-id", "draft-1", "--expected-email", service.account,
                     *options[command]]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["errors"] == ["draft_not_active"]
    assert result["label_ids"] == ["SENT"]
    assert result["message_id"] == "message-1"
    assert not result["applied"] and not result["verified"]
    assert "Private body" not in json.dumps(result)
    assert not output.exists()
    assert len(service.writes) == 1


def test_second_read_detects_send_during_update_preparation(setup, monkeypatch):
    client, service = setup
    save(client)
    before = client.get_draft("draft-1")
    original = client.get_draft

    def get(draft_id):
        result = original(draft_id)
        service.drafts_data[draft_id]["message"]["labelIds"] = ["SENT"]
        return result

    monkeypatch.setattr(client, "get_draft", get)
    with pytest.raises(module.GmailDraftStateError):
        client.update_draft("draft-1", expected_email=service.account,
                            expected_fingerprint=before.fingerprint, body_text="Changed")
    assert len(service.writes) == 1


def test_second_read_detects_edit_during_preparation(setup, monkeypatch):
    client, service = setup
    save(client)
    before = client.get_draft("draft-1")
    original = client.get_draft
    calls = 0

    def get(draft_id):
        nonlocal calls
        calls += 1
        result = original(draft_id)
        if calls > 1:
            result.fingerprint = "changed"
        return result
    monkeypatch.setattr(client, "get_draft", get)
    with pytest.raises(ValueError, match="changed"):
        client.update_draft("draft-1", expected_email=service.account,
                            expected_fingerprint=before.fingerprint, body_text="Changed")
    assert len(service.writes) == 1


def test_html_and_inline_preserved_when_replacing_ordinary_attachment():
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "other@example.com"
    message.set_content("plain")
    message.add_alternative('<img src="cid:logo">', subtype="html")
    message.get_payload()[1].add_related(b"image", maintype="image", subtype="png", cid="<logo>", filename="logo.png")
    message.add_attachment(b"old", maintype="application", subtype="pdf", filename="old.pdf")
    content = GmailDraftContent(message)
    changed = content.updated(attachments=[("new.pdf", b"new", "application/pdf")])
    assert [a["filename"] for a in changed.summary()["attachments"]] == ["logo.png", "new.pdf"]
    assert content.summary()["body_parts"] == changed.summary()["body_parts"]
    assert b"Content-ID: <logo>" in changed.message.as_bytes()
    with pytest.raises(ValueError, match="plain-text"):
        content.updated(body_text="flatten")


def test_signed_mime_rejected():
    content = new_content()
    content.message.set_type("multipart/signed")
    with pytest.raises(ValueError, match="Unsupported"):
        content.updated(cc=["another@example.com"])


def test_root_attachment_edit_rejected():
    message = EmailMessage()
    message.set_content(b"file", maintype="application", subtype="pdf")
    message["Content-Disposition"] = 'attachment; filename="old.pdf"'
    with pytest.raises(ValueError, match="Attachment-only"):
        GmailDraftContent(message).updated(attachments=[])


def test_missing_write_response_id_is_not_reported_as_no_write(setup, monkeypatch):
    client, service = setup
    monkeypatch.setattr(service, "write", lambda *args: {})
    result = save(client)
    assert result["applied"] and not result["ok"]
    assert result["errors"] == ["write_response_missing_draft_id"]


def test_threaded_subject_change_rejected(setup):
    client, service = setup
    content = client.prepare_reply("source-1", expected_email=service.account, body_text="reply")
    with pytest.raises(ValueError, match="subject"):
        content.updated(subject="Different")


def test_write_timeout_is_unknown_and_not_retried(setup):
    client, service = setup
    service.fail_write = True
    result = save(client)
    assert result["applied"] is None and not result["verified"]
    assert len(service.writes) == 1
    assert "private response content" not in json.dumps(result)


def test_readback_failure_preserves_saved_id(setup):
    client, service = setup
    service.fail_read = True
    result = save(client)
    assert result["applied"] and not result["verified"]
    assert result["draft_id"] == "draft-1"
    assert len(service.writes) == 1
    assert "private response content" not in json.dumps(result)


def test_readback_mismatch_fails(setup):
    client, service = setup
    service.corrupt = True
    result = save(client)
    assert result["applied"] and not result["verified"]
    assert "mismatch:to" in result["errors"]


def test_summary_requires_complete_contract_and_correct_id(setup):
    client, _ = setup
    result = save(client)
    draft = client.get_draft(result["draft_id"])
    with pytest.raises(ValueError, match="complete"):
        client.verify_draft_summary(draft, expected={})
    result["draft_id"] = "wrong"
    assert not client.verify_draft_summary(draft, expected=result)["ok"]


@pytest.mark.parametrize("token", [{"type": "service_account"}, {"private_key": "secret"}])
def test_service_account_token_rejected(token):
    with pytest.raises(ValueError, match="service account"):
        module.credentials_from_authorized_user_info(token, module.SCOPES_REPLY)


def test_service_account_object_rejected():
    with pytest.raises(ValueError, match="authorized-user"):
        GmailClient(SimpleNamespace())


def test_auth_loader_never_uses_adc(monkeypatch):
    monkeypatch.delenv("GMAIL_DRAFT_TOKEN_JSON", raising=False)
    monkeypatch.delenv("GMAIL_DRAFT_TOKEN_PATH", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/not/a/gmail/token")
    with pytest.raises(ValueError, match="user OAuth"):
        module.load_draft_credentials_from_env()


def test_token_file_rejects_service_account(tmp_path):
    token = tmp_path / "service.json"
    token.write_text(json.dumps({"type": "service_account", "private_key": "secret"}))
    with pytest.raises(ValueError, match="service account"):
        module.credentials_from_authorized_user_file(token, module.SCOPES_REPLY)


def test_auth_loader_uses_shared_prefix_and_scopes(monkeypatch):
    monkeypatch.setenv("TEST_GMAIL_DRAFT_TOKEN_JSON", "token")
    captured = []
    monkeypatch.setattr(module, "credentials_from_authorized_user_info",
                        lambda value, scopes: captured.append((value, scopes)))
    module.load_draft_credentials_from_env(env_prefix="test", scopes=module.SCOPES_REPLY)
    assert captured == [("token", module.SCOPES_REPLY)]


def test_authorize_existing_token_checks_actual_identity(setup, monkeypatch, tmp_path):
    client, service = setup
    token = tmp_path / "token.json"
    token.write_text("existing token")
    monkeypatch.setattr(module, "credentials_from_authorized_user_file", lambda *args: object())
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    with pytest.raises(ValueError, match="does not match"):
        module.authorize(tmp_path / "client.json", token, module.SCOPES_REPLY, "wrong@example.com")
    assert token.read_text() == "existing token"


def test_authorize_new_token_is_private_and_checked(setup, monkeypatch, tmp_path):
    client, service = setup
    creds = SimpleNamespace(to_json=lambda: '{"token": "private"}')
    flow = SimpleNamespace(run_local_server=lambda **kw: creds)
    flow_class = SimpleNamespace(from_client_secrets_file=lambda *args: flow)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow", SimpleNamespace(InstalledAppFlow=flow_class))
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    token = tmp_path / "credentials" / "token.json"
    with pytest.raises(ValueError, match="does not match"):
        module.authorize(tmp_path / "client.json", token, module.SCOPES_REPLY, "wrong@example.com")
    assert not token.exists()
    assert module.authorize(tmp_path / "client.json", token, module.SCOPES_REPLY, service.account) is creds
    assert token.stat().st_mode & 0o777 == 0o600
    assert not list(token.parent.glob(".gmail-token-*"))


def test_display_name_comma_and_recipient_dedup():
    content = GmailDraftContent.new(sender="sender@example.com",
                                    to=['"Doe, Jane" <jane@example.com>'],
                                    cc=["JANE@example.com", "cc@example.com"], bcc=["CC@example.com"],
                                    subject="Subject", body_text="Body")
    assert content.summary()["to"] == ["jane@example.com"]
    assert content.summary()["cc"] == ["cc@example.com"]
    assert content.summary()["bcc"] == []


@pytest.mark.parametrize("value,expected", [
    ("undisclosed-recipients:;", []),
    (["undisclosed-recipients:;"], []),
    ("x@example.com, undisclosed-recipients:;", ["x@example.com"]),
    (["x@example.com, undisclosed-recipients:;"], ["x@example.com"]),
    ("Team: x@example.com, y@example.com;", ["x@example.com", "y@example.com"]),
    ('"Doe; Jane" <x@example.com>', ['"Doe; Jane" <x@example.com>']),
    ("x@example.com; y@example.com\nz@example.com", ["x@example.com", "y@example.com", "z@example.com"]),
])
def test_address_groups_and_legacy_separators(value, expected):
    assert module.parse_email_list(value) == expected


@pytest.mark.parametrize("value", ["<>", "Name <>", "bad@@example.com", "<>, x@example.com",
                                    "<>;x@example.com", "Broken: x@example.com"])
def test_malformed_mailboxes_are_not_silently_dropped(value):
    with pytest.raises(ValueError, match="Invalid email"):
        module.parse_email_list(value)


def test_reply_all_to_empty_group(setup):
    client, service = setup
    service.source.replace_header("To", "undisclosed-recipients:;")
    del service.source["Cc"]
    result = client.create_reply_draft("source-1", expected_email=service.account,
                                       body_text="Reply", reply_all=True)
    assert result["verified"]
    assert result["to"] == ["external@example.com"]
    assert result["cc"] == result["bcc"] == []


def test_bcc_only_group_draft_get_verify_update(setup):
    client, service = setup
    content = new_content()
    content.message.replace_header("To", "undisclosed-recipients:;")
    content.message["Bcc"] = "hidden@example.com"
    result = save(client, content)
    assert result["verified"] and result["to"] == []
    draft = client.get_draft(result["draft_id"])
    assert client.verify_draft(draft.id, expected=content)["verified"]
    updated = client.update_draft(draft.id, expected_email=service.account,
                                  expected_fingerprint=draft.fingerprint, body_text="Updated")
    assert updated["verified"] and updated["bcc"] == ["hidden@example.com"]
    assert updated["to"] == []


def test_empty_group_is_not_enough_to_save(setup):
    client, service = setup
    content = new_content()
    content.message.replace_header("To", "undisclosed-recipients:;")
    with pytest.raises(ValueError, match="recipient is required"):
        save(client, content)
    assert not service.writes


@pytest.fixture
def cli_setup(setup, monkeypatch):
    client, service = setup
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    monkeypatch.setattr(module, "load_draft_credentials_from_env", lambda **kw: None)
    monkeypatch.setenv("GMAIL_DRAFT_EXPECTED_EMAIL", service.account)
    return client, service


@pytest.mark.parametrize("command", ["create", "reply", "get", "update", "verify"])
def test_cli_requests_minimum_scopes(command, cli_setup, monkeypatch, tmp_path, capsys):
    client, _ = cli_setup
    saved = save(client)
    body = tmp_path / "body.txt"
    body.write_text("Reply")
    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps(saved))
    options = {
        "create": ["--to", "x@example.com", "--subject", "New", "--body-file", str(body)],
        "reply": ["--message-id", "source-1", "--body-file", str(body)],
        "get": ["--draft-id", saved["draft_id"]],
        "update": ["--draft-id", saved["draft_id"], "--expected-fingerprint", saved["fingerprint"]],
        "verify": ["--draft-id", saved["draft_id"], "--expected-json", str(expected)],
    }
    scopes = []
    monkeypatch.setattr(module, "load_draft_credentials_from_env", lambda **kw: scopes.append(kw["scopes"]))
    assert cli.main([command, *options[command]]) == 0
    assert scopes == [module.SCOPES_REPLY if command == "reply" else module.SCOPES_DRAFT]
    capsys.readouterr()


@pytest.mark.parametrize("flags,reply_all", [([], True), (["--reply-all"], True), (["--sender-only"], False)])
def test_cli_reply_recipient_selection(cli_setup, tmp_path, capsys, flags, reply_all):
    _, service = cli_setup
    service.source["Reply-To"] = "reply@example.com"
    body = tmp_path / "body.txt"
    body.write_text("Reply")
    assert cli.main(["reply", "--message-id", "source-1", "--body-file", str(body),
                     "--self-alias", "alias@example.com", *flags]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["to"] == (["reply@example.com", "team@example.com"] if reply_all else ["reply@example.com"])
    assert result["cc"] == (["other@example.com"] if reply_all else [])
    assert result["bcc"] == []
    assert not service.writes


def test_cli_reply_modes_are_mutually_exclusive():
    with pytest.raises(SystemExit) as caught:
        cli.create_parser().parse_args(["reply", "--message-id", "id", "--body-file", "body.txt",
                                       "--reply-all", "--sender-only"])
    assert caught.value.code == 2


def test_cli_preview_apply_get_verify_and_update(cli_setup, tmp_path, capsys):
    client, service = cli_setup
    body = tmp_path / "body.txt"
    body.write_text("Private reply")
    command = ["reply", "--message-id", "source-1", "--body-file", str(body)]
    assert cli.main(command) == 0
    preview = json.loads(capsys.readouterr().out)
    assert not preview["applied"] and not preview["verified"]
    assert not service.writes
    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps(preview))
    assert cli.main([*command, "--apply"]) == 0
    output = capsys.readouterr()
    assert "writing once" in output.err
    result = json.loads(output.out)
    assert result["verified"] and "Private reply" not in output.out
    assert cli.main(["verify", "--draft-id", result["draft_id"], "--expected-json", str(expected)]) == 0
    assert json.loads(capsys.readouterr().out)["verified"]
    body_output = tmp_path / "export.txt"
    assert cli.main(["get", "--draft-id", result["draft_id"], "--body-output", str(body_output)]) == 0
    observed = json.loads(capsys.readouterr().out)
    assert body_output.stat().st_mode & 0o777 == 0o600
    attachment = tmp_path / "report.pdf"
    attachment.write_bytes(b"PDF")
    command = ["update", "--draft-id", result["draft_id"], "--expected-fingerprint", observed["fingerprint"],
               "--attach", str(attachment)]
    assert cli.main(command) == 0
    assert not json.loads(capsys.readouterr().out)["applied"]
    assert len(service.writes) == 1
    assert cli.main([*command, "--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["verified"]
    assert len(service.writes) == 2


def test_cli_needs_account_and_rejects_send(cli_setup, monkeypatch, capsys):
    monkeypatch.delenv("GMAIL_DRAFT_EXPECTED_EMAIL")
    assert cli.main(["get", "--draft-id", "id"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert "EXPECTED_EMAIL" in result["next_action"]
    with pytest.raises(SystemExit) as exc:
        cli.main(["send"])
    assert exc.value.code == 2


def test_cli_error_does_not_expose_api_body(cli_setup, capsys):
    _, service = cli_setup
    service.fail_read = True
    assert cli.main(["get", "--draft-id", "draft-1"]) == 1
    result = capsys.readouterr().out
    assert "private response content" not in result


def test_cli_create_and_clear_fields(cli_setup, tmp_path, capsys):
    _, service = cli_setup
    body = tmp_path / "body.txt"
    body.write_text("New draft")
    assert cli.main(["create", "--to", "other@example.com", "--cc", "cc@example.com",
                     "--subject", "Report", "--body-file", str(body), "--apply"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["verified"]
    assert cli.main(["update", "--draft-id", created["draft_id"], "--expected-fingerprint", created["fingerprint"],
                     "--clear-cc", "--clear-bcc", "--clear-attachments", "--apply"]) == 0
    updated = json.loads(capsys.readouterr().out)
    assert updated["cc"] == [] and updated["bcc"] == [] and updated["attachments"] == []
    assert updated["verified"]
    assert len(service.writes) == 2


def test_cli_export_never_overwrites_existing_file(cli_setup, tmp_path, capsys):
    client, _ = cli_setup
    save(client)
    output = tmp_path / "existing.txt"
    output.write_text("User edits")
    assert cli.main(["get", "--draft-id", "draft-1", "--body-output", str(output)]) == 1
    assert output.read_text() == "User edits"
    assert not json.loads(capsys.readouterr().out)["applied"]


def test_cli_timeout_preserves_unknown_outcome(cli_setup, tmp_path, capsys):
    _, service = cli_setup
    service.fail_write = True
    body = tmp_path / "body.txt"
    body.write_text("New draft")
    assert cli.main(["create", "--to", "other@example.com", "--subject", "Report",
                     "--body-file", str(body), "--apply"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["applied"] is None
    assert "to" in result and "attachments" in result and "thread_id" in result
    assert len(service.writes) == 1
