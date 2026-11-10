from __future__ import annotations

import base64
import json
from copy import deepcopy
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


def html_content(layout="alternative-related", *, image_filename="logo.png"):
    message = EmailMessage(policy=policy.SMTP)
    for key, value in {
        "From": "sender@example.com", "To": '"Reader, One" <other@example.com>',
        "Cc": "Team <team@example.com>", "Bcc": "hidden@example.com", "Subject": "Re: Report",
        "In-Reply-To": "<parent@example.com>", "References": "<ancestor@example.com> <parent@example.com>",
        "X-Workflow": "preserve",
    }.items():
        message[key] = value
    html = '<p>Old private text</p><img src="cid:logo" alt="Logo"><blockquote>Quoted history</blockquote>'
    if "related" not in layout:
        html = html.replace('<img src="cid:logo" alt="Logo">', '')
    if layout.startswith("alternative"):
        message.set_content("Old private text")
        message.add_alternative(html, subtype="html")
    else:
        message.set_content(html, subtype="html")
    html_part = message.get_body(preferencelist=("html",))
    html_part["Content-ID"] = "<html-body>"
    html_part["Content-Location"] = "body.html"
    if "related" in layout:
        html_part.add_related(b"image bytes", maintype="image", subtype="png", cid="<logo>",
                              filename=image_filename)
        html_part.set_param("start", "<html-body>")
    if layout != "html-only":
        message.add_attachment(b"ordinary bytes", maintype="text", subtype="plain", filename="notes.txt")
    return GmailDraftContent(message, "thread-1")


NEW_HTML = ('<p>更新 &amp; review <strong>内容</strong></p><p><a href="https://example.com/report">Report</a></p>'
            '<img src="cid:logo" alt="Logo"><blockquote>Quoted history</blockquote>')


@pytest.mark.parametrize("layout", ["alternative-related", "alternative", "html-related", "html-only"])
@pytest.mark.parametrize("image_filename", ["logo.png", None])
def test_html_update_keeps_envelope_tree_assets_and_generates_plain(layout, image_filename):
    before = html_content(layout, image_filename=image_filename)
    original = before.message.as_bytes()
    assets = [p.as_bytes() for p in before.message.walk()
              if p.get_content_maintype() == "image" or p.get_filename()]
    replacement = NEW_HTML if "related" in layout else NEW_HTML.replace('<img src="cid:logo" alt="Logo">', '')
    changed = before.updated(body_html=replacement)
    assert before.message.as_bytes() == original
    for header in ("From", "To", "Cc", "Bcc", "Subject", "In-Reply-To", "References", "X-Workflow"):
        assert str(changed.message[header]) == str(before.message[header])
    assert changed.thread_id == before.thread_id
    assert changed.summary()["attachments"] == before.summary()["attachments"]
    assert assets == [p.as_bytes() for p in changed.message.walk()
                      if p.get_content_maintype() == "image" or p.get_filename()]
    plain = changed.message.get_body(preferencelist=("plain",)).get_content()
    assert "更新 & review 内容" in plain
    assert "https://example.com/report" in plain
    assert "Quoted history" in plain and "Old private text" not in plain
    html = changed.message.get_body(preferencelist=("html",))
    assert html.get_content().strip() == replacement
    assert str(html["Content-ID"]) == "<html-body>"
    assert str(html["Content-Location"]) == "body.html"
    assert not any(p.defects for p in parsed(raw(changed.message)).walk())


def test_html_update_rejects_ambiguous_or_conflicting_body_inputs():
    content = html_content()
    with pytest.raises(ValueError, match="only one"):
        content.updated(body_text="plain", body_html=NEW_HTML)
    with pytest.raises(ValueError, match="string"):
        content.updated(body_html=123)
    with pytest.raises(ValueError, match="existing HTML"):
        new_content().updated(body_html=NEW_HTML)
    content.message.add_attachment(b"file", maintype="application", subtype="pdf", filename="extra.pdf")
    content.message.attach(EmailMessage())
    content.message.get_payload()[-1].set_content("Ambiguous second body")
    with pytest.raises(ValueError, match="unambiguous"):
        content.updated(body_html=NEW_HTML)


def test_html_update_rejects_mixed_sibling_bodies():
    content = new_content()
    html = EmailMessage()
    html.set_content("<p>Different body</p>", subtype="html")
    content.message.attach(html)
    with pytest.raises(ValueError, match="same multipart/alternative"):
        content.updated(body_html="<p>Replacement</p>")


def test_html_conversion_does_not_log_private_body_or_include_scripts():
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        changed = html_content("html-only").updated(body_html="https://private.example.com/secret")
    assert not caught
    assert changed.message.get_body(preferencelist=("plain",)).get_content().strip() == "https://private.example.com/secret"
    changed = html_content("html-only").updated(body_html="<style>private-style</style><script>private-script</script><p>Visible</p>")
    assert changed.message.get_body(preferencelist=("plain",)).get_content().strip() == "Visible"


@pytest.mark.parametrize("html", ["<p>No image</p>", NEW_HTML.replace("cid:logo", "cid:missing")])
def test_html_update_rejects_lost_or_unknown_cid_without_disclosing_body(html):
    content = html_content()
    before = content.message.as_bytes()
    with pytest.raises(ValueError, match="CID") as caught:
        content.updated(body_html=html)
    assert html not in str(caught.value)
    assert content.message.as_bytes() == before


@pytest.mark.parametrize("corruption", ["plain", "html", "cid", "image", "thread"])
def test_html_apply_verifies_both_bodies_assets_and_thread(setup, monkeypatch, corruption):
    client, service = setup
    client.save_draft(html_content(image_filename=None), expected_email=service.account)
    before = client.get_draft("draft-1")
    real_write = service.write

    def corrupt_write(action, draft_id, kwargs):
        response = real_write(action, draft_id, kwargs)
        saved = service.drafts_data[draft_id]["message"]
        message = parsed(saved["raw"])
        if corruption in {"plain", "html"}:
            message.get_body(preferencelist=(corruption,)).set_content("stale", subtype=corruption)
        elif corruption == "thread":
            saved["threadId"] = "wrong-thread"
        else:
            image = next(p for p in message.walk() if p.get_content_maintype() == "image")
            if corruption == "cid":
                image.replace_header("Content-ID", "<wrong>")
            else:
                image.set_content(b"wrong bytes", maintype="image", subtype="png")
        saved["raw"] = raw(message)
        return response

    monkeypatch.setattr(service, "write", corrupt_write)
    result = client.update_draft(before.id, expected_email=service.account,
                                 expected_fingerprint=before.fingerprint, body_html=NEW_HTML)
    assert result["applied"] and not result["verified"] and not result["ok"]
    assert len(service.writes) == 2


def test_html_cli_preview_apply_conflict_and_verify(cli_setup, tmp_path, capsys):
    client, service = cli_setup
    client.save_draft(html_content(), expected_email=service.account)
    before = client.get_draft("draft-1")
    body = tmp_path / "body.html"
    assert cli.main(["get", "--draft-id", before.id, "--body-format", "html",
                     "--body-output", str(body)]) == 0
    capsys.readouterr()
    assert body.stat().st_mode & 0o777 == 0o600
    assert "Old private text" in body.read_text()
    body.write_text(NEW_HTML, encoding="utf-8")
    command = ["update", "--draft-id", before.id, "--expected-fingerprint", before.fingerprint,
               "--body-html-file", str(body)]
    assert cli.main(command) == 0
    output = capsys.readouterr()
    assert "更新" not in output.out + output.err
    preview = json.loads(output.out)
    assert not preview["applied"] and len(service.writes) == 1
    assert service.drafts_data[before.id]["message"]["raw"] == raw(before.content.message)
    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps(preview))
    assert cli.main([*command, "--apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["verified"] and applied["applied"] and len(service.writes) == 2
    assert cli.main(["verify", "--draft-id", before.id, "--expected-json", str(expected)]) == 0
    assert json.loads(capsys.readouterr().out)["verified"]
    assert cli.main([*command, "--apply"]) == 1
    assert not json.loads(capsys.readouterr().out)["applied"] and len(service.writes) == 2
    with pytest.raises(SystemExit):
        cli.create_parser().parse_args([*command, "--body-file", str(body)])


def test_html_write_timeout_is_not_retried(setup):
    client, service = setup
    client.save_draft(html_content(), expected_email=service.account)
    before = client.get_draft("draft-1")
    service.fail_write = True
    result = client.update_draft(before.id, expected_email=service.account,
                                 expected_fingerprint=before.fingerprint, body_html=NEW_HTML)
    assert result["applied"] is None and not result["verified"]
    assert len(service.writes) == 2


@pytest.mark.parametrize("cid", ["<>", "< >", ""])
def test_empty_content_id_does_not_preserve_replaced_attachment(cid):
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "other@example.com"
    message.set_content("plain")
    message.add_attachment(b"old", maintype="application", subtype="pdf", filename="old.pdf")
    message.get_payload()[-1]["Content-ID"] = cid
    content = GmailDraftContent(message)
    changed = content.updated(attachments=[("new.pdf", b"new", "application/pdf")])
    assert [a["filename"] for a in changed.summary()["attachments"]] == ["new.pdf"]
    assert len(content.summary()["attachments"]) == 1


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


@pytest.mark.parametrize("granted,required", [
    (["https://www.googleapis.com/auth/gmail.modify"], module.SCOPES_REPLY),
    (["https://mail.google.com/"], module.SCOPES_REPLY),
    (module.SCOPES_REPLY, module.SCOPES_DRAFT),
    (module.SCOPES_DRAFT, module.SCOPES_DRAFT),
    (module.SCOPES_DETECT, module.SCOPES_DETECT),
])
def test_oauth_refresh_preserves_original_grant(monkeypatch, granted, required):
    captured = []
    creds = SimpleNamespace(granted_scopes=None)
    monkeypatch.setattr(module.Credentials, "from_authorized_user_info",
                        lambda info, scopes: captured.append(scopes) or creds)
    monkeypatch.setattr(module, "refresh_credentials", lambda value: value)
    assert module.credentials_from_authorized_user_info({"scopes": granted}, required) is creds
    assert captured == [granted]


@pytest.mark.parametrize("granted", [module.SCOPES_DETECT, module.SCOPES_DRAFT, [],
    ["https://www.googleapis.com/auth/gmail.send"]])
def test_insufficient_reply_grant_stops_before_refresh(monkeypatch, granted):
    def unexpected_refresh(*args, **kwargs):
        pytest.fail("Insufficient grant must not trigger OAuth refresh")
    monkeypatch.setattr(module.Credentials, "from_authorized_user_info", unexpected_refresh)
    with pytest.raises(ValueError, match="lacks required"):
        module.credentials_from_authorized_user_info({"scopes": granted}, module.SCOPES_REPLY)


def test_oauth_refresh_checks_actual_granted_scopes(monkeypatch):
    creds = SimpleNamespace(granted_scopes=module.SCOPES_DETECT)
    monkeypatch.setattr(module.Credentials, "from_authorized_user_info", lambda *args: creds)
    monkeypatch.setattr(module, "refresh_credentials", lambda value: value)
    with pytest.raises(ValueError, match="lacks required"):
        module.credentials_from_authorized_user_info({"scopes": module.SCOPES_REPLY}, module.SCOPES_REPLY)


def test_oauth_token_without_scope_metadata_preserves_legacy_behavior(monkeypatch):
    captured = []
    creds = SimpleNamespace(granted_scopes=None)
    monkeypatch.setattr(module.Credentials, "from_authorized_user_info",
                        lambda info, scopes: captured.append(scopes) or creds)
    monkeypatch.setattr(module, "refresh_credentials", lambda value: value)
    module.credentials_from_authorized_user_info({}, module.SCOPES_REPLY)
    assert captured == [module.SCOPES_REPLY]


def test_oauth_scope_string_is_normalized(monkeypatch):
    captured = []
    creds = SimpleNamespace(granted_scopes=None)
    monkeypatch.setattr(module.Credentials, "from_authorized_user_info",
                        lambda info, scopes: captured.append(scopes) or creds)
    monkeypatch.setattr(module, "refresh_credentials", lambda value: value)
    module.credentials_from_authorized_user_info({"scopes": " ".join(module.SCOPES_REPLY)}, module.SCOPES_REPLY)
    assert captured == [module.SCOPES_REPLY]


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


def test_explicit_authorize_reconsents_insufficient_existing_grant(setup, monkeypatch, tmp_path):
    client, service = setup
    token=tmp_path/'token.json'
    token.write_text('old token')
    def insufficient(*args):
        raise module.GmailScopeError('insufficient grant')
    monkeypatch.setattr(module,'credentials_from_authorized_user_file',insufficient)
    creds=SimpleNamespace(to_json=lambda:'{"token":"replacement"}')
    run=SimpleNamespace(run_local_server=lambda **kw:creds)
    monkeypatch.setitem(sys.modules,'google_auth_oauthlib.flow',SimpleNamespace(InstalledAppFlow=SimpleNamespace(from_client_secrets_file=lambda *args:run)))
    monkeypatch.setattr(module,'GmailClient',lambda creds:client)
    assert module.authorize(tmp_path/'client.json',token,module.SCOPES_REPLY,service.account) is creds
    assert json.loads(token.read_text())['token']=='replacement'


def test_authorize_reconsents_when_saved_token_refresh_is_rejected(setup, monkeypatch, tmp_path):
    from google.auth.exceptions import RefreshError
    client, service = setup
    token = tmp_path / "token.json"
    token.write_text("revoked token")
    def rejected(*args):
        raise RefreshError("invalid_grant: Token has been expired or revoked.")
    monkeypatch.setattr(module, "credentials_from_authorized_user_file", rejected)
    creds = SimpleNamespace(to_json=lambda: '{"token": "replacement"}')
    run = SimpleNamespace(run_local_server=lambda **kw: creds)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow",
                        SimpleNamespace(InstalledAppFlow=SimpleNamespace(from_client_secrets_file=lambda *args: run)))
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    assert module.authorize(tmp_path / "client.json", token, module.SCOPES_REPLY, service.account) is creds
    assert json.loads(token.read_text())["token"] == "replacement"


def test_authorize_keeps_revoked_token_when_reconsent_fails(setup, monkeypatch, tmp_path):
    from google.auth.exceptions import RefreshError
    client, service = setup
    token = tmp_path / "token.json"
    token.write_text("revoked token")
    def rejected(*args):
        raise RefreshError("invalid_grant")
    monkeypatch.setattr(module, "credentials_from_authorized_user_file", rejected)
    creds = SimpleNamespace(to_json=lambda: '{"token": "other account"}')
    run = SimpleNamespace(run_local_server=lambda **kw: creds)
    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow",
                        SimpleNamespace(InstalledAppFlow=SimpleNamespace(from_client_secrets_file=lambda *args: run)))
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    with pytest.raises(ValueError, match="does not match"):
        module.authorize(tmp_path / "client.json", token, module.SCOPES_REPLY, "wrong@example.com")
    assert token.read_text() == "revoked token"

@pytest.mark.parametrize('grant,required', [('modify','metadata'),('modify','send'),('modify','insert'),('modify','labels'),('compose','send'),('readonly','metadata')])
def test_documented_additional_scope_equivalents(grant, required):
    prefix='https://www.googleapis.com/auth/gmail.'
    module._require_gmail_scopes([prefix+grant],[prefix+required])


def test_modify_does_not_imply_unrelated_admin_scopes():
    with pytest.raises(module.GmailScopeError):
        module._require_gmail_scopes(['https://www.googleapis.com/auth/gmail.modify'],['https://www.googleapis.com/auth/gmail.settings.sharing'])


def test_reconsent_keeps_scopes_the_saved_token_already_held(setup, monkeypatch, tmp_path):
    from google.auth.exceptions import RefreshError
    client, service = setup
    token = tmp_path / "token.json"
    modify = "https://www.googleapis.com/auth/gmail.modify"
    token.write_text(json.dumps({"refresh_token": "r", "scopes": [modify]}))
    monkeypatch.setattr(module, "credentials_from_authorized_user_file",
                        lambda *a: (_ for _ in ()).throw(RefreshError("invalid_grant")))
    requested = []
    creds = SimpleNamespace(to_json=lambda: '{"token": "replacement"}')
    run = SimpleNamespace(run_local_server=lambda **kw: creds)

    def flow(path, scopes):
        requested.append(list(scopes))
        return run

    monkeypatch.setitem(sys.modules, "google_auth_oauthlib.flow",
                        SimpleNamespace(InstalledAppFlow=SimpleNamespace(from_client_secrets_file=flow)))
    monkeypatch.setattr(module, "GmailClient", lambda creds: client)
    module.authorize(tmp_path / "client.json", token, module.SCOPES_DRAFT, service.account)
    assert requested == [[modify, *module.SCOPES_DRAFT]]


def test_saved_summary_without_new_body_keys_still_verifies():
    message = EmailMessage(policy=policy.SMTP)
    message["To"] = "to@example.com"
    message["Subject"] = "s"
    message.set_content("plain")
    message.add_alternative("<p>html</p>", subtype="html")
    message.get_payload()[1]["Content-ID"] = "<body@example>"
    content = module.GmailDraftContent(message)
    summary = {"schema_version": "gmail-draft/v1", **content.summary()}
    legacy = deepcopy(summary)
    for part in legacy["body_parts"]:
        part.pop("content_id", None)
    draft = module.GmailDraft("d", "m", content, "f")
    gc = module.GmailClient.__new__(module.GmailClient)
    assert gc.verify_draft_summary(draft, expected=legacy)["verified"]
    changed = deepcopy(summary)
    changed["body_parts"][1]["content_id"] = "<other@example>"
    assert not gc.verify_draft_summary(draft, expected=changed)["verified"]
