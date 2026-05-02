from __future__ import annotations

import base64
from email import policy
from email.parser import BytesParser

from megaton_lib.gmail_client import SCOPES_DRAFT, GmailClient, parse_email_list


def test_draft_scope_is_compose_only():
    assert SCOPES_DRAFT == ["https://www.googleapis.com/auth/gmail.compose"]


class _Execute:
    def __init__(self, response):
        self.response = response

    def execute(self):
        return self.response


class _Drafts:
    def __init__(self, service):
        self.service = service

    def create(self, **kwargs):
        self.service.create_kwargs = kwargs
        return _Execute({"id": "draft-1"})


class _Users:
    def __init__(self, service):
        self.service = service

    def drafts(self):
        return _Drafts(self.service)


class _Service:
    create_kwargs = None

    def users(self):
        return _Users(self)


def _decode_created_message(service: _Service):
    raw = service.create_kwargs["body"]["message"]["raw"]
    return BytesParser(policy=policy.default).parsebytes(base64.urlsafe_b64decode(raw))


def test_parse_email_list_accepts_common_separators():
    assert parse_email_list("a@example.com, b@example.com\nc@example.com; d@example.com") == [
        "a@example.com",
        "b@example.com",
        "c@example.com",
        "d@example.com",
    ]


def test_create_draft_builds_mime_message_with_attachment():
    service = _Service()
    client = object.__new__(GmailClient)
    client._service = service

    response = client.create_draft(
        sender="sender@example.com",
        to=["to@example.com"],
        cc=["cc@example.com"],
        bcc=["bcc@example.com"],
        subject="Monthly report",
        body_text="Please check the report.",
        attachments=[("report.txt", b"hello", "text/plain")],
    )

    assert response == {"id": "draft-1"}
    assert service.create_kwargs["userId"] == "me"
    message = _decode_created_message(service)
    assert message["From"] == "sender@example.com"
    assert message["To"] == "to@example.com"
    assert message["Cc"] == "cc@example.com"
    assert message["Bcc"] == "bcc@example.com"
    assert message["Subject"] == "Monthly report"
    parts = list(message.iter_parts())
    assert parts[0].get_content().strip() == "Please check the report."
    assert parts[1].get_filename() == "report.txt"
    assert parts[1].get_content() == "hello"
