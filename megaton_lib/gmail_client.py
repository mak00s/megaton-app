"""Small Gmail API helpers for message lookup and draft creation."""

from __future__ import annotations

import base64
import binascii
import json
import hashlib
import os
import re
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from email import policy
from email.headerregistry import HeaderRegistry
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import getaddresses, formataddr
from pathlib import Path
from typing import Any, Iterable, Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES_DETECT = ["https://www.googleapis.com/auth/gmail.readonly"]
SCOPES_DRAFT = ["https://www.googleapis.com/auth/gmail.compose"]
SCOPES_REPLY = [*SCOPES_DETECT, *SCOPES_DRAFT]


def parse_email_list(value: str | Sequence[str] | None) -> list[str]:
    """Parse RFC address lists/groups, with legacy semicolon/newline separators.

    Empty groups contain no recipients. Null or malformed mailboxes still fail.
    Parse RFC syntax first so group terminators and quoted punctuation survive.
    """
    if value is None:
        return []
    parts = [value] if isinstance(value, str) else list(value)

    def parse(part):
        try:
            header = HeaderRegistry()("To", part)
        except (ValueError, IndexError) as exc:
            raise ValueError("Invalid email address") from exc
        if header.defects or any(not a.username or not a.domain for a in header.addresses):
            raise ValueError("Invalid email address")
        return [formataddr((a.display_name, a.addr_spec)) for a in header.addresses]

    result = []
    for part in parts:
        if not part.strip():
            continue
        try:
            addresses = parse(part)
        except ValueError:
            # Only accept the legacy form if every separate fragment is valid.
            fragments = [s for s in re.split(r"[;\r\n]+", part) if s.strip()]
            if len(fragments) < 2:
                raise
            addresses = [address for fragment in fragments for address in parse(fragment)]
        result.extend(addresses)
    return result


def refresh_credentials(creds: Credentials) -> Credentials:
    """Return valid credentials, refreshing an existing refresh token if needed."""
    if creds.valid:
        return creds
    if creds.refresh_token:
        creds.refresh(Request())
        if creds.valid:
            return creds
    raise RuntimeError("Gmail credentials are invalid and cannot be refreshed.")


def credentials_from_authorized_user_info(
    token_info: dict[str, Any] | str,
    scopes: list[str],
) -> Credentials:
    """Load and refresh OAuth user credentials from token JSON data."""
    if isinstance(token_info, str):
        token_info = json.loads(token_info)
    if token_info.get("type") == "service_account" or "private_key" in token_info:
        raise ValueError("Gmail requires a user OAuth token, not a service account.")
    return refresh_credentials(Credentials.from_authorized_user_info(token_info, scopes))


def credentials_from_authorized_user_file(token_path: str | Path, scopes: list[str]) -> Credentials:
    """Load and refresh OAuth user credentials from a token JSON file."""
    return credentials_from_authorized_user_info(Path(token_path).read_text(), scopes)


def load_draft_credentials_from_env(*, env_prefix: str = "", scopes=None) -> Credentials:
    """Load only an explicitly configured user token; never use ADC/service accounts."""
    prefix = env_prefix.strip().upper()

    def value(key):
        return (os.getenv(f"{prefix}_GMAIL_DRAFT_{key}", "") if prefix else "").strip() or os.getenv(
            f"GMAIL_DRAFT_{key}", ""
        ).strip()

    scopes = SCOPES_DRAFT if scopes is None else scopes
    if value("TOKEN_JSON"):
        return credentials_from_authorized_user_info(value("TOKEN_JSON"), scopes)
    if value("TOKEN_PATH"):
        return credentials_from_authorized_user_file(value("TOKEN_PATH"), scopes)
    raise ValueError("Set GMAIL_DRAFT_TOKEN_PATH or GMAIL_DRAFT_TOKEN_JSON to a user OAuth token")


def authorize(
    client_secrets_path: Path,
    token_path: Path,
    scopes: list[str],
    expected_email: str | None = None,
) -> Credentials:
    """Run a desktop OAuth flow once, then reuse/refresh the saved token."""
    if token_path.exists():
        try:
            creds = credentials_from_authorized_user_file(token_path, scopes)
        except RuntimeError:
            pass
        else:
            if expected_email:
                GmailClient(creds).assert_account(expected_email)
            return creds

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise RuntimeError(
            "google-auth-oauthlib is required for interactive Gmail authorization."
        ) from exc

    flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_path), scopes)
    creds = flow.run_local_server(
        port=0,
        open_browser=True,
        authorization_prompt_message=(
            f"ブラウザで {expected_email or '対象アカウント'} にログインして認可してください..."
        ),
        success_message="認可完了。ブラウザを閉じて OK です。",
    )
    if expected_email:
        GmailClient(creds).assert_account(expected_email)

    token_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=token_path.parent,
                                         prefix=".gmail-token-", delete=False) as token_file:
            temporary = Path(token_file.name)
            token_file.write(creds.to_json())
        os.replace(temporary, token_path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return creds


@dataclass
class GmailAttachment:
    message_id: str
    filename: str
    mime_type: str
    data: bytes


@dataclass
class GmailMessage:
    id: str
    thread_id: str
    date: str
    subject: str
    from_: str
    to: str
    snippet: str
    body_text: str
    attachments: list[GmailAttachment]


def _addresses(values: Sequence[str]) -> list[str]:
    return [address.casefold() for _, address in getaddresses(parse_email_list(values))]


def _recipients(to, cc, bcc, *, exclude=()):
    seen = set(exclude)
    groups = []
    for group in (to, cc, bcc):
        out = []
        for value in parse_email_list(group):
            key = _addresses([value])[0]
            if key not in seen:
                seen.add(key)
                out.append(value)
        groups.append(out)
    return groups


def _attach(message: EmailMessage, attachments):
    for filename, data, mime_type in attachments or []:
        if not filename or any(c in filename for c in "\r\n"):
            raise ValueError("Attachment requires a safe filename")
        maintype, separator, subtype = (mime_type or "application/octet-stream").partition("/")
        if not separator or not maintype or not subtype:
            raise ValueError("Invalid attachment MIME type")
        message.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)


def _new_message(sender, to, subject, body_text, cc=None, bcc=None, attachments=None):
    message = EmailMessage(policy=policy.SMTP)
    message["From"] = sender
    for name, values in zip(("To", "Cc", "Bcc"), _recipients(to, cc, bcc)):
        if values:
            message[name] = ", ".join(values)
    message["Subject"] = subject
    message.set_content(body_text)
    _attach(message, attachments)
    return message


def _encode_message(message: EmailMessage) -> str:
    return base64.urlsafe_b64encode(message.as_bytes(policy=policy.SMTP)).decode("ascii")


def _decode_raw(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def _parse_message(raw: str) -> EmailMessage:
    message = BytesParser(policy=policy.SMTP).parsebytes(_decode_raw(raw))
    if any(part.defects for part in message.walk()):
        raise ValueError("Malformed MIME; refusing unsafe draft operation")
    return message


@dataclass
class GmailDraftContent:
    """Prepared MIME and thread metadata; never contains an implied send action."""

    message: EmailMessage
    thread_id: str = ""

    @classmethod
    def new(cls, *, sender: str, to: list[str], subject: str, body_text: str,
            cc=None, bcc=None, attachments=None) -> GmailDraftContent:
        return cls(_new_message(sender, to, subject, body_text, cc, bcc, attachments))

    def summary(self) -> dict[str, Any]:
        message = self.message
        for name in ("From", "To", "Cc", "Bcc", "Subject", "In-Reply-To", "References"):
            if len(message.get_all(name, [])) > 1:
                raise ValueError(f"Duplicate MIME header: {name}")
        attachments, bodies = [], []
        for part in message.walk():
            if part.is_multipart():
                continue
            data = part.get_payload(decode=True) or b""
            if part.get_filename() or part.get_content_disposition() == "attachment":
                attachments.append({"filename": part.get_filename() or "",
                                    "mime_type": part.get_content_type(), "size": len(data),
                                    "content_id": str(part.get("Content-ID", "")),
                                    "disposition": part.get_content_disposition(),
                                    "sha256": hashlib.sha256(data).hexdigest()})
            else:
                # MIME transfer encodings/newline conventions may change in Gmail.
                if part.get_content_maintype() == "text":
                    data = part.get_content().replace("\r\n", "\n").encode("utf-8")
                bodies.append({"mime_type": part.get_content_type(),
                               "sha256": hashlib.sha256(data).hexdigest()})
        return {"thread_id": self.thread_id,
                "sender": _addresses(message.get_all("From", [])),
                "to": _addresses(message.get_all("To", [])),
                "cc": _addresses(message.get_all("Cc", [])),
                "bcc": _addresses(message.get_all("Bcc", [])),
                "subject": str(message.get("Subject", "")),
                "in_reply_to": " ".join(str(message.get("In-Reply-To", "")).split()),
                "references": " ".join(str(message.get("References", "")).split()),
                "attachments": attachments, "body_parts": bodies}

    def updated(self, *, body_text=None, to=None, cc=None, bcc=None,
                subject=None, attachments=None) -> GmailDraftContent:
        """None preserves a field; [] clears attachments/CC/BCC.

        Body replacement is deliberately limited to plain-text drafts. Other
        edits retain HTML and inline parts. Signed/encrypted/embedded MIME is
        rejected, not silently flattened.
        """
        message = deepcopy(self.message)
        if message.get_filename() or message.get_content_disposition() == "attachment":
            raise ValueError("Attachment-only root MIME cannot be safely edited")
        allowed = {"multipart/mixed", "multipart/alternative", "multipart/related"}
        for part in message.walk():
            if (part.is_multipart() and part.get_content_type() not in allowed
                    or part.get_content_type() in {"application/pkcs7-mime", "application/pkcs7-signature"}):
                raise ValueError("Unsupported signed/encrypted/embedded MIME; draft left unchanged")
        if subject is not None and self.thread_id and subject != str(message.get("Subject", "")):
            raise ValueError("Threaded draft subject must remain unchanged")
        groups = _recipients(
            to if to is not None else message.get_all("To", []),
            cc if cc is not None else message.get_all("Cc", []),
            bcc if bcc is not None else message.get_all("Bcc", []),
        )
        for key, values in zip(("To", "Cc", "Bcc"), groups):
            del message[key]
            if values:
                message[key] = ", ".join(values)
        if subject is not None:
            del message["Subject"]
            message["Subject"] = subject
        if body_text is not None:
            leaves = [p for p in message.walk() if not p.is_multipart()
                      and not p.get_filename() and p.get_content_disposition() != "attachment"]
            if len(leaves) != 1 or leaves[0].get_content_type() != "text/plain":
                raise ValueError("Body replacement requires a plain-text draft; HTML/inline content preserved")
            leaves[0].set_content(body_text)
        if attachments is not None:
            for part in message.walk():
                if part.is_multipart():
                    part.set_payload([p for p in part.iter_parts()
                                      if p.get("Content-ID") or not (
                                          p.get_filename() or p.get_content_disposition() == "attachment")])
            _attach(message, attachments)
        return GmailDraftContent(message, self.thread_id)


@dataclass
class GmailDraft:
    id: str
    message_id: str
    content: GmailDraftContent
    fingerprint: str

    def summary(self) -> dict[str, Any]:
        return {"draft_id": self.id, "message_id": self.message_id,
                "fingerprint": self.fingerprint, **self.content.summary()}


def _draft_result(draft: GmailDraft, *, errors=(), verified=False) -> dict[str, Any]:
    return {"schema_version": "gmail-draft/v1", "ok": not errors,
            "exit_code": 1 if errors else 0, "action": "verify", "applied": False,
            "verified": verified, "errors": list(errors), **draft.summary()}


class GmailClient:
    def __init__(self, creds: Credentials):
        if not isinstance(creds, Credentials):
            raise ValueError("Gmail requires authorized-user OAuth credentials.")
        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    def assert_account(self, expected_email: str) -> str:
        """Check the real OAuth mailbox; a sender string is not identity proof."""
        expected = _addresses([expected_email])
        if len(expected) != 1:
            raise ValueError("Exactly one expected Gmail account is required")
        actual = self._service.users().getProfile(userId="me").execute()["emailAddress"]
        if actual.casefold() != expected[0].casefold():
            raise ValueError("Gmail OAuth account does not match expected_email")
        return actual

    def search_messages(self, query: str, max_results: int = 50) -> list[dict[str, str]]:
        """Search Gmail with q syntax and return message references."""
        out: list[dict[str, str]] = []
        page_token: str | None = None
        fetched = 0
        while True:
            req: dict[str, Any] = {
                "userId": "me",
                "q": query,
                "maxResults": min(100, max_results - fetched),
            }
            if page_token:
                req["pageToken"] = page_token
            resp = self._service.users().messages().list(**req).execute()
            msgs = resp.get("messages", []) or []
            out.extend(msgs)
            fetched += len(msgs)
            page_token = resp.get("nextPageToken")
            if not page_token or fetched >= max_results:
                break
        return out

    def get_message(self, message_id: str, *, with_attachments: bool = False) -> GmailMessage:
        full = (
            self._service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
            .execute()
        )
        headers = {h["name"].lower(): h["value"] for h in full.get("payload", {}).get("headers", [])}
        body_text = _extract_plain_text(full.get("payload", {}))
        attachments: list[GmailAttachment] = []
        if with_attachments:
            attachments = list(self._walk_attachments(full.get("payload", {}), message_id))
        return GmailMessage(
            id=full["id"],
            thread_id=full["threadId"],
            date=headers.get("date", ""),
            subject=headers.get("subject", ""),
            from_=headers.get("from", ""),
            to=headers.get("to", ""),
            snippet=full.get("snippet", ""),
            body_text=body_text,
            attachments=attachments,
        )

    def create_draft(
        self,
        *,
        sender: str,
        to: list[str],
        subject: str,
        body_text: str,
        attachments: list[tuple[str, bytes, str]] | None = None,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a Gmail draft.

        attachments: [(filename, data, mime_type), ...]
        Bcc recipients are stored in the draft MIME until Gmail sends it.
        From must match the OAuth mailbox. This compatibility API returns the
        raw Gmail response; prefer save_draft() for structured readback verification.
        """
        self.assert_account(sender)
        msg = _new_message(sender, to, subject, body_text, cc, bcc, attachments)
        raw = _encode_message(msg)
        return (
            self._service.users()
            .drafts()
            .create(userId="me", body={"message": {"raw": raw}})
            .execute()
        )

    def prepare_reply(
        self, message_id: str, *, expected_email: str, body_text: str,
        reply_all: bool = False, self_aliases: Sequence[str] = (),
        attachments: list[tuple[str, bytes, str]] | None = None,
    ) -> GmailDraftContent:
        """Read a source message and prepare a reply without creating a draft.

        Bcc and original attachments are never inherited. Self-originated sources
        are rejected: use a received message or an explicit new draft instead.
        """
        account = self.assert_account(expected_email)
        source = self._service.users().messages().get(
            userId="me", id=message_id, format="raw",
        ).execute()
        msg = _parse_message(source["raw"])
        for key in ("From", "Reply-To", "Message-ID", "Subject", "References"):
            if len(msg.get_all(key, [])) > 1:
                raise ValueError(f"Ambiguous source header: {key}")
        parent = str(msg.get("Message-ID", "")).strip()
        if not re.fullmatch(r"<[^<>\s]+@[^<>\s]+>", parent):
            raise ValueError("Source has no valid RFC Message-ID; cannot safely thread reply")
        own = {v.casefold() for v in _addresses([account, *self_aliases])}
        if own.intersection(v.casefold() for v in _addresses(msg.get_all("From", []))):
            raise ValueError("Reply source is self-originated; select a received message")
        to = parse_email_list(msg.get_all("Reply-To", []) or msg.get_all("From", []))
        cc = []
        if reply_all:
            to += parse_email_list(msg.get_all("To", []))
            cc = parse_email_list(msg.get_all("Cc", []))
        to, cc, _ = _recipients(to, cc, [], exclude=own)
        if not to:
            raise ValueError("Reply has no external To recipient")
        reply = _new_message(account, to, str(msg.get("Subject", "")), body_text, cc, [], attachments)
        refs = str(msg.get("References", "")).split()
        if any(not re.fullmatch(r"<[^<>\s]+@[^<>\s]+>", ref) for ref in refs):
            raise ValueError("Invalid References header")
        reply["In-Reply-To"] = parent
        reply["References"] = " ".join(dict.fromkeys([*refs, parent]))
        return GmailDraftContent(reply, source["threadId"])

    def create_reply_draft(self, message_id: str, *, expected_email: str, **kwargs) -> dict[str, Any]:
        content = self.prepare_reply(message_id, expected_email=expected_email, **kwargs)
        return self.save_draft(content, expected_email=expected_email)

    def get_draft(self, draft_id: str) -> GmailDraft:
        """Fetch raw MIME; draft ID is stable but the contained message ID can change."""
        draft = self._service.users().drafts().get(userId="me", id=draft_id, format="raw").execute()
        message = draft["message"]
        raw = _decode_raw(message["raw"])
        content = GmailDraftContent(_parse_message(message["raw"]), message.get("threadId", ""))
        fingerprint = hashlib.sha256(message.get("threadId", "").encode() + b"\0" + raw).hexdigest()
        return GmailDraft(draft["id"], message["id"], content, fingerprint)

    def verify_draft(self, draft_id: str, *, expected: GmailDraftContent) -> dict[str, Any]:
        observed = self.get_draft(draft_id)
        wanted = expected.summary()
        actual = observed.content.summary()
        fields = [key for key in wanted if key != "thread_id" or expected.thread_id]
        errors = [f"mismatch:{key}" for key in fields if wanted[key] != actual[key]]
        return _draft_result(observed, errors=errors, verified=not errors)

    def verify_draft_summary(self, draft: GmailDraft, *, expected: dict[str, Any]) -> dict[str, Any]:
        """Verify against saved CLI JSON, without requiring plaintext body in that file."""
        actual = draft.content.summary()
        if expected.get("schema_version") != "gmail-draft/v1" or any(k not in expected for k in actual):
            raise ValueError("Expected JSON must contain a complete gmail-draft/v1 content summary")
        errors = [f"mismatch:{key}" for key in actual
                  if (key != "thread_id" or expected[key]) and actual[key] != expected[key]]
        if expected.get("draft_id") and expected["draft_id"] != draft.id:
            errors.append("mismatch:draft_id")
        return _draft_result(draft, errors=errors, verified=not errors)

    def save_draft(
        self, content: GmailDraftContent, *, expected_email: str,
        draft_id: str | None = None, expected_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Create/update once, then read back. Never retry an ambiguous write.

        A fingerprint detects prior edits, but is not an atomic lock against
        concurrent Gmail UI edits. Coordinate ownership outside this client.
        """
        account = self.assert_account(expected_email)
        if _addresses(content.message.get_all("From", [])) != [account.casefold()]:
            raise ValueError("Draft From must match the OAuth mailbox")
        content.summary()  # Validate MIME before writing.
        if not any(_addresses(content.message.get_all(name, [])) for name in ("To", "Cc", "Bcc")):
            raise ValueError("At least one To, Cc or Bcc recipient is required")
        if draft_id:
            if not expected_fingerprint:
                raise ValueError("Updates require expected_fingerprint")
            if self.get_draft(draft_id).fingerprint != expected_fingerprint:
                raise ValueError("Draft changed since read; get and review it again")
        body: dict[str, Any] = {"message": {"raw": _encode_message(content.message)}}
        if content.thread_id:
            body["message"]["threadId"] = content.thread_id
        resource = self._service.users().drafts()
        try:
            response = (
                resource.update(userId="me", id=draft_id, body=body) if draft_id
                else resource.create(userId="me", body=body)
            ).execute()
        except Exception as exc:
            # Do not expose API response bodies containing private MIME in JSON.
            return {"schema_version": "gmail-draft/v1", "ok": False, "exit_code": 1,
                    "action": "update" if draft_id else "create", "applied": None,
                    "verified": False, "account": account, "draft_id": draft_id,
                    "errors": [f"write_outcome_unknown:{type(exc).__name__}"],
                    "next_action": "Inspect Gmail before retrying; do not automatically create another draft."}
        saved_id = response.get("id") if isinstance(response, dict) else None
        if not saved_id:
            return {"schema_version": "gmail-draft/v1", "ok": False, "exit_code": 1,
                    "action": "update" if draft_id else "create", "applied": True,
                    "account": account, "draft_id": draft_id, "verified": False,
                    "errors": ["write_response_missing_draft_id"],
                    "next_action": "Inspect Gmail; do not automatically create another draft."}
        try:
            result = self.verify_draft(saved_id, expected=content)
        except Exception as exc:
            result = {"schema_version": "gmail-draft/v1", "ok": False, "exit_code": 1,
                      "draft_id": saved_id, "verified": False,
                      "errors": [f"readback_failed:{type(exc).__name__}"],
                      "next_action": "Verify this draft ID; do not recreate it."}
        result.update(applied=True, account=account, action="update" if draft_id else "create")
        return result

    def update_draft(
        self, draft_id: str, *, expected_email: str, expected_fingerprint: str, **changes,
    ) -> dict[str, Any]:
        draft = self.get_draft(draft_id)
        if draft.fingerprint != expected_fingerprint:
            raise ValueError("Draft changed since read; get and review it again")
        return self.save_draft(draft.content.updated(**changes), expected_email=expected_email,
                               draft_id=draft_id, expected_fingerprint=expected_fingerprint)

    def _walk_attachments(self, payload: dict[str, Any], message_id: str) -> Iterable[GmailAttachment]:
        parts = [payload]
        while parts:
            part = parts.pop()
            parts.extend(reversed(part.get("parts") or []))
            filename = part.get("filename") or ""
            if not filename:
                continue
            body = part.get("body") or {}
            att_id = body.get("attachmentId")
            if not att_id:
                data = body.get("data") or ""
                if not data:
                    continue
                raw = base64.urlsafe_b64decode(data)
            else:
                resp = (
                    self._service.users()
                    .messages()
                    .attachments()
                    .get(userId="me", messageId=message_id, id=att_id)
                    .execute()
                )
                raw = base64.urlsafe_b64decode(resp.get("data") or "")
            yield GmailAttachment(
                message_id=message_id,
                filename=filename,
                mime_type=part.get("mimeType") or "",
                data=raw,
            )


def _extract_plain_text(payload: dict[str, Any]) -> str:
    parts = [payload]
    out_plain: list[str] = []
    out_html: list[str] = []
    while parts:
        part = parts.pop()
        parts.extend(reversed(part.get("parts") or []))
        mime = (part.get("mimeType") or "").lower()
        body = part.get("body") or {}
        data = body.get("data") or ""
        if not data:
            continue
        try:
            decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        except (binascii.Error, TypeError, UnicodeError):
            continue
        if mime == "text/plain":
            out_plain.append(decoded)
        elif mime == "text/html":
            out_html.append(decoded)
    return "\n".join(out_plain or out_html)
