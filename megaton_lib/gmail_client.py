"""Small Gmail API helpers for message lookup and draft creation."""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable, Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES_DETECT = ["https://www.googleapis.com/auth/gmail.readonly"]
SCOPES_DRAFT = ["https://www.googleapis.com/auth/gmail.compose"]


def parse_email_list(value: str | Sequence[str] | None) -> list[str]:
    """Parse comma/semicolon/newline separated email addresses."""
    if value is None:
        return []
    if isinstance(value, str):
        parts = re.split(r"[,;\n]+", value)
    else:
        parts = list(value)
    return [str(part).strip() for part in parts if str(part).strip()]


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
    return refresh_credentials(Credentials.from_authorized_user_info(token_info, scopes))


def credentials_from_authorized_user_file(token_path: str | Path, scopes: list[str]) -> Credentials:
    """Load and refresh OAuth user credentials from a token JSON file."""
    return refresh_credentials(Credentials.from_authorized_user_file(str(token_path), scopes))


def authorize(
    client_secrets_path: Path,
    token_path: Path,
    scopes: list[str],
    expected_email: str | None = None,
) -> Credentials:
    """Run a desktop OAuth flow once, then reuse/refresh the saved token."""
    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), scopes)

    if creds:
        try:
            return refresh_credentials(creds)
        except RuntimeError:
            pass

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

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    token_path.chmod(0o600)
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


class GmailClient:
    def __init__(self, creds: Credentials):
        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)

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
        """
        msg = EmailMessage()
        msg["From"] = sender
        msg["To"] = ", ".join(to)
        if cc:
            msg["Cc"] = ", ".join(cc)
        if bcc:
            msg["Bcc"] = ", ".join(bcc)
        msg["Subject"] = subject
        msg.set_content(body_text)

        for filename, data, mime_type in attachments or []:
            maintype, _, subtype = (mime_type or "application/octet-stream").partition("/")
            msg.add_attachment(
                data,
                maintype=maintype or "application",
                subtype=subtype or "octet-stream",
                filename=filename,
            )

        raw = base64.urlsafe_b64encode(bytes(msg)).decode("ascii")
        return (
            self._service.users()
            .drafts()
            .create(userId="me", body={"message": {"raw": raw}})
            .execute()
        )

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
