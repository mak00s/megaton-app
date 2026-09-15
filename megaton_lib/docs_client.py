"""Guarded, caller-selected Google Docs text edits; no Drive management.

Plans contain document text. Treat them as confidential artifacts, not logs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import difflib
import json
from pathlib import Path
import re
from typing import Any

SCOPES_EDIT = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/userinfo.email",
]


class DocsEditError(ValueError):
    """A safe-to-display validation error that never includes document content."""


def _id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise DocsEditError("Use an explicit document ID, not a URL.")
    return value


def _tab_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise DocsEditError("Use an explicit tab ID, not a URL.")
    return value


def _tabs(document: dict) -> list[dict]:
    result = []

    def visit(tabs):
        for tab in tabs:
            result.append(tab)
            visit(tab.get("childTabs", []))

    visit(document.get("tabs", []))
    return result


def _select(document: dict, tab_id: str | None) -> dict:
    tabs = _tabs(document)
    if tab_id is None and len(tabs) == 1:
        return tabs[0]
    matches = [t for t in tabs if t["tabProperties"]["tabId"] == tab_id]
    if len(matches) != 1:
        raise DocsEditError("Specify one tab_id from get(); no default for multi-tab documents.")
    return matches[0]


def _has_suggestions(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            (key.startswith("suggest") and bool(item)) or _has_suggestions(item)
            for key, item in value.items()
        )
    return isinstance(value, list) and any(_has_suggestions(item) for item in value)


def _single_line(text: str, *, empty: bool = False) -> None:
    if not isinstance(text, str) or (not text and not empty):
        raise DocsEditError("Text must be a nonempty string.")
    if any(ord(c) < 32 or c in "\x85\u2028\u2029" or 0xD800 <= ord(c) <= 0xDFFF
           or 0xE000 <= ord(c) <= 0xF8FF for c in text):
        raise DocsEditError("Only single-line text without control/private-use characters is supported.")


@dataclass(frozen=True)
class DocsEditPlan:
    document_id: str
    tab_id: str
    revision_id: str
    operation: str
    match: str
    text: str
    paragraph_index: int
    start_index: int
    end_index: int
    before: str
    after: str

    def to_dict(self) -> dict:
        return asdict(self)

    def preview(self) -> dict:
        return {
            "schema_version": 1,
            "mode": "dry_run",
            "plan": self.to_dict(),
            "diff": "".join(difflib.unified_diff(
                self.before.splitlines(keepends=True),
                self.after.splitlines(keepends=True), fromfile="before", tofile="after",
            )),
        }


def plan_edit(document: dict, *, match: str, text: str,
              operation: str = "replace", tab_id: str | None = None) -> DocsEditPlan:
    """Plan one literal replace/insert-after in a plain body paragraph.

    Tables, multi-run paragraphs, suggestions, newlines and ambiguous matches
    are deliberately unsupported. Non-BMP characters use API UTF-16 indexes.
    """
    if operation not in {"replace", "insert_after"}:
        raise DocsEditError("operation must be replace or insert_after")
    _single_line(match)
    _single_line(text, empty=operation == "replace")
    tab = _select(document, tab_id)
    if _has_suggestions(tab):
        raise DocsEditError("Resolve suggestions in the selected tab before editing.")
    candidates = []
    for block in tab["documentTab"].get("body", {}).get("content", []):
        elements = block.get("paragraph", {}).get("elements", [])
        before = "".join(e.get("textRun", {}).get("content", "") for e in elements)
        # Count overlapping matches as ambiguous as well.
        offsets = [i for i in range(len(before)) if before.startswith(match, i)]
        for offset in offsets:
            candidates.append((block, elements, before, offset))
    if len(candidates) != 1:
        raise DocsEditError(f"Expected one body-paragraph match; found {len(candidates)}.")
    block, elements, before, offset = candidates[0]
    if len(elements) != 1 or "textRun" not in elements[0]:
        raise DocsEditError("Editing a multi-run or non-text paragraph is unsupported.")
    if not before.endswith("\n") or "\n" in before[:-1]:
        raise DocsEditError("Unsupported paragraph structure.")
    revision = document.get("revisionId")
    if not revision:
        raise DocsEditError("No revisionId: an editable document snapshot is required.")
    index = elements[0]["startIndex"]
    start = index + len(before[:offset].encode("utf-16-le")) // 2
    end = start + len(match.encode("utf-16-le")) // 2
    if operation == "insert_after":
        start = end
        offset += len(match)
        after = before[:offset] + text + before[offset:]
    else:
        after = before[:offset] + text + before[offset + len(match):]
    if after == before:
        raise DocsEditError("The edit would not change the paragraph.")
    return DocsEditPlan(
        _id(document["documentId"]), _tab_id(tab["tabProperties"]["tabId"]),
        revision, operation, match, text, block["startIndex"], start, end, before, after,
    )


class DocsClient:
    """Use from_oauth_file for production; service injection supports offline tests."""

    def __init__(self, service):
        self._service = service

    @classmethod
    def from_oauth_file(cls, token_path: str | Path, *, expected_email: str) -> DocsClient:
        """Load an existing user token; never launch auth, use ADC, or write tokens.

        Requires documents + userinfo.email authorization. No account defaults
        or service-account fallback; the verified OAuth identity must match.
        """
        if not expected_email or not expected_email.strip():
            raise DocsEditError("expected_email is required")
        from google.oauth2.credentials import Credentials
        from .google_workspace import build_service, refresh_user_credentials

        payload = json.loads(Path(token_path).expanduser().read_text(encoding="utf-8"))
        if payload.get("type") not in (None, "authorized_user") or "private_key" in payload:
            raise DocsEditError("Docs requires an authorized_user OAuth token, not a service account.")
        creds = refresh_user_credentials(Credentials.from_authorized_user_info(payload, SCOPES_EDIT))
        identity = build_service("oauth2", "v2", credentials=creds).userinfo().get().execute()
        if not identity.get("verified_email") or identity.get("email", "").casefold() != expected_email.strip().casefold():
            raise DocsEditError("OAuth account does not match the expected verified email.")
        return cls(build_service("docs", "v1", credentials=creds))

    def get(self, document_id: str) -> dict:
        """Return the raw structured document, including nested tabs and tables."""
        return self._service.documents().get(
            documentId=_id(document_id), includeTabsContent=True,
            suggestionsViewMode="SUGGESTIONS_INLINE",
        ).execute(num_retries=0)

    def plan(self, document_id: str, **kwargs) -> DocsEditPlan:
        return plan_edit(self.get(document_id), **kwargs)

    def verify(self, plan: DocsEditPlan) -> dict:
        """Read only; verify target paragraph text, not layout or collaborators' edits."""
        document = self.get(plan.document_id)
        tab = _select(document, plan.tab_id)
        blocks = tab["documentTab"].get("body", {}).get("content", [])
        block = next((b for b in blocks if b.get("startIndex") == plan.paragraph_index), {})
        actual = "".join(e.get("textRun", {}).get("content", "")
                         for e in block.get("paragraph", {}).get("elements", []))
        return {"verified": actual == plan.after and not _has_suggestions(tab),
                "revision_id": document.get("revisionId"), "scope": "target_paragraph_text"}

    def apply(self, plan: DocsEditPlan, *, apply: bool = False) -> dict:
        """Revalidate a saved plan and require its revision atomically.

        Never retries a write. Unknown write outcomes and failed readback retain
        the target identifiers; callers must inspect, not automatically replay.
        """
        if not apply:
            return plan.preview()
        fresh = self.plan(plan.document_id, match=plan.match, text=plan.text,
                          operation=plan.operation, tab_id=plan.tab_id)
        if fresh != plan:
            raise DocsEditError("Document or plan changed; generate and review a new plan.")
        requests = []
        if plan.start_index != plan.end_index:
            requests.append({"deleteContentRange": {"range": {
                "tabId": plan.tab_id, "startIndex": plan.start_index, "endIndex": plan.end_index,
            }}})
        if plan.text:
            requests.append({"insertText": {"location": {
                "tabId": plan.tab_id, "index": plan.start_index,
            }, "text": plan.text}})
        result = {"schema_version": 1, "document_id": plan.document_id,
                  "url": f"https://docs.google.com/document/d/{plan.document_id}/edit",
                  "tab_id": plan.tab_id, "operation": plan.operation,
                  "start_index": plan.start_index, "end_index": plan.end_index,
                  "ok": False, "verified": False, "write_status": "unknown"}
        try:
            self._service.documents().batchUpdate(documentId=plan.document_id, body={
                "requests": requests, "writeControl": {"requiredRevisionId": plan.revision_id},
            }).execute(num_retries=0)
        except Exception as exc:
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status in {400, 401, 403, 404, 409, 412, 429}:
                return {**result, "write_status": "rejected", "error": "write_rejected",
                        "http_status": status, "next_step": "Check access/revision; review a new plan before retrying."}
            return {**result, "error": "write_outcome_unknown", "next_step": "Inspect the document; do not retry automatically."}
        result["write_status"] = "acknowledged"
        try:
            result.update(self.verify(plan))
        except Exception:
            return {**result, "error": "readback_failed", "next_step": "Run verify with the same plan; do not reapply."}
        result["ok"] = result["verified"]
        if not result["ok"]:
            result["error"] = "verification_mismatch"
        return result
