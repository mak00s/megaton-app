"""Create Gmail drafts from report execution summaries."""

from __future__ import annotations

import argparse
import json
import os
import string
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .gmail_client import (
    SCOPES_DRAFT,
    GmailClient,
    credentials_from_authorized_user_file,
    credentials_from_authorized_user_info,
    parse_email_list,
)


@dataclass(frozen=True)
class ReportDraftContent:
    subject: str
    body: str


@dataclass(frozen=True)
class ReportDraftResult:
    draft: dict[str, Any]
    subject: str
    to: list[str]


def env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return default


def prefixed_env(prefix: str, key: str, *, default: str = "") -> str:
    names: list[str] = []
    normalized_prefix = prefix.strip().upper()
    if normalized_prefix:
        names.append(f"{normalized_prefix}_GMAIL_DRAFT_{key}")
    names.append(f"GMAIL_DRAFT_{key}")
    return env_value(*names, default=default)


def load_report_summary(path: str | Path) -> dict[str, Any]:
    if not str(path or "").strip():
        raise ValueError("Report summary path is required. Pass --summary-path or set MEGATON_RUN_SUMMARY_PATH.")
    summary_path = Path(path).expanduser()
    if not summary_path.is_file():
        raise FileNotFoundError(f"Report summary JSON not found: {summary_path}")
    return json.loads(summary_path.read_text(encoding="utf-8"))


def assert_report_success(summary: dict[str, Any]) -> None:
    status = str(summary.get("status", "")).strip().lower()
    validation_status = str((summary.get("validation") or {}).get("status", "")).strip().lower()
    if status != "success" or validation_status != "passed":
        raise RuntimeError(
            "Refusing to create Gmail draft because report did not finish successfully: "
            f"status={status or '(empty)'} validation={validation_status or '(empty)'}"
        )


def _parse_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return None


def period_label(summary: dict[str, Any]) -> str:
    window = summary.get("window") or {}
    start = _parse_date(str(window.get("report_start", "")))
    end = _parse_date(str(window.get("report_end", "")))
    if start and end and start.year == end.year and start.month == end.month:
        return f"{start.year}年{start.month}月"
    if start and end:
        return f"{start:%Y-%m-%d} - {end:%Y-%m-%d}"
    return "対象期間"


def first_sheet_url(summary: dict[str, Any]) -> str:
    for entry in summary.get("entries", []) or []:
        url = str(entry.get("target_url", "")).strip()
        if url:
            return url
    return ""


def box_upload_items(summary: dict[str, Any]) -> list[dict[str, str]]:
    uploads = ((summary.get("artifacts") or {}).get("box_uploads") or [])
    items: list[dict[str, str]] = []
    for upload in uploads:
        if not isinstance(upload, dict):
            continue
        shared_url = _first_nonempty(
            upload,
            "shared_url",
            "shared_link",
            "box_shared_url",
            "url",
            "web_url",
        )
        file_name = str(upload.get("uploaded_file_name", "")).strip()
        subfolder = str(upload.get("target_subfolder_name", "")).strip()
        label = str(upload.get("label", "")).strip()
        items.append(
            {
                "file_name": file_name,
                "subfolder": subfolder,
                "label": label,
                "shared_url": shared_url,
                "display_name": _display_box_name(file_name=file_name, subfolder=subfolder, label=label),
            }
        )
    return items


def _first_nonempty(values: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(values.get(key, "")).strip()
        if value:
            return value
    return ""


def _display_box_name(*, file_name: str, subfolder: str, label: str) -> str:
    if label and file_name:
        return f"{label}: {file_name}"
    if subfolder and file_name:
        return f"{subfolder}/{file_name}"
    return file_name or label or "Box file"


def box_upload_lines(summary: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for item in box_upload_items(summary):
        if item["shared_url"]:
            lines.append(f"- Box: {item['display_name']}\n  {item['shared_url']}")
        else:
            lines.append(f"- Box: {item['display_name']}")
    return lines


def build_report_draft_content(
    summary: dict[str, Any],
    *,
    report_label: str,
    subject_template: str = "",
    body_template: str = "",
) -> ReportDraftContent:
    context = _template_context(summary, report_label=report_label)
    subject = (
        _format_template(subject_template, context, label="subject")
        if subject_template
        else _format_template("{report_label}（{period_label}）", context, label="subject")
    )
    body = _format_template(body_template, context, label="body") if body_template else _default_body(context)
    return ReportDraftContent(subject=subject, body=body)


def _template_context(summary: dict[str, Any], *, report_label: str) -> dict[str, str]:
    box_items = box_upload_items(summary)
    box_lines = _box_upload_lines_from_items(box_items)
    box_urls = [item["shared_url"] for item in box_items if item["shared_url"]]
    box_files = [item["display_name"] for item in box_items]
    return {
        "report_label": report_label,
        "period_label": period_label(summary),
        "sheet_url": first_sheet_url(summary),
        "run_url": str(summary.get("run_url", "")).strip(),
        "box_links": "\n".join(box_lines),
        "box_urls": "\n".join(box_urls),
        "box_files": "\n".join(box_files),
    }


def _box_upload_lines_from_items(items: list[dict[str, str]]) -> list[str]:
    lines: list[str] = []
    for item in items:
        if item["shared_url"]:
            lines.append(f"- Box: {item['display_name']}\n  {item['shared_url']}")
        else:
            lines.append(f"- Box: {item['display_name']}")
    return lines


def _format_template(template: str, context: dict[str, str], *, label: str) -> str:
    names = {
        field_name
        for _, field_name, _, _ in string.Formatter().parse(template)
        if field_name
    }
    missing = sorted(names - set(context))
    if missing:
        available = ", ".join(sorted(context))
        raise ValueError(
            f"Unknown Gmail draft {label} template placeholder(s): {', '.join(missing)}. "
            f"Available placeholders: {available}"
        )
    return template.format(**context)


def _default_body(context: dict[str, str]) -> str:
    lines = [
        "お疲れさまです。",
        "",
        f"{context['report_label']}（{context['period_label']}）を作成しました。",
        "以下よりご確認ください。",
        "",
    ]
    if context["box_links"]:
        lines.append(context["box_links"])
    elif context["sheet_url"]:
        lines.append(f"Google Sheets: {context['sheet_url']}")
    else:
        lines.append("レポートの出力先をご確認ください。")
    lines.extend(["", "よろしくお願いいたします。"])
    return "\n".join(lines)


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_gmail_draft_credentials_from_env(*, env_prefix: str = ""):
    token_json = prefixed_env(env_prefix, "TOKEN_JSON")
    if token_json:
        return credentials_from_authorized_user_info(token_json, SCOPES_DRAFT)

    token_path = prefixed_env(env_prefix, "TOKEN_PATH")
    if token_path:
        return credentials_from_authorized_user_file(token_path, SCOPES_DRAFT)

    raise RuntimeError("Set GMAIL_DRAFT_TOKEN_JSON or GMAIL_DRAFT_TOKEN_PATH for Gmail draft creation.")


def create_report_gmail_draft_from_env(
    *,
    summary_path: str | Path,
    report_label: str,
    env_prefix: str = "",
    subject_template: str = "",
    body_template: str = "",
    client_factory: Callable[[Any], GmailClient] = GmailClient,
) -> ReportDraftResult:
    summary = load_report_summary(summary_path)
    assert_report_success(summary)

    sender = prefixed_env(env_prefix, "SENDER")
    to = parse_email_list(prefixed_env(env_prefix, "TO"))
    cc = parse_email_list(prefixed_env(env_prefix, "CC"))
    bcc = parse_email_list(prefixed_env(env_prefix, "BCC"))
    if not sender:
        raise RuntimeError("Set GMAIL_DRAFT_SENDER for Gmail draft creation.")
    if not to:
        raise RuntimeError("Set GMAIL_DRAFT_TO for Gmail draft creation.")

    content = build_report_draft_content(
        summary,
        report_label=report_label,
        subject_template=subject_template or prefixed_env(env_prefix, "SUBJECT"),
        body_template=body_template or prefixed_env(env_prefix, "BODY"),
    )
    if _truthy(prefixed_env(env_prefix, "REQUIRE_BOX_URL")):
        if not any(item["shared_url"] for item in box_upload_items(summary)):
            raise RuntimeError("Gmail draft requires a Box shared URL, but none was found in report summary.")
    client = client_factory(load_gmail_draft_credentials_from_env(env_prefix=env_prefix))
    draft = client.create_draft(
        sender=sender,
        to=to,
        cc=cc,
        bcc=bcc,
        subject=content.subject,
        body_text=content.body,
    )
    return ReportDraftResult(draft=draft, subject=content.subject, to=to)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a Gmail draft from a successful report summary.")
    parser.add_argument(
        "--summary-path",
        default=env_value("MEGATON_RUN_SUMMARY_PATH"),
        help="Path to the report execution summary JSON.",
    )
    parser.add_argument(
        "--report-label",
        default="",
        help="Human-readable report label used in the default subject/body.",
    )
    parser.add_argument(
        "--env-prefix",
        default="",
        help="Optional prefix for report-specific env vars, e.g. DEI uses DEI_GMAIL_DRAFT_TO.",
    )
    args = parser.parse_args()

    env_prefix = args.env_prefix.strip()
    report_label = (
        args.report_label.strip()
        or prefixed_env(env_prefix, "REPORT_LABEL")
        or "月次レポート"
    )
    result = create_report_gmail_draft_from_env(
        summary_path=args.summary_path,
        report_label=report_label,
        env_prefix=env_prefix,
    )
    print(f"Created Gmail draft: {result.draft.get('id', '(no id)')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
