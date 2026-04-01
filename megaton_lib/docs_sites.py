"""Helpers for generating docs/sites.md tables from config.py structures."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def format_md_value(value: Any) -> str:
    """Format one Python value for a Markdown table cell."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (list, tuple, set)):
        items = [format_md_value(item) for item in value]
        return "<br>".join(item for item in items if item and item != "—") or "—"
    if isinstance(value, dict):
        if not value:
            return "—"
        parts = [f"`{key}`={format_md_value(raw)}" for key, raw in value.items()]
        return "<br>".join(parts)

    text = str(value).strip()
    if not text:
        return "—"
    return text.replace("\n", "<br>")


def render_markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    """Render a Markdown table."""
    head = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    body = [
        "| " + " | ".join(format_md_value(value) for value in row) + " |"
        for row in rows
    ]
    return "\n".join([head, sep, *body])


def replace_generated_section(text: str, name: str, content: str) -> str:
    """Replace one named generated block in Markdown text."""
    begin = f"<!-- BEGIN GENERATED: {name} -->"
    end = f"<!-- END GENERATED: {name} -->"
    if begin not in text or end not in text:
        raise ValueError(f"Missing generated markers for section '{name}'")
    start = text.index(begin) + len(begin)
    finish = text.index(end)
    normalized = "\n\n" + content.strip() + "\n\n"
    return text[:start] + normalized + text[finish:]


def update_generated_markdown(path: str | Path, sections: dict[str, str]) -> None:
    """Replace multiple generated sections in one Markdown file."""
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    updated = text
    for name, content in sections.items():
        updated = replace_generated_section(updated, name, content)
    target.write_text(updated, encoding="utf-8")
