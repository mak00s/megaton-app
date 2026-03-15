"""Shared helpers for Adobe Tags export/apply workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.adobe_tags import apply_custom_code


def slugify_component_name(name: str) -> str:
    """Convert a component name to a stable ASCII slug for file matching."""
    s = name.lower().replace(" ", "-")
    return "".join(c for c in s if c.isascii() and (c.isalnum() or c == "-")).strip("-")


def find_component_id(code_file: Path) -> str:
    """Resolve a Reactor component/data-element ID from an exported custom-code file.

    Supported layouts:
    1. ``rules/<slug>/actions/<name>.custom-code.<ext>`` + ``rule-components.json``
    2. ``data-elements/<slug>/<name>.custom-code.<ext>`` + ``data-element.json``
    3. ``rules/<slug>/<comp-slug>.custom-code.<ext>`` + sibling ``<comp-slug>.json``
    4. ``data-elements/<comp-slug>.custom-code.<ext>`` + sibling ``<comp-slug>.json``
    """
    base_name = code_file.name.split(".custom-code.")[0]

    if code_file.parent.name == "actions":
        rule_dir = code_file.parent.parent
        rc_file = rule_dir / "rule-components.json"
        if rc_file.exists():
            try:
                components = json.loads(rc_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return ""
            if not isinstance(components, list):
                return ""
            for comp in components:
                if not isinstance(comp, dict):
                    continue
                attrs = comp.get("attributes", {})
                dd_id = attrs.get("delegate_descriptor_id", "")
                comp_name = attrs.get("name", "")
                if "custom-code" in dd_id and slugify_component_name(comp_name) == base_name.lower():
                    return str(comp.get("id", ""))
        return ""

    de_file = code_file.parent / "data-element.json"
    if de_file.exists():
        try:
            de_data = json.loads(de_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return ""
        if isinstance(de_data, dict):
            sid = de_data.get("id", "")
            if sid:
                return str(sid)

    sibling_json = code_file.parent / f"{base_name}.json"
    if sibling_json.exists():
        try:
            sibling_data = json.loads(sibling_json.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return ""
        if isinstance(sibling_data, dict):
            return str(sibling_data.get("id", ""))

    return ""


def apply_custom_code_tree(
    config: AdobeTagsConfig,
    root: str | Path,
    *,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Apply all exported custom-code files under a property root."""
    base = Path(root)
    results: list[dict[str, Any]] = []

    for code_file in sorted(base.rglob("*.custom-code.*")):
        component_id = find_component_id(code_file)
        if not component_id:
            continue

        new_code = code_file.read_text(encoding="utf-8")
        result = apply_custom_code(config, component_id, new_code, dry_run=dry_run)
        result["path"] = str(code_file.relative_to(base))
        results.append(result)

    return results
