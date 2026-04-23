"""Shared helpers for Adobe Tags export/apply workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from megaton_lib.audit.providers.tag_config.baseline import (
    hash_normalized_text,
    hash_settings_object,
    load_apply_baseline_manifest,
)
from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.adobe_tags import (
    apply_custom_code,
    apply_data_element_settings,
    get_component_settings,
)


class StaleBaseConflictError(RuntimeError):
    """Raised when local and remote both changed since the last export baseline."""


def slugify_component_name(name: str) -> str:
    """Convert a component name to a stable ASCII slug for file matching."""
    s = name.lower().replace(" ", "-")
    return "".join(c for c in s if c.isascii() and (c.isalnum() or c == "-")).strip("-")


def _extract_code_value(settings: dict[str, Any]) -> str:
    for key in ("source", "customCode", "code", "html", "script"):
        value = settings.get(key)
        if isinstance(value, str):
            return value
    return ""


def _build_effective_data_element_settings(settings_file: Path) -> dict[str, Any] | None:
    try:
        new_settings = json.loads(settings_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(new_settings, dict):
        return None

    code_base = settings_file.name.removesuffix(".settings.json")
    for ext in (".custom-code.js", ".custom-code.html", ".custom-code.css"):
        code_file = settings_file.parent / f"{code_base}{ext}"
        if code_file.exists():
            try:
                new_settings["source"] = code_file.read_text(encoding="utf-8")
            except OSError:
                pass
            break
    return new_settings


def _evaluate_custom_code_staleness(
    config: AdobeTagsConfig,
    component_id: str,
    rel_path: str,
    new_code: str,
    baseline_resources: dict[str, Any],
) -> tuple[str | None, str | None]:
    entry = baseline_resources.get(rel_path)
    if not isinstance(entry, dict):
        return None, None

    base_hash = str(entry.get("source_hash", "")).strip()
    if not base_hash:
        return None, None

    current = get_component_settings(config, component_id)
    remote_code = _extract_code_value(dict(current["settings"]))
    local_hash = hash_normalized_text(new_code)
    remote_hash = hash_normalized_text(remote_code)

    local_changed = local_hash != base_hash
    remote_changed = remote_hash != base_hash

    if remote_changed and not local_changed:
        return "remote_only", "remote custom code changed since export; local file still matches baseline"
    if local_changed and remote_changed and local_hash != remote_hash:
        return "conflict", "local and remote custom code both changed since export"
    return None, None


def _evaluate_settings_staleness(
    config: AdobeTagsConfig,
    component_id: str,
    rel_path: str,
    new_settings: dict[str, Any],
    baseline_resources: dict[str, Any],
) -> tuple[str | None, str | None]:
    entry = baseline_resources.get(rel_path)
    if not isinstance(entry, dict):
        return None, None

    base_hash = str(entry.get("settings_hash", "")).strip()
    if not base_hash:
        return None, None

    current = get_component_settings(config, component_id)
    remote_settings = dict(current["settings"])
    local_hash = hash_settings_object(new_settings)
    remote_hash = hash_settings_object(remote_settings)

    local_changed = local_hash != base_hash
    remote_changed = remote_hash != base_hash

    if remote_changed and not local_changed:
        return "remote_only", "remote data-element settings changed since export; local file still matches baseline"
    if local_changed and remote_changed and local_hash != remote_hash:
        return "conflict", "local and remote data-element settings both changed since export"
    return None, None


def _blocked_result(
    *,
    component_id: str,
    path: str,
    stale_status: str,
    stale_detail: str,
) -> dict[str, Any]:
    return {
        "component_id": component_id,
        "changed": False,
        "blocked": True,
        "stale_status": stale_status,
        "stale_detail": stale_detail,
        "path": path,
    }


def format_stale_base_conflict_message(results: list[dict[str, Any]]) -> str:
    """Render a user-facing summary of stale-base conflicts."""
    conflicts = [r for r in results if r.get("stale_status") == "conflict"]
    remote_only = [r for r in results if r.get("stale_status") == "remote_only"]
    lines = [
        "Refusing to apply Adobe Tags changes because stale-base conflicts were detected.",
        "The local export baseline is older than the current remote state for at least one resource.",
        "Re-export, re-apply your local edit on top of the new baseline, then run apply again.",
        "Use --allow-stale-base only when you intentionally want to overwrite the newer remote state.",
    ]
    if conflicts:
        lines.append("")
        lines.append("Conflicts:")
        lines.extend(
            f"  - {result['path']} -> {result['component_id']}: {result.get('stale_detail', 'conflict')}"
            for result in conflicts
        )
    if remote_only:
        lines.append("")
        lines.append("Remote-only drift (skipped automatically):")
        lines.extend(
            f"  - {result['path']} -> {result['component_id']}: {result.get('stale_detail', 'remote drift')}"
            for result in remote_only[:20]
        )
        if len(remote_only) > 20:
            lines.append(f"  - ... and {len(remote_only) - 20} more")
    return "\n".join(lines)


def raise_for_stale_base_conflicts(
    results: list[dict[str, Any]],
    *,
    allow_stale_base: bool = False,
) -> None:
    """Raise when stale-base conflicts are present and override is not allowed."""
    if allow_stale_base:
        return
    if any(result.get("stale_status") == "conflict" for result in results):
        raise StaleBaseConflictError(format_stale_base_conflict_message(results))


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
    code_files = sorted(base.rglob("*.custom-code.*"))
    baseline_resources = load_apply_baseline_manifest(base).get("resources", {})

    print(f"  [SCAN] custom code files: {len(code_files)}", flush=True)

    for index, code_file in enumerate(code_files, start=1):
        if index == 1 or index % 100 == 0 or index == len(code_files):
            print(
                f"  [PROGRESS] custom code {index}/{len(code_files)} "
                f"{code_file.relative_to(base)}",
                flush=True,
            )
        component_id = find_component_id(code_file)
        if not component_id:
            continue

        new_code = code_file.read_text(encoding="utf-8")
        rel_path = str(code_file.relative_to(base)).replace("\\", "/")
        stale_status, stale_detail = _evaluate_custom_code_staleness(
            config,
            component_id,
            rel_path,
            new_code,
            baseline_resources,
        )
        if stale_status:
            results.append(
                _blocked_result(
                    component_id=component_id,
                    path=rel_path,
                    stale_status=stale_status,
                    stale_detail=stale_detail or "",
                ),
            )
            continue
        result = apply_custom_code(config, component_id, new_code, dry_run=dry_run)
        result["path"] = rel_path
        results.append(result)

    return results


def find_data_element_id(settings_file: Path) -> str:
    """Resolve a data element ID from an exported settings sidecar path."""
    if settings_file.name == "settings.json":
        de_file = settings_file.parent / "data-element.json"
        if de_file.exists():
            try:
                data = json.loads(de_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return ""
            if isinstance(data, dict):
                return str(data.get("id", ""))
        return ""

    base_name = settings_file.name.removesuffix(".settings.json")
    sibling_json = settings_file.parent / f"{base_name}.json"
    if not sibling_json.exists():
        return ""
    try:
        data = json.loads(sibling_json.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    if not isinstance(data, dict):
        return ""
    return str(data.get("id", ""))


def apply_data_element_settings_tree(
    config: AdobeTagsConfig,
    root: str | Path,
    *,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Apply exported data-element settings sidecars under a property root."""
    base = Path(root)
    results: list[dict[str, Any]] = []
    settings_files = sorted(base.rglob("*.settings.json"))
    baseline_resources = load_apply_baseline_manifest(base).get("resources", {})

    print(f"  [SCAN] settings files: {len(settings_files)}", flush=True)

    for index, settings_file in enumerate(settings_files, start=1):
        if index == 1 or index % 200 == 0 or index == len(settings_files):
            print(
                f"  [PROGRESS] settings {index}/{len(settings_files)} "
                f"{settings_file.relative_to(base)}",
                flush=True,
            )
        component_id = find_data_element_id(settings_file)
        if not component_id:
            continue

        new_settings = _build_effective_data_element_settings(settings_file)
        if not isinstance(new_settings, dict):
            continue
        rel_path = str(settings_file.relative_to(base)).replace("\\", "/")
        stale_status, stale_detail = _evaluate_settings_staleness(
            config,
            component_id,
            rel_path,
            new_settings,
            baseline_resources,
        )
        if stale_status:
            results.append(
                _blocked_result(
                    component_id=component_id,
                    path=rel_path,
                    stale_status=stale_status,
                    stale_detail=stale_detail or "",
                ),
            )
            continue

        result = apply_data_element_settings(
            config,
            component_id,
            new_settings,
            dry_run=dry_run,
        )
        result["path"] = rel_path
        results.append(result)

    return results


def apply_exported_changes_tree(
    config: AdobeTagsConfig,
    root: str | Path,
    *,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Apply exported Adobe Tags sidecars under a property root."""
    results = apply_custom_code_tree(config, root, dry_run=dry_run)
    results.extend(apply_data_element_settings_tree(config, root, dry_run=dry_run))
    return results
