"""Baseline manifest helpers for Adobe Tags export/apply workflows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


APPLY_BASELINE_FILENAME = ".apply-baseline.json"
_CODE_KEYS = ("source", "customCode", "code", "html", "script")


def stable_json_dumps(value: Any) -> str:
    """Return a deterministic JSON encoding for hashing/comparison."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def hash_normalized_text(value: str) -> str:
    """Return a stable hash for source-like text values."""
    normalized = value.strip()
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def hash_settings_object(value: MappingLike | dict[str, Any]) -> str:
    """Return a stable hash for a settings object."""
    return "sha256:" + hashlib.sha256(stable_json_dumps(value).encode("utf-8")).hexdigest()


class MappingLike(dict[str, Any]):
    """Typed alias shim for static checkers without runtime cost."""


def _slugify_component_name(name: str) -> str:
    s = name.lower().replace(" ", "-")
    return "".join(c for c in s if c.isascii() and (c.isalnum() or c == "-")).strip("-")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _extract_component_id_for_code_file(code_file: Path) -> str:
    base_name = code_file.name.split(".custom-code.")[0]

    if code_file.parent.name == "actions":
        rule_dir = code_file.parent.parent
        rc_file = rule_dir / "rule-components.json"
        components = _read_json(rc_file)
        if isinstance(components, list):
            for comp in components:
                if not isinstance(comp, dict):
                    continue
                attrs = comp.get("attributes", {})
                dd_id = attrs.get("delegate_descriptor_id", "")
                comp_name = attrs.get("name", "")
                if "custom-code" in dd_id and _slugify_component_name(comp_name) == base_name.lower():
                    return str(comp.get("id", ""))
        return ""

    de_file = code_file.parent / "data-element.json"
    de_data = _read_json(de_file)
    if isinstance(de_data, dict):
        component_id = str(de_data.get("id", ""))
        if component_id:
            return component_id

    sibling_json = code_file.parent / f"{base_name}.json"
    sibling_data = _read_json(sibling_json)
    if isinstance(sibling_data, dict):
        return str(sibling_data.get("id", ""))

    return ""


def _extract_data_element_id(settings_file: Path) -> str:
    if settings_file.name == "settings.json":
        de_file = settings_file.parent / "data-element.json"
        data = _read_json(de_file)
        if isinstance(data, dict):
            return str(data.get("id", ""))
        return ""

    base_name = settings_file.name.removesuffix(".settings.json")
    sibling_json = settings_file.parent / f"{base_name}.json"
    data = _read_json(sibling_json)
    if isinstance(data, dict):
        return str(data.get("id", ""))
    return ""


def _load_component_payload(path: Path) -> dict[str, Any]:
    if path.suffix == ".json" and path.exists():
        data = _read_json(path)
        if isinstance(data, dict):
            return data
    return {}


def _component_payload_for_code_file(code_file: Path) -> dict[str, Any]:
    base_name = code_file.name.split(".custom-code.")[0]
    if code_file.parent.name == "actions":
        return {}
    de_file = code_file.parent / "data-element.json"
    if de_file.exists():
        return _load_component_payload(de_file)
    sibling_json = code_file.parent / f"{base_name}.json"
    return _load_component_payload(sibling_json)


def _component_payload_for_settings_file(settings_file: Path) -> dict[str, Any]:
    if settings_file.name == "settings.json":
        return _load_component_payload(settings_file.parent / "data-element.json")
    base_name = settings_file.name.removesuffix(".settings.json")
    return _load_component_payload(settings_file.parent / f"{base_name}.json")


def _extract_latest_revision_number(payload: dict[str, Any]) -> int | None:
    meta = payload.get("meta", {})
    attrs = payload.get("attributes", {})
    latest = meta.get("latest_revision_number")
    if latest is None:
        latest = attrs.get("revision_number")
    if isinstance(latest, int):
        return latest
    return None


def _read_effective_settings(settings_file: Path) -> dict[str, Any] | None:
    data = _read_json(settings_file)
    if not isinstance(data, dict):
        return None

    code_base = settings_file.name.removesuffix(".settings.json")
    for ext in (".custom-code.js", ".custom-code.html", ".custom-code.css"):
        code_file = settings_file.parent / f"{code_base}{ext}"
        if code_file.exists():
            try:
                data["source"] = code_file.read_text(encoding="utf-8")
            except OSError:
                pass
            break
    return data


def build_apply_baseline_manifest(root: str | Path) -> dict[str, Any]:
    """Build a local baseline manifest from an exported Adobe Tags tree."""
    base = Path(root)
    property_payload = _load_component_payload(base / "property.json")
    property_id = str(property_payload.get("id", ""))
    resources: dict[str, Any] = {}

    for code_file in sorted(base.rglob("*.custom-code.*")):
        rel_path = str(code_file.relative_to(base)).replace("\\", "/")
        component_id = _extract_component_id_for_code_file(code_file)
        if not component_id:
            continue
        try:
            source = code_file.read_text(encoding="utf-8")
        except OSError:
            continue
        payload = _component_payload_for_code_file(code_file)
        attrs = payload.get("attributes", {})
        resources[rel_path] = {
            "kind": "custom_code",
            "component_id": component_id,
            "resource_type": "data_elements" if component_id.startswith("DE") else "rule_components",
            "updated_at": attrs.get("updated_at"),
            "latest_revision_number": _extract_latest_revision_number(payload),
            "source_hash": hash_normalized_text(source),
        }

    for settings_file in sorted(base.rglob("*.settings.json")):
        rel_path = str(settings_file.relative_to(base)).replace("\\", "/")
        component_id = _extract_data_element_id(settings_file)
        if not component_id:
            continue
        settings = _read_effective_settings(settings_file)
        if not isinstance(settings, dict):
            continue
        payload = _component_payload_for_settings_file(settings_file)
        attrs = payload.get("attributes", {})
        resources[rel_path] = {
            "kind": "settings",
            "component_id": component_id,
            "resource_type": "data_elements",
            "updated_at": attrs.get("updated_at"),
            "latest_revision_number": _extract_latest_revision_number(payload),
            "settings_hash": hash_settings_object(settings),
        }

    return {
        "schema_version": 1,
        "property_id": property_id,
        "resources": resources,
    }


def render_apply_baseline_manifest(root: str | Path) -> str:
    """Render the apply baseline manifest as formatted JSON text."""
    manifest = build_apply_baseline_manifest(root)
    return json.dumps(manifest, ensure_ascii=False, indent=2)


def write_apply_baseline_manifest(root: str | Path) -> Path:
    """Write the apply baseline manifest under one property export root."""
    base = Path(root)
    path = base / APPLY_BASELINE_FILENAME
    path.write_text(render_apply_baseline_manifest(base), encoding="utf-8")
    return path


def load_apply_baseline_manifest(root: str | Path) -> dict[str, Any]:
    """Load the apply baseline manifest if it exists, else return an empty manifest."""
    path = Path(root) / APPLY_BASELINE_FILENAME
    data = _read_json(path)
    if not isinstance(data, dict):
        return {"schema_version": 1, "resources": {}}
    resources = data.get("resources")
    if not isinstance(resources, dict):
        data["resources"] = {}
    return data
