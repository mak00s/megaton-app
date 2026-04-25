"""Library-scope Adobe Tags workspace workflows.

This module provides a safer command model for analysis repos:

- ``checkout``: destructive local overwrite from remote library scope
- ``pull``: non-destructive sync from remote library scope
- ``status``: compare local vs remote library scope
- ``add``: explicitly add local-exported new resources to a library
- ``push``: apply local edits for existing in-library resources
- ``build``: build-only
- ``full-export``: full property mirror to a non-canonical path
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import difflib
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any

from ...config import AdobeTagsConfig
from .adobe_tags import (
    _paginated_list,
    _reactor_get,
    _resource_basename,
    build_library,
    export_property,
    extract_custom_code,
    list_data_elements,
    list_rule_components,
    list_rules,
    parse_settings_object,
    refresh_library_resources,
    revise_library_data_elements,
    revise_library_rules,
)
from .baseline import (
    APPLY_BASELINE_FILENAME,
    hash_normalized_text,
    hash_settings_object,
    load_apply_baseline_manifest,
    stable_json_dumps,
)
from .build_workflow import (
    BUILD_SUCCESS_STATUSES,
    verify_build_markers,
    wait_for_build_completion,
)
from .sync import (
    apply_exported_changes_tree,
    find_component_id,
    find_data_element_id,
    raise_for_stale_base_conflicts,
)


LIBRARY_SCOPE_FILENAME = ".library-scope.json"
TAG_CONFLICTS_FILENAME = ".tag-conflicts.json"
MANAGED_RESOURCE_DIRS = ("rules", "data-elements")
SNAPSHOT_PROGRESS_INTERVAL = 20
SNAPSHOT_HEARTBEAT_SECONDS = 5.0
DEFAULT_SNAPSHOT_WORKERS = 10
WORKSPACE_RESULT_SCHEMA_VERSION = 1
WORKSPACE_EXIT_OK = 0
WORKSPACE_EXIT_ERROR = 1
WORKSPACE_EXIT_CONFLICTS = 2
WORKSPACE_EXIT_STALE_REMOTE = 3
WORKSPACE_EXIT_OUTSIDE_SCOPE = 4


@dataclass(frozen=True)
class ScopeItem:
    """One origin resource in the current library scope."""

    resource_type: str
    origin_id: str
    revision_id: str
    name: str


def _log(message: str = "") -> None:
    print(message, file=sys.stderr, flush=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def workspace_result_exit_code(result: dict[str, Any]) -> int:
    """Return the stable wrapper exit code for a workspace result dict."""
    summary = result.get("summary", {})
    if not isinstance(summary, dict):
        return WORKSPACE_EXIT_ERROR
    if summary.get("conflicts", 0) or summary.get("name_conflicts", 0):
        return WORKSPACE_EXIT_CONFLICTS
    if summary.get("stale_remote", 0) or summary.get("kept_local_removed", 0):
        return WORKSPACE_EXIT_STALE_REMOTE
    if (
        summary.get("outside_library_scope", 0)
        or summary.get("outside_library_scope_resources", 0)
        or summary.get("outside_library_scope_kept_local", 0)
    ):
        return WORKSPACE_EXIT_OUTSIDE_SCOPE
    return WORKSPACE_EXIT_OK


def _workspace_result_severity(exit_code: int) -> str:
    if exit_code == WORKSPACE_EXIT_OK:
        return "ok"
    if exit_code == WORKSPACE_EXIT_CONFLICTS:
        return "conflict"
    if exit_code == WORKSPACE_EXIT_STALE_REMOTE:
        return "stale_remote"
    if exit_code == WORKSPACE_EXIT_OUTSIDE_SCOPE:
        return "outside_scope"
    return "error"


def _finalize_workspace_result(
    result: dict[str, Any],
    *,
    command: str,
    mode: str = "",
) -> dict[str, Any]:
    exit_code = workspace_result_exit_code(result)
    result.setdefault("schema_version", WORKSPACE_RESULT_SCHEMA_VERSION)
    result.setdefault("command", command)
    if mode:
        result.setdefault("mode", mode)
    result.setdefault("summary", {})
    result.setdefault("details", {})
    result["exit_code"] = exit_code
    result["ok"] = exit_code == WORKSPACE_EXIT_OK
    result["severity"] = _workspace_result_severity(exit_code)
    return result


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _content_hash(value: str) -> str:
    return hash_normalized_text(value)


def _write_text(path: Path, content: str) -> str:
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            return "unchanged"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return "updated"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return "added"


def _delete_file(path: Path) -> None:
    if path.exists():
        path.unlink()
    parent = path.parent
    while parent.exists() and parent.name in MANAGED_RESOURCE_DIRS:
        if any(parent.iterdir()):
            break
        parent.rmdir()
        parent = parent.parent


def _iter_managed_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for resource_dir in MANAGED_RESOURCE_DIRS:
        base = root / resource_dir
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if path.is_file():
                files.append(path)
    return files


def _render_library_scope_manifest(
    *,
    property_id: str,
    library_id: str,
    items: list[ScopeItem],
) -> str:
    payload = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "property_id": property_id,
        "library_id": library_id,
        "rules": [
            {"id": item.origin_id, "revision_id": item.revision_id, "name": item.name}
            for item in items
            if item.resource_type == "rules"
        ],
        "data_elements": [
            {"id": item.origin_id, "revision_id": item.revision_id, "name": item.name}
            for item in items
            if item.resource_type == "data_elements"
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def _build_baseline_manifest(
    *,
    property_id: str,
    file_entries: dict[str, dict[str, Any]],
) -> str:
    payload = {
        "schema_version": 1,
        "property_id": property_id,
        "resources": file_entries,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def _library_scope_items(config: AdobeTagsConfig, library_id: str) -> list[ScopeItem]:
    items: list[ScopeItem] = []
    for resource_type in ("rules", "data_elements"):
        raw_items = _paginated_list(config, f"/libraries/{library_id}/{resource_type}")
        for item in raw_items:
            attrs = item.get("attributes", {})
            rel_origin = item.get("relationships", {}).get("origin", {}).get("data", {})
            origin_id = str(rel_origin.get("id", "") or item.get("id", ""))
            revision_id = str(item.get("id", ""))
            if not origin_id:
                continue
            items.append(
                ScopeItem(
                    resource_type=resource_type,
                    origin_id=origin_id,
                    revision_id=revision_id,
                    name=str(attrs.get("name", "")),
                ),
            )
    return items


def _snapshot_worker_count(max_workers: int | None) -> int:
    if max_workers is None:
        raw = os.getenv("TAGS_SNAPSHOT_WORKERS", "").strip()
        if raw:
            try:
                max_workers = int(raw)
            except ValueError:
                max_workers = DEFAULT_SNAPSHOT_WORKERS
        else:
            max_workers = DEFAULT_SNAPSHOT_WORKERS
    return max(1, int(max_workers))


def _rule_snapshot_entry(config: AdobeTagsConfig, rule_id: str) -> tuple[dict[str, str], dict[str, dict[str, Any]], dict[str, Any]]:
    rule = _reactor_get(config, f"/rules/{rule_id}").get("data", {})
    rule_attrs = rule.get("attributes", {})
    rule_name = str(rule_attrs.get("name", rule_id))
    rule_dir = Path("rules") / _resource_basename(rule_id, rule_name)

    files: dict[str, str] = {
        str(rule_dir / "rule.json"): json.dumps(rule, ensure_ascii=False, indent=2) + "\n",
    }
    baseline_entries: dict[str, dict[str, Any]] = {}
    baseline_entries[str(rule_dir / "rule.json")] = {
        "kind": "metadata",
        "component_id": rule_id,
        "resource_type": "rules",
        "baseline_text": files[str(rule_dir / "rule.json")],
        "content_hash": _content_hash(files[str(rule_dir / "rule.json")]),
    }

    components = list_rule_components(config, rule_id)
    for comp in components:
        comp_id = str(comp.get("id", ""))
        comp_attrs = comp.get("attributes", {})
        comp_name = str(comp_attrs.get("name", comp_id))
        comp_path_base = rule_dir / _resource_basename(comp_id, comp_name)
        comp_json_path = str(comp_path_base.with_suffix(".json"))
        files[comp_json_path] = json.dumps(comp, ensure_ascii=False, indent=2) + "\n"
        baseline_entries[comp_json_path] = {
            "kind": "metadata",
            "component_id": comp_id,
            "resource_type": "rule_components",
            "baseline_text": files[comp_json_path],
            "content_hash": _content_hash(files[comp_json_path]),
        }

        code_info = extract_custom_code(comp)
        if code_info:
            source, lang = code_info
            ext = ".html" if lang == "html" else ".js"
            code_path = str(Path(f"{comp_path_base}.custom-code{ext}"))
            files[code_path] = source
            baseline_entries[code_path] = {
                "kind": "custom_code",
                "component_id": comp_id,
                "resource_type": "rule_components",
                "updated_at": comp_attrs.get("updated_at"),
                "latest_revision_number": comp.get("meta", {}).get(
                    "latest_revision_number",
                    comp_attrs.get("revision_number"),
                ),
                "baseline_text": source,
                "content_hash": _content_hash(source),
                "source_hash": hash_normalized_text(source),
            }

    return files, baseline_entries, {"id": rule_id, "name": rule_name, "component_count": len(components)}


def _data_element_snapshot_entry(
    config: AdobeTagsConfig,
    data_element_id: str,
) -> tuple[dict[str, str], dict[str, dict[str, Any]], dict[str, Any]]:
    elem = _reactor_get(config, f"/data_elements/{data_element_id}").get("data", {})
    attrs = elem.get("attributes", {})
    elem_name = str(attrs.get("name", data_element_id))
    base_name = _resource_basename(data_element_id, elem_name)

    files: dict[str, str] = {
        str(Path("data-elements") / f"{base_name}.json"): json.dumps(elem, ensure_ascii=False, indent=2) + "\n",
    }
    baseline_entries: dict[str, dict[str, Any]] = {}
    baseline_entries[str(Path("data-elements") / f"{base_name}.json")] = {
        "kind": "metadata",
        "component_id": data_element_id,
        "resource_type": "data_elements",
        "baseline_text": files[str(Path("data-elements") / f"{base_name}.json")],
        "content_hash": _content_hash(files[str(Path("data-elements") / f"{base_name}.json")]),
    }

    code_info = extract_custom_code(elem)
    if code_info:
        source, lang = code_info
        ext = ".html" if lang == "html" else ".js"
        code_path = str(Path("data-elements") / f"{base_name}.custom-code{ext}")
        files[code_path] = source
        baseline_entries[code_path] = {
            "kind": "custom_code",
            "component_id": data_element_id,
            "resource_type": "data_elements",
            "updated_at": attrs.get("updated_at"),
            "latest_revision_number": elem.get("meta", {}).get(
                "latest_revision_number",
                attrs.get("revision_number"),
            ),
            "baseline_text": source,
            "content_hash": _content_hash(source),
            "source_hash": hash_normalized_text(source),
        }

    settings = parse_settings_object(attrs.get("settings"))
    if settings:
        settings_path = str(Path("data-elements") / f"{base_name}.settings.json")
        files[settings_path] = json.dumps(settings, ensure_ascii=False, indent=2) + "\n"
        effective_settings = dict(settings)
        if code_info:
            effective_settings["source"] = code_info[0]
        baseline_entries[settings_path] = {
            "kind": "settings",
            "component_id": data_element_id,
            "resource_type": "data_elements",
            "updated_at": attrs.get("updated_at"),
            "latest_revision_number": elem.get("meta", {}).get(
                "latest_revision_number",
                attrs.get("revision_number"),
            ),
            "baseline_text": files[settings_path],
            "content_hash": _content_hash(files[settings_path]),
            "settings_hash": hash_settings_object(effective_settings),
        }

    return files, baseline_entries, {"id": data_element_id, "name": elem_name}


def build_library_scope_snapshot(
    config: AdobeTagsConfig,
    library_id: str,
    *,
    max_workers: int | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    items = _library_scope_items(config, library_id)
    workers = min(_snapshot_worker_count(max_workers), max(1, len(items)))
    _log(
        f"[library-scope] fetched membership for {library_id}: "
        f"{sum(1 for item in items if item.resource_type == 'rules')} rules, "
        f"{sum(1 for item in items if item.resource_type == 'data_elements')} data elements "
        f"(snapshot_workers={workers})",
    )

    prop_body = _reactor_get(config, f"/properties/{config.property_id}")
    prop_data = prop_body.get("data", {})

    files: dict[str, str] = {
        "property.json": json.dumps(prop_data, ensure_ascii=False, indent=2) + "\n",
    }
    baseline_entries: dict[str, dict[str, Any]] = {}
    rule_index: list[dict[str, Any]] = []
    data_element_index: list[dict[str, Any]] = []

    def build_item_entry(
        item: ScopeItem,
    ) -> tuple[dict[str, str], dict[str, dict[str, Any]], dict[str, Any]]:
        if item.resource_type == "rules":
            return _rule_snapshot_entry(config, item.origin_id)
        return _data_element_snapshot_entry(config, item.origin_id)

    entries: list[tuple[int, ScopeItem, dict[str, str], dict[str, dict[str, Any]], dict[str, Any]]] = []
    last_progress_at = start

    def log_progress(completed: int, item: ScopeItem) -> None:
        nonlocal last_progress_at
        now = time.monotonic()
        if (
            completed == 1
            or completed % SNAPSHOT_PROGRESS_INTERVAL == 0
            or completed == len(items)
            or now - last_progress_at >= SNAPSHOT_HEARTBEAT_SECONDS
        ):
            _log(
                f"[library-scope] exported {completed}/{len(items)} "
                f"{item.resource_type}:{item.origin_id} "
                f"elapsed={now - start:.1f}s",
            )
            last_progress_at = now

    if not items:
        pass
    elif workers == 1:
        for index, item in enumerate(items, start=1):
            entry_files, entry_baseline, index_item = build_item_entry(item)
            entries.append((index, item, entry_files, entry_baseline, index_item))
            log_progress(index, item)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(build_item_entry, item): (index, item)
                for index, item in enumerate(items, start=1)
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                index, item = futures[future]
                entry_files, entry_baseline, index_item = future.result()
                entries.append((index, item, entry_files, entry_baseline, index_item))
                log_progress(completed, item)

    for _, item, entry_files, entry_baseline, index_item in sorted(entries, key=lambda row: row[0]):
        if item.resource_type == "rules":
            rule_index.append(index_item)
        else:
            data_element_index.append(index_item)
        files.update(entry_files)
        baseline_entries.update(entry_baseline)

    files[str(Path("rules") / "index.json")] = json.dumps(rule_index, ensure_ascii=False, indent=2) + "\n"
    files[str(Path("data-elements") / "index.json")] = json.dumps(data_element_index, ensure_ascii=False, indent=2) + "\n"
    baseline_entries[str(Path("rules") / "index.json")] = {
        "kind": "metadata",
        "component_id": "rules-index",
        "resource_type": "rules",
        "baseline_text": files[str(Path("rules") / "index.json")],
        "content_hash": _content_hash(files[str(Path("rules") / "index.json")]),
    }
    baseline_entries[str(Path("data-elements") / "index.json")] = {
        "kind": "metadata",
        "component_id": "data-elements-index",
        "resource_type": "data_elements",
        "baseline_text": files[str(Path("data-elements") / "index.json")],
        "content_hash": _content_hash(files[str(Path("data-elements") / "index.json")]),
    }
    files[APPLY_BASELINE_FILENAME] = _build_baseline_manifest(
        property_id=config.property_id,
        file_entries=baseline_entries,
    )
    files[LIBRARY_SCOPE_FILENAME] = _render_library_scope_manifest(
        property_id=config.property_id,
        library_id=library_id,
        items=items,
    )

    return {
        "property_id": config.property_id,
        "library_id": library_id,
        "generated_at": _now_iso(),
        "items": items,
        "files": files,
        "baseline_entries": baseline_entries,
        "elapsed": time.monotonic() - start,
    }


def _build_library_scope_snapshot_for_workflow(
    config: AdobeTagsConfig,
    library_id: str,
    snapshot_workers: int | None,
) -> dict[str, Any]:
    if snapshot_workers is None:
        return build_library_scope_snapshot(config, library_id)
    return build_library_scope_snapshot(config, library_id, max_workers=snapshot_workers)


def _baseline_hash_for(path_str: str, baseline: dict[str, Any]) -> str | None:
    entry = baseline.get("resources", {}).get(path_str)
    if not isinstance(entry, dict):
        return None
    value = entry.get("content_hash")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _baseline_entry_for(path_str: str, baseline: dict[str, Any]) -> dict[str, Any]:
    entry = baseline.get("resources", {}).get(path_str)
    return dict(entry) if isinstance(entry, dict) else {}


def _classify_pull_action(
    *,
    rel_path: str,
    remote_text: str | None,
    local_text: str | None,
    baseline_hash: str | None,
) -> str:
    if remote_text is None:
        if local_text is None:
            return "unchanged"
        if baseline_hash is not None and _content_hash(local_text) == baseline_hash:
            return "delete"
        return "kept-local-removed"

    if local_text is None:
        return "add"

    if local_text == remote_text:
        return "unchanged"

    if baseline_hash is None:
        return "conflict"

    local_hash = _content_hash(local_text)
    remote_hash = _content_hash(remote_text)
    local_changed = local_hash != baseline_hash
    remote_changed = remote_hash != baseline_hash
    if not local_changed and remote_changed:
        return "update"
    if local_changed and not remote_changed:
        return "keep-local"
    if local_changed and remote_changed:
        return "conflict"
    return "unchanged"


def _managed_remote_paths(snapshot: dict[str, Any]) -> set[str]:
    return {
        path
        for path in snapshot["files"].keys()
        if path.startswith("rules/") or path.startswith("data-elements/")
    }


def _build_conflict_record(
    *,
    rel_path: str,
    local_text: str | None,
    remote_text: str | None,
    baseline_hash: str | None,
    baseline_entry: dict[str, Any],
) -> dict[str, Any]:
    return {
        "path": rel_path,
        "kind": "local_remote_changed" if baseline_hash else "missing_baseline",
        "resource_type": baseline_entry.get("resource_type"),
        "component_id": baseline_entry.get("component_id"),
        "baseline_hash": baseline_hash,
        "local_hash": _content_hash(local_text) if local_text is not None else None,
        "remote_hash": _content_hash(remote_text) if remote_text is not None else None,
        "baseline_text": baseline_entry.get("baseline_text") if isinstance(baseline_entry.get("baseline_text"), str) else None,
        "local_text": local_text,
        "remote_text": remote_text,
    }


def _write_conflict_artifact(
    *,
    root: Path,
    snapshot: dict[str, Any],
    conflicts: list[dict[str, Any]],
) -> None:
    path = root / TAG_CONFLICTS_FILENAME
    if not conflicts:
        if path.exists():
            path.unlink()
        return
    payload = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "property_id": snapshot.get("property_id"),
        "library_id": snapshot.get("library_id"),
        "conflicts": conflicts,
    }
    _write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def list_workspace_conflicts(root: str | Path) -> dict[str, Any]:
    """Return the saved conflict artifact for wrapper ``tags conflict --list`` commands."""
    path = Path(root) / TAG_CONFLICTS_FILENAME
    if not path.exists():
        return {"schema_version": 1, "conflicts": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{TAG_CONFLICTS_FILENAME} is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{TAG_CONFLICTS_FILENAME} must contain a JSON object: {path}")
    conflicts = payload.get("conflicts", [])
    if not isinstance(conflicts, list):
        raise RuntimeError(f"{TAG_CONFLICTS_FILENAME} conflicts must be a list: {path}")
    return payload


def render_workspace_conflict(root: str | Path, rel_path: str) -> str:
    """Render saved conflict diffs for wrapper ``tags conflict --show`` commands."""
    payload = list_workspace_conflicts(root)
    for raw in payload.get("conflicts", []):
        if not isinstance(raw, dict) or raw.get("path") != rel_path:
            continue
        local_text = raw.get("local_text")
        remote_text = raw.get("remote_text")
        if not isinstance(local_text, str) or not isinstance(remote_text, str):
            raise RuntimeError(f"Conflict text is unavailable for: {rel_path}")
        header = [
            f"path: {rel_path}",
            f"kind: {raw.get('kind', '')}",
            f"component_id: {raw.get('component_id') or ''}",
            f"resource_type: {raw.get('resource_type') or ''}",
            f"baseline_hash: {raw.get('baseline_hash') or ''}",
            f"local_hash: {raw.get('local_hash') or ''}",
            f"remote_hash: {raw.get('remote_hash') or ''}",
            "",
        ]
        baseline_text = raw.get("baseline_text")
        if isinstance(baseline_text, str):
            baseline_to_local = difflib.unified_diff(
                baseline_text.splitlines(keepends=True),
                local_text.splitlines(keepends=True),
                fromfile=f"baseline/{rel_path}",
                tofile=f"local/{rel_path}",
            )
            baseline_to_remote = difflib.unified_diff(
                baseline_text.splitlines(keepends=True),
                remote_text.splitlines(keepends=True),
                fromfile=f"baseline/{rel_path}",
                tofile=f"remote/{rel_path}",
            )
            local_to_remote = difflib.unified_diff(
                local_text.splitlines(keepends=True),
                remote_text.splitlines(keepends=True),
                fromfile=f"local/{rel_path}",
                tofile=f"remote/{rel_path}",
            )
            sections = [
                "=== baseline -> local ===\n",
                "".join(baseline_to_local),
                "\n=== baseline -> remote ===\n",
                "".join(baseline_to_remote),
                "\n=== local -> remote ===\n",
                "".join(local_to_remote),
            ]
            return "\n".join(header) + "".join(sections)

        diff = difflib.unified_diff(
            local_text.splitlines(keepends=True),
            remote_text.splitlines(keepends=True),
            fromfile=f"local/{rel_path}",
            tofile=f"remote/{rel_path}",
        )
        return "\n".join(header) + "".join(diff)
    raise RuntimeError(f"No saved conflict for path: {rel_path}")


def resolve_workspace_conflict(
    root: str | Path,
    rel_path: str,
    *,
    use: str,
    apply: bool = False,
) -> dict[str, Any]:
    """Resolve one saved conflict by keeping local, remote, or baseline text."""
    if use not in {"local", "remote", "baseline"}:
        raise ValueError("use must be one of: local, remote, baseline")

    root_path = Path(root)
    artifact_path = root_path / TAG_CONFLICTS_FILENAME
    payload = list_workspace_conflicts(root_path)
    conflicts = [item for item in payload.get("conflicts", []) if isinstance(item, dict)]
    target = next((item for item in conflicts if item.get("path") == rel_path), None)
    if target is None:
        raise RuntimeError(f"No saved conflict for path: {rel_path}")

    result = {
        "summary": {"resolved": 1 if apply else 0, "ready_to_resolve": 0 if apply else 1},
        "details": {"resolved": [rel_path] if apply else [], "ready_to_resolve": [rel_path] if not apply else []},
        "path": rel_path,
        "use": use,
        "applied": apply,
        "next": "rerun with --apply to update the conflict artifact" if not apply else "run 'tags status --since-pull' to verify local state",
    }
    if not apply:
        return _finalize_workspace_result(result, command="conflict", mode="resolve")

    if use in {"remote", "baseline"}:
        text_key = "remote_text" if use == "remote" else "baseline_text"
        target_text = target.get(text_key)
        if not isinstance(target_text, str):
            raise RuntimeError(f"{use.title()} text is unavailable for: {rel_path}")
        _write_text(root_path / rel_path, target_text)

    remaining = [item for item in conflicts if item.get("path") != rel_path]
    if remaining:
        payload["conflicts"] = remaining
        payload["generated_at"] = _now_iso()
        _write_text(artifact_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    elif artifact_path.exists():
        artifact_path.unlink()

    return _finalize_workspace_result(result, command="conflict", mode="resolve")


def _pull_or_checkout(
    *,
    root: Path,
    snapshot: dict[str, Any],
    destructive: bool,
    force: bool = False,
) -> dict[str, Any]:
    baseline = load_apply_baseline_manifest(root)
    local_managed = {
        str(path.relative_to(root)).replace("\\", "/"): path
        for path in _iter_managed_files(root)
    }
    remote_managed = _managed_remote_paths(snapshot)
    outside_scope_file_set: set[str] = set()
    if not destructive:
        membership = {
            (str(getattr(item, "resource_type", "")), str(getattr(item, "origin_id", "")))
            for item in snapshot.get("items", [])
        }
        outside_scope_files, _, _ = _outside_library_scope_files(
            local_managed=local_managed,
            local_resources=_local_origin_members(root),
            membership=membership,
        )
        outside_scope_file_set = set(outside_scope_files)

    if destructive and not force:
        dirty: list[str] = []
        if baseline.get("resources"):
            for rel_path, entry in baseline.get("resources", {}).items():
                if not rel_path.startswith(("rules/", "data-elements/")):
                    continue
                local_text = _read_text(root / rel_path)
                if local_text is None:
                    continue
                expected_hash = _baseline_hash_for(rel_path, baseline)
                if expected_hash is not None and _content_hash(local_text) != expected_hash:
                    dirty.append(rel_path)
        elif local_managed:
            dirty.extend(sorted(local_managed.keys()))
        if dirty:
            raise RuntimeError(
                "Refusing destructive checkout because local Adobe Tags files are dirty. "
                "Use --force to overwrite them.\n"
                + "\n".join(f"  - {path}" for path in dirty[:20])
                + (f"\n  - ... and {len(dirty) - 20} more" if len(dirty) > 20 else ""),
            )

    summary = defaultdict(int)
    details: dict[str, list[str]] = defaultdict(list)
    conflicts: list[dict[str, Any]] = []

    all_paths = sorted(remote_managed | set(local_managed.keys()))
    for rel_path in all_paths:
        remote_text = snapshot["files"].get(rel_path)
        local_text = _read_text(root / rel_path)
        baseline_hash = _baseline_hash_for(rel_path, baseline)
        baseline_entry = _baseline_entry_for(rel_path, baseline)

        if destructive:
            if remote_text is None:
                if local_text is not None:
                    _delete_file(root / rel_path)
                    summary["deleted"] += 1
                    details["deleted"].append(rel_path)
                continue
            action = _write_text(root / rel_path, remote_text)
            summary[action] += 1
            if action != "unchanged":
                details[action].append(rel_path)
            continue

        if remote_text is None and rel_path in outside_scope_file_set:
            summary["outside_library_scope_kept_local"] += 1
            details["outside_library_scope_kept_local"].append(rel_path)
            continue

        action = _classify_pull_action(
            rel_path=rel_path,
            remote_text=remote_text,
            local_text=local_text,
            baseline_hash=baseline_hash,
        )
        if action == "add":
            _write_text(root / rel_path, remote_text or "")
            summary["added"] += 1
            details["added"].append(rel_path)
        elif action == "update":
            _write_text(root / rel_path, remote_text or "")
            summary["updated"] += 1
            details["updated"].append(rel_path)
        elif action == "delete":
            _delete_file(root / rel_path)
            summary["deleted"] += 1
            details["deleted"].append(rel_path)
        elif action == "keep-local":
            summary["kept_local"] += 1
            details["kept_local"].append(rel_path)
        elif action == "kept-local-removed":
            summary["kept_local_removed"] += 1
            details["kept_local_removed"].append(rel_path)
        elif action == "conflict":
            summary["conflicts"] += 1
            details["conflicts"].append(rel_path)
            conflicts.append(
                _build_conflict_record(
                    rel_path=rel_path,
                    local_text=local_text,
                    remote_text=remote_text,
                    baseline_hash=baseline_hash,
                    baseline_entry=baseline_entry,
                ),
            )
        else:
            summary["unchanged"] += 1

    _write_conflict_artifact(root=root, snapshot=snapshot, conflicts=conflicts)

    _write_text(root / "property.json", snapshot["files"]["property.json"])
    _write_text(root / APPLY_BASELINE_FILENAME, snapshot["files"][APPLY_BASELINE_FILENAME])
    _write_text(root / LIBRARY_SCOPE_FILENAME, snapshot["files"][LIBRARY_SCOPE_FILENAME])

    return {"summary": dict(summary), "details": dict(details)}


def _group_paths_for_summary(paths: list[str]) -> dict[str, int]:
    grouped: dict[str, int] = defaultdict(int)
    for path in paths:
        if path.startswith("rules/"):
            grouped["rules"] += 1
        elif path.startswith("data-elements/"):
            grouped["data_elements"] += 1
        else:
            grouped[path.split("/", 1)[0] or "(root)"] += 1
    return dict(sorted(grouped.items(), key=lambda item: (-item[1], item[0])))


def _print_summary(
    title: str,
    result: dict[str, Any],
    *,
    summary_only: bool = False,
    verbose: bool = False,
) -> None:
    _log(f"\nSummary: {title}")
    summary_aliases = {
        "kept_local_removed": "remote_removed_with_local_edits",
        "outside_library_scope": "outside_library_scope_files",
        "outside_library_scope_kept_local": "outside_library_scope_files",
        "local_added": "local_added_since_pull",
    }
    for key in (
        "added",
        "updated",
        "deleted",
        "unchanged",
        "outside_library_scope",
        "outside_library_scope_resources",
        "outside_library_scope_rules",
        "outside_library_scope_data_elements",
        "outside_library_scope_kept_local",
        "kept_local",
        "kept_local_removed",
        "conflicts",
        "patched",
        "applied",
        "stale_remote",
        "no_op",
        "local_added",
    ):
        value = result.get("summary", {}).get(key, 0)
        if value:
            _log(f"  {summary_aliases.get(key, key)}: {value}")

    elapsed = result.get("elapsed")
    if summary_only:
        if isinstance(elapsed, (int, float)):
            _log(f"Elapsed: {elapsed:.1f}s")
        return

    warning_keys = (
        "conflicts",
        "outside_library_scope_resources",
        "outside_library_scope",
        "outside_library_scope_kept_local",
        "kept_local_removed",
        "stale_remote",
    )
    warnings = [key for key in warning_keys if result.get("summary", {}).get(key, 0)]
    if "outside_library_scope_resources" in warnings and "outside_library_scope" in warnings:
        warnings.remove("outside_library_scope")
    if warnings:
        _log("\nWarnings:")
        for key in warnings:
            warning_label = summary_aliases.get(key, key).upper()
            paths = result.get("details", {}).get(key, [])
            if key.startswith("outside_library_scope") and not verbose:
                noun = "resources" if key == "outside_library_scope_resources" else "files"
                _log(f"  [{warning_label}] {len(paths)} {noun} skipped")
                for label, count in list(_group_paths_for_summary(paths).items())[:10]:
                    _log(f"    {label}: {count}")
                if len(_group_paths_for_summary(paths)) > 10:
                    _log(f"    ... and {len(_group_paths_for_summary(paths)) - 10} more groups")
                _log("    --verbose for full list")
                continue
            limit = len(paths) if verbose else 10
            for path in paths[:limit]:
                _log(f"  [{warning_label}] {path}")
            extra = len(paths) - limit
            if extra > 0 and not verbose:
                _log(f"  ... and {extra} more")

    next_hint = result.get("next")
    if next_hint:
        _log(f"\nNext: {next_hint}")

    if isinstance(elapsed, (int, float)):
        _log(f"Elapsed: {elapsed:.1f}s")


def _local_origin_members(root: Path) -> list[dict[str, str]]:
    resources: list[dict[str, str]] = []
    rules_root = root / "rules"
    if rules_root.exists():
        for rule_json in sorted(rules_root.glob("*/rule.json")):
            try:
                data = json.loads(rule_json.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            rule_id = str(data.get("id", ""))
            if rule_id:
                resources.append(
                    {
                        "type": "rules",
                        "origin_id": rule_id,
                        "path": str(rule_json.parent.relative_to(root)).replace("\\", "/"),
                    },
                )
    de_root = root / "data-elements"
    if de_root.exists():
        for de_json in sorted(de_root.glob("*.json")):
            if de_json.name == "index.json":
                continue
            try:
                data = json.loads(de_json.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            de_id = str(data.get("id", ""))
            if de_id:
                resources.append(
                    {
                        "type": "data_elements",
                        "origin_id": de_id,
                        "path": str(de_json.relative_to(root)).replace("\\", "/"),
                    },
                )
    return resources


def _outside_library_scope_files(
    *,
    local_managed: dict[str, Path],
    local_resources: list[dict[str, str]],
    membership: set[tuple[str, str]],
) -> tuple[list[str], list[str], dict[str, int]]:
    resource_paths: list[str] = []
    file_paths: set[str] = set()
    resource_counts = {"rules": 0, "data_elements": 0}
    for resource in local_resources:
        if (resource["type"], resource["origin_id"]) in membership:
            continue
        resource_paths.append(resource["path"])
        if resource["type"] in resource_counts:
            resource_counts[resource["type"]] += 1
        if resource["type"] == "rules":
            prefix = resource["path"].rstrip("/") + "/"
            for rel_path in local_managed:
                if rel_path == resource["path"] or rel_path.startswith(prefix):
                    file_paths.add(rel_path)
            continue

        rel_path = Path(resource["path"])
        base_name = rel_path.name[:-5] if rel_path.name.endswith(".json") else rel_path.stem
        base_prefix = str(rel_path.parent / base_name).replace("\\", "/")
        for managed_path in local_managed:
            if managed_path == resource["path"] or managed_path.startswith(f"{base_prefix}."):
                file_paths.add(managed_path)

    return sorted(file_paths), sorted(resource_paths), resource_counts


def _read_library_scope_items(root: Path, library_id: str) -> list[ScopeItem]:
    manifest_path = root / LIBRARY_SCOPE_FILENAME
    if not manifest_path.exists():
        raise RuntimeError(
            f"{LIBRARY_SCOPE_FILENAME} is missing. "
            "Run 'tags pull' or 'tags checkout --force' before using local status.",
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{LIBRARY_SCOPE_FILENAME} is not valid JSON: {manifest_path}") from exc

    manifest_library_id = str(manifest.get("library_id", ""))
    if manifest_library_id and manifest_library_id != library_id:
        raise RuntimeError(
            f"{LIBRARY_SCOPE_FILENAME} was generated for {manifest_library_id}, "
            f"not requested library {library_id}",
        )

    items: list[ScopeItem] = []
    for raw in manifest.get("rules", []):
        if not isinstance(raw, dict):
            continue
        origin_id = str(raw.get("id", "")).strip()
        if origin_id:
            items.append(
                ScopeItem(
                    resource_type="rules",
                    origin_id=origin_id,
                    revision_id=str(raw.get("revision_id", "")),
                    name=str(raw.get("name", "")),
                ),
            )
    for raw in manifest.get("data_elements", []):
        if not isinstance(raw, dict):
            continue
        origin_id = str(raw.get("id", "")).strip()
        if origin_id:
            items.append(
                ScopeItem(
                    resource_type="data_elements",
                    origin_id=origin_id,
                    revision_id=str(raw.get("revision_id", "")),
                    name=str(raw.get("name", "")),
                ),
            )
    return items


def _status_since_pull(root: Path, library_id: str) -> dict[str, Any]:
    baseline = load_apply_baseline_manifest(root)
    if not baseline.get("resources"):
        raise RuntimeError(
            f"{APPLY_BASELINE_FILENAME} has no resources. "
            "Run 'tags pull' or 'tags checkout --force' before using local status.",
        )

    items = _read_library_scope_items(root, library_id)
    membership = {(item.resource_type, item.origin_id) for item in items}
    local_managed = {
        str(path.relative_to(root)).replace("\\", "/"): path
        for path in _iter_managed_files(root)
    }
    baseline_managed = {
        rel_path
        for rel_path in baseline.get("resources", {})
        if rel_path.startswith(("rules/", "data-elements/"))
    }
    local_resources = _local_origin_members(root)
    outside_scope_files, outside_scope_resources, outside_scope_counts = _outside_library_scope_files(
        local_managed=local_managed,
        local_resources=local_resources,
        membership=membership,
    )
    outside_scope_file_set = set(outside_scope_files)

    summary = defaultdict(int)
    details: dict[str, list[str]] = defaultdict(list)
    for rel_path in sorted(set(local_managed) | baseline_managed):
        if rel_path in outside_scope_file_set and rel_path not in baseline_managed:
            summary["outside_library_scope"] += 1
            details["outside_library_scope"].append(rel_path)
            continue

        local_text = _read_text(root / rel_path)
        baseline_hash = _baseline_hash_for(rel_path, baseline)
        if baseline_hash is None:
            summary["local_added"] += 1
            details["local_added"].append(rel_path)
            continue
        if local_text is None:
            summary["deleted"] += 1
            details["deleted"].append(rel_path)
            continue
        if _content_hash(local_text) == baseline_hash:
            summary["unchanged"] += 1
        else:
            summary["kept_local"] += 1
            details["kept_local"].append(rel_path)

    if outside_scope_resources:
        summary["outside_library_scope_resources"] = len(outside_scope_resources)
        summary["outside_library_scope_rules"] = outside_scope_counts["rules"]
        summary["outside_library_scope_data_elements"] = outside_scope_counts["data_elements"]
        details["outside_library_scope_resources"] = outside_scope_resources

    result = {"summary": dict(summary), "details": dict(details), "mode": "since_pull"}
    if summary.get("outside_library_scope_resources"):
        result["next"] = "use 'tags add --from-path ...' for intentional new resources or clean them up"
    elif summary.get("kept_local") or summary.get("local_added") or summary.get("deleted"):
        result["next"] = "run remote 'tags status' before push if remote drift matters, then 'tags push --apply'"
    else:
        result["next"] = "local files are aligned with the last pull/checkout snapshot"
    return _finalize_workspace_result(result, command="status", mode="since_pull")


def _resolve_local_resources_from_paths(root: Path, raw_paths: list[str]) -> list[dict[str, str]]:
    resolved: dict[tuple[str, str], dict[str, str]] = {}
    for raw in raw_paths:
        path = Path(raw)
        if not path.is_absolute():
            path = (root / path).resolve()
        if not path.exists():
            raise RuntimeError(f"Path not found: {path}")

        if path.is_dir():
            if (path / "rule.json").exists():
                rule_data = json.loads((path / "rule.json").read_text(encoding="utf-8"))
                origin_id = str(rule_data.get("id", ""))
                if not origin_id:
                    raise RuntimeError(f"rule.json has no id: {path}")
                key = ("rules", origin_id)
                resolved[key] = {"type": "rules", "origin_id": origin_id, "path": str(path)}
                continue
            raise RuntimeError(f"Unsupported directory for tags add: {path}")

        rel_to_root = str(path.relative_to(root)).replace("\\", "/")
        if "/rules/" in rel_to_root or rel_to_root.startswith("rules/"):
            rule_dir = path.parent
            while rule_dir != root and not (rule_dir / "rule.json").exists():
                rule_dir = rule_dir.parent
            if not (rule_dir / "rule.json").exists():
                raise RuntimeError(f"Could not find rule.json for path: {path}")
            rule_data = json.loads((rule_dir / "rule.json").read_text(encoding="utf-8"))
            origin_id = str(rule_data.get("id", ""))
            if not origin_id:
                raise RuntimeError(f"rule.json has no id: {rule_dir / 'rule.json'}")
            key = ("rules", origin_id)
            resolved[key] = {"type": "rules", "origin_id": origin_id, "path": str(rule_dir)}
            continue

        if "/data-elements/" in rel_to_root or rel_to_root.startswith("data-elements/"):
            de_id = ""
            if path.name.endswith(".settings.json"):
                de_id = find_data_element_id(path)
            elif ".custom-code." in path.name:
                sibling_json = path.parent / f"{path.name.split('.custom-code.')[0]}.json"
                if sibling_json.exists():
                    de_id = str(json.loads(sibling_json.read_text(encoding='utf-8')).get("id", ""))
            elif path.suffix == ".json":
                de_id = str(json.loads(path.read_text(encoding="utf-8")).get("id", ""))
            if not de_id:
                raise RuntimeError(f"Could not resolve data element id for path: {path}")
            key = ("data_elements", de_id)
            resolved[key] = {"type": "data_elements", "origin_id": de_id, "path": str(path)}
            continue

        raise RuntimeError(f"Path is outside managed Adobe Tags resources: {path}")
    return list(resolved.values())


def checkout_library_scope(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    force: bool = False,
    snapshot_workers: int | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    root_path = Path(root)
    local_managed = _iter_managed_files(root_path)
    if local_managed and not force:
        raise RuntimeError(
            "Refusing destructive checkout without --force because local Adobe Tags files already exist. "
            "Use 'tags pull' for a non-destructive sync, or rerun with --force to overwrite them.",
        )
    _log("Checkout is destructive: local library-scope files may be overwritten or deleted.")
    _log("Step 1/2: fetch remote library scope")
    snapshot = _build_library_scope_snapshot_for_workflow(config, library_id, snapshot_workers)
    _log("Step 2/2: overwrite local files from remote library scope")
    result = _pull_or_checkout(root=root_path, snapshot=snapshot, destructive=True, force=force)
    result["next"] = "edit local files, then run 'tags push --apply'"
    result["elapsed"] = time.monotonic() - start
    _finalize_workspace_result(result, command="checkout")
    _print_summary("checkout completed", result)
    return result


def pull_library_scope(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    snapshot_workers: int | None = None,
    summary_only: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
    start = time.monotonic()
    _log("Pull is non-destructive: local-only edits and conflicts stay in place.")
    _log("Step 1/2: fetch remote library scope")
    snapshot = _build_library_scope_snapshot_for_workflow(config, library_id, snapshot_workers)
    _log("Step 2/2: sync remote-only changes into local files")
    result = _pull_or_checkout(root=Path(root), snapshot=snapshot, destructive=False)
    if not load_apply_baseline_manifest(Path(root)).get("resources"):
        result["next"] = "initial sync complete; edit files or run 'tags status'"
    elif result.get("summary", {}).get("conflicts", 0):
        result["next"] = "review conflicts, then rerun 'tags pull' or use 'tags checkout --force'"
    elif result.get("summary", {}).get("kept_local_removed", 0):
        result["next"] = (
            "review local edits on files removed from remote, then delete them manually "
            "or re-add the parent resource intentionally"
        )
    elif result.get("summary", {}).get("outside_library_scope_kept_local", 0):
        result["next"] = (
            "review local files outside the current library scope; delete them locally if no longer needed "
            "or use 'tags add --from-path ...' for intentional resources"
        )
    else:
        result["next"] = "review changes or run 'tags status'"
    result["elapsed"] = time.monotonic() - start
    _finalize_workspace_result(result, command="pull")
    _print_summary("pull completed", result, summary_only=summary_only, verbose=verbose)
    return result


def status_library_scope(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    since_pull: bool = False,
    summary_only: bool = False,
    verbose: bool = False,
    snapshot_workers: int | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    root_path = Path(root)
    if since_pull:
        _log("Local status only: comparing files against the last pull/checkout snapshot; no Adobe API calls.")
        result = _status_since_pull(root_path, library_id)
        result["elapsed"] = time.monotonic() - start
        _finalize_workspace_result(result, command="status", mode="since_pull")
        _print_summary("status since pull", result, summary_only=summary_only, verbose=verbose)
        return result

    _log("Step 1/2: fetch remote library scope")
    snapshot = _build_library_scope_snapshot_for_workflow(config, library_id, snapshot_workers)
    _log("Step 2/2: compare local files against remote library scope")
    baseline = load_apply_baseline_manifest(root_path)
    local_managed = {
        str(path.relative_to(root_path)).replace("\\", "/"): path
        for path in _iter_managed_files(root_path)
    }
    remote_managed = _managed_remote_paths(snapshot)
    local_resources = _local_origin_members(root_path)
    membership = {(item.resource_type, item.origin_id) for item in snapshot["items"]}
    outside_scope_files, outside_scope_resources, outside_scope_counts = _outside_library_scope_files(
        local_managed=local_managed,
        local_resources=local_resources,
        membership=membership,
    )
    outside_scope_file_set = set(outside_scope_files)
    summary = defaultdict(int)
    details: dict[str, list[str]] = defaultdict(list)
    conflicts: list[dict[str, Any]] = []
    for rel_path in sorted(remote_managed | set(local_managed.keys())):
        if rel_path in outside_scope_file_set and rel_path not in remote_managed:
            summary["outside_library_scope"] += 1
            details["outside_library_scope"].append(rel_path)
            continue
        remote_text = snapshot["files"].get(rel_path)
        local_text = _read_text(root_path / rel_path)
        baseline_hash = _baseline_hash_for(rel_path, baseline)
        baseline_entry = _baseline_entry_for(rel_path, baseline)
        action = _classify_pull_action(
            rel_path=rel_path,
            remote_text=remote_text,
            local_text=local_text,
            baseline_hash=baseline_hash,
        )
        summary[action.replace("-", "_")] += 1
        if action not in ("unchanged",):
            details[action.replace("-", "_")].append(rel_path)
        if action == "conflict":
            conflicts.append(
                _build_conflict_record(
                    rel_path=rel_path,
                    local_text=local_text,
                    remote_text=remote_text,
                    baseline_hash=baseline_hash,
                    baseline_entry=baseline_entry,
                ),
            )

    if outside_scope_resources:
        summary["outside_library_scope_resources"] = len(outside_scope_resources)
        summary["outside_library_scope_rules"] = outside_scope_counts["rules"]
        summary["outside_library_scope_data_elements"] = outside_scope_counts["data_elements"]
        details["outside_library_scope_resources"] = outside_scope_resources

    _write_conflict_artifact(root=root_path, snapshot=snapshot, conflicts=conflicts)

    result = {"summary": dict(summary), "details": dict(details)}
    if summary.get("conflicts"):
        result["next"] = "review conflicts, then run 'tags pull' or 'tags checkout --force'"
    elif summary.get("outside_library_scope"):
        result["next"] = "use 'tags add --from-path ...' for intentional new resources or clean them up"
    elif summary.get("kept_local_removed"):
        result["next"] = (
            "review local edits on files removed from remote, then delete them manually "
            "or re-add the parent resource intentionally"
        )
    elif summary.get("keep_local"):
        result["next"] = "run 'tags push --apply' to publish local-only edits"
    else:
        result["next"] = "workspace is aligned; edit files or run 'tags push --apply' after changes"
    result["elapsed"] = time.monotonic() - start
    _finalize_workspace_result(result, command="status", mode="remote")
    _print_summary("status", result, summary_only=summary_only, verbose=verbose)
    return result


def add_to_library_scope(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    from_paths: list[str],
    apply: bool,
) -> dict[str, Any]:
    start = time.monotonic()
    root_path = Path(root)
    _log("Step 1/3: resolve local resources from --from-path")
    resources = _resolve_local_resources_from_paths(root_path, from_paths)
    _log(f"  resolved {len(resources)} resource(s)")

    _log("Step 2/3: validate against current library membership")
    items = _library_scope_items(config, library_id)
    membership = {(item.resource_type, item.origin_id): item for item in items}
    summary = defaultdict(int)
    details: dict[str, list[str]] = defaultdict(list)
    additions_by_type: dict[str, list[str]] = defaultdict(list)

    for resource in resources:
        key = (resource["type"], resource["origin_id"])
        if key in membership:
            summary["already_in_library"] += 1
            details["already_in_library"].append(resource["path"])
            continue

        endpoint = "/rules" if resource["type"] == "rules" else "/data_elements"
        remote = _reactor_get(config, f"{endpoint}/{resource['origin_id']}").get("data", {})
        remote_name = str(remote.get("attributes", {}).get("name", resource["origin_id"]))
        same_name_items = (
            list_rules(config, name_contains=remote_name)
            if resource["type"] == "rules"
            else list_data_elements(config, name_contains=remote_name)
        )
        name_conflict = [
            item for item in same_name_items
            if str(item.get("attributes", {}).get("name", "")) == remote_name
            and str(item.get("id", "")) != resource["origin_id"]
        ]
        if name_conflict:
            summary["name_conflicts"] += 1
            details["name_conflicts"].append(
                f"{resource['path']} -> {remote_name} conflicts with {name_conflict[0].get('id', '')}",
            )
            continue

        additions_by_type[resource["type"]].append(resource["origin_id"])
        summary["ready_to_add"] += 1
        details["ready_to_add"].append(resource["path"])

    if apply:
        _log("Step 3/3: add resources to library")
        if additions_by_type["rules"]:
            revise_library_rules(config, library_id, additions_by_type["rules"])
        if additions_by_type["data_elements"]:
            revise_library_data_elements(config, library_id, additions_by_type["data_elements"])
        summary["added"] = len(additions_by_type["rules"]) + len(additions_by_type["data_elements"])
    else:
        _log("Step 3/3: dry-run only (no library changes)")

    result = {"summary": dict(summary), "details": dict(details)}
    if apply and summary.get("added"):
        result["next"] = "resources were added to the library but no build was started; run 'tags push --apply' or 'tags build'"
    elif summary.get("ready_to_add"):
        result["next"] = "rerun with --apply to add these resources to the library"
    else:
        result["next"] = "no add candidates; review conflicts or run 'tags status'"
    result["elapsed"] = time.monotonic() - start
    _finalize_workspace_result(result, command="add")
    _print_summary("add", result)
    return result


def push_library_scope(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    apply: bool,
    verify_asset_url: str | None = None,
    allow_stale_base: bool = False,
    skip_build: bool = False,
    markers: list[str] | None = None,
    snapshot_workers: int | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    root_path = Path(root)
    markers = markers or []

    _log("Step 1/6: fetch current library membership")
    items = _library_scope_items(config, library_id)
    membership = {(item.resource_type, item.origin_id) for item in items}
    local_resources = _local_origin_members(root_path)
    out_of_library = [
        resource for resource in local_resources
        if (resource["type"], resource["origin_id"]) not in membership
    ]
    if out_of_library:
        details = {"outside_library_scope": [resource["path"] for resource in out_of_library]}
        result = {
            "summary": {
                "outside_library_scope": len(out_of_library),
                "outside_library_scope_resources": len(out_of_library),
                "outside_library_scope_rules": sum(1 for resource in out_of_library if resource["type"] == "rules"),
                "outside_library_scope_data_elements": sum(1 for resource in out_of_library if resource["type"] == "data_elements"),
            },
            "details": details,
            "next": "use 'tags add --from-path ...' for intentional new resources or clean them up",
            "elapsed": time.monotonic() - start,
        }
        _finalize_workspace_result(result, command="push", mode="dry_run" if not apply else "apply")
        if not apply:
            _print_summary("push dry-run blocked", result)
            return result
        raise RuntimeError(
            "Refusing push because local resources are not in the current library scope. "
            "Use 'tags add --from-path ...' for intentional new resources.\n"
            + "\n".join(f"  - {resource['path']}" for resource in out_of_library[:20])
            + (f"\n  - ... and {len(out_of_library) - 20} more" if len(out_of_library) > 20 else ""),
        )

    _log("Step 2/6: compare local files against remote resources")
    dry_results = apply_exported_changes_tree(config, root_path, dry_run=True)
    raise_for_stale_base_conflicts(dry_results, allow_stale_base=allow_stale_base)

    changed_origins: list[dict[str, str]] = []
    summary = defaultdict(int)
    details: dict[str, list[str]] = defaultdict(list)
    for result in dry_results:
        stale_status = result.get("stale_status")
        if stale_status == "remote_only":
            summary["stale_remote"] += 1
            details["stale_remote"].append(result["path"])
            continue
        if result.get("changed"):
            summary["patched"] += 1
            details["patched"].append(result["path"])
            if "rules/" in result["path"]:
                for resource in local_resources:
                    if result["path"].startswith(resource["path"]) and resource["type"] == "rules":
                        changed_origins.append({"id": resource["origin_id"], "type": "rules"})
                        break
            elif "data-elements/" in result["path"]:
                changed_origins.append({"id": result["component_id"], "type": "data_elements"})
        else:
            summary["no_op"] += 1

    deduped: list[dict[str, str]] = []
    seen = set()
    for item in changed_origins:
        key = (item["id"], item["type"])
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    changed_origins = deduped

    if not apply:
        result = {"summary": dict(summary), "details": dict(details)}
        if summary.get("patched"):
            result["next"] = "rerun with --apply to patch and refresh the library"
        else:
            result["next"] = "no local changes detected; run 'tags status' or edit files first"
        result["elapsed"] = time.monotonic() - start
        _finalize_workspace_result(result, command="push", mode="dry_run")
        _print_summary("push dry-run", result)
        return result

    _log("Step 3/6: patch changed resources")
    apply_results = apply_exported_changes_tree(config, root_path, dry_run=False)
    raise_for_stale_base_conflicts(apply_results, allow_stale_base=allow_stale_base)

    _log("Step 4/6: refresh library revisions from changed origins")
    counts = refresh_library_resources(config, library_id, new_resources=changed_origins)
    for resource_type, count in counts.items():
        _log(f"  {resource_type}: {count}")

    if skip_build:
        result = {"summary": dict(summary), "details": dict(details)}
        result["next"] = "library revisions were refreshed but no build was started; run 'tags build'"
        result["elapsed"] = time.monotonic() - start
        _finalize_workspace_result(result, command="push", mode="skip_build")
        _print_summary("push completed without build", result)
        return result

    _log("Step 5/6: build library")
    build = build_library(config, library_id)
    _log(f"  build: {build['id']} status={build['status']}")
    _log("  waiting for build completion (this can take 1-3 min)...")
    final_build = wait_for_build_completion(config, build["id"])
    _log(f"  final build status: {final_build['status']}")
    if final_build["status"] not in BUILD_SUCCESS_STATUSES:
        raise RuntimeError(f"Adobe Tags build failed: {final_build['status']}")

    if markers:
        if not verify_asset_url:
            raise ValueError("verify_asset_url is required when markers are provided")
        _log("Step 5b/6: verify CDN markers")
        ok = verify_build_markers(asset_url=verify_asset_url, markers=markers)
        if not ok:
            raise RuntimeError("Adobe Tags build completed but marker verification failed")

    _log("Step 6/6: pull remote library scope back into local files")
    pull_result = _pull_or_checkout(
        root=root_path,
        snapshot=_build_library_scope_snapshot_for_workflow(config, library_id, snapshot_workers),
        destructive=False,
    )
    result = {"summary": dict(summary), "details": dict(details)}
    result["summary"]["applied"] = summary.get("patched", 0)
    for key in ("added", "updated", "deleted", "conflicts", "kept_local"):
        if pull_result["summary"].get(key, 0):
            result["summary"][key] = pull_result["summary"][key]
            result["details"][key] = pull_result["details"].get(key, [])
    result["next"] = "verify the site, then run 'tags status' if you need a post-build diff summary"
    result["elapsed"] = time.monotonic() - start
    _finalize_workspace_result(result, command="push", mode="apply")
    _print_summary("push completed", result)
    return result


def build_library_scope(
    config: AdobeTagsConfig,
    *,
    library_id: str,
    verify_asset_url: str | None = None,
    markers: list[str] | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    markers = markers or []
    _log("Step 1/2: trigger build")
    build = build_library(config, library_id)
    _log(f"  build: {build['id']} status={build['status']}")
    _log("Step 2/2: wait for build completion")
    final_build = wait_for_build_completion(config, build["id"])
    _log(f"  final build status: {final_build['status']}")
    if final_build["status"] not in BUILD_SUCCESS_STATUSES:
        raise RuntimeError(f"Adobe Tags build failed: {final_build['status']}")
    if markers:
        if not verify_asset_url:
            raise ValueError("verify_asset_url is required when markers are provided")
        _log("Step 2b/2: verify CDN markers")
        ok = verify_build_markers(asset_url=verify_asset_url, markers=markers)
        if not ok:
            raise RuntimeError("Adobe Tags build completed but marker verification failed")
    result = {"summary": {"builds": 1}, "details": {}, "next": "run 'tags pull' if you want local files resynced", "elapsed": time.monotonic() - start}
    _finalize_workspace_result(result, command="build")
    _print_summary("build completed", result)
    return result


def full_export_property(
    config: AdobeTagsConfig,
    *,
    output_root: str | Path,
    resources: list[str] | None = None,
) -> dict[str, Any]:
    start = time.monotonic()
    resources = resources or ["rules", "data-elements", "extensions", "environments", "libraries"]
    _log("Step 1/1: export full property mirror")
    _log(f"  output: {output_root}")
    _log(f"  resources: {resources}")
    summary = export_property(config, output_root, resources=resources)
    result = {
        "summary": {"resources": len(resources)},
        "details": {"resources": resources},
        "next": "review the mirror under output/tmp; it is not the canonical local working tree",
        "elapsed": time.monotonic() - start,
    }
    _finalize_workspace_result(result, command="full-export")
    _print_summary("full-export completed", result)
    result["export_summary"] = summary
    return result
