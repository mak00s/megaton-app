"""GTM Tag configuration provider."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from megaton_lib.credentials import list_service_account_paths
from megaton_lib.audit.config import GtmConfig

ALL_RESOURCES = ("tags", "triggers", "variables", "built_in_variables", "folders")


def parse_regex_table_variable(var: Mapping[str, Any]) -> dict[str, str]:
    """Parse GTM regex table (remm) variable definition into mapping."""
    mapping: dict[str, str] = {}

    for param in var.get("parameter", []):
        if param.get("key") == "map" and str(param.get("type", "")).lower() == "list":
            for entry in param.get("list", []):
                pattern = None
                output = None
                for item in entry.get("map", []):
                    if item.get("key") == "key":
                        pattern = item.get("value")
                    elif item.get("key") == "value":
                        output = item.get("value")
                if pattern and output:
                    mapping[str(pattern)] = str(output)

    for param in var.get("parameter", []):
        if param.get("key") == "defaultValue":
            default_value = str(param.get("value", "")).strip()
            if default_value:
                mapping["__default__"] = default_value
            break

    return mapping


def _extract_mapping_from_container(
    service: Any,
    *,
    container_path: str,
    variable_name: str,
) -> tuple[dict[str, str], str | None]:
    workspaces = (
        service.accounts()
        .containers()
        .workspaces()
        .list(parent=container_path)
        .execute()
        .get("workspace", [])
    )
    for ws in workspaces:
        workspace_path = ws.get("path")
        if not workspace_path:
            continue
        variables = (
            service.accounts()
            .containers()
            .workspaces()
            .variables()
            .list(parent=workspace_path)
            .execute()
            .get("variable", [])
        )
        for var in variables:
            if var.get("name") == variable_name and var.get("type") == "remm":
                return parse_regex_table_variable(var), workspace_path
    return {}, None


def fetch_gtm_mapping(
    config: GtmConfig,
    *,
    credentials_paths: Sequence[str] | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Fetch GTM regex mapping via Tag Manager API v2."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "google-api-python-client is required for GTM audit. "
            "Install it with: pip install google-api-python-client",
        ) from exc

    paths = list(credentials_paths) if credentials_paths else list_service_account_paths()
    if not paths:
        raise RuntimeError("No service account JSON found for GTM API access")

    scopes = ["https://www.googleapis.com/auth/tagmanager.readonly"]
    errors: list[str] = []

    for creds_path in paths:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=scopes,
            )
            service = build("tagmanager", "v2", credentials=credentials)
            accounts = service.accounts().list().execute().get("account", [])

            for account in accounts:
                account_path = account.get("path")
                if not account_path:
                    continue
                containers = (
                    service.accounts()
                    .containers()
                    .list(parent=account_path)
                    .execute()
                    .get("container", [])
                )
                for container in containers:
                    if container.get("publicId") != config.container_public_id:
                        continue

                    container_path = container.get("path")
                    if not container_path:
                        continue

                    mapping, workspace_path = _extract_mapping_from_container(
                        service,
                        container_path=container_path,
                        variable_name=config.variable_name,
                    )
                    if mapping:
                        return mapping, {
                            "provider": "gtm",
                            "credentials_path": creds_path,
                            "container_public_id": config.container_public_id,
                            "container_path": container_path,
                            "workspace_path": workspace_path,
                            "variable_name": config.variable_name,
                            "mapping_count": len(mapping),
                        }

            errors.append(f"{creds_path}: container/variable not found")
        except Exception as exc:  # pragma: no cover - network/env dependent
            errors.append(f"{creds_path}: {exc}")

    joined = " | ".join(errors)
    raise RuntimeError(
        "Failed to fetch GTM mapping from all credentials. "
        f"container={config.container_public_id}, variable={config.variable_name}, errors={joined}",
    )


# ---------------------------------------------------------------------------
# Container-level export
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    """Convert a resource name to a filesystem-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", name.strip().lower())
    slug = re.sub(r"[\s]+", "-", slug)
    return slug[:80] or "unnamed"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_json_if_changed(path: Path, data: Any) -> str:
    """Write JSON only if content differs. Returns 'added'|'updated'|'unchanged'."""
    new_content = json.dumps(data, ensure_ascii=False, indent=2)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing == new_content:
            return "unchanged"
        path.write_text(new_content, encoding="utf-8")
        return "updated"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(new_content, encoding="utf-8")
    return "added"


def _resource_id(item: Mapping[str, Any]) -> str:
    """Extract the numeric ID from a GTM resource path like 'accounts/.../tags/123'."""
    path = item.get("path", "") or item.get("tagId", "") or item.get("triggerId", "") or item.get("variableId", "")
    return str(path).rsplit("/", 1)[-1] if "/" in str(path) else str(path)


def _fetch_workspace_resources(
    service: Any,
    workspace_path: str,
    resource_types: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """Fetch multiple resource types from a workspace."""
    ws = service.accounts().containers().workspaces()
    fetchers: dict[str, Any] = {
        "tags": lambda: ws.tags().list(parent=workspace_path).execute().get("tag", []),
        "triggers": lambda: ws.triggers().list(parent=workspace_path).execute().get("trigger", []),
        "variables": lambda: ws.variables().list(parent=workspace_path).execute().get("variable", []),
        "built_in_variables": lambda: ws.built_in_variables().list(parent=workspace_path).execute().get("builtInVariable", []),
        "folders": lambda: ws.folders().list(parent=workspace_path).execute().get("folder", []),
    }

    result: dict[str, list[dict[str, Any]]] = {}
    for rt in resource_types:
        fetcher = fetchers.get(rt)
        if not fetcher:
            continue
        try:
            result[rt] = fetcher()
        except Exception:  # pragma: no cover - some containers lack certain resources
            result[rt] = []
    return result


def _sync_resource_dir(
    items: list[dict[str, Any]],
    resource_dir: Path,
    *,
    index_only: bool = False,
) -> dict[str, int]:
    """Sync resource items to a directory. Returns {added, updated, deleted, unchanged}."""
    resource_dir.mkdir(parents=True, exist_ok=True)
    stats = {"added": 0, "updated": 0, "deleted": 0, "unchanged": 0}

    # Build index and write individual files
    expected_files: set[str] = set()
    index_entries = []
    for item in items:
        rid = _resource_id(item)
        name = item.get("name", "unnamed")
        entry: dict[str, Any] = {"id": rid, "name": name}
        if "type" in item:
            entry["type"] = item["type"]
        index_entries.append(entry)

        if not index_only:
            slug = _slugify(name)
            filename = f"{rid}_{slug}.json"
            expected_files.add(filename)
            status = _write_json_if_changed(resource_dir / filename, item)
            stats[status] += 1

    # Always write index
    _write_json_if_changed(resource_dir / "index.json", index_entries)

    # Delete local files that no longer exist in API
    if not index_only:
        for existing in resource_dir.glob("*.json"):
            if existing.name == "index.json":
                continue
            if existing.name not in expected_files:
                existing.unlink()
                stats["deleted"] += 1

    return stats


def _find_container(
    container_public_id: str,
    credentials_paths: Sequence[str],
) -> tuple[Any, dict[str, Any], str]:
    """Find a GTM container by public ID across all credentials.

    Returns (service, container_dict, credentials_path).
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build as build_service
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "google-api-python-client is required for GTM export. "
            "Install it with: pip install google-api-python-client",
        ) from exc

    scopes = ["https://www.googleapis.com/auth/tagmanager.readonly"]
    errors: list[str] = []

    for creds_path in credentials_paths:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                creds_path, scopes=scopes,
            )
            svc = build_service("tagmanager", "v2", credentials=credentials)
            accounts = svc.accounts().list().execute().get("account", [])

            for account in accounts:
                account_path = account.get("path")
                if not account_path:
                    continue
                containers = (
                    svc.accounts().containers()
                    .list(parent=account_path).execute()
                    .get("container", [])
                )
                for container in containers:
                    if container.get("publicId") == container_public_id:
                        return svc, container, creds_path

            errors.append(f"{creds_path}: container not found")
        except Exception as exc:  # pragma: no cover
            errors.append(f"{creds_path}: {exc}")

    joined = " | ".join(errors)
    raise RuntimeError(
        f"GTM container {container_public_id} not found. errors={joined}",
    )


def sync_container(
    config: GtmConfig,
    output_root: str | Path,
    *,
    resources: Sequence[str] | None = None,
    credentials_paths: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Sync a GTM container's workspace resources to a local directory tree.

    Only writes files that changed and deletes files for removed resources.
    Returns a summary dict with per-resource change counts.
    """
    paths = list(credentials_paths) if credentials_paths else list_service_account_paths()
    if not paths:
        raise RuntimeError("No service account JSON found for GTM API access")

    resource_types = list(resources or config.export_resources or ALL_RESOURCES)
    root = Path(output_root)

    service, container, _creds = _find_container(
        config.container_public_id, paths,
    )
    container_path = container["path"]

    summary: dict[str, Any] = {}

    # Sync container metadata
    summary["container.json"] = _write_json_if_changed(root / "container.json", container)

    # Get default workspace (prefer "Default Workspace" by name)
    workspaces = (
        service.accounts().containers().workspaces()
        .list(parent=container_path).execute()
        .get("workspace", [])
    )
    if not workspaces:
        raise RuntimeError(f"No workspaces found in container {config.container_public_id}")

    workspace = next(
        (ws for ws in workspaces if ws.get("name") == "Default Workspace"),
        workspaces[0],
    )
    workspace_path = workspace["path"]
    summary["workspace.json"] = _write_json_if_changed(root / "workspace.json", workspace)

    # Fetch and sync resources
    all_items = _fetch_workspace_resources(service, workspace_path, resource_types)

    for rt, items in all_items.items():
        index_only = rt in ("built_in_variables", "folders")
        summary[rt] = _sync_resource_dir(items, root / rt, index_only=index_only)

    return summary


# Backwards-compatible alias
export_container = sync_container
