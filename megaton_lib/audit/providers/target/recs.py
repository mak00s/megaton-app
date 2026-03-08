"""Adobe Target Recommendations export and apply.

Mirrors the behaviour of the at-recs shell scripts:
- ``export_target_recs.sh`` → ``export_recs()``
- ``apply_target_recs.sh``  → ``apply_recs()``
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from megaton_lib.audit.providers.target.client import AdobeTargetClient

RESOURCE_TYPES = ("criteria", "designs", "collections", "exclusions", "promotions")

# Criteria sub-type mapping.
# The generic ``/criteria/{id}`` returns a slim response (no ``configuration``,
# ``backupDisabled``, etc.).  The sub-type endpoint returns the full detail
# including ``configuration.inclusionRules``.
# Sub-type endpoints only support GET and PUT (PATCH returns 405).
_CRITERIA_GROUP_TO_SUBTYPE: dict[str, str] = {
    "POPULARITY": "popularity",
    "ITEM": "item",
    "CUSTOM": "custom",
    "CATEGORY": "category",
    "SEQUENCE": "sequence",
}

# Keys to strip when comparing local vs remote for apply diff.
# Target API returns: lastModified, lastModifiersEmail, lastModifiersName.
_METADATA_KEYS = {
    "lastModified", "lastModifiersEmail", "lastModifiersName",
    # Also cover alternative naming seen in some endpoints
    "lastModifiedAt", "lastModifiedBy", "modifiedAt", "modifiedBy",
}


def export_recs(
    client: AdobeTargetClient,
    output_root: str | Path,
    resources: list[str] | None = None,
    *,
    name_regex: str | dict[str, str] | None = None,
    id_list: list[int] | dict[str, list[int]] | None = None,
    max_items: int | dict[str, int] | None = None,
) -> dict[str, int]:
    """Export Target Recommendations resources to local files.

    Parameters
    ----------
    client : AdobeTargetClient
    output_root : path for output directory
    resources : resource types to export (default: all 5)
    name_regex : filter by name. str applies to all; dict per-resource.
    id_list : filter by ID. list applies to all; dict per-resource.
    max_items : max items per resource. int applies to all; dict per-resource.

    Returns
    -------
    dict mapping resource type → export count.
    """
    if resources is None:
        resources = list(RESOURCE_TYPES)

    root = Path(output_root)
    summary: dict[str, int] = {}

    for resource in resources:
        if resource not in RESOURCE_TYPES:
            continue

        res_dir = root / resource
        res_dir.mkdir(parents=True, exist_ok=True)

        # Resolve per-resource filters
        nr = _resolve_param(name_regex, resource)
        il = _resolve_param(id_list, resource)
        mi = _resolve_param(max_items, resource)

        count = _export_resource(client, resource, res_dir, name_regex=nr, id_list=il, max_items=mi)
        summary[resource] = count

    return summary


def _export_resource(
    client: AdobeTargetClient,
    resource: str,
    out_dir: Path,
    *,
    name_regex: str | None = None,
    id_list: list[int] | None = None,
    max_items: int | None = None,
) -> int:
    """Export a single resource type."""
    items = client.get_all(f"/{resource}", max_items=max_items or 10000)

    # Apply filters
    if name_regex:
        pattern = re.compile(name_regex)
        items = [item for item in items if pattern.search(str(item.get("name", "")))]

    if id_list:
        id_set = set(id_list)
        items = [item for item in items if item.get("id") in id_set]

    if max_items is not None:
        items = items[:max_items]

    # Fetch detail for each item (some endpoints provide more detail per-item)
    index_entries: list[dict[str, Any]] = []
    for item in items:
        item_id = item.get("id", "unknown")
        name = item.get("name", str(item_id))

        detail = _fetch_detail(client, resource, item_id, item)

        # Save detail JSON
        (out_dir / f"{item_id}.json").write_text(
            json.dumps(detail, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # For designs, extract script to separate file
        if resource == "designs" and isinstance(detail, dict):
            _extract_design_script(detail, out_dir, item_id)

        index_entries.append({
            "id": item_id,
            "name": name,
        })

    # Write index
    (out_dir / "index.json").write_text(
        json.dumps(index_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return len(items)


def _criteria_detail_endpoint(
    client: AdobeTargetClient, item_id: Any,
) -> tuple[str, bool]:
    """Return the detail endpoint and whether it is a sub-type endpoint.

    The generic ``/criteria/{id}`` returns a slim response.  The sub-type
    endpoint (e.g. ``/criteria/popularity/{id}``) returns the full detail
    including ``configuration.inclusionRules`` and backup settings.

    Returns
    -------
    (endpoint, is_subtype) — ``is_subtype`` is ``True`` when a sub-type
    endpoint was resolved (PUT required), ``False`` when falling back to
    the generic endpoint (PATCH required).
    """
    try:
        slim = client.get(f"/criteria/{item_id}")
    except RuntimeError:
        return f"/criteria/{item_id}", False

    if not isinstance(slim, dict):
        return f"/criteria/{item_id}", False

    group = slim.get("criteriaGroup", "")
    subtype = _CRITERIA_GROUP_TO_SUBTYPE.get(group)
    if subtype:
        return f"/criteria/{subtype}/{item_id}", True
    return f"/criteria/{item_id}", False


def _fetch_detail(
    client: AdobeTargetClient,
    resource: str,
    item_id: Any,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Fetch full detail for a single resource item."""
    try:
        if resource == "designs":
            detail = client.get(
                f"/{resource}/{item_id}", params={"includeScript": "true"},
            )
        elif resource == "criteria":
            endpoint, _ = _criteria_detail_endpoint(client, item_id)
            detail = client.get(endpoint)
        else:
            detail = client.get(f"/{resource}/{item_id}")
    except RuntimeError:
        return fallback

    if isinstance(detail, list):
        return fallback
    return detail  # type: ignore[return-value]


def _extract_design_script(design: dict[str, Any], out_dir: Path, item_id: Any) -> None:
    """Extract design script/template to a separate file."""
    content = design.get("script") or design.get("content") or ""
    if not isinstance(content, str) or not content.strip():
        return

    # Determine file extension from content type
    content_lower = content.strip().lower()
    if content_lower.startswith("{") or content_lower.startswith("["):
        ext = ".vtl"  # Velocity template (JSON-like)
    elif re.search(r"<\w+[\s>]", content):
        ext = ".html"
    else:
        ext = ".js"

    (out_dir / f"{item_id}{ext}").write_text(content, encoding="utf-8")


# ---- apply ----


def apply_recs(
    client: AdobeTargetClient,
    source_root: str | Path,
    resources: list[str] | None = None,
    *,
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """Apply local changes to remote Target Recommendations.

    Parameters
    ----------
    client : AdobeTargetClient
    source_root : directory with local resource JSON files
    resources : resource types to process (default: all 5)
    dry_run : if True, report changes without applying

    Returns
    -------
    list of change records with ``resource``, ``id``, ``name``, ``changed``, ``applied``.
    """
    if resources is None:
        resources = list(RESOURCE_TYPES)

    root = Path(source_root)
    changes: list[dict[str, Any]] = []

    for resource in resources:
        if resource not in RESOURCE_TYPES:
            continue

        res_dir = root / resource
        if not res_dir.is_dir():
            continue

        for json_file in sorted(res_dir.glob("*.json")):
            if json_file.name == "index.json":
                continue

            try:
                local = json.loads(json_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue

            if not isinstance(local, dict):
                continue

            # For designs, merge sidecar script file back into content
            if resource == "designs":
                local = _merge_design_sidecar(local, json_file)

            item_id = local.get("id") or json_file.stem
            name = local.get("name", str(item_id))

            # Fetch remote current state (criteria uses sub-type endpoint)
            try:
                if resource == "criteria":
                    _ep, _is_subtype = _criteria_detail_endpoint(client, item_id)
                else:
                    _ep = f"/{resource}/{item_id}"
                    _is_subtype = False
                remote = client.get(_ep)
            except RuntimeError:
                changes.append({
                    "resource": resource,
                    "id": item_id,
                    "name": name,
                    "changed": True,
                    "applied": False,
                    "error": "remote not found",
                })
                continue

            if not isinstance(remote, dict):
                continue

            # Compare (strip metadata keys)
            local_clean = _strip_metadata(local)
            remote_clean = _strip_metadata(remote)

            changed = local_clean != remote_clean
            record: dict[str, Any] = {
                "resource": resource,
                "id": item_id,
                "name": name,
                "changed": changed,
                "applied": False,
            }

            if changed and not dry_run:
                try:
                    payload = _strip_metadata(local)
                    # Designs require PUT (PATCH ignores script).
                    # Criteria sub-type endpoints require PUT (PATCH returns 405).
                    # If sub-type resolution failed, _ep is the generic
                    # ``/criteria/{id}`` which only supports PATCH.
                    if resource == "designs":
                        client.put(f"/{resource}/{item_id}", payload)
                    elif resource == "criteria" and _is_subtype:
                        client.put(_ep, payload)
                    else:
                        client.patch(_ep, payload)
                    record["applied"] = True
                except RuntimeError as exc:
                    record["error"] = str(exc)

            changes.append(record)

    return changes


def _merge_design_sidecar(local: dict[str, Any], json_file: Path) -> dict[str, Any]:
    """Merge sidecar script file (.vtl/.html/.js) back into design ``script``.

    Searches for sidecar files in multiple locations/naming conventions:
    1. ``<id>.<ext>`` in same directory (megaton_lib export layout)
    2. ``<stem>.<ext>`` when JSON is named ``<id>_<slug>.json``
    3. ``code/<stem>.<ext>`` in a ``code/`` subdirectory (at-recs layout)

    Always merges into the ``script`` key (Target API canonical field).
    """
    item_id = local.get("id") or json_file.stem
    stem = json_file.stem  # e.g. "13628_default-template" or "13628"
    parent = json_file.parent

    # Candidate (directory, basename) pairs, tried in order
    candidates = [
        (parent, str(item_id)),           # <dir>/<id>.ext
        (parent, stem),                   # <dir>/<id>_<slug>.ext
        (parent / "code", stem),          # <dir>/code/<id>_<slug>.ext
        (parent / "code", str(item_id)),  # <dir>/code/<id>.ext
    ]

    for search_dir, base in candidates:
        if not search_dir.is_dir():
            continue
        for ext in (".vtl", ".html", ".js"):
            sidecar = search_dir / f"{base}{ext}"
            if sidecar.exists():
                try:
                    script = sidecar.read_text(encoding="utf-8")
                except OSError:
                    continue
                merged = dict(local)
                # Always use "script" — the canonical Target API field
                merged["script"] = script
                merged.pop("content", None)
                return merged
    return local


def _strip_metadata(obj: dict[str, Any]) -> dict[str, Any]:
    """Remove volatile metadata keys for comparison."""
    return {k: v for k, v in obj.items() if k not in _METADATA_KEYS}


def _resolve_param(value: Any, resource: str) -> Any:
    """Resolve a parameter that can be a scalar or per-resource dict."""
    if isinstance(value, dict):
        return value.get(resource)
    return value
