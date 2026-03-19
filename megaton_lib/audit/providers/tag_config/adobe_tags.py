"""Adobe Tags (Reactor API) provider.

Supports both OAuth (via AdobeOAuthClient) and legacy bearer token auth.
Provides read-only audit functions and full export/apply for property management.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote as _url_quote

import requests

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient


# ---- settings helpers (unchanged) ----


def parse_settings_object(raw: Any) -> dict[str, Any]:
    """Parse Reactor settings payload into a dictionary."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _mapping_from_list(entries: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(entries, list):
        return out

    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        pattern = (
            entry.get("key")
            or entry.get("pattern")
            or entry.get("regex")
            or entry.get("source")
        )
        value = (
            entry.get("value")
            or entry.get("output")
            or entry.get("site")
            or entry.get("target")
        )
        if isinstance(pattern, str) and isinstance(value, str):
            p = pattern.strip()
            v = value.strip()
            if p and v:
                out[p] = v
    return out


def extract_mapping_from_settings(settings: Mapping[str, Any], *, mapping_setting_key: str = "map") -> dict[str, str]:
    """Extract regex->value mapping from a data element settings object."""
    candidates = [
        mapping_setting_key,
        "map",
        "mappings",
        "regexMap",
        "regex_map",
    ]

    for key in candidates:
        entries = settings.get(key)
        if isinstance(entries, dict):
            mapping = {
                str(k).strip(): str(v).strip()
                for k, v in entries.items()
                if str(k).strip() and str(v).strip()
            }
            if mapping:
                return mapping
        mapping = _mapping_from_list(entries)
        if mapping:
            return mapping

    return {}


# ---- auth helpers ----


def _get_auth_headers(config: AdobeTagsConfig) -> dict[str, str]:
    """Build Reactor API headers, supporting OAuth or legacy bearer token.

    If ``config.oauth`` is set, creates an ``AdobeOAuthClient`` for
    automatic token management.  Otherwise falls back to the legacy
    ``bearer_token_env`` / ``api_key_env`` env-var approach.
    """
    if config.oauth is not None:
        auth = AdobeOAuthClient(
            client_id_env=config.oauth.client_id_env,
            client_secret_env=config.oauth.client_secret_env,
            org_id=config.oauth.org_id or "",
            org_id_env=config.oauth.org_id_env,
            scopes=config.oauth.scopes,
            token_cache_file=config.oauth.token_cache_file,
        )
        return auth.get_headers(
            extra={
                "Accept": config.accept_header,
                "Content-Type": config.content_type_header,
            },
        )

    # Legacy bearer token path
    api_key = os.getenv(config.api_key_env, "").strip()
    token = os.getenv(config.bearer_token_env, "").strip()
    ims_org_id = os.getenv(config.ims_org_id_env, "").strip()

    if not api_key or not token:
        raise RuntimeError(
            "Adobe Tags credentials are missing. "
            f"Set {config.api_key_env} and {config.bearer_token_env}, "
            "or configure oauth for automatic token management.",
        )

    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": config.accept_header,
        "Content-Type": config.content_type_header,
    }
    if ims_org_id:
        headers["x-gw-ims-org-id"] = ims_org_id
    return headers


# ---- low-level HTTP ----


def _reactor_get(config: AdobeTagsConfig, endpoint: str, *, query: str = "") -> dict[str, Any]:
    headers = _get_auth_headers(config)
    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"
    if query:
        url = f"{url}?{query}"

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Adobe Tags API failed: {resp.status_code} {resp.text}",
        )

    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Adobe Tags API response format")
    return payload


def _reactor_patch(config: AdobeTagsConfig, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Send a PATCH request to the Reactor API."""
    headers = _get_auth_headers(config)
    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"

    resp = requests.patch(url, headers=headers, json=payload, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Adobe Tags API PATCH failed: {resp.status_code} {resp.text}",
        )
    return resp.json() if resp.text.strip() else {}


def _reactor_post(config: AdobeTagsConfig, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Send a POST request to the Reactor API."""
    headers = _get_auth_headers(config)
    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Adobe Tags API POST failed: {resp.status_code} {resp.text}",
        )
    return resp.json() if resp.text.strip() else {}


def _reactor_delete(config: AdobeTagsConfig, endpoint: str, payload: dict[str, Any]) -> None:
    """Send a DELETE request to the Reactor API (relationship removal)."""
    headers = _get_auth_headers(config)
    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"

    resp = requests.delete(url, headers=headers, json=payload, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Adobe Tags API DELETE failed: {resp.status_code} {resp.text}",
        )


def _attach_library_resources(
    config: AdobeTagsConfig,
    endpoint: str,
    resource_type: str,
    resource_ids: list[str],
) -> None:
    """Attach existing resource revisions back to a library relationship."""
    if not resource_ids:
        return
    payload = {
        "data": [{"id": rid, "type": resource_type} for rid in resource_ids],
    }
    _reactor_post(config, endpoint, payload)


# ---- paginated list helpers ----


def _paginated_list(
    config: AdobeTagsConfig,
    endpoint: str,
    *,
    sort: str = "name",
    extra_query: str = "",
) -> list[dict[str, Any]]:
    """Generic paginated fetch for Reactor API list endpoints."""
    all_items: list[dict[str, Any]] = []
    page = 1

    while True:
        query = f"sort={sort}&page[number]={page}&page[size]={config.page_size}"
        if extra_query:
            query = f"{query}&{extra_query}"
        body = _reactor_get(config, endpoint, query=query)
        items = body.get("data", [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    all_items.append(item)

        next_page = body.get("meta", {}).get("pagination", {}).get("next_page")
        if not next_page:
            break
        page = int(next_page)

    return all_items


# ---- existing public API (unchanged signatures) ----


def list_data_elements(
    config: AdobeTagsConfig,
    *,
    name_contains: str | None = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    """Fetch data elements in a property.

    Parameters
    ----------
    name_contains : str | None
        API-side filter: ``filter[name][CONTAINS]=<value>``.
    enabled_only : bool
        API-side filter: ``filter[enabled][EQ]=true``.
    """
    parts: list[str] = []
    if name_contains:
        parts.append(f"filter[name][CONTAINS]={_url_quote(name_contains, safe='')}")
    if enabled_only:
        parts.append("filter[enabled][EQ]=true")
    return _paginated_list(
        config,
        f"/properties/{config.property_id}/data_elements",
        extra_query="&".join(parts),
    )


def fetch_adobe_tags_mapping(config: AdobeTagsConfig) -> tuple[dict[str, str], dict[str, Any]]:
    """Fetch mapping from Adobe Tags data element settings."""
    elements = list_data_elements(config)
    mapping: dict[str, str] = {}
    scanned = 0

    for item in elements:
        attrs = item.get("attributes", {})
        if not isinstance(attrs, dict):
            continue
        de_name = str(attrs.get("name", "")).strip()
        if config.mapping_data_element_name and de_name != config.mapping_data_element_name:
            continue

        scanned += 1
        settings = parse_settings_object(attrs.get("settings"))
        extracted = extract_mapping_from_settings(
            settings,
            mapping_setting_key=config.mapping_setting_key,
        )
        mapping.update(extracted)

    if not mapping:
        target = config.mapping_data_element_name or "(all data elements)"
        raise RuntimeError(
            "No mapping extracted from Adobe Tags data elements. "
            f"target={target}, setting_key={config.mapping_setting_key}",
        )

    return mapping, {
        "provider": "adobe_tags",
        "property_id": config.property_id,
        "mapping_data_element_name": config.mapping_data_element_name,
        "mapping_setting_key": config.mapping_setting_key,
        "data_element_count": len(elements),
        "scanned_data_element_count": scanned,
        "mapping_count": len(mapping),
    }


# ---- new list methods ----


def list_rules(
    config: AdobeTagsConfig,
    *,
    name_contains: str | None = None,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    """Fetch rules in a property.

    Parameters
    ----------
    name_contains : str | None
        API-side filter: ``filter[name][CONTAINS]=<value>``.
    enabled_only : bool
        API-side filter: ``filter[enabled][EQ]=true``.
    """
    parts: list[str] = []
    if name_contains:
        parts.append(f"filter[name][CONTAINS]={_url_quote(name_contains, safe='')}")
    if enabled_only:
        parts.append("filter[enabled][EQ]=true")
    return _paginated_list(config, f"/properties/{config.property_id}/rules", extra_query="&".join(parts))


def list_rule_components(config: AdobeTagsConfig, rule_id: str) -> list[dict[str, Any]]:
    """Fetch all rule components for a given rule."""
    return _paginated_list(config, f"/rules/{rule_id}/rule_components")


def list_rule_revisions(config: AdobeTagsConfig, rule_id: str) -> list[dict[str, Any]]:
    """Fetch revision history for a rule.

    Returns list of dicts with id, revision_number, and created_at,
    sorted by revision number ascending.
    """
    raw = _paginated_list(config, f"/rules/{rule_id}/revisions", sort="revision_number")
    return [
        {
            "id": item.get("id", ""),
            "revision_number": item.get("attributes", {}).get("revision_number", 0),
            "created_at": item.get("attributes", {}).get("created_at", ""),
        }
        for item in raw
    ]


def list_extensions(config: AdobeTagsConfig) -> list[dict[str, Any]]:
    """Fetch all extensions in a property."""
    return _paginated_list(config, f"/properties/{config.property_id}/extensions")


def list_environments(config: AdobeTagsConfig) -> list[dict[str, Any]]:
    """Fetch all environments in a property."""
    return _paginated_list(config, f"/properties/{config.property_id}/environments")


def list_libraries(config: AdobeTagsConfig) -> list[dict[str, Any]]:
    """Fetch all libraries in a property."""
    return _paginated_list(config, f"/properties/{config.property_id}/libraries")


# ---- library management ----


def build_library(config: AdobeTagsConfig, library_id: str) -> dict[str, Any]:
    """Trigger a library build via POST /libraries/{library_id}/builds.

    Returns dict with id, status, and created_at of the new build.
    """
    payload = {"data": {"type": "builds", "attributes": {}}}
    body = _reactor_post(config, f"/libraries/{library_id}/builds", payload)
    data = body.get("data", {})
    attrs = data.get("attributes", {})
    return {
        "id": data.get("id", ""),
        "status": attrs.get("status", ""),
        "created_at": attrs.get("created_at", ""),
    }


def list_library_resources(config: AdobeTagsConfig, library_id: str) -> dict[str, Any]:
    """Fetch rules and data elements in a library with revision status.

    Returns dict with:
    - ``rules``: list of resource summaries
    - ``data_elements``: list of resource summaries
    - ``stale``: resources where revision_number != latest_revision_number
    """
    rules_raw = _paginated_list(config, f"/libraries/{library_id}/rules")
    de_raw = _paginated_list(config, f"/libraries/{library_id}/data_elements")

    stale: list[dict[str, Any]] = []

    def _summarize(items: list[dict[str, Any]], resource_type: str) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for item in items:
            attrs = item.get("attributes", {})
            meta = item.get("meta", {})
            rev = attrs.get("revision_number", 0)
            latest = meta.get("latest_revision_number", rev)
            dirty = attrs.get("dirty", False)
            summary = {
                "id": item.get("id", ""),
                "name": attrs.get("name", ""),
                "revision_number": rev,
                "latest_revision_number": latest,
                "dirty": dirty,
                "type": resource_type,
            }
            summaries.append(summary)
            if rev != latest:
                stale.append(summary)
        return summaries

    return {
        "rules": _summarize(rules_raw, "rules"),
        "data_elements": _summarize(de_raw, "data_elements"),
        "stale": stale,
    }


def _revise_library_resources(
    config: AdobeTagsConfig,
    library_id: str,
    resource_type: str,
    origin_ids: list[str],
) -> dict[str, Any]:
    """Revise resources of a given type in a library.

    Uses ``POST /libraries/{library_id}/relationships/{resource_type}``
    with ``meta.action = "revise"`` on each origin ID.

    IMPORTANT: Uses POST (add/update) not PATCH (full replacement).
    PATCH would remove all other resources from the library.

    If a revision of the same origin already exists (409 conflict),
    the existing revision is removed from the library first, then a
    new revision is created from the current origin HEAD.

    Returns dict with ``revised_count`` and ``new_ids``.
    """
    if not origin_ids:
        return {"revised_count": 0, "new_ids": []}

    endpoint = f"/libraries/{library_id}/relationships/{resource_type}"

    payload = {
        "data": [
            {"id": rid, "type": resource_type, "meta": {"action": "revise"}}
            for rid in origin_ids
        ],
    }

    headers = _get_auth_headers(config)
    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)

    if 200 <= resp.status_code < 300:
        body = resp.json() if resp.text.strip() else {}
        new_ids = [item.get("id", "") for item in body.get("data", [])]
        return {"revised_count": len(new_ids), "new_ids": new_ids}

    if resp.status_code != 409:
        raise RuntimeError(
            f"Adobe Tags API POST revise ({resource_type}) failed: "
            f"{resp.status_code} {resp.text}",
        )

    # 409 conflict: remove existing revisions for these origins, then retry
    origin_set = set(origin_ids)
    existing = _paginated_list(config, f"/libraries/{library_id}/{resource_type}")
    to_remove = []
    for item in existing:
        origin_data = (
            item.get("relationships", {}).get("origin", {}).get("data", {})
        )
        if origin_data.get("id", "") in origin_set:
            rev_id = item.get("id", "")
            if rev_id:
                to_remove.append({"id": rev_id, "type": resource_type})

    if to_remove:
        _reactor_delete(config, endpoint, {"data": to_remove})

    # Retry revise after removing old revisions
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if 200 <= resp.status_code < 300:
        body = resp.json() if resp.text.strip() else {}
        new_ids = [item.get("id", "") for item in body.get("data", [])]
        return {"revised_count": len(new_ids), "new_ids": new_ids}

    raise RuntimeError(
        f"Adobe Tags API POST revise ({resource_type}) failed after retry: "
        f"{resp.status_code} {resp.text}",
    )


def revise_library_rules(
    config: AdobeTagsConfig,
    library_id: str,
    origin_rule_ids: list[str],
) -> dict[str, Any]:
    """Revise rules in a library to pick up dirty component changes.

    See :func:`_revise_library_resources` for details.

    Args:
        origin_rule_ids: HEAD rule IDs (``revision_number=0``), not
            revision copies.

    Returns dict with ``revised_count`` and ``new_rule_ids``.
    """
    result = _revise_library_resources(config, library_id, "rules", origin_rule_ids)
    return {"revised_count": result["revised_count"], "new_rule_ids": result["new_ids"]}


def revise_library_data_elements(
    config: AdobeTagsConfig,
    library_id: str,
    origin_de_ids: list[str],
) -> dict[str, Any]:
    """Revise data elements in a library to pick up changes.

    See :func:`_revise_library_resources` for details.

    Args:
        origin_de_ids: HEAD data element IDs (``revision_number=0``),
            not revision copies.

    Returns dict with ``revised_count`` and ``new_de_ids``.
    """
    result = _revise_library_resources(config, library_id, "data_elements", origin_de_ids)
    return {"revised_count": result["revised_count"], "new_de_ids": result["new_ids"]}


def refresh_library_resources(
    config: AdobeTagsConfig,
    library_id: str,
    new_resources: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Refresh library resources from origins with per-origin rollback.

    Existing library revisions are replaced one origin at a time. When a
    replacement revise fails after removing the current revision, the
    removed revision is re-attached so the library does not lose unrelated
    resources. New origin IDs from ``new_resources`` are revised in place.

    Optionally merges *new_resources* (origin IDs not yet in the library).
    Each entry must have ``{"id": "...", "type": "rules"|"data_elements"}``.

    Returns dict with ``rules_count`` and ``data_elements_count``.
    """
    origins_by_type: dict[str, set[str]] = {}
    existing_by_type: dict[str, dict[str, list[str]]] = {}

    for rtype in ("rules", "data_elements"):
        existing = _paginated_list(config, f"/libraries/{library_id}/{rtype}")
        if not existing:
            continue

        for item in existing:
            origin_data = (
                item.get("relationships", {}).get("origin", {}).get("data", {})
            )
            origin_id = origin_data.get("id", "")
            revision_id = item.get("id", "")
            if not origin_id or not revision_id:
                continue
            origins_by_type.setdefault(rtype, set()).add(origin_id)
            existing_by_type.setdefault(rtype, {}).setdefault(origin_id, []).append(revision_id)

    # Merge in new resources
    for r in new_resources or []:
        origins_by_type.setdefault(r["type"], set()).add(r["id"])

    counts: dict[str, int] = {}
    for rtype, origin_ids in origins_by_type.items():
        endpoint = f"/libraries/{library_id}/relationships/{rtype}"
        existing_by_origin = existing_by_type.get(rtype, {})
        refreshed = 0

        for origin_id in sorted(origin_ids):
            revision_ids = existing_by_origin.get(origin_id, [])
            if not revision_ids:
                result = _revise_library_resources(config, library_id, rtype, [origin_id])
                refreshed += result["revised_count"]
                continue

            payload = {
                "data": [{"id": rid, "type": rtype} for rid in revision_ids],
            }
            _reactor_delete(config, endpoint, payload)
            try:
                result = _revise_library_resources(config, library_id, rtype, [origin_id])
            except Exception:
                _attach_library_resources(config, endpoint, rtype, revision_ids)
                raise
            refreshed += result["revised_count"]

        counts[f"{rtype}_count"] = refreshed

    return counts


def find_dirty_origin_rules(
    config: AdobeTagsConfig,
    library_id: str,
) -> list[str]:
    """Find origin rules that have dirty components needing revision.

    For each rule in the library, fetches its origin rule and checks
    if the origin has ``dirty=True`` (component-level changes pending).

    Returns list of origin rule IDs that need revising.
    """
    rules_raw = _paginated_list(config, f"/libraries/{library_id}/rules")

    dirty_origins: list[str] = []
    seen: set[str] = set()

    for rule in rules_raw:
        origin_data = (
            rule.get("relationships", {}).get("origin", {}).get("data", {})
        )
        origin_id = origin_data.get("id", "")
        if not origin_id:
            rule_name = rule.get("attributes", {}).get("name", rule.get("id", "?"))
            logging.getLogger(__name__).warning(
                "Library rule %s has no origin relationship — skipped", rule_name,
            )
            continue
        if origin_id in seen:
            continue
        seen.add(origin_id)

        origin = _reactor_get(config, f"/rules/{origin_id}")
        origin_attrs = origin.get("data", {}).get("attributes", {})
        if origin_attrs.get("dirty", False):
            dirty_origins.append(origin_id)

    return dirty_origins


def deploy_library(
    config: AdobeTagsConfig,
    library_id: str,
) -> dict[str, Any]:
    """Revise dirty rules and trigger a build.

    Combines :func:`find_dirty_origin_rules`, :func:`revise_library_rules`,
    and :func:`build_library` into a single deployment operation.

    Returns dict with ``dirty_count``, ``revised_count``, and ``build``.
    """
    dirty = find_dirty_origin_rules(config, library_id)
    revised: dict[str, Any] = {"revised_count": 0, "new_rule_ids": []}
    if dirty:
        revised = revise_library_rules(config, library_id, dirty)

    build = build_library(config, library_id)

    return {
        "dirty_count": len(dirty),
        "revised_count": revised["revised_count"],
        "build": build,
    }


# ---- custom code extraction ----


def extract_custom_code(component: dict[str, Any]) -> tuple[str, str] | None:
    """Extract custom code and language from a rule component or data element.

    Returns ``(source_code, language)`` or ``None`` if no custom code found.
    Language is ``"javascript"`` or ``"html"``.
    """
    attrs = component.get("attributes", {})
    if not isinstance(attrs, dict):
        return None

    settings = parse_settings_object(attrs.get("settings"))
    if not settings:
        return None

    # Try known source fields
    source = None
    for key in ("source", "customCode", "code", "html", "script"):
        candidate = settings.get(key)
        if isinstance(candidate, str) and candidate.strip():
            source = candidate.strip()
            break

    if not source:
        return None

    # Detect language
    lang = settings.get("language", "").lower()
    if lang not in ("javascript", "html"):
        # Heuristic: check content for HTML tags
        if re.search(r"<\w+[\s>]", source):
            lang = "html"
        else:
            lang = "javascript"

    return source, lang


# ---- export property ----


def export_property(
    config: AdobeTagsConfig,
    output_root: str | Path,
    resources: list[str] | None = None,
    *,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Export an Adobe Tags property to a local directory.

    Mirrors the structure produced by the at-recs ``export_adobe_tags.sh``.

    Parameters
    ----------
    config : AdobeTagsConfig
        Property configuration (property_id, auth, etc.).
    output_root : str | Path
        Root directory for output files.
    resources : list[str] | None
        Resource types to export.
        Default: ``["rules", "data-elements", "extensions", "environments", "libraries"]``
    filters : dict[str, Any] | None
        Per-resource API-side filters.  Keys are resource type names
        (``"rules"``, ``"data-elements"``); values are dicts of filter kwargs
        forwarded to the corresponding ``list_*`` function.

        Example::

            filters={
                "rules": {"name_contains": "Adobe Target"},
                "data-elements": {"enabled_only": True},
            }

    Returns
    -------
    dict with summary counts per resource type.
    """
    if resources is None:
        resources = ["rules", "data-elements", "extensions", "environments", "libraries"]
    if filters is None:
        filters = {}

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    # Export property metadata
    prop_body = _reactor_get(config, f"/properties/{config.property_id}")
    prop_data = prop_body.get("data", {})
    (root / "property.json").write_text(
        json.dumps(prop_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary: dict[str, int] = {}

    for resource in resources:
        res_dir = root / resource
        res_dir.mkdir(parents=True, exist_ok=True)
        resource_filters = filters.get(resource, {})

        if resource == "rules":
            count = _export_rules(config, res_dir, **resource_filters)
        elif resource == "data-elements":
            count = _export_data_elements(config, res_dir, **resource_filters)
        elif resource in ("extensions", "environments", "libraries"):
            list_fn = {
                "extensions": list_extensions,
                "environments": list_environments,
                "libraries": list_libraries,
            }[resource]
            items = list_fn(config)
            count = _export_items(items, res_dir)
        else:
            count = 0

        summary[resource] = count

    return summary


def _export_items(items: list[dict[str, Any]], out_dir: Path) -> int:
    """Write items to index.json and individual detail files."""
    index_entries: list[dict[str, Any]] = []
    for item in items:
        item_id = item.get("id", "unknown")
        attrs = item.get("attributes", {})
        name = attrs.get("name", item_id) if isinstance(attrs, dict) else item_id
        basename = _resource_basename(item_id, name)

        index_entries.append({"id": item_id, "name": name})
        (out_dir / f"{basename}.json").write_text(
            json.dumps(item, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    (out_dir / "index.json").write_text(
        json.dumps(index_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return len(items)


def _export_rules(
    config: AdobeTagsConfig,
    out_dir: Path,
    *,
    name_contains: str | None = None,
    enabled_only: bool = False,
) -> int:
    """Export rules with their rule components and custom code."""
    rules = list_rules(config, name_contains=name_contains, enabled_only=enabled_only)
    index_entries: list[dict[str, Any]] = []

    for rule in rules:
        rule_id = rule.get("id", "unknown")
        attrs = rule.get("attributes", {})
        name = attrs.get("name", rule_id) if isinstance(attrs, dict) else rule_id

        rule_dir = out_dir / _resource_basename(rule_id, name)
        rule_dir.mkdir(parents=True, exist_ok=True)

        # Save rule metadata
        (rule_dir / "rule.json").write_text(
            json.dumps(rule, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # Fetch and save rule components
        components = list_rule_components(config, rule_id)
        for comp in components:
            comp_id = comp.get("id", "unknown")
            comp_attrs = comp.get("attributes", {})
            comp_name = comp_attrs.get("name", comp_id) if isinstance(comp_attrs, dict) else comp_id
            comp_base = _resource_basename(comp_id, comp_name)

            (rule_dir / f"{comp_base}.json").write_text(
                json.dumps(comp, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            # Extract custom code
            code_info = extract_custom_code(comp)
            if code_info:
                source, lang = code_info
                ext = ".html" if lang == "html" else ".js"
                code_file = rule_dir / f"{comp_base}.custom-code{ext}"
                code_file.write_text(source, encoding="utf-8")

        index_entries.append({
            "id": rule_id,
            "name": name,
            "component_count": len(components),
        })

    (out_dir / "index.json").write_text(
        json.dumps(index_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return len(rules)


def _export_data_elements(
    config: AdobeTagsConfig,
    out_dir: Path,
    *,
    name_contains: str | None = None,
    enabled_only: bool = False,
) -> int:
    """Export data elements with custom code extraction."""
    elements = list_data_elements(config, name_contains=name_contains, enabled_only=enabled_only)
    index_entries: list[dict[str, Any]] = []

    for elem in elements:
        elem_id = elem.get("id", "unknown")
        attrs = elem.get("attributes", {})
        name = attrs.get("name", elem_id) if isinstance(attrs, dict) else elem_id
        basename = _resource_basename(elem_id, name)

        (out_dir / f"{basename}.json").write_text(
            json.dumps(elem, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # Extract custom code
        code_info = extract_custom_code(elem)
        if code_info:
            source, lang = code_info
            ext = ".html" if lang == "html" else ".js"
            (out_dir / f"{basename}.custom-code{ext}").write_text(
                source, encoding="utf-8",
            )

        index_entries.append({"id": elem_id, "name": name})

    (out_dir / "index.json").write_text(
        json.dumps(index_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return len(elements)


# ---- apply custom code ----


def apply_custom_code(
    config: AdobeTagsConfig,
    component_id: str,
    new_code: str,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Update custom code for a rule component or data element.

    Parameters
    ----------
    component_id : str
        The Reactor resource ID (e.g. ``RC...``).
    new_code : str
        The new source code to set.
    dry_run : bool
        If True, compare but do not apply.

    Returns
    -------
    dict with ``changed``, ``component_id``, and diff info.
    """
    # Determine resource type from ID prefix
    if component_id.startswith("RC"):
        endpoint = f"/rule_components/{component_id}"
        resource_type = "rule_components"
    elif component_id.startswith("DE"):
        endpoint = f"/data_elements/{component_id}"
        resource_type = "data_elements"
    else:
        raise ValueError(f"Unknown component ID prefix: {component_id}")

    # Fetch current state
    current = _reactor_get(config, endpoint)
    current_data = current.get("data", {})
    attrs = current_data.get("attributes", {})
    settings = parse_settings_object(attrs.get("settings"))

    # Find current code
    current_code = ""
    code_key = ""
    for key in ("source", "customCode", "code", "html", "script"):
        candidate = settings.get(key)
        if isinstance(candidate, str):
            current_code = candidate
            code_key = key
            break

    if not code_key:
        code_key = "source"

    changed = current_code.strip() != new_code.strip()
    result: dict[str, Any] = {
        "component_id": component_id,
        "changed": changed,
        "code_key": code_key,
    }

    if not changed or dry_run:
        return result

    # Apply update
    settings[code_key] = new_code
    patch_payload = {
        "data": {
            "id": component_id,
            "type": resource_type,
            "attributes": {
                "settings": json.dumps(settings),
            },
        },
    }
    _reactor_patch(config, endpoint, patch_payload)
    result["applied"] = True
    return result


# ---- utilities ----


def _safe_filename(name: str) -> str:
    """Convert a resource name to a safe filename slug."""
    slug = re.sub(r"[^\w\s\-.]", "", str(name).strip())
    slug = re.sub(r"\s+", "-", slug).strip("-")
    return slug.lower() or "unnamed"


def _resource_basename(resource_id: Any, name: Any) -> str:
    """Build a collision-resistant export basename using id + slug."""
    id_slug = _safe_filename(str(resource_id))
    name_slug = _safe_filename(str(name))
    if id_slug == name_slug:
        return id_slug
    return f"{id_slug}_{name_slug}"
