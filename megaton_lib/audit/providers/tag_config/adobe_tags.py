"""Adobe Tags (Reactor API) provider."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from typing import Any

import requests

from megaton_lib.audit.config import AdobeTagsConfig


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


def _reactor_get(config: AdobeTagsConfig, endpoint: str, *, query: str = "") -> dict[str, Any]:
    api_key = os.getenv(config.api_key_env, "").strip()
    token = os.getenv(config.bearer_token_env, "").strip()
    ims_org_id = os.getenv(config.ims_org_id_env, "").strip()

    if not api_key or not token:
        raise RuntimeError(
            "Adobe Tags credentials are missing. "
            f"Set {config.api_key_env} and {config.bearer_token_env}.",
        )

    base = config.base_url.rstrip("/")
    url = f"{base}{endpoint}"
    if query:
        url = f"{url}?{query}"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": api_key,
        "Accept": config.accept_header,
        "Content-Type": config.content_type_header,
    }
    if ims_org_id:
        headers["x-gw-ims-org-id"] = ims_org_id

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Adobe Tags API failed: {resp.status_code} {resp.text}",
        )

    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Adobe Tags API response format")
    return payload


def list_data_elements(config: AdobeTagsConfig) -> list[dict[str, Any]]:
    """Fetch all data elements in a property."""
    all_items: list[dict[str, Any]] = []
    page = 1

    while True:
        query = f"sort=name&page[number]={page}&page[size]={config.page_size}"
        body = _reactor_get(config, f"/properties/{config.property_id}/data_elements", query=query)
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
