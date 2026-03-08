"""Shared site alias resolution for CLI, batch, and Streamlit handoff."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONFIG_FILENAMES = ("sites.example.json", "sites.json", "sites.local.json")

_sites_cache: dict[str, dict[str, Any]] | None = None


def clear_cache() -> None:
    """Clear the default config-directory cache."""
    global _sites_cache
    _sites_cache = None


def load_sites(config_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    """Load site alias definitions from config files.

    Precedence (later wins): ``sites.example.json`` < ``sites.json`` < ``sites.local.json``.
    The default project ``configs/`` directory is cached because it is read often.
    """
    global _sites_cache

    if config_dir is None:
        if _sites_cache is not None:
            return _sites_cache
        config_dir = Path(__file__).resolve().parent.parent / "configs"

    merged: dict[str, dict[str, Any]] = {}
    for filename in CONFIG_FILENAMES:
        sites_path = config_dir / filename
        if not sites_path.exists():
            continue
        with open(sites_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"{sites_path} must be a JSON object")
        merged.update(data)

    if config_dir == Path(__file__).resolve().parent.parent / "configs":
        _sites_cache = merged
    return merged


def resolve_site_alias(raw: dict[str, Any], config_dir: Path | None = None) -> dict[str, Any]:
    """Expand ``site`` alias to source-specific identifiers.

    Supported mappings:
    - ``gsc`` -> ``site_url`` via ``gsc_site_url``
    - ``ga4`` -> ``property_id`` via ``ga4_property_id``
    - ``aa`` -> ``rsid`` / ``company_id`` / ``org_id`` via ``aa_*``
    """
    alias = raw.get("site")
    if not alias:
        return raw

    sites = load_sites(config_dir)
    entry = sites.get(alias)
    if entry is None:
        available = ", ".join(sorted(sites.keys())) if sites else "(none)"
        raise ValueError(f"Unknown site alias '{alias}'. Available: {available}")
    if not isinstance(entry, dict):
        raise ValueError(f"Site alias '{alias}' must map to an object")

    resolved = dict(raw)
    source = str(resolved.get("source", "")).lower()

    if source == "gsc" and "site_url" not in resolved:
        resolved["site_url"] = _require_alias_field(entry, "gsc_site_url", alias)
    elif source == "ga4" and "property_id" not in resolved:
        resolved["property_id"] = _require_alias_field(entry, "ga4_property_id", alias)
    elif source == "aa":
        if "rsid" not in resolved and "aa_rsid" in entry:
            resolved["rsid"] = str(entry["aa_rsid"])
        if "company_id" not in resolved and "aa_company_id" in entry:
            resolved["company_id"] = str(entry["aa_company_id"])
        if "org_id" not in resolved and "aa_org_id" in entry:
            resolved["org_id"] = str(entry["aa_org_id"])

    resolved.pop("site", None)
    return resolved


def _require_alias_field(entry: dict[str, Any], key: str, alias: str) -> str:
    value = entry.get(key)
    if isinstance(value, str) and value.strip():
        return value
    raise ValueError(f"Site alias '{alias}' is missing '{key}'")
