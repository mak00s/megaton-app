"""Helpers for resolving named local auth profiles from JSON credential stores."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .storefront_runtime import load_json_credentials


def load_auth_profile_store(path: Path) -> dict[str, Any]:
    """Load a local auth profile store JSON file."""
    payload = load_json_credentials(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Credential store must be a JSON object: {path}")
    return payload


def _coerce_profile_map(store: Mapping[str, Any]) -> Mapping[str, Any]:
    profiles = store.get("profiles")
    if isinstance(profiles, Mapping):
        return profiles
    return store


def resolve_auth_profile(
    store_or_path: Mapping[str, Any] | str | Path,
    profile_name: str,
    *,
    required_fields: Sequence[str] = (),
) -> dict[str, Any]:
    """Resolve one named auth profile from a credential store."""
    if isinstance(store_or_path, (str, Path)):
        store = load_auth_profile_store(Path(store_or_path))
    else:
        store = dict(store_or_path)

    profile_key = str(profile_name).strip()
    if not profile_key:
        raise ValueError("profile_name is required")

    profiles = _coerce_profile_map(store)
    raw_profile = profiles.get(profile_key)
    if not isinstance(raw_profile, Mapping):
        available = ", ".join(sorted(str(key) for key in profiles.keys()))
        raise KeyError(f"Unknown auth profile '{profile_key}'. Available: {available}")

    profile = dict(raw_profile)
    missing = [field for field in required_fields if not str(profile.get(field, "")).strip()]
    if missing:
        names = ", ".join(missing)
        raise ValueError(f"Auth profile '{profile_key}' is missing required fields: {names}")
    return profile
