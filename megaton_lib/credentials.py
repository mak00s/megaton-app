"""Credential discovery helpers for Google service accounts and Adobe OAuth."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


DEFAULT_CREDS_DIR = Path("credentials")
DEFAULT_ENV_VAR = "MEGATON_CREDS_PATH"
DEFAULT_ADOBE_ENV_VAR = "ADOBE_CREDS_PATH"


def _find_dir_upwards(dir_name: str, *, start: Path | None = None) -> Path | None:
    """Find a directory named ``dir_name`` by walking up from CWD (or ``start``)."""
    d = (start or Path.cwd()).resolve()
    while True:
        candidate = d / dir_name
        if candidate.exists() and candidate.is_dir():
            return candidate
        if d == d.parent:
            return None
        d = d.parent


def _resolve_default_dir(default_dir: Path | str) -> Path:
    """Resolve default credentials directory."""
    directory = Path(default_dir)
    if directory.is_absolute():
        return directory
    if directory.exists():
        return directory
    if directory == DEFAULT_CREDS_DIR:
        found = _find_dir_upwards(directory.name)
        if found is not None:
            return found
        pkg_creds = Path(__file__).resolve().parent.parent / directory.name
        if pkg_creds.exists() and pkg_creds.is_dir():
            return pkg_creds
    return directory


def _load_json_if_possible(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _is_service_account_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("type", "")).strip() == "service_account":
        return True
    return bool(
        str(payload.get("client_email", "")).strip()
        and str(payload.get("private_key", "")).strip()
    )


def _normalize_adobe_org_id(payload: dict[str, Any]) -> str:
    for key in ("org_id", "ims_org_id", "imsOrgId"):
        value = str(payload.get(key, "")).strip()
        if value:
            return value
    return ""


def _is_adobe_oauth_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(
        str(payload.get("client_id", "")).strip()
        and str(payload.get("client_secret", "")).strip()
        and _normalize_adobe_org_id(payload)
    )


def _list_matching_json_paths(
    *,
    env_var: str,
    default_dir: Path | str,
    predicate,
) -> list[str]:
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        path = Path(env_value).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"{env_var} points to missing path: {path}")
        if path.is_file():
            payload = _load_json_if_possible(path)
            return [str(path)] if predicate(payload) else []
        files = sorted(p for p in path.glob("*.json") if p.is_file())
        return [str(p) for p in files if predicate(_load_json_if_possible(p))]

    directory = _resolve_default_dir(default_dir)
    if not directory.exists():
        return []
    files = sorted(p for p in directory.glob("*.json") if p.is_file())
    return [str(p) for p in files if predicate(_load_json_if_possible(p))]


def resolve_service_account_path(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> str:
    """Resolve one Google service account JSON path.

    Resolution order:
    1) If env_var is set:
       - file path: must be a service-account JSON
       - directory path: must contain exactly one service-account JSON
    2) Fallback to default_dir, which must contain exactly one service-account JSON
    """
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        return _resolve_single_json(
            Path(env_value).expanduser(),
            env_var,
            predicate=_is_service_account_payload,
            kind_label="service account JSON",
        )

    return _resolve_single_json(
        _resolve_default_dir(default_dir),
        f"{env_var} or default credentials directory",
        predicate=_is_service_account_payload,
        kind_label="service account JSON",
    )


def list_service_account_paths(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> list[str]:
    """Return all valid Google service-account JSON file paths."""
    return _list_matching_json_paths(
        env_var=env_var,
        default_dir=default_dir,
        predicate=_is_service_account_payload,
    )


def list_adobe_oauth_paths(
    env_var: str = DEFAULT_ADOBE_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> list[str]:
    """Return all valid Adobe OAuth JSON file paths."""
    return _list_matching_json_paths(
        env_var=env_var,
        default_dir=default_dir,
        predicate=_is_adobe_oauth_payload,
    )


def load_adobe_oauth_credentials(path: str | Path) -> dict[str, str]:
    """Load one Adobe OAuth JSON file with normalized keys."""
    raw_path = Path(path).expanduser()
    if not raw_path.exists():
        raise FileNotFoundError(f"Adobe OAuth credentials file not found: {raw_path}")

    payload = _load_json_if_possible(raw_path)
    if not _is_adobe_oauth_payload(payload):
        raise ValueError(f"Invalid Adobe OAuth credentials file: {raw_path}")

    assert payload is not None
    scopes = str(payload.get("scopes") or payload.get("scope") or "").strip()
    return {
        "client_id": str(payload.get("client_id", "")).strip(),
        "client_secret": str(payload.get("client_secret", "")).strip(),
        "org_id": _normalize_adobe_org_id(payload),
        "scopes": scopes,
        "source_path": str(raw_path),
    }


def _resolve_single_json(
    path: Path,
    source_label: str,
    *,
    predicate,
    kind_label: str,
) -> str:
    if path.is_file():
        payload = _load_json_if_possible(path)
        if predicate(payload):
            return str(path)
        raise RuntimeError(f"{source_label} is not a valid {kind_label}: {path}")

    if not path.exists():
        raise FileNotFoundError(f"Credentials directory not found: {path}")

    files = sorted(
        p for p in path.glob("*.json")
        if p.is_file() and predicate(_load_json_if_possible(p))
    )
    if len(files) == 1:
        return str(files[0])
    if not files:
        raise FileNotFoundError(
            f"No {kind_label} found in {path}. "
            f"Set {source_label} to a JSON file path.",
        )
    raise RuntimeError(
        f"Multiple {kind_label}s found in {path}: "
        f"{', '.join(str(p.name) for p in files)}. "
        f"Set {source_label} to the target JSON file path.",
    )


__all__ = [
    "DEFAULT_ADOBE_ENV_VAR",
    "DEFAULT_CREDS_DIR",
    "DEFAULT_ENV_VAR",
    "list_adobe_oauth_paths",
    "list_service_account_paths",
    "load_adobe_oauth_credentials",
    "resolve_service_account_path",
]
