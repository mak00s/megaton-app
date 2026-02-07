"""Credential path resolution helpers."""

from __future__ import annotations

import os
from pathlib import Path


DEFAULT_CREDS_DIR = Path("credentials")
DEFAULT_ENV_VAR = "MEGATON_CREDS_PATH"


def resolve_service_account_path(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> str:
    """Resolve service account json path from env var or credentials directory.

    Resolution order:
    1) If env_var is set:
       - file path: must exist
       - directory path: must contain exactly one *.json
    2) Fallback to default_dir, which must contain exactly one *.json
    """

    env_value = os.getenv(env_var, "").strip()
    if env_value:
        return _resolve_from_path(Path(env_value).expanduser(), env_var)

    return _resolve_single_json(Path(default_dir), f"{env_var} or default credentials directory")


def _resolve_from_path(path: Path, env_var: str) -> str:
    if path.is_dir():
        return _resolve_single_json(path, env_var)
    if not path.exists():
        raise FileNotFoundError(f"{env_var} points to missing path: {path}")
    return str(path)


def list_service_account_paths(
    env_var: str = DEFAULT_ENV_VAR,
    default_dir: Path | str = DEFAULT_CREDS_DIR,
) -> list[str]:
    """Return all valid service account JSON file paths.

    Resolution order:
    1) If env_var points to a single file → [that_file]
    2) If env_var points to a directory → all *.json in it (sorted)
    3) If env_var is not set → all *.json in default_dir (sorted)
    Returns empty list if no files found in default_dir.
    Raises FileNotFoundError if env_var points to missing path.
    """
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        path = Path(env_value).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"{env_var} points to missing path: {path}")
        if path.is_file():
            return [str(path)]
        return sorted(str(p) for p in path.glob("*.json") if p.is_file())

    directory = Path(default_dir)
    if not directory.exists():
        return []
    return sorted(str(p) for p in directory.glob("*.json") if p.is_file())


def _resolve_single_json(directory: Path, source_label: str) -> str:
    if not directory.exists():
        raise FileNotFoundError(f"Credentials directory not found: {directory}")

    files = sorted(p for p in directory.glob("*.json") if p.is_file())
    if len(files) == 1:
        return str(files[0])
    if not files:
        raise FileNotFoundError(
            f"No service account JSON found in {directory}. "
            f"Set {DEFAULT_ENV_VAR} to a JSON file path."
        )
    raise RuntimeError(
        f"Multiple service account JSON files found in {directory}: "
        f"{', '.join(str(p.name) for p in files)}. "
        f"Set {DEFAULT_ENV_VAR} to the target JSON file path."
    )

