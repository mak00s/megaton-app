from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def load_env_file(path: Path, *, override: bool = False) -> list[str]:
    """Load a simple .env file (KEY=VALUE) into os.environ."""
    loaded: list[str] = []
    if not path.exists():
        return loaded

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value
            loaded.append(key)

    return loaded


def load_env_files(paths: Iterable[Path], *, override: bool = False) -> dict[str, list[str]]:
    loaded_map: dict[str, list[str]] = {}
    for p in paths:
        loaded = load_env_file(p, override=override)
        if loaded:
            loaded_map[str(p)] = loaded
    return loaded_map


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable '{name}' is required")
    return value


def resolve_dotenv_paths(raw: str, *, notebooks_root: Path, project_root: Path) -> list[Path]:
    """Resolve comma-separated dotenv paths from notebooks/project roots."""
    out: list[Path] = []
    for token in str(raw).split(","):
        p = token.strip()
        if not p:
            continue

        p = p.replace("{notebooks_root}", str(notebooks_root))
        p = p.replace("{project_root}", str(project_root))

        path = Path(p).expanduser()
        if not path.is_absolute():
            path = (notebooks_root / path).resolve()
        else:
            path = path.resolve()
        out.append(path)
    return out
