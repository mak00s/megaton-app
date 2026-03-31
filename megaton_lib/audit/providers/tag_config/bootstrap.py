"""Bootstrap helpers for Adobe Tags config setup."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from ...config import AdobeOAuthConfig, AdobeTagsConfig, DEFAULT_ADOBE_SCOPES


def load_env_file(path: str | Path) -> None:
    """Load simple ``KEY=VALUE`` pairs into ``os.environ`` if present."""
    env_path = Path(path).expanduser().resolve()
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def merge_adobe_scopes(
    base_scopes: str | None,
    *,
    required: tuple[str, ...] = ("read_organizations", "additional_info.roles"),
) -> str:
    """Return a comma-separated scope string with required scopes appended."""
    source = (base_scopes or DEFAULT_ADOBE_SCOPES).strip()
    ordered = [item.strip() for item in source.split(",") if item.strip()]
    seen = set(ordered)
    for item in required:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ",".join(ordered)


def seed_adobe_oauth_env(
    *,
    payload: Mapping[str, Any] | None = None,
    creds_file: str | Path | None = None,
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
) -> tuple[str, str, str]:
    """Resolve Adobe OAuth values from explicit args, env, JSON file, and optional payload.

    Resolution order (first non-empty wins):
    1. Explicit function arguments (``client_id``, ``client_secret``, ``org_id``)
    2. Environment variables (``ADOBE_CLIENT_ID``, etc.)
    3. JSON credential file (``creds_file``) — loaded via ``load_adobe_oauth_credentials``
    4. ``payload`` dict
    """
    merged_payload: dict[str, Any] = dict(payload or {})

    if creds_file is not None:
        creds_path = Path(creds_file).expanduser().resolve()
        if creds_path.exists():
            from ....credentials import load_adobe_oauth_credentials  # noqa: WPS433

            file_creds = load_adobe_oauth_credentials(creds_path)
            for key in ("client_id", "client_secret", "org_id"):
                if file_creds.get(key) and not merged_payload.get(key):
                    merged_payload[key] = file_creds[key]

    payload = merged_payload

    resolved_client_id = (
        client_id.strip()
        or os.getenv(client_id_env, "").strip()
        or str(payload.get("client_id", "")).strip()
    )
    resolved_client_secret = (
        client_secret.strip()
        or os.getenv(client_secret_env, "").strip()
        or str(payload.get("client_secret", "")).strip()
    )
    resolved_org_id = (
        org_id.strip()
        or os.getenv(org_id_env, "").strip()
        or str(payload.get("org_id", "")).strip()
    )

    if not resolved_client_id:
        raise RuntimeError(
            f"Adobe client_id is missing: set {client_id_env} env var, "
            "pass client_id explicitly, or provide a creds_file"
        )
    if not resolved_client_secret:
        raise RuntimeError(
            f"Adobe client_secret is missing: set {client_secret_env} env var, "
            "pass client_secret explicitly, or provide a creds_file"
        )

    os.environ[client_id_env] = resolved_client_id
    os.environ[client_secret_env] = resolved_client_secret
    if resolved_org_id:
        os.environ[org_id_env] = resolved_org_id

    return resolved_client_id, resolved_client_secret, resolved_org_id


def build_tags_config(
    *,
    property_id: str,
    page_size: int = 100,
    scopes: str | None = None,
    token_cache_file: str | Path = "credentials/.adobe_token_cache.json",
    payload: Mapping[str, Any] | None = None,
    creds_file: str | Path | None = None,
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
) -> AdobeTagsConfig:
    """Build ``AdobeTagsConfig`` after resolving OAuth settings."""
    if not property_id.strip():
        raise RuntimeError("Adobe Tags property_id is required")

    payload = payload or {}
    _, _, resolved_org_id = seed_adobe_oauth_env(
        payload=payload,
        creds_file=creds_file,
        client_id=client_id,
        client_secret=client_secret,
        org_id=org_id,
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        org_id_env=org_id_env,
    )

    cache_path = Path(token_cache_file).expanduser().resolve()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    oauth = AdobeOAuthConfig(
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        org_id_env=org_id_env,
        org_id=resolved_org_id or None,
        scopes=merge_adobe_scopes(scopes or str(payload.get("scopes", "")).strip()),
        token_cache_file=str(cache_path),
    )

    return AdobeTagsConfig(
        property_id=property_id.strip(),
        oauth=oauth,
        page_size=page_size,
    )
