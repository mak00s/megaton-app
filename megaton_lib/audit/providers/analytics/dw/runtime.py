"""Runtime helpers for Adobe Data Warehouse client setup."""

from __future__ import annotations

from pathlib import Path

from megaton_lib.audit.config import DEFAULT_ADOBE_SCOPES
from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient
from megaton_lib.credentials import load_adobe_oauth_credentials

from .api import AdobeDataWarehouseClient


def _default_token_cache_file(source_path: str | None = None) -> str:
    if source_path:
        stem = Path(source_path).stem.strip() or "adobe"
        return str(Path("credentials") / f".adobe_token_cache_{stem}.json")
    return "credentials/.adobe_token_cache.json"


def build_adobe_auth(
    *,
    creds_file: str = "",
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    scopes: str = "",
    token_cache_file: str = "",
) -> AdobeOAuthClient:
    """Build Adobe OAuth auth with explicit args, creds file, and env fallback."""
    file_creds: dict[str, str] = {}
    if creds_file.strip():
        file_creds = load_adobe_oauth_credentials(creds_file)

    resolved_client_id = client_id.strip() or file_creds.get("client_id", "")
    resolved_client_secret = client_secret.strip() or file_creds.get("client_secret", "")
    resolved_org_id = org_id.strip() or file_creds.get("org_id", "")
    resolved_scopes = scopes.strip() or file_creds.get("scopes", "") or DEFAULT_ADOBE_SCOPES
    resolved_cache = token_cache_file.strip() or _default_token_cache_file(file_creds.get("source_path"))

    cache_path = Path(resolved_cache).expanduser().resolve()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    return AdobeOAuthClient(
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
        org_id=resolved_org_id,
        scopes=resolved_scopes,
        token_cache_file=str(cache_path),
    )


def build_dw_client(
    *,
    company_id: str,
    creds_file: str = "",
    client_id: str = "",
    client_secret: str = "",
    org_id: str = "",
    scopes: str = "",
    token_cache_file: str = "",
) -> AdobeDataWarehouseClient:
    """Build a Data Warehouse client from Adobe credential inputs."""
    auth = build_adobe_auth(
        creds_file=creds_file,
        client_id=client_id,
        client_secret=client_secret,
        org_id=org_id,
        scopes=scopes,
        token_cache_file=token_cache_file,
    )
    return AdobeDataWarehouseClient(auth=auth, company_id=company_id)
