"""Shared helpers for Adobe Analytics API follow-up verification scripts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from megaton_lib.audit.config import AdobeAnalyticsConfig
from megaton_lib.audit.providers.analytics.aa import AdobeAnalyticsClient

from .followups import finalize_followup_verification


DEFAULT_RUNTIME_SCOPES = (
    "openid,AdobeID,read_organizations,"
    "additional_info.projectedProductContext,additional_info.roles"
)


def resolve_adobe_credentials_path(
    explicit: str | Path | None = None,
    *,
    candidates: list[str | Path] | tuple[str | Path, ...] = (),
) -> Path:
    """Resolve an Adobe credential JSON path from an explicit path or candidates."""
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if path.exists():
            return path
        raise FileNotFoundError(f"Adobe config file not found: {path}")

    resolved_candidates = [Path(item).expanduser().resolve() for item in candidates]
    for path in resolved_candidates:
        if path.exists():
            return path
    joined = ", ".join(str(path) for path in resolved_candidates)
    raise FileNotFoundError(f"Adobe config file not found. Tried: {joined}")


def load_adobe_credentials_file(config_path: str | Path) -> dict[str, Any]:
    """Load one Adobe credential JSON file."""
    path = Path(config_path).expanduser().resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Adobe config JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Adobe config must be object JSON: {path}")
    return payload


def build_adobe_analytics_client(
    *,
    adobe_config_path: str | Path,
    company_id: str,
    org_id: str,
    rsid: str,
    dimension: str = "daterangeday",
    metric: str = "revenue",
    token_cache_file: str | Path | None = None,
    client_id_env: str = "ADOBE_CLIENT_ID",
    client_secret_env: str = "ADOBE_CLIENT_SECRET",
    org_id_env: str = "ADOBE_ORG_ID",
    default_scopes: str = DEFAULT_RUNTIME_SCOPES,
) -> AdobeAnalyticsClient:
    """Build an Adobe Analytics client from a local credential JSON plus env fallbacks."""
    payload = load_adobe_credentials_file(adobe_config_path)
    client_id = (
        str(payload.get("client_id", "")).strip() or os.getenv("ADOBE_CLIENT_ID", "").strip()
    )
    client_secret = (
        str(payload.get("client_secret", "")).strip()
        or os.getenv("ADOBE_CLIENT_SECRET", "").strip()
    )
    resolved_company = company_id.strip() or str(payload.get("company_id", "")).strip()
    resolved_org = (
        org_id.strip()
        or str(payload.get("org_id", "")).strip()
        or os.getenv("ADOBE_ORG_ID", "").strip()
    )
    scopes = str(payload.get("scopes", "")).strip() or default_scopes

    if not client_id:
        raise RuntimeError(f"Adobe client_id is missing in {adobe_config_path}")
    if not client_secret:
        raise RuntimeError(f"Adobe client_secret is missing in {adobe_config_path}")
    if not resolved_company:
        raise RuntimeError("Adobe company_id is empty.")
    if not resolved_org:
        raise RuntimeError("Adobe org_id is empty.")

    os.environ[client_id_env] = client_id
    os.environ[client_secret_env] = client_secret
    os.environ[org_id_env] = resolved_org

    cache_path = (
        Path(token_cache_file).expanduser().resolve()
        if token_cache_file is not None
        else Path("credentials/.adobe_token_cache.json").expanduser().resolve()
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    return AdobeAnalyticsClient(
        AdobeAnalyticsConfig(
            company_id=resolved_company,
            rsid=rsid,
            dimension=dimension,
            metric=metric,
            org_id=resolved_org,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
            org_id_env=org_id_env,
            scopes=scopes,
            token_cache_file=str(cache_path),
        )
    )


def run_aa_api_followup_verifier(
    *,
    json_path: str | Path,
    pending_file: str | Path,
    verification_type: str,
    project: str,
    scenario: str,
    verifier,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Load one verification JSON, run a verifier callback, and finalize the task."""
    path = Path(json_path).expanduser().resolve()
    verification = json.loads(path.read_text(encoding="utf-8"))
    outcome = verifier(verification)
    if isinstance(outcome, tuple):
        result, extra = outcome
    else:
        result, extra = outcome, None
    task = finalize_followup_verification(
        verification,
        json_path=path,
        pending_file=pending_file,
        verification_type=verification_type,
        result=str(result),
        project=project,
        scenario=scenario,
        extra=extra,
    )
    return verification, task


__all__ = [
    "DEFAULT_RUNTIME_SCOPES",
    "build_adobe_analytics_client",
    "load_adobe_credentials_file",
    "resolve_adobe_credentials_path",
    "run_aa_api_followup_verifier",
]
