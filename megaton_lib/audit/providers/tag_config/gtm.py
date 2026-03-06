"""GTM Tag configuration provider."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from megaton_lib.credentials import list_service_account_paths
from megaton_lib.audit.config import GtmConfig


def parse_regex_table_variable(var: Mapping[str, Any]) -> dict[str, str]:
    """Parse GTM regex table (remm) variable definition into mapping."""
    mapping: dict[str, str] = {}

    for param in var.get("parameter", []):
        if param.get("key") == "map" and str(param.get("type", "")).lower() == "list":
            for entry in param.get("list", []):
                pattern = None
                output = None
                for item in entry.get("map", []):
                    if item.get("key") == "key":
                        pattern = item.get("value")
                    elif item.get("key") == "value":
                        output = item.get("value")
                if pattern and output:
                    mapping[str(pattern)] = str(output)

    for param in var.get("parameter", []):
        if param.get("key") == "defaultValue":
            default_value = str(param.get("value", "")).strip()
            if default_value:
                mapping["__default__"] = default_value
            break

    return mapping


def _extract_mapping_from_container(
    service: Any,
    *,
    container_path: str,
    variable_name: str,
) -> tuple[dict[str, str], str | None]:
    workspaces = (
        service.accounts()
        .containers()
        .workspaces()
        .list(parent=container_path)
        .execute()
        .get("workspace", [])
    )
    for ws in workspaces:
        workspace_path = ws.get("path")
        if not workspace_path:
            continue
        variables = (
            service.accounts()
            .containers()
            .workspaces()
            .variables()
            .list(parent=workspace_path)
            .execute()
            .get("variable", [])
        )
        for var in variables:
            if var.get("name") == variable_name and var.get("type") == "remm":
                return parse_regex_table_variable(var), workspace_path
    return {}, None


def fetch_gtm_mapping(
    config: GtmConfig,
    *,
    credentials_paths: Sequence[str] | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Fetch GTM regex mapping via Tag Manager API v2."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "google-api-python-client is required for GTM audit. "
            "Install it with: pip install google-api-python-client",
        ) from exc

    paths = list(credentials_paths) if credentials_paths else list_service_account_paths()
    if not paths:
        raise RuntimeError("No service account JSON found for GTM API access")

    scopes = ["https://www.googleapis.com/auth/tagmanager.readonly"]
    errors: list[str] = []

    for creds_path in paths:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=scopes,
            )
            service = build("tagmanager", "v2", credentials=credentials)
            accounts = service.accounts().list().execute().get("account", [])

            for account in accounts:
                account_path = account.get("path")
                if not account_path:
                    continue
                containers = (
                    service.accounts()
                    .containers()
                    .list(parent=account_path)
                    .execute()
                    .get("container", [])
                )
                for container in containers:
                    if container.get("publicId") != config.container_public_id:
                        continue

                    container_path = container.get("path")
                    if not container_path:
                        continue

                    mapping, workspace_path = _extract_mapping_from_container(
                        service,
                        container_path=container_path,
                        variable_name=config.variable_name,
                    )
                    if mapping:
                        return mapping, {
                            "provider": "gtm",
                            "credentials_path": creds_path,
                            "container_public_id": config.container_public_id,
                            "container_path": container_path,
                            "workspace_path": workspace_path,
                            "variable_name": config.variable_name,
                            "mapping_count": len(mapping),
                        }

            errors.append(f"{creds_path}: container/variable not found")
        except Exception as exc:  # pragma: no cover - network/env dependent
            errors.append(f"{creds_path}: {exc}")

    joined = " | ".join(errors)
    raise RuntimeError(
        "Failed to fetch GTM mapping from all credentials. "
        f"container={config.container_public_id}, variable={config.variable_name}, errors={joined}",
    )
