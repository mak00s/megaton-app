"""Configuration model and loader for reusable audit tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None

DEFAULT_CONFIG_ROOT = Path("configs/audit/projects")
SUPPORTED_SUFFIXES = (".yaml", ".yml", ".json")


class ConfigError(ValueError):
    """Raised when audit project configuration is invalid."""


@dataclass(frozen=True)
class AdobeOAuthConfig:
    """Shared Adobe IMS OAuth client_credentials settings."""

    client_id: str | None = None
    client_secret: str | None = None
    client_id_env: str = "ADOBE_CLIENT_ID"
    client_secret_env: str = "ADOBE_CLIENT_SECRET"
    org_id_env: str = "ADOBE_ORG_ID"
    org_id: str | None = None
    scopes: str = (
        "openid,AdobeID,read_organizations,"
        "additional_info.projectedProductContext,additional_info.roles"
    )
    token_cache_file: str = "credentials/.adobe_token_cache.json"


@dataclass(frozen=True)
class GtmConfig:
    container_public_id: str
    variable_name: str = "Site Name"


@dataclass(frozen=True)
class AdobeTagsConfig:
    property_id: str
    mapping_data_element_name: str | None = None
    mapping_setting_key: str = "map"
    api_key_env: str = "ADOBE_TAGS_API_KEY"
    bearer_token_env: str = "ADOBE_TAGS_BEARER_TOKEN"
    ims_org_id_env: str = "ADOBE_TAGS_IMS_ORG_ID"
    base_url: str = "https://reactor.adobe.io"
    accept_header: str = "application/vnd.api+json;revision=1"
    content_type_header: str = "application/vnd.api+json"
    page_size: int = 100
    oauth: AdobeOAuthConfig | None = None


@dataclass(frozen=True)
class TagSourceConfig:
    source: str
    gtm: GtmConfig | None = None
    adobe_tags: AdobeTagsConfig | None = None


@dataclass(frozen=True)
class Ga4Config:
    property_id: str
    site_dimension: str = "customEvent:site"
    host_dimension: str = "hostName"
    page_dimension: str = "pagePath"
    sessions_metric: str = "sessions"


@dataclass(frozen=True)
class AdobeAnalyticsConfig:
    company_id: str
    rsid: str
    dimension: str
    metric: str = "occurrences"
    client_id: str | None = None
    client_secret: str | None = None
    org_id: str | None = None
    client_id_env: str = "ADOBE_CLIENT_ID"
    client_secret_env: str = "ADOBE_CLIENT_SECRET"
    org_id_env: str = "ADOBE_ORG_ID"
    scopes: str = (
        "openid,AdobeID,read_organizations,"
        "additional_info.projectedProductContext,additional_info.roles"
    )
    token_cache_file: str = "credentials/.adobe_token_cache.json"


@dataclass(frozen=True)
class AdobeTargetConfig:
    """Adobe Target Recommendations API settings."""

    tenant_id: str
    oauth: AdobeOAuthConfig | None = None
    base_url: str = "https://mc.adobe.io"
    accept_header: str = "application/vnd.adobe.target.v1+json"


@dataclass(frozen=True)
class AuditProjectConfig:
    project_id: str
    tag_source: TagSourceConfig
    ga4: Ga4Config
    aa: AdobeAnalyticsConfig | None = None
    fallback_mapping_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _expect_mapping(value: Any, *, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be an object")
    return value


def _expect_nonempty_str(data: dict[str, Any], key: str, *, path: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path}.{key} must be a non-empty string")
    return value.strip()


def _optional_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be a string")
    stripped = value.strip()
    return stripped or None


def resolve_project_config_path(project: str, config_root: str | Path | None = None) -> Path:
    """Resolve project config path from id or explicit file path."""
    raw = Path(project)
    if raw.exists() and raw.is_file():
        if raw.suffix.lower() not in SUPPORTED_SUFFIXES:
            raise ConfigError(
                f"Unsupported config suffix: {raw.suffix}. use one of {SUPPORTED_SUFFIXES}",
            )
        return raw

    root = Path(config_root) if config_root else DEFAULT_CONFIG_ROOT
    for suffix in SUPPORTED_SUFFIXES:
        candidate = root / f"{project}{suffix}"
        if candidate.exists() and candidate.is_file():
            return candidate

    searched = ", ".join(str(root / f"{project}{s}") for s in SUPPORTED_SUFFIXES)
    raise FileNotFoundError(f"Audit project config not found: {searched}")


def _read_config_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix == ".json":
        payload = json.loads(text)
    else:
        if yaml is None:
            raise ConfigError(
                "PyYAML is required for .yaml config files. "
                "Install with: pip install pyyaml",
            )
        payload = yaml.safe_load(text)

    if not isinstance(payload, dict):
        raise ConfigError(f"Root of {path} must be an object")
    return payload


def _parse_tag_source(node: Any) -> TagSourceConfig:
    if isinstance(node, str):
        source = node.strip().lower()
        node = {"source": source}
    else:
        node = _expect_mapping(node, path="tag_source")
        source = _expect_nonempty_str(node, "source", path="tag_source").lower()

    if source not in {"gtm", "adobe_tags"}:
        raise ConfigError("tag_source.source must be 'gtm' or 'adobe_tags'")

    gtm_cfg = None
    adobe_cfg = None

    if source == "gtm":
        gtm_node = _expect_mapping(node.get("gtm") or {}, path="tag_source.gtm")
        gtm_cfg = GtmConfig(
            container_public_id=_expect_nonempty_str(
                gtm_node,
                "container_public_id",
                path="tag_source.gtm",
            ),
            variable_name=(gtm_node.get("variable_name") or "Site Name").strip(),
        )

    if source == "adobe_tags":
        adobe_node = _expect_mapping(node.get("adobe_tags") or {}, path="tag_source.adobe_tags")
        page_size = adobe_node.get("page_size", 100)
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
            raise ConfigError("tag_source.adobe_tags.page_size must be a positive integer")

        # Parse optional OAuth config
        oauth_cfg: AdobeOAuthConfig | None = None
        oauth_node = adobe_node.get("oauth")
        if isinstance(oauth_node, dict):
            oauth_cfg = AdobeOAuthConfig(
                client_id=_optional_str(oauth_node, "client_id"),
                client_secret=_optional_str(oauth_node, "client_secret"),
                client_id_env=(oauth_node.get("client_id_env") or "ADOBE_CLIENT_ID").strip(),
                client_secret_env=(oauth_node.get("client_secret_env") or "ADOBE_CLIENT_SECRET").strip(),
                org_id_env=(oauth_node.get("org_id_env") or "ADOBE_ORG_ID").strip(),
                org_id=_optional_str(oauth_node, "org_id"),
                scopes=(
                    oauth_node.get("scopes")
                    or "openid,AdobeID,read_organizations,"
                    "additional_info.projectedProductContext,additional_info.roles"
                ).strip(),
                token_cache_file=(
                    oauth_node.get("token_cache_file")
                    or "credentials/.adobe_token_cache.json"
                ).strip(),
            )
        elif oauth_node is True:
            # Shorthand: oauth: true → use all defaults
            oauth_cfg = AdobeOAuthConfig()

        adobe_cfg = AdobeTagsConfig(
            property_id=_expect_nonempty_str(
                adobe_node,
                "property_id",
                path="tag_source.adobe_tags",
            ),
            mapping_data_element_name=_optional_str(adobe_node, "mapping_data_element_name"),
            mapping_setting_key=(adobe_node.get("mapping_setting_key") or "map").strip(),
            api_key_env=(adobe_node.get("api_key_env") or "ADOBE_TAGS_API_KEY").strip(),
            bearer_token_env=(adobe_node.get("bearer_token_env") or "ADOBE_TAGS_BEARER_TOKEN").strip(),
            ims_org_id_env=(adobe_node.get("ims_org_id_env") or "ADOBE_TAGS_IMS_ORG_ID").strip(),
            base_url=(adobe_node.get("base_url") or "https://reactor.adobe.io").strip(),
            accept_header=(
                adobe_node.get("accept_header")
                or "application/vnd.api+json;revision=1"
            ).strip(),
            content_type_header=(
                adobe_node.get("content_type_header")
                or "application/vnd.api+json"
            ).strip(),
            page_size=page_size,
            oauth=oauth_cfg,
        )

    return TagSourceConfig(source=source, gtm=gtm_cfg, adobe_tags=adobe_cfg)


def _parse_ga4(node: Any) -> Ga4Config:
    ga4_node = _expect_mapping(node, path="ga4")
    return Ga4Config(
        property_id=_expect_nonempty_str(ga4_node, "property_id", path="ga4"),
        site_dimension=(ga4_node.get("site_dimension") or "customEvent:site").strip(),
        host_dimension=(ga4_node.get("host_dimension") or "hostName").strip(),
        page_dimension=(ga4_node.get("page_dimension") or "pagePath").strip(),
        sessions_metric=(ga4_node.get("sessions_metric") or "sessions").strip(),
    )


def _parse_aa(node: Any) -> AdobeAnalyticsConfig:
    aa_node = _expect_mapping(node, path="aa")
    return AdobeAnalyticsConfig(
        company_id=_expect_nonempty_str(aa_node, "company_id", path="aa"),
        rsid=_expect_nonempty_str(aa_node, "rsid", path="aa"),
        dimension=_expect_nonempty_str(aa_node, "dimension", path="aa"),
        metric=(aa_node.get("metric") or "occurrences").strip(),
        client_id=_optional_str(aa_node, "client_id"),
        client_secret=_optional_str(aa_node, "client_secret"),
        org_id=_optional_str(aa_node, "org_id"),
        client_id_env=(aa_node.get("client_id_env") or "ADOBE_CLIENT_ID").strip(),
        client_secret_env=(aa_node.get("client_secret_env") or "ADOBE_CLIENT_SECRET").strip(),
        org_id_env=(aa_node.get("org_id_env") or "ADOBE_ORG_ID").strip(),
        scopes=(
            aa_node.get("scopes")
            or "openid,AdobeID,read_organizations,"
            "additional_info.projectedProductContext,additional_info.roles"
        ).strip(),
        token_cache_file=(
            aa_node.get("token_cache_file")
            or "credentials/.adobe_token_cache.json"
        ).strip(),
    )


def parse_project_config(payload: dict[str, Any], *, project_id: str | None = None) -> AuditProjectConfig:
    """Parse dict payload into a typed audit config object."""
    pid = project_id or str(payload.get("project_id") or "").strip()
    if not pid:
        raise ConfigError("project_id is required")

    if "tag_source" not in payload:
        raise ConfigError("tag_source is required")
    if "ga4" not in payload:
        raise ConfigError("ga4 is required")

    tag_source = _parse_tag_source(payload["tag_source"])
    ga4 = _parse_ga4(payload["ga4"])
    aa = _parse_aa(payload["aa"]) if payload.get("aa") else None

    fallback_mapping_path = payload.get("fallback_mapping_path")
    if fallback_mapping_path is not None and not isinstance(fallback_mapping_path, str):
        raise ConfigError("fallback_mapping_path must be a string")

    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ConfigError("metadata must be an object")

    return AuditProjectConfig(
        project_id=pid,
        tag_source=tag_source,
        ga4=ga4,
        aa=aa,
        fallback_mapping_path=fallback_mapping_path.strip() if isinstance(fallback_mapping_path, str) else None,
        metadata=metadata,
    )


def load_project_config(project: str, config_root: str | Path | None = None) -> AuditProjectConfig:
    """Load project audit config from JSON/YAML file."""
    path = resolve_project_config_path(project, config_root=config_root)
    payload = _read_config_file(path)
    return parse_project_config(payload, project_id=payload.get("project_id") or path.stem)
