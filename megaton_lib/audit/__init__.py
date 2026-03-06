"""Reusable audit framework for GA4/AA and GTM/Adobe Tags."""

from __future__ import annotations

from .config import (
    AdobeAnalyticsConfig,
    AdobeTagsConfig,
    AuditProjectConfig,
    ConfigError,
    Ga4Config,
    GtmConfig,
    TagSourceConfig,
    load_project_config,
    parse_project_config,
    resolve_project_config_path,
)


def __getattr__(name: str):
    """Lazy-load runtime-heavy symbols to avoid import cycles."""
    if name == "AuditRunner":
        from .runner import AuditRunner

        return AuditRunner
    raise AttributeError(name)

__all__ = [
    "AdobeAnalyticsConfig",
    "AdobeTagsConfig",
    "AuditProjectConfig",
    "AuditRunner",
    "ConfigError",
    "Ga4Config",
    "GtmConfig",
    "TagSourceConfig",
    "load_project_config",
    "parse_project_config",
    "resolve_project_config_path",
]
