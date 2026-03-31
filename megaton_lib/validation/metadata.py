"""Shared metadata helpers for validation results."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

from .playwright_pages import (
    GtmPreviewOverride,
    TagsLaunchOverride,
    describe_gtm_preview_override,
    describe_tags_launch_override,
)


def build_validation_run_metadata(
    *,
    execution_mode: str = "live",
    project: str | None = None,
    scenario: str | None = None,
    config_path: str | Path | None = None,
    gtm_preview: GtmPreviewOverride | None = None,
    tags_override: TagsLaunchOverride | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build stable metadata fields for saved validation results."""
    metadata: dict[str, Any] = {
        "executionMode": execution_mode,
    }
    if project:
        metadata["project"] = str(project)
    if scenario:
        metadata["scenario"] = str(scenario)
    if config_path:
        metadata["configPath"] = str(config_path)

    gtm_preview_meta = describe_gtm_preview_override(gtm_preview)
    if gtm_preview_meta is not None:
        metadata["gtmPreview"] = gtm_preview_meta

    tags_override_meta = describe_tags_launch_override(tags_override)
    if tags_override_meta is not None:
        metadata["tagsOverride"] = tags_override_meta

    if extra:
        metadata.update(dict(extra))

    return metadata


def write_validation_json(
    path: str | Path,
    payload: Mapping[str, Any] | list[Any],
    *,
    ensure_ascii: bool = False,
    indent: int = 2,
    trailing_newline: bool = True,
) -> Path:
    """Write validation JSON with consistent formatting and parent creation."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent)
    if trailing_newline:
        text += "\n"
    output_path.write_text(text, encoding="utf-8")
    return output_path
