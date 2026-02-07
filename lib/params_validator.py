"""input/params.json のバリデーション"""
from __future__ import annotations

from datetime import datetime
from typing import Any

SCHEMA_VERSION = "1.0"
MAX_LIMIT = 100000
DEFAULT_LIMIT = 1000
ALLOWED_SOURCES = {"ga4", "gsc", "bigquery"}


def _err(code: str, message: str, path: str, hint: str) -> dict[str, str]:
    return {
        "error_code": code,
        "message": message,
        "path": path,
        "hint": hint,
    }


def _is_valid_date(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def validate_params(data: Any) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """クエリパラメータを検証して正規化結果を返す。"""
    errors: list[dict[str, str]] = []
    if not isinstance(data, dict):
        return None, [_err("INVALID_TYPE", "Root must be a JSON object", "$", "Use an object like {\"schema_version\": \"1.0\", ...}")]

    normalized = dict(data)

    source = normalized.get("source")
    if isinstance(source, str):
        normalized["source"] = source.lower()

    schema_version = normalized.get("schema_version")
    if schema_version != SCHEMA_VERSION:
        errors.append(
            _err(
                "INVALID_SCHEMA_VERSION",
                f"schema_version must be '{SCHEMA_VERSION}'",
                "$.schema_version",
                f"Set schema_version to '{SCHEMA_VERSION}'.",
            )
        )

    source = normalized.get("source")
    if source not in ALLOWED_SOURCES:
        errors.append(
            _err(
                "INVALID_SOURCE",
                "source must be one of: ga4, gsc, bigquery",
                "$.source",
                "Set source to one valid value.",
            )
        )
        return None, errors

    common_required = {"schema_version", "source"}
    source_required = {
        "ga4": {"property_id", "date_range", "dimensions", "metrics"},
        "gsc": {"site_url", "date_range", "dimensions"},
        "bigquery": {"project_id", "sql"},
    }
    source_optional = {
        "ga4": {"filter_d", "limit"},
        "gsc": {"filter", "limit"},
        "bigquery": set(),
    }

    required_keys = common_required | source_required[source]
    allowed_keys = required_keys | source_optional[source]

    for key in sorted(required_keys):
        if key not in normalized:
            errors.append(
                _err(
                    "MISSING_REQUIRED",
                    f"Missing required field: {key}",
                    "$",
                    f"Add '{key}' to params.json.",
                )
            )

    extra_keys = sorted(set(normalized.keys()) - allowed_keys)
    for key in extra_keys:
        errors.append(
            _err(
                "UNKNOWN_FIELD",
                f"Unknown field: {key}",
                f"$.{key}",
                "Remove unsupported fields for the selected source.",
            )
        )

    if "date_range" in normalized:
        date_range = normalized["date_range"]
        if not isinstance(date_range, dict):
            errors.append(
                _err(
                    "INVALID_TYPE",
                    "date_range must be an object",
                    "$.date_range",
                    "Use {\"start\":\"YYYY-MM-DD\",\"end\":\"YYYY-MM-DD\"}.",
                )
            )
        else:
            date_extra = sorted(set(date_range.keys()) - {"start", "end"})
            for key in date_extra:
                errors.append(
                    _err(
                        "UNKNOWN_FIELD",
                        f"Unknown date_range field: {key}",
                        f"$.date_range.{key}",
                        "Only start/end are allowed.",
                    )
                )
            if not _is_valid_date(date_range.get("start")):
                errors.append(
                    _err(
                        "INVALID_DATE",
                        "start must be YYYY-MM-DD",
                        "$.date_range.start",
                        "Use an absolute date like 2026-02-01.",
                    )
                )
            if not _is_valid_date(date_range.get("end")):
                errors.append(
                    _err(
                        "INVALID_DATE",
                        "end must be YYYY-MM-DD",
                        "$.date_range.end",
                        "Use an absolute date like 2026-02-03.",
                    )
                )

    if "dimensions" in normalized:
        dims = normalized["dimensions"]
        if not isinstance(dims, list) or not all(isinstance(v, str) and v for v in dims):
            errors.append(
                _err(
                    "INVALID_TYPE",
                    "dimensions must be a string array",
                    "$.dimensions",
                    "Example: [\"date\"]",
                )
            )

    if source == "ga4" and "metrics" in normalized:
        metrics = normalized["metrics"]
        if not isinstance(metrics, list) or not all(isinstance(v, str) and v for v in metrics):
            errors.append(
                _err(
                    "INVALID_TYPE",
                    "metrics must be a string array",
                    "$.metrics",
                    "Example: [\"sessions\"]",
                )
            )

    for key in ("property_id", "site_url", "project_id", "sql", "filter_d", "filter"):
        if key in normalized and not isinstance(normalized[key], str):
            errors.append(
                _err(
                    "INVALID_TYPE",
                    f"{key} must be a string",
                    f"$.{key}",
                    f"Set {key} to a string value.",
                )
            )

    if source in {"ga4", "gsc"}:
        if "limit" not in normalized:
            normalized["limit"] = DEFAULT_LIMIT
        limit = normalized.get("limit")
        if not isinstance(limit, int):
            errors.append(
                _err(
                    "INVALID_TYPE",
                    "limit must be an integer",
                    "$.limit",
                    f"Use 1-{MAX_LIMIT}.",
                )
            )
        elif limit < 1 or limit > MAX_LIMIT:
            errors.append(
                _err(
                    "OUT_OF_RANGE",
                    f"limit must be between 1 and {MAX_LIMIT}",
                    "$.limit",
                    f"Use 1-{MAX_LIMIT}.",
                )
            )

    if errors:
        return None, errors
    return normalized, []
