"""Pure helpers for building Streamlit query/pipeline parameters."""
from __future__ import annotations

from datetime import date

import pandas as pd


def parse_gsc_filter(filter_str: str) -> list[dict] | None:
    """Parse GSC filter expression into API payload list."""
    if not filter_str or not filter_str.strip():
        return None
    filters = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) == 3:
            filters.append(
                {
                    "dimension": parts[0],
                    "operator": parts[1],
                    "expression": parts[2],
                }
            )
    return filters if filters else None


def detect_url_columns(df: pd.DataFrame) -> list[str]:
    """Detect object columns likely containing URLs."""
    url_cols: list[str] = []
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().head(5).astype(str)
        if sample.str.startswith("http").any():
            url_cols.append(col)
    return url_cols


def build_transform_expression(
    *,
    has_date_col: bool,
    url_cols: list[str],
    tf_date: bool,
    tf_url_decode: bool,
    tf_strip_qs: bool,
    keep_params: str,
    tf_path_only: bool,
) -> str | None:
    """Build transform expression from UI state."""
    parts: list[str] = []
    if tf_date and has_date_col:
        parts.append("date:date_format")
    if tf_url_decode and url_cols:
        parts.extend(f"{c}:url_decode" for c in url_cols)
    if tf_strip_qs and url_cols:
        kp = keep_params.strip()
        if kp:
            parts.extend(f"{c}:strip_qs:{kp}" for c in url_cols)
        else:
            parts.extend(f"{c}:strip_qs" for c in url_cols)
    if tf_path_only and url_cols:
        parts.extend(f"{c}:path_only" for c in url_cols)
    return ",".join(parts) if parts else None


def build_pipeline_kwargs(
    *,
    transform_expr: str | None,
    where_expr: str,
    selected_cols: list[str],
    group_cols: list[str],
    agg_map: dict[str, str],
    head_val: int,
) -> tuple[dict, list[str]]:
    """Build pipeline kwargs and derived aggregate columns."""
    kwargs: dict = {}
    if transform_expr:
        kwargs["transform"] = transform_expr
    if where_expr.strip():
        kwargs["where"] = where_expr.strip()
    if selected_cols:
        kwargs["columns"] = ",".join(selected_cols)

    agg_exprs: list[str] = []
    for col, func in agg_map.items():
        if func and func != "（なし）":
            agg_exprs.append(f"{func}:{col}")
    if group_cols and agg_exprs:
        kwargs["group_by"] = ",".join(group_cols)
        kwargs["aggregate"] = ",".join(agg_exprs)

    if head_val > 0:
        kwargs["head"] = head_val

    derived_cols = [f"{x.split(':')[0]}_{x.split(':')[1]}" for x in agg_exprs]
    return kwargs, derived_cols


def build_agent_params(
    *,
    source: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
    property_id: str = "",
    site_url: str = "",
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    filter_d: str = "",
    gsc_filter: str = "",
    bq_project: str = "",
    sql: str = "",
) -> dict:
    """Build sidebar JSON params payload for AI agent handoff."""
    dimensions = dimensions or []
    metrics = metrics or []

    if source == "GA4":
        return {
            "source": "ga4",
            "property_id": property_id,
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d") if start_date else "",
                "end": end_date.strftime("%Y-%m-%d") if end_date else "",
            },
            "dimensions": dimensions,
            "metrics": metrics,
            "filter_d": filter_d,
            "limit": limit,
        }
    if source == "GSC":
        return {
            "source": "gsc",
            "site_url": site_url,
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d") if start_date else "",
                "end": end_date.strftime("%Y-%m-%d") if end_date else "",
            },
            "dimensions": dimensions,
            "filter": gsc_filter,
            "limit": limit,
        }
    return {
        "source": "bigquery",
        "project_id": bq_project,
        "sql": sql,
    }
