"""Reusable query execution helpers for params-style configs.

This module provides the library form of the common ``scripts/query.py`` path
so analysis repos do not need to shell out to the CLI for simple jobs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

import megaton_lib.site_aliases as site_aliases
from megaton_lib.megaton_client import query_aa, query_bq, query_ga4, query_gsc
from megaton_lib.params_validator import validate_params
from megaton_lib.result_inspector import apply_pipeline


@dataclass(frozen=True)
class QueryResult:
    """Query result plus lightweight execution metadata."""

    df: pd.DataFrame
    header_lines: list[str]
    params: dict[str, Any]
    pipeline: dict[str, Any] | None = None


@dataclass(frozen=True)
class QueryExecutors:
    """Backend query callables used by params execution."""

    query_ga4: Callable[..., pd.DataFrame]
    query_gsc: Callable[..., pd.DataFrame]
    query_aa: Callable[..., pd.DataFrame]
    query_bq: Callable[..., pd.DataFrame]


def default_query_executors() -> QueryExecutors:
    """Return executors bound to the current module-level query functions."""
    return QueryExecutors(
        query_ga4=query_ga4,
        query_gsc=query_gsc,
        query_aa=query_aa,
        query_bq=query_bq,
    )


def parse_gsc_filter(filter_str: str) -> list[dict[str, str]] | None:
    """Parse a CLI-style GSC filter expression."""
    if not filter_str:
        return None

    filters: list[dict[str, str]] = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) != 3:
            raise ValueError(
                f"Invalid filter format: {part}. Expected dimension:operator:expression"
            )
        filters.append(
            {
                "dimension": parts[0],
                "operator": parts[1],
                "expression": parts[2],
            }
        )
    return filters


def resolve_and_validate_params(raw: dict[str, Any]) -> dict[str, Any]:
    """Resolve site aliases and validate a params dictionary."""
    resolved = site_aliases.resolve_site_alias(raw)
    params, errors = validate_params(resolved)
    if errors:
        details = "; ".join(
            f"{err.get('path')}: {err.get('message')}" for err in errors
        )
        raise ValueError(f"Params validation failed: {details}")
    return params


def load_query_params(params_path: str | Path) -> dict[str, Any]:
    """Load, resolve, and validate params JSON from disk."""
    path = Path(params_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("params JSON must contain an object")
    return resolve_and_validate_params(raw)


def execute_query_params(
    params: dict[str, Any],
    *,
    executors: QueryExecutors | None = None,
    gsc_page_to_path: bool | None = None,
) -> QueryResult:
    """Execute validated query params without applying pipeline/save config."""
    runners = executors or default_query_executors()
    source = params["source"]
    if source == "ga4":
        start_date = params["date_range"]["start"]
        end_date = params["date_range"]["end"]
        df = runners.query_ga4(
            property_id=params["property_id"],
            start_date=start_date,
            end_date=end_date,
            dimensions=params["dimensions"],
            metrics=params["metrics"],
            filter_d=params.get("filter_d"),
            limit=params.get("limit", 1000),
        )
        return QueryResult(
            df=df,
            header_lines=[
                f"期間: {start_date} 〜 {end_date}",
                f"プロパティ: {params['property_id']}",
            ],
            params=params,
        )

    if source == "gsc":
        start_date = params["date_range"]["start"]
        end_date = params["date_range"]["end"]
        dimension_filter = (
            parse_gsc_filter(params.get("filter", "")) if params.get("filter") else None
        )
        if gsc_page_to_path is None:
            gsc_page_to_path = bool(params.get("page_to_path", False))
        df = runners.query_gsc(
            site_url=params["site_url"],
            start_date=start_date,
            end_date=end_date,
            dimensions=params["dimensions"],
            limit=params.get("limit", 1000),
            dimension_filter=dimension_filter,
            page_to_path=gsc_page_to_path,
        )
        if df is not None and "clicks" in df.columns:
            df = df.sort_values("clicks", ascending=False)
        header_lines = [
            f"期間: {start_date} 〜 {end_date}",
            f"サイト: {params['site_url']}",
        ]
        if params.get("filter"):
            header_lines.append(f"フィルタ: {params['filter']}")
        return QueryResult(df=df, header_lines=header_lines, params=params)

    if source == "aa":
        start_date = params["date_range"]["start"]
        end_date = params["date_range"]["end"]
        segment_raw = params.get("segment")
        segment: list[str] | None = None
        if isinstance(segment_raw, str):
            segment = [s.strip() for s in segment_raw.split(",") if s.strip()] or None
        elif isinstance(segment_raw, list):
            segment = [str(s).strip() for s in segment_raw if str(s).strip()] or None

        segment_definition = params.get("segment_definition")
        breakdown = params.get("breakdown")
        df = runners.query_aa(
            company_id=params["company_id"],
            rsid=params["rsid"],
            start_date=start_date,
            end_date=end_date,
            dimension=params["dimension"],
            metrics=params["metrics"],
            segment=segment,
            segment_definition=segment_definition,
            breakdown=breakdown,
            limit=params.get("limit", 1000),
            org_id=params.get("org_id"),
        )
        header_lines = [
            f"期間: {start_date} 〜 {end_date}",
            f"Company: {params['company_id']}",
            f"RSID: {params['rsid']}",
            f"Dimension: {params['dimension']}",
            f"Metrics: {', '.join(params['metrics'])}",
        ]
        if segment:
            header_lines.append(f"Segment: {', '.join(segment)}")
        if segment_definition:
            if isinstance(segment_definition, list):
                header_lines.append(f"Inline segment definitions: {len(segment_definition)}")
            else:
                header_lines.append("Inline segment definitions: 1")
        if breakdown:
            if isinstance(breakdown, list):
                header_lines.append(f"Breakdowns: {len(breakdown)}")
            else:
                header_lines.append("Breakdowns: 1")
        return QueryResult(
            df=df,
            header_lines=header_lines,
            params=params,
        )

    if source == "bigquery":
        df = runners.query_bq(params["project_id"], params["sql"])
        return QueryResult(
            df=df,
            header_lines=[f"プロジェクト: {params['project_id']}"],
            params=params,
        )

    raise ValueError(f"Unknown source: {source}")


def run_query_params(
    raw_params: dict[str, Any],
    *,
    validate: bool = True,
    apply_params_pipeline: bool = True,
    executors: QueryExecutors | None = None,
    gsc_page_to_path: bool | None = None,
) -> QueryResult:
    """Run params dict and optionally apply its ``pipeline`` block."""
    params = resolve_and_validate_params(raw_params) if validate else raw_params
    result = execute_query_params(
        params,
        executors=executors,
        gsc_page_to_path=gsc_page_to_path,
    )
    pipeline_conf = params.get("pipeline") or {}
    if not apply_params_pipeline or not pipeline_conf:
        return result

    df = apply_pipeline(
        result.df,
        transform=pipeline_conf.get("transform"),
        where=pipeline_conf.get("where"),
        group_by=pipeline_conf.get("group_by"),
        aggregate=pipeline_conf.get("aggregate"),
        sort=pipeline_conf.get("sort"),
        columns=pipeline_conf.get("columns"),
        head=pipeline_conf.get("head"),
    )
    return QueryResult(
        df=df,
        header_lines=result.header_lines,
        params=params,
        pipeline={
            **pipeline_conf,
            "input_rows": int(len(result.df)),
            "output_rows": int(len(df)),
        },
    )


def run_query_to_csv(
    raw_params: dict[str, Any],
    *,
    output_path: str | Path,
    params_path: str | Path | None = None,
    validate: bool = True,
    encoding: str = "utf-8-sig",
    executors: QueryExecutors | None = None,
    gsc_page_to_path: bool | None = None,
) -> Path:
    """Run params dict, save the result CSV, and optionally write params JSON."""
    if params_path is not None:
        path = Path(params_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(raw_params, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    result = run_query_params(
        raw_params,
        validate=validate,
        executors=executors,
        gsc_page_to_path=gsc_page_to_path,
    )
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.df.to_csv(out, index=False, encoding=encoding)
    return out
