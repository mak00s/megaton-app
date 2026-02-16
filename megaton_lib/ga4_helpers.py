"""Shared GA4 helper functions for report execution and DataFrame conversion."""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def run_report_df(
    mg,
    dimensions,
    metrics,
    *,
    filter_d=None,
    sort=None,
    limit=None,
) -> pd.DataFrame:
    """Execute ``mg.report.run`` and return a DataFrame.

    Returns an empty DataFrame when the report result is ``None``.
    """
    kwargs: dict = {
        "d": dimensions,
        "m": metrics,
        "filter_d": filter_d,
        "sort": sort,
        "show": False,
    }
    if limit is not None:
        kwargs["limit"] = limit

    result = mg.report.run(**kwargs)
    if result is None:
        return pd.DataFrame()
    return result.df


def build_filter(*parts: str | None) -> str | None:
    """Build GA4 filter string by joining non-empty parts with ``;``."""
    cleaned = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return ";".join(cleaned) if cleaned else None


def to_datetime_col(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Return a copy with ``col`` converted to datetime when present."""
    if df.empty or col not in df.columns:
        return df
    out = df.copy()
    out[col] = pd.to_datetime(out[col])
    return out


def to_numeric_cols(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    fillna: int | float | None = None,
    as_int: bool = False,
) -> pd.DataFrame:
    """Return a copy with selected columns converted to numeric."""
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        series = pd.to_numeric(out[col], errors="coerce")
        if fillna is not None:
            series = series.fillna(fillna)
        if as_int:
            series = series.astype(int)
        out[col] = series
    return out
