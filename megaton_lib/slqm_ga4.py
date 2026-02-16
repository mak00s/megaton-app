"""SLQM GA4 helpers.

Functions specific to the SLQM report that are reusable across notebooks.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


def get_13month_start(end_date: str) -> str:
    """Return the first day of the month 12 months before *end_date*'s month.

    This gives a 13-month window (inclusive of both start and end months).

    >>> get_13month_start("2026-01-31")
    '2025-01-01'
    >>> get_13month_start("2025-03-15")
    '2024-03-01'
    """
    latest = datetime.strptime(end_date, "%Y-%m-%d")
    start = (latest.replace(day=1) - relativedelta(months=12))
    return start.strftime("%Y-%m-%d")


def ym_from_year_month(df: pd.DataFrame, *, ym_col: str = "ym") -> pd.DataFrame:
    """Convert ``year`` + ``month`` columns to a ``ym`` column (``YYYY/M/1``).

    The ``year`` and ``month`` columns are dropped. The ``ym`` column is moved
    to the first position. Returns a copy.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    required = {"year", "month"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required - set(df.columns)}")
    df = df.copy()
    df[ym_col] = df["year"].astype(str) + "/" + df["month"].astype(str) + "/1"
    df.drop(columns=["year", "month"], inplace=True)
    cols = [ym_col] + [c for c in df.columns if c != ym_col]
    return df[cols]


def safe_merge_many(
    dfs: list[pd.DataFrame | tuple[str, pd.DataFrame]],
    *,
    on: str | list[str] = "month",
    how: str = "outer",
    preserve_int: bool = True,
) -> pd.DataFrame:
    """Merge multiple DataFrames on a common key, preserving integer types.

    Args:
        dfs: list of DataFrames or (name, DataFrame) tuples.
        on: column(s) to merge on.
        how: merge strategy (default ``"outer"``).
        preserve_int: if True, restore float columns that are actually ints
            to ``Int64``.

    Returns:
        Merged DataFrame.
    """
    def _restore_int(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                if df[col].dropna().apply(float.is_integer).all():
                    df[col] = df[col].astype("Int64")
        return df

    if not dfs:
        return pd.DataFrame()

    if isinstance(on, str):
        on = [on]

    # Normalise to (name, df) pairs
    named: list[tuple[str, pd.DataFrame]] = []
    for item in dfs:
        if isinstance(item, tuple):
            named.append(item)
        else:
            named.append((f"df_{len(named)}", item))

    valid = [(n, df) for n, df in named if df is not None and set(on).issubset(df.columns)]
    if not valid:
        return pd.DataFrame()

    _, base = valid[0]
    result = base.copy()
    for _, df in valid[1:]:
        result = result.merge(df, on=on, how=how)

    return _restore_int(result) if preserve_int else result


def fillna_int(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Fill NaN and convert to int for specified columns, in-place.

    Uses ``pd.to_numeric`` + ``fillna(0).astype(int)`` to avoid FutureWarning.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def compute_sp_ratio(
    df_raw: pd.DataFrame,
    *,
    group_cols: list[str],
) -> pd.DataFrame:
    """Compute SP (mobile + tablet) ratio from device-level UU data.

    Args:
        df_raw: DataFrame with ``device`` and ``uu`` columns, plus ``group_cols``.
        group_cols: columns to group by (e.g. ``["ym", "page"]`` or ``["ym"]``).

    Returns:
        DataFrame with ``group_cols`` + ``sp_ratio``.
    """
    df = df_raw.copy()
    df["is_sp"] = df["device"].isin(["mobile", "tablet"]).astype(int)
    agg = df.groupby(group_cols).agg(
        sp_uu=("uu", lambda x: x[df.loc[x.index, "is_sp"] == 1].sum()),
        total_uu=("uu", "sum"),
    ).reset_index()
    agg["sp_ratio"] = agg["sp_uu"] / agg["total_uu"]
    return agg[group_cols + ["sp_ratio"]]
