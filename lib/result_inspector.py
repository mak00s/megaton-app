"""ジョブ結果CSVの部分読み込み/要約"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

SUPPORTED_AGG_FUNCS = {"sum", "mean", "count", "min", "max", "median"}


def read_head(csv_path: str | Path, rows: int) -> pd.DataFrame:
    """CSVの先頭N行を読み込む"""
    if rows <= 0:
        raise ValueError("rows must be greater than 0")
    return pd.read_csv(csv_path, nrows=rows)


def apply_where(df: pd.DataFrame, expr: str) -> pd.DataFrame:
    """DataFrame.query()で行フィルタ"""
    try:
        return df.query(expr, engine="python")
    except Exception as e:
        raise ValueError(f"Invalid where expression: {e}") from e


def apply_sort(df: pd.DataFrame, sort_expr: str) -> pd.DataFrame:
    """'col DESC,col2 ASC' 形式のソート"""
    parts = [p.strip() for p in sort_expr.split(",") if p.strip()]
    if not parts:
        raise ValueError("Invalid sort expression: expression is empty")

    sort_cols: list[str] = []
    ascending: list[bool] = []

    for part in parts:
        tokens = part.split()
        if len(tokens) == 1:
            col, direction = tokens[0], "ASC"
        elif len(tokens) == 2:
            col, direction = tokens[0], tokens[1].upper()
        else:
            raise ValueError(f"Invalid sort expression: {part}")

        if col not in df.columns:
            raise ValueError(f"Invalid sort column: {col}")
        if direction not in {"ASC", "DESC"}:
            raise ValueError(f"Invalid sort direction: {direction}")

        sort_cols.append(col)
        ascending.append(direction == "ASC")

    return df.sort_values(by=sort_cols, ascending=ascending)


def apply_columns(df: pd.DataFrame, columns_expr: str) -> pd.DataFrame:
    """カンマ区切りの列名で射影"""
    cols = [c.strip() for c in columns_expr.split(",") if c.strip()]
    if not cols:
        raise ValueError("Invalid columns expression: no columns specified")

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Invalid columns: {', '.join(missing)}")

    return df.loc[:, cols]


def apply_group_aggregate(df: pd.DataFrame, group_by: str, aggregate: str) -> pd.DataFrame:
    """グループ集計。結果列名は {func}_{col}"""
    group_cols = [c.strip() for c in group_by.split(",") if c.strip()]
    if not group_cols:
        raise ValueError("Invalid aggregate: group_by is empty")

    missing_group = [c for c in group_cols if c not in df.columns]
    if missing_group:
        raise ValueError(f"Invalid aggregate group columns: {', '.join(missing_group)}")

    agg_parts = [p.strip() for p in aggregate.split(",") if p.strip()]
    if not agg_parts:
        raise ValueError("Invalid aggregate: aggregate expression is empty")

    named_aggs: dict[str, Any] = {}
    for part in agg_parts:
        func_col = [x.strip() for x in part.split(":", 1)]
        if len(func_col) != 2 or not func_col[0] or not func_col[1]:
            raise ValueError(f"Invalid aggregate expression: {part}")
        func, col = func_col[0].lower(), func_col[1]
        if func not in SUPPORTED_AGG_FUNCS:
            raise ValueError(f"Invalid aggregate function: {func}")
        if col not in df.columns:
            raise ValueError(f"Invalid aggregate column: {col}")

        out_col = f"{func}_{col}"
        if out_col in named_aggs:
            raise ValueError(f"Invalid aggregate expression: duplicate output column {out_col}")
        named_aggs[out_col] = pd.NamedAgg(column=col, aggfunc=func)

    return df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()


def apply_pipeline(
    df: pd.DataFrame,
    *,
    where: str | None = None,
    group_by: str | None = None,
    aggregate: str | None = None,
    sort: str | None = None,
    columns: str | None = None,
    head: int | None = None,
) -> pd.DataFrame:
    """where→group/aggregate→sort→columns→head の順に適用"""
    result = df.copy()

    if where:
        result = apply_where(result, where)

    if group_by or aggregate:
        if not group_by or not aggregate:
            raise ValueError("Invalid aggregate: group_by and aggregate must be used together")
        result = apply_group_aggregate(result, group_by, aggregate)

    if sort:
        result = apply_sort(result, sort)

    if columns:
        result = apply_columns(result, columns)

    if head is not None:
        if head <= 0:
            raise ValueError("Invalid head: head must be greater than 0")
        result = result.head(head)

    return result


def build_summary(csv_path: str | Path) -> dict[str, Any]:
    """CSV全体の要約統計を返す"""
    df = pd.read_csv(csv_path)

    summary: dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_counts": {col: int(v) for col, v in df.isna().sum().to_dict().items()},
    }

    numeric_df = df.select_dtypes(include="number")
    if not numeric_df.empty:
        stats = numeric_df.describe().transpose()
        numeric_summary: dict[str, Any] = {}
        for col in stats.index:
            row = stats.loc[col]
            numeric_summary[col] = {
                "count": int(row["count"]),
                "mean": float(row["mean"]) if pd.notna(row["mean"]) else None,
                "std": float(row["std"]) if pd.notna(row["std"]) else None,
                "min": float(row["min"]) if pd.notna(row["min"]) else None,
                "p25": float(row["25%"]) if pd.notna(row["25%"]) else None,
                "p50": float(row["50%"]) if pd.notna(row["50%"]) else None,
                "p75": float(row["75%"]) if pd.notna(row["75%"]) else None,
                "max": float(row["max"]) if pd.notna(row["max"]) else None,
            }
        summary["numeric_summary"] = numeric_summary

    non_numeric_df = df.select_dtypes(exclude="number")
    if not non_numeric_df.empty:
        top_values: dict[str, list[dict[str, Any]]] = {}
        for col in non_numeric_df.columns:
            vc = non_numeric_df[col].astype("string").fillna("<NA>").value_counts().head(5)
            top_values[col] = [{"value": str(idx), "count": int(cnt)} for idx, cnt in vc.items()]
        summary["top_values"] = top_values

    return summary
