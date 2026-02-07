"""ジョブ結果CSVの部分読み込み/要約"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def read_head(csv_path: str | Path, rows: int) -> pd.DataFrame:
    """CSVの先頭N行を読み込む"""
    if rows <= 0:
        raise ValueError("rows must be greater than 0")
    return pd.read_csv(csv_path, nrows=rows)


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
