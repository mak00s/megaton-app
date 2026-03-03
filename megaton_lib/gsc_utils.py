"""Google Search Console DataFrame processing helpers."""

from __future__ import annotations

from urllib.parse import unquote

import numpy as np
import pandas as pd


def aggregate_search_console_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Clean and aggregate GSC rows by detected dimensions.

    - ``page`` is normalized (decode + strip query/fragment + lowercase)
    - ``clicks`` / ``impressions`` are summed
    - ``position`` is weighted by impressions
    """
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()
    if "page" in df.columns:
        df["page"] = (
            df["page"].astype(str).apply(unquote).str.split("?", n=1).str[0].str.split("#", n=1).str[0].str.lower()
        )

    metrics = ["clicks", "impressions", "position"]
    metric_cols = [c for c in metrics if c in df.columns]
    if not {"clicks", "impressions"}.issubset(metric_cols):
        return df

    if "position" in df.columns:
        df["weighted_position"] = pd.to_numeric(df["position"], errors="coerce").fillna(0) * pd.to_numeric(df["impressions"], errors="coerce").fillna(0)
        group_cols = [c for c in df.columns if c not in metric_cols + ["weighted_position"]]
        agg = (
            df.groupby(group_cols, as_index=False)
            .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"), weighted_position=("weighted_position", "sum"))
        )
        agg["position"] = np.where(agg["impressions"] > 0, agg["weighted_position"] / agg["impressions"], 0.0)
        return agg.drop(columns=["weighted_position"])

    group_cols = [c for c in df.columns if c not in metric_cols]
    return df.groupby(group_cols, as_index=False).agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))


def deduplicate_queries(df_in: pd.DataFrame) -> pd.DataFrame:
    """Merge query variants that differ only by whitespace.

    Keeps representative query string with highest impressions per key.
    """
    if df_in.empty:
        return df_in

    required = {"month", "clinic", "query", "page", "impressions", "clicks", "position"}
    missing = sorted(required - set(df_in.columns))
    if missing:
        raise ValueError(f"deduplicate_queries: missing columns {missing}")

    df = df_in.copy()
    df["query"] = df["query"].astype(str)
    df["query_key"] = df["query"].str.replace(r"\s+", "", regex=True)

    sorted_df = df.sort_values(
        by=["month", "clinic", "page", "query_key", "impressions"],
        ascending=[True, True, True, True, False],
    )
    top_query = (
        sorted_df.groupby(["month", "clinic", "page", "query_key"], as_index=False)
        .first()[["month", "clinic", "page", "query_key", "query"]]
    )

    try:
        agg = (
            df.groupby(["month", "clinic", "page", "query_key"], as_index=False)
            .apply(
                lambda g: pd.Series({
                    "impressions": g["impressions"].sum(),
                    "clicks": g["clicks"].sum(),
                    "position": (
                        (g["position"] * g["impressions"]).sum() / g["impressions"].sum()
                        if g["impressions"].sum() > 0 else 0.0
                    ),
                }),
                include_groups=False,
            )
        )
    except TypeError:
        agg = (
            df.groupby(["month", "clinic", "page", "query_key"], as_index=False)
            .apply(
                lambda g: pd.Series({
                    "impressions": g["impressions"].sum(),
                    "clicks": g["clicks"].sum(),
                    "position": (
                        (g["position"] * g["impressions"]).sum() / g["impressions"].sum()
                        if g["impressions"].sum() > 0 else 0.0
                    ),
                })
            )
        )
        agg = agg.reset_index(drop=True)

    out = agg.merge(top_query, on=["month", "clinic", "page", "query_key"], how="left")
    return out[["month", "clinic", "query", "page", "impressions", "clicks", "position"]]


def filter_by_clinic_thresholds(df: pd.DataFrame, threshold_df: pd.DataFrame) -> pd.DataFrame:
    """Filter low-value query rows by clinic-specific thresholds.

    Clinics not defined in ``threshold_df`` are intentionally excluded from output.
    """
    if df.empty or threshold_df.empty:
        return df

    rows = []
    for _, rule in threshold_df.iterrows():
        clinic = str(rule.get("clinic", "")).strip()
        if not clinic:
            continue
        min_imp = pd.to_numeric(rule.get("min_impressions", 10), errors="coerce")
        max_pos = pd.to_numeric(rule.get("max_position", 50), errors="coerce")
        min_imp = 10 if pd.isna(min_imp) else float(min_imp)
        max_pos = 50 if pd.isna(max_pos) else float(max_pos)

        sub = df[df["clinic"] == clinic].copy()
        sub = sub[
            ~(
                (pd.to_numeric(sub["clicks"], errors="coerce").fillna(0) == 0)
                & (
                    (pd.to_numeric(sub["impressions"], errors="coerce").fillna(0) < min_imp)
                    | (pd.to_numeric(sub["position"], errors="coerce").fillna(9999) > max_pos)
                )
            )
        ]
        rows.append(sub)

    if not rows:
        return pd.DataFrame(columns=df.columns)
    return pd.concat(rows, ignore_index=True)


def force_text_on_numeric_column(df: pd.DataFrame, *, column: str = "query") -> pd.DataFrame:
    """Prefix apostrophe for pure-numeric values to keep text in Sheets."""
    out = df.copy()
    if column in out.columns:
        out[column] = out[column].apply(lambda v: f"'{v}" if str(v or "").isdigit() else v)
    return out
