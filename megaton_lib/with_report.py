"""Helpers for the WITH report notebook.

Keep notebook code thin by moving reusable, deterministic transformations here.
"""

from __future__ import annotations

from typing import Mapping, Sequence

import pandas as pd

from megaton_lib.periods import parse_summary_tokens
from megaton_lib.sheets import save_sheet_from_template


REGION_MAP_DEFAULT: dict[str, str] = {
    "GHQ": "HQ",
    "JPN": "SJ",
    "EU": "EMEA",
    "Amricas": "America",
    "China": "China",
    "AP": "APAC",
    "TR": "TR",
}

REGIONS_DEFAULT: list[str] = ["HQ", "SJ", "EMEA", "America", "China", "APAC", "TR"]


def build_month_sheet_df(
    df_articles: pd.DataFrame,
    df_meta: pd.DataFrame,
    target_months: Sequence[str],
    *,
    region_map: Mapping[str, str] | None = None,
    regions: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Build the month sheet DataFrame written by the WITH notebook.

    Expected inputs:
    - df_articles columns: month, article_id, region, pv, uu, vstart (vstart optional)
    - df_meta columns: article_id, article_title, article_category, article_date, language, likes
    """
    if df_articles is None or df_meta is None:
        return pd.DataFrame()
    if len(target_months) == 0:
        return pd.DataFrame()

    region_map = dict(region_map or REGION_MAP_DEFAULT)
    regions = list(regions or REGION_MAP_DEFAULT.values())

    df_m = df_articles[df_articles["month"].isin(list(target_months))].copy()
    if df_m.empty:
        return pd.DataFrame()

    df_m["region"] = df_m["region"].map(region_map).fillna(df_m["region"])

    uu_pivot = df_m.pivot_table(
        index="article_id", columns="region", values="uu", aggfunc="sum", fill_value=0
    )
    pv_pivot = df_m.pivot_table(
        index="article_id", columns="region", values="pv", aggfunc="sum", fill_value=0
    )

    for r in regions:
        if r not in uu_pivot.columns:
            uu_pivot[r] = 0
        if r not in pv_pivot.columns:
            pv_pivot[r] = 0
    uu_pivot = uu_pivot[list(regions)]
    pv_pivot = pv_pivot[list(regions)]

    uu_pivot["合計"] = uu_pivot.sum(axis=1)
    pv_pivot["合計"] = pv_pivot.sum(axis=1)

    if "vstart" in df_m.columns:
        vstart_sum = df_m.groupby("article_id")["vstart"].sum().reset_index()
    else:
        vstart_sum = pd.DataFrame(columns=["article_id", "vstart"])

    df_sheet = df_meta[
        ["article_id", "article_title", "article_category", "article_date", "language", "likes"]
    ].copy()
    df_sheet = df_sheet.merge(vstart_sum, on="article_id", how="left")
    df_sheet["vstart"] = df_sheet["vstart"].fillna(0).astype(int)

    uu_cols = {r: f"UU_{r}" for r in list(regions) + ["合計"]}
    pv_cols = {r: f"PV_{r}" for r in list(regions) + ["合計"]}
    uu_renamed = uu_pivot.rename(columns=uu_cols).reset_index()
    pv_renamed = pv_pivot.rename(columns=pv_cols).reset_index()

    df_sheet = df_sheet.merge(uu_renamed, on="article_id", how="left")
    df_sheet = df_sheet.merge(pv_renamed, on="article_id", how="left")

    num_cols = [c for c in df_sheet.columns if c.startswith(("UU_", "PV_"))]
    if num_cols:
        df_sheet[num_cols] = df_sheet[num_cols].fillna(0).astype(int)

    if {"UU_合計", "PV_合計"}.issubset(df_sheet.columns):
        df_sheet = df_sheet[df_sheet[["UU_合計", "PV_合計"]].sum(axis=1) > 0]

    if df_sheet.empty:
        return pd.DataFrame()

    df_sheet = df_sheet.sort_values("PV_合計", ascending=False).reset_index(drop=True)
    df_sheet.insert(0, "No", range(1, len(df_sheet) + 1))
    return df_sheet


def write_summary_sheets(
    mg,
    df_articles: pd.DataFrame,
    df_meta: pd.DataFrame,
    summary_tokens: str,
    *,
    start_row: int = 3,
    template_regex: str = r"^\d{6}$",
    region_map: Mapping[str, str] | None = None,
    regions: Sequence[str] | None = None,
) -> None:
    """Write summary sheets (yyyymm / YYYY / YYYYQn) for the WITH report."""
    region_map = dict(region_map or REGION_MAP_DEFAULT)
    regions = list(regions or REGION_MAP_DEFAULT.values())

    available_months = set(df_articles["month"].dropna().astype(str).unique()) if df_articles is not None else set()
    for sheet_name, target_months in parse_summary_tokens(summary_tokens):
        if available_months and not (set(target_months) & available_months):
            print(
                f"[skip] {sheet_name}: 対象月が取得期間に含まれません "
                f"(have={min(available_months) if available_months else 'n/a'}..{max(available_months) if available_months else 'n/a'}, "
                f"want={target_months[0]}..{target_months[-1]})"
            )
            continue

        df_sheet = build_month_sheet_df(
            df_articles,
            df_meta,
            target_months,
            region_map=region_map,
            regions=regions,
        )
        if df_sheet is None or df_sheet.empty:
            print(f"[skip] {sheet_name}: 対象月のデータが0件のため、既存シートを更新しません")
            continue

        save_sheet_from_template(
            mg,
            sheet_name,
            df_sheet,
            start_row=start_row,
            template_regex=template_regex,
        )

