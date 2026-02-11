"""Report output orchestration for Corp Talks.

Analogous to ``with_report.py`` — builds DataFrames for monthly / ARTICLE sheets
and writes them to Google Sheets.
"""

from __future__ import annotations

import pandas as pd

from .sheets import save_sheet_from_template
from .talks_scraping import normalize_meta_sheet


# ---------------------------------------------------------------------------
# Monthly sheets (yyyymm)
# ---------------------------------------------------------------------------

def build_monthly_view(
    df_page: pd.DataFrame,
    df_meta: pd.DataFrame,
) -> pd.DataFrame:
    """Merge page metrics with metadata for human-readable monthly sheets.

    *df_meta* should have columns ``URL, Title, Date`` (from ``_meta`` sheet).
    Returns a DataFrame with ``published_date`` and ``Title`` columns added.
    """
    df = df_page.merge(
        df_meta[["URL", "Title", "Date"]],
        left_on="page",
        right_on="URL",
        how="left",
    ).drop(columns=["URL"])

    df["published_date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.drop(columns=["Date"])
    df["language"] = df["language"].astype(str).str.upper()
    return df


def write_monthly_sheets(mg, df_month_view: pd.DataFrame) -> list[str]:
    """Write one sheet per ``yyyymm`` month found in *df_month_view*.

    Returns a list of written sheet names.
    """
    months = sorted(df_month_view["month"].dropna().astype(str).unique().tolist())
    written: list[str] = []

    for yyyymm in months:
        df_m = df_month_view[df_month_view["month"].astype(str) == str(yyyymm)].copy()
        if len(df_m) == 0:
            continue

        # Sort: JP first, then by published_date desc
        df_m["_lang_sort"] = df_m["language"].map({"JP": 2, "EN": 1}).fillna(0).astype(int)
        df_m = df_m.sort_values(
            ["_lang_sort", "published_date"],
            ascending=[False, False],
            kind="mergesort",
        ).drop(columns=["_lang_sort"])

        df_out = df_m[[
            "language", "published_date", "Title", "page",
            "pv", "sessions", "nav_clicks", "nav_rate",
            "entrances", "total_users", "new_users", "bounces", "footer_views",
        ]].copy()
        df_out = df_out.rename(columns={"Title": "title"})
        df_out["published_date"] = df_out["published_date"].dt.strftime("%Y-%m-%d")
        df_out["nav_rate"] = df_out["nav_rate"].round(6)

        save_sheet_from_template(
            mg, str(yyyymm), df_out,
            start_row=1,
            template_regex=r"^\d{6}$",
            save_kwargs={"freeze_header": True},
        )
        written.append(str(yyyymm))

    return written


# ---------------------------------------------------------------------------
# ARTICLE cumulative sheet
# ---------------------------------------------------------------------------

_TALK_PATH_REGEX = r"^/(en|jp)/company/talk/[^.]+\.html$"


def build_article_sheet(
    df_article_m: pd.DataFrame,
    df_meta: pd.DataFrame,
    *,
    path_regex: str = _TALK_PATH_REGEX,
) -> pd.DataFrame:
    """Build the ARTICLE cumulative sheet from accumulated ``_article-m`` data.

    Aggregates monthly rows in *df_article_m* across all months per page,
    merges with *df_meta* for title/tag/lang/date, and computes derived
    metrics (nav_rate, read_rate).

    This is a pure DataFrame transformation — no GA4 queries.
    """
    # Filter to article pages only (exclude Top pages)
    df = df_article_m.copy()
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "lang", "published_date", "tag", "title", "page",
            "uu_total", "new_users",
            "nav_clicks", "nav_rate", "read_rate",
        ])

    df["page"] = df["page"].astype(str).str.strip()
    df = df[df["page"].str.match(path_regex)].copy()

    # Aggregate across months per page (sum additive metrics)
    sum_cols = ["pv", "sessions", "nav_clicks", "total_users", "new_users", "footer_views"]
    for c in sum_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    df_agg = df.groupby("page", as_index=False)[sum_cols].sum()

    # Metadata
    meta = normalize_meta_sheet(df_meta)
    meta = meta.rename(columns={
        "URL": "page", "Title": "title", "Language": "lang",
        "Tag": "tag", "Date": "published_date",
    })[["page", "title", "lang", "tag", "published_date"]].copy()
    meta["lang"] = meta["lang"].astype(str).str.upper().str.strip()
    meta["published_date"] = pd.to_datetime(meta["published_date"], errors="coerce")
    meta = meta[meta["page"].astype(str).str.match(path_regex)].copy()
    meta = meta[(meta["title"].astype(str).str.strip() != "") & meta["published_date"].notna()].copy()

    # Merge
    df_all = meta.merge(df_agg, on="page", how="left")
    for c in sum_cols:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce").fillna(0).astype(int)

    df_all["nav_rate"] = (df_all["nav_clicks"] / df_all["sessions"]).where(df_all["sessions"] > 0, 0.0)
    df_all["uu_total"] = df_all["total_users"].fillna(0).astype(int)

    pv = pd.to_numeric(df_all["pv"], errors="coerce").fillna(0)
    fv = pd.to_numeric(df_all["footer_views"], errors="coerce").fillna(0)
    df_all["read_rate"] = 0.0
    mask = pv > 0
    df_all.loc[mask, "read_rate"] = (fv[mask] / pv[mask]).astype(float)

    # Sort: JP first, published_date desc
    df_all["_lang_sort"] = df_all["lang"].map({"JP": 2, "EN": 1}).fillna(0).astype(int)
    df_all = df_all.sort_values(
        ["_lang_sort", "published_date"], ascending=[False, False], kind="mergesort",
    ).drop(columns=["_lang_sort"])

    df_out = df_all[[
        "lang", "published_date", "tag", "title", "page",
        "uu_total", "new_users",
        "nav_clicks", "nav_rate", "read_rate",
    ]].copy()
    df_out["published_date"] = pd.to_datetime(df_out["published_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out["nav_rate"] = df_out["nav_rate"].round(6)
    df_out["read_rate"] = df_out["read_rate"].round(6)

    return df_out
