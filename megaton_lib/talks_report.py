"""Report output orchestration for Corp Talks.

Analogous to ``with_report.py`` — builds DataFrames for monthly / ALL sheets
and writes them to Google Sheets.
"""

from __future__ import annotations

import pandas as pd

from .date_utils import month_ranges_between
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
# ALL cumulative sheet
# ---------------------------------------------------------------------------

_TALK_PATH_REGEX = r"^/(en|jp)/company/talk/[^.]+\.html$"


def build_all_sheet(
    mg,
    *,
    hostname: str,
    path_regex: str = _TALK_PATH_REGEX,
    cumulative_start: str,
    cumulative_end: str,
    df_meta: pd.DataFrame,
    df_link_sheet: pd.DataFrame,
) -> pd.DataFrame:
    """Build the ALL cumulative sheet DataFrame.

    Runs GA4 queries for the full cumulative range, computes nav_clicks,
    and merges with metadata. Returns a DataFrame ready for sheet output.
    """
    from .talks_ga4 import fetch_nav_clicks

    all_ranges = month_ranges_between(cumulative_start, cumulative_end)

    # Page metrics (no yearMonth dimension — cumulative)
    mg.report.set.dates(cumulative_start, cumulative_end)
    mg.report.run(
        d=["pagePath"],
        m=[
            (["screenPageViews", "sessions", "totalUsers", "newUsers"], {}),
            (["eventCount"], {"filter_d": "eventName==footer_view"}),
        ],
        filter_d=f"hostName=={hostname};pagePath=~{path_regex}",
        show=False,
    )
    df_metrics = mg.report.data.copy() if mg.report.data is not None else pd.DataFrame()

    if len(df_metrics) > 0:
        df_metrics = df_metrics.rename(columns={
            "pagePath": "page",
            "screenPageViews": "pv",
            "totalUsers": "total_users",
            "newUsers": "new_users",
            "eventCount": "footer_views",
        })
        df_metrics["page"] = df_metrics["page"].astype(str).str.strip()
        for c in ["pv", "sessions", "total_users", "new_users", "footer_views"]:
            if c in df_metrics.columns:
                df_metrics[c] = pd.to_numeric(df_metrics[c], errors="coerce").fillna(0).astype(int)
    else:
        df_metrics = pd.DataFrame(columns=["page", "pv", "sessions", "total_users", "new_users", "footer_views"])

    # Navigation clicks (cumulative, no month grouping)
    df_nav_m = fetch_nav_clicks(
        mg, all_ranges,
        hostname=hostname, path_regex=path_regex,
        df_link_sheet=df_link_sheet,
        group_by_month=False,
    )

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
    df_all = meta.merge(df_metrics, on="page", how="left")
    df_all = df_all.merge(df_nav_m.rename(columns={"fromPath": "page"}), on="page", how="left")
    for c in ["pv", "sessions", "nav_clicks", "total_users", "new_users", "footer_views"]:
        if c in df_all.columns:
            df_all[c] = pd.to_numeric(df_all[c], errors="coerce").fillna(0).astype(int)

    df_all["nav_rate"] = (df_all["nav_clicks"] / df_all["sessions"]).where(df_all["sessions"] > 0, 0.0)
    df_all["uu_total"] = df_all["total_users"].fillna(0).astype(int)

    pv = pd.to_numeric(df_all.get("pv"), errors="coerce").fillna(0)
    fv = pd.to_numeric(df_all.get("footer_views"), errors="coerce").fillna(0)
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
        "uu_total", "total_users", "new_users",
        "nav_clicks", "nav_rate", "read_rate",
    ]].copy()
    df_out["published_date"] = pd.to_datetime(df_out["published_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out["nav_rate"] = df_out["nav_rate"].round(6)
    df_out["read_rate"] = df_out["read_rate"].round(6)

    return df_out
