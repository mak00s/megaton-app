"""GA4 data preprocessing for Corp Talks reports.

Handles page-metric/LP merges, Talks Top normalization, and navigation click analysis.
"""

from __future__ import annotations

import re

import pandas as pd

from .talks_scraping import normalize_url


# ---------------------------------------------------------------------------
# Page metric preprocessing
# ---------------------------------------------------------------------------

_PAGE_COLUMNS = [
    "month", "language", "page", "pv", "sessions", "entrances",
    "total_users", "new_users", "bounces", "footer_views",
]

_INT_COLS = ["pv", "sessions", "entrances", "total_users", "new_users", "bounces", "footer_views"]


def preprocess_page_metrics(
    df_page_raw: pd.DataFrame,
    df_lp_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Merge page-view and landing-page DataFrames, compute derived columns.

    Returns a DataFrame with columns: month, language, page, pv, sessions,
    entrances, total_users, new_users, bounces, footer_views.
    """
    df = df_page_raw.copy()

    # LP side
    lp = df_lp_raw.copy()
    if len(lp) > 0:
        lp.loc[:, ["sessions", "engagedSessions"]] = (
            lp[["sessions", "engagedSessions"]].fillna(0).astype(int)
        )
    else:
        lp = pd.DataFrame(columns=["yearMonth", "landingPage", "sessions", "engagedSessions"])

    # Merge page + LP
    df = df.merge(
        lp,
        left_on=["yearMonth", "pagePath"],
        right_on=["yearMonth", "landingPage"],
        how="left",
        suffixes=("", "_lp"),
    )
    df["entrances"] = pd.to_numeric(df["sessions_lp"], errors="coerce").fillna(0).astype(int)
    df["bounces"] = (
        pd.to_numeric(df["sessions_lp"], errors="coerce").fillna(0).astype(int)
        - pd.to_numeric(df["engagedSessions"], errors="coerce").fillna(0).astype(int)
    ).clip(lower=0).astype(int)
    df = df.drop(columns=["landingPage", "sessions_lp", "engagedSessions"], errors="ignore")

    # Language from path
    df["language"] = df["pagePath"].str.extract(
        r"^/(en|jp)/company/talk/", expand=False,
    )

    # Rename to standard columns
    df = df.rename(columns={
        "yearMonth": "month",
        "pagePath": "page",
        "screenPageViews": "pv",
        "totalUsers": "total_users",
        "newUsers": "new_users",
        "eventCount": "footer_views",
    })

    # Select and int-cast
    for c in _INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    cols = [c for c in _PAGE_COLUMNS if c in df.columns]
    return df[cols].copy()


def preprocess_top_pages(
    df_top_raw: pd.DataFrame,
    df_top_lp_raw: pd.DataFrame,
    *,
    canonical_paths: list[str] | None = None,
) -> pd.DataFrame:
    """Preprocess Talks Top page metrics (normalize path variants, dedup).

    Returns a DataFrame with the same columns as :func:`preprocess_page_metrics`,
    or an empty DataFrame if no data.
    """
    if canonical_paths is None:
        canonical_paths = ["/jp/company/talk/", "/en/company/talk/"]

    if len(df_top_raw) == 0:
        return pd.DataFrame(columns=_PAGE_COLUMNS)

    df = preprocess_page_metrics(df_top_raw, df_top_lp_raw)

    # Normalize path variants to canonical /talk/
    df["page"] = df["page"].astype(str).str.strip()
    df["page"] = df["page"].str.replace(
        r"^/(en|jp)/company/talk/index\.html$", r"/\1/company/talk/", regex=True,
    )
    df["page"] = df["page"].str.replace(
        r"^/(en|jp)/company/talk$", r"/\1/company/talk/", regex=True,
    )
    df["language"] = df["page"].str.extract(r"^/(en|jp)/company/talk", expand=False)
    df = df[df["page"].isin(canonical_paths)].copy()

    # Dedup: keep row with highest sessions per (month, language, page)
    df["_s"] = df["sessions"].fillna(0).astype(int)
    df["_pv"] = df["pv"].fillna(0).astype(int)
    df = df.sort_values(
        ["month", "language", "page", "_s", "_pv"],
        ascending=[True, True, True, False, False],
        kind="mergesort",
    ).drop_duplicates(subset=["month", "language", "page"], keep="first")
    df = df.drop(columns=["_s", "_pv"])

    cols = [c for c in _PAGE_COLUMNS if c in df.columns]
    return df[cols].copy()


# ---------------------------------------------------------------------------
# Navigation (inline link clicks)
# ---------------------------------------------------------------------------

def fetch_nav_clicks(
    mg,
    report_ranges: list[tuple[str, str]],
    *,
    hostname: str,
    path_regex: str,
    df_link_sheet: pd.DataFrame,
    group_by_month: bool = True,
) -> pd.DataFrame:
    """Fetch click/file_download events and match against ``_link`` sheet.

    When *group_by_month* is True, returns ``[month, fromPath, nav_clicks]``.
    When False, returns ``[fromPath, nav_clicks]`` (for ARTICLE cumulative sheet).
    """
    dims_base = ["pagePath", "linkUrl"]
    if group_by_month:
        dims_base = ["yearMonth"] + dims_base

    df_click = mg.report.run.ranges(
        report_ranges,
        d=dims_base,
        m=[("sessions", "nav_clicks")],
        filter_d=f"hostName=={hostname};pagePath=~{path_regex};eventName==click",
    )

    try:
        df_file = mg.report.run.ranges(
            report_ranges,
            d=dims_base,
            m=[("sessions", "nav_clicks")],
            filter_d=f"hostName=={hostname};pagePath=~{path_regex};eventName==file_download",
        )
    except Exception as e:
        print(f"[warn] file_download query skipped: {e}")
        df_file = pd.DataFrame(columns=dims_base + ["nav_clicks"])

    df_nav = pd.concat([df_click, df_file], ignore_index=True)
    if len(df_nav) == 0:
        cols = (["month", "fromPath", "nav_clicks"] if group_by_month
                else ["fromPath", "nav_clicks"])
        return pd.DataFrame(columns=cols)

    df_nav = df_nav.rename(columns={"pagePath": "fromPath", "linkUrl": "link"})
    if group_by_month and "yearMonth" in df_nav.columns:
        df_nav = df_nav.rename(columns={"yearMonth": "month"})
    df_nav["fromPath"] = df_nav["fromPath"].astype(str).str.strip()
    df_nav["link"] = df_nav["link"].map(lambda v: normalize_url(v, hostname)).astype(str).str.strip()
    df_nav = df_nav[(df_nav["fromPath"] != "") & (df_nav["link"] != "")]
    df_nav["nav_clicks"] = pd.to_numeric(df_nav["nav_clicks"], errors="coerce").fillna(0).astype(int)

    # Match against _link sheet (only article inline links)
    link_ref = df_link_sheet[["fromPath", "link"]].copy()
    link_ref["fromPath"] = link_ref["fromPath"].astype(str).str.strip()
    link_ref["link"] = link_ref["link"].astype(str).str.strip()
    link_ref = link_ref[(link_ref["fromPath"] != "") & (link_ref["link"] != "")].drop_duplicates()

    df_nav_in = df_nav.merge(link_ref, on=["fromPath", "link"], how="inner")

    group_cols = ["month", "fromPath"] if group_by_month else ["fromPath"]
    if len(df_nav_in) == 0:
        return pd.DataFrame(columns=group_cols + ["nav_clicks"])

    return df_nav_in.groupby(group_cols, as_index=False)["nav_clicks"].sum()


def attach_nav_metrics(df_page: pd.DataFrame, df_nav_m: pd.DataFrame) -> pd.DataFrame:
    """Attach nav_clicks and nav_rate columns to *df_page*."""
    df = df_page.merge(
        df_nav_m.rename(columns={"fromPath": "page"}),
        on=[c for c in ["month", "page"] if c in df_nav_m.columns or c == "page"],
        how="left",
    )
    df["nav_clicks"] = pd.to_numeric(df["nav_clicks"], errors="coerce").fillna(0).astype(int)
    df["nav_rate"] = (df["nav_clicks"] / df["sessions"]).where(df["sessions"] > 0, 0.0)

    ordered = [
        "month", "language", "page", "pv", "sessions",
        "nav_clicks", "nav_rate", "entrances",
        "total_users", "new_users", "bounces", "footer_views",
    ]
    return df[[c for c in ordered if c in df.columns]].copy()
