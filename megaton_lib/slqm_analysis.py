"""SLQM アドホック分析ヘルパー.

月次定型レポート (slqm_ga4.py) とは別に、
探索的な分析でよく使うクエリパターンを関数化したもの。
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# 低レベルユーティリティ
# ---------------------------------------------------------------------------

def _run(mg, dimensions, metrics, *, filter_d=None, sort=None, limit=None) -> pd.DataFrame:
    """mg.report.run() を実行し DataFrame を返す薄いラッパー。

    ReportResult を受け取り .df で返す。結果が None なら空 DataFrame。
    """
    kwargs: dict = dict(
        d=dimensions, m=metrics,
        filter_d=filter_d, sort=sort, show=False,
    )
    if limit is not None:
        kwargs["limit"] = limit
    result = mg.report.run(**kwargs)
    if result is None:
        return pd.DataFrame()
    return result.df


def _date_col(df: pd.DataFrame) -> pd.DataFrame:
    """date 列があれば datetime に変換して返す（コピー）。"""
    if df.empty or "date" not in df.columns:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# 日別メトリクス
# ---------------------------------------------------------------------------

def fetch_daily_metrics(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    page_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """SLQM の日別 UU / sessions / PV を返す。

    Returns:
        DataFrame[date, uu, sessions, pv]
    """
    mg.report.set.dates(start_date, end_date)
    df = _run(
        mg,
        ["date"],
        [("totalUsers", "uu"), "sessions", ("eventCount", "pv")],
        filter_d=f"hostName=~{hostname};pagePath=~{page_pattern};eventName==page_view",
        sort="date",
    )
    return _date_col(df)


# ---------------------------------------------------------------------------
# ページ別メトリクス + 読了率
# ---------------------------------------------------------------------------

def fetch_page_metrics(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    page_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """ページ別 UU と読了率 (footer_view / UU) を返す。

    Returns:
        DataFrame[page, uu, pv, footer_views, read_rate]
    """
    mg.report.set.dates(start_date, end_date)

    # UU / PV
    df_pv = _run(
        mg,
        [("pagePath", "page")],
        [("totalUsers", "uu"), ("eventCount", "pv")],
        filter_d=f"hostName=~{hostname};pagePath=~{page_pattern};eventName==page_view",
        sort="-totalUsers",
    )

    # footer_view
    df_ft = _run(
        mg,
        [("pagePath", "page")],
        [("totalUsers", "footer_views")],
        filter_d=f"hostName=~{hostname};pagePath=~{page_pattern};eventName==footer_view",
    )

    if df_pv.empty:
        return pd.DataFrame()

    df = df_pv.merge(df_ft, on="page", how="left")
    df["footer_views"] = pd.to_numeric(df["footer_views"], errors="coerce").fillna(0).astype(int)
    df["read_rate"] = df["footer_views"] / df["uu"]
    return df.sort_values("uu", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# チャネル別流入
# ---------------------------------------------------------------------------

def fetch_channel_breakdown(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    landing_pattern: Optional[str] = None,
    page_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """チャネル別の UU / sessions を返す。

    Args:
        landing_pattern: 指定すると landingPage フィルタに使用。
            例: ``r"/slqm/(en|jp)/70th"``
        page_pattern: landing_pattern 未指定時の pagePath フィルタ。

    Returns:
        DataFrame[channel, uu, sessions]
    """
    mg.report.set.dates(start_date, end_date)

    if landing_pattern:
        flt = f"hostName=~{hostname};landingPage=~{landing_pattern};eventName==session_start"
    else:
        flt = f"hostName=~{hostname};pagePath=~{page_pattern};eventName==page_view"

    return _run(
        mg,
        [("sessionDefaultChannelGroup", "channel")],
        [("totalUsers", "uu"), "sessions"],
        filter_d=flt,
        sort="-sessions",
    )


# ---------------------------------------------------------------------------
# ソース/メディア別流入
# ---------------------------------------------------------------------------

def fetch_source_medium(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    landing_pattern: Optional[str] = None,
    page_pattern: str = r"^/slqm/(en|jp)/",
    limit: int = 20,
) -> pd.DataFrame:
    """ソース/メディア別の UU / sessions を返す。

    Returns:
        DataFrame[source_medium, uu, sessions]
    """
    mg.report.set.dates(start_date, end_date)

    if landing_pattern:
        flt = f"hostName=~{hostname};landingPage=~{landing_pattern};eventName==session_start"
    else:
        flt = f"hostName=~{hostname};pagePath=~{page_pattern};eventName==page_view"

    return _run(
        mg,
        [("sessionSourceMedium", "source_medium")],
        [("totalUsers", "uu"), "sessions"],
        filter_d=flt,
        sort="-sessions",
        limit=limit,
    )


# ---------------------------------------------------------------------------
# ランディングページ
# ---------------------------------------------------------------------------

def fetch_landing_pages(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    landing_pattern: str = r"^/slqm/(en|jp)/",
    limit: int = 20,
) -> pd.DataFrame:
    """ランディングページ別の UU / sessions を返す。

    Returns:
        DataFrame[landing, uu, sessions]
    """
    mg.report.set.dates(start_date, end_date)
    return _run(
        mg,
        [("landingPage", "landing")],
        [("totalUsers", "uu"), "sessions"],
        filter_d=f"hostName=~{hostname};landingPage=~{landing_pattern};eventName==session_start",
        sort="-sessions",
        limit=limit,
    )


# ---------------------------------------------------------------------------
# セッション品質（滞在時間・ページ/セッション）
# ---------------------------------------------------------------------------

def fetch_session_quality(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    landing_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """ランディングページ別のセッション品質指標を返す。

    Returns:
        DataFrame[landing, uu, sessions, avg_duration, pages_per_session]
    """
    mg.report.set.dates(start_date, end_date)
    df = _run(
        mg,
        [("landingPage", "landing")],
        [
            ("totalUsers", "uu"),
            "sessions",
            ("averageSessionDuration", "avg_duration"),
            ("screenPageViewsPerSession", "pages_per_session"),
        ],
        filter_d=f"hostName=~{hostname};landingPage=~{landing_pattern}",
        sort="-sessions",
    )
    if df.empty:
        return df
    for col in ["avg_duration", "pages_per_session"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 新規 vs 既存
# ---------------------------------------------------------------------------

def fetch_new_vs_returning(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    page_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """新規 / 既存 ユーザーの UU / sessions を返す。

    Returns:
        DataFrame[user_type, uu, sessions]
    """
    mg.report.set.dates(start_date, end_date)
    return _run(
        mg,
        [("newVsReturning", "user_type")],
        [("totalUsers", "uu"), "sessions"],
        filter_d=f"hostName=~{hostname};pagePath=~{page_pattern};eventName==page_view",
        sort="-totalUsers",
    )


# ---------------------------------------------------------------------------
# ページ間遷移
# ---------------------------------------------------------------------------

def fetch_page_transitions(
    mg,
    start_date: str,
    end_date: str,
    *,
    hostname: str = "corp.shiseido.com",
    from_pattern: str,
    to_pattern: str = r"^/slqm/(en|jp)/",
) -> pd.DataFrame:
    """from_pattern → to_pattern へのページ遷移を返す。

    Args:
        from_pattern: 遷移元の pageReferrer に含まれるパターン。
            例: ``r"corp.shiseido.com/slqm/(en|jp)/70th/"``
        to_pattern: 遷移先の pagePath パターン。

    Returns:
        DataFrame[from_page, to_page, users]
    """
    mg.report.set.dates(start_date, end_date)
    df = _run(
        mg,
        [("pageReferrer", "from_page"), ("pagePath", "to_page")],
        [("totalUsers", "users")],
        filter_d=f"hostName=~{hostname};pagePath=~{to_pattern};pageReferrer=~{from_pattern}",
        sort="-totalUsers",
    )
    if df.empty:
        return df
    # from_page の URL を簡略化（UTM除去、ホスト除去）
    import re
    df = df.copy()
    df["from_short"] = df["from_page"].apply(
        lambda u: re.sub(r"\?.*$", "", re.sub(r"^https://[^/]+", "", str(u)))
    )
    return df
