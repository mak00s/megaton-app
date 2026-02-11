"""Tests for megaton_lib.talks_ga4."""

import pandas as pd

from megaton_lib.talks_ga4 import (
    attach_nav_metrics,
    preprocess_page_metrics,
    preprocess_top_pages,
)


def _page_raw():
    return pd.DataFrame({
        "yearMonth": ["202601", "202601"],
        "pagePath": ["/jp/company/talk/20260101.html", "/en/company/talk/20260102.html"],
        "screenPageViews": [100, 50],
        "sessions": [80, 40],
        "totalUsers": [70, 35],
        "newUsers": [30, 15],
        "eventCount": [10, 5],
    })


def _lp_raw():
    return pd.DataFrame({
        "yearMonth": ["202601"],
        "landingPage": ["/jp/company/talk/20260101.html"],
        "sessions": [60],
        "engagedSessions": [50],
    })


class TestPreprocessPageMetrics:
    def test_basic_merge(self):
        df = preprocess_page_metrics(_page_raw(), _lp_raw())
        assert "entrances" in df.columns
        assert "bounces" in df.columns
        assert "language" in df.columns
        assert len(df) == 2

        jp_row = df[df["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        assert jp_row["entrances"] == 60
        assert jp_row["bounces"] == 10  # 60 - 50
        assert jp_row["language"] == "jp"

    def test_no_lp_match(self):
        df = preprocess_page_metrics(
            _page_raw(), pd.DataFrame(columns=["yearMonth", "landingPage", "sessions", "engagedSessions"]),
        )
        assert all(df["entrances"] == 0)
        assert all(df["bounces"] == 0)

    def test_int_columns(self):
        df = preprocess_page_metrics(_page_raw(), _lp_raw())
        for col in ["pv", "sessions", "total_users", "new_users", "footer_views"]:
            assert df[col].dtype in ("int64", "int32")


class TestPreprocessTopPages:
    def test_empty_input(self):
        df = preprocess_top_pages(pd.DataFrame(), pd.DataFrame())
        assert len(df) == 0

    def test_normalizes_index_html(self):
        raw = pd.DataFrame({
            "yearMonth": ["202601"],
            "pagePath": ["/jp/company/talk/index.html"],
            "screenPageViews": [200],
            "sessions": [150],
            "totalUsers": [100],
            "newUsers": [50],
            "eventCount": [20],
        })
        lp = pd.DataFrame(columns=["yearMonth", "landingPage", "sessions", "engagedSessions"])
        df = preprocess_top_pages(raw, lp)
        assert len(df) == 1
        assert df.iloc[0]["page"] == "/jp/company/talk/"


class TestAttachNavMetrics:
    def test_attaches_nav_columns(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "page": ["/jp/company/talk/20260101.html"],
            "sessions": [80],
            "pv": [100],
            "language": ["jp"],
        })
        df_nav = pd.DataFrame({
            "month": ["202601"],
            "fromPath": ["/jp/company/talk/20260101.html"],
            "nav_clicks": [20],
        })
        result = attach_nav_metrics(df_page, df_nav)
        assert result.iloc[0]["nav_clicks"] == 20
        assert result.iloc[0]["nav_rate"] == 20 / 80

    def test_missing_nav_fills_zero(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "page": ["/en/company/talk/20260102.html"],
            "sessions": [40],
            "pv": [50],
            "language": ["en"],
        })
        df_nav = pd.DataFrame(columns=["month", "fromPath", "nav_clicks"])
        result = attach_nav_metrics(df_page, df_nav)
        assert result.iloc[0]["nav_clicks"] == 0
        assert result.iloc[0]["nav_rate"] == 0.0
