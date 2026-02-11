"""Tests for megaton_lib.talks_report."""

import pandas as pd

from megaton_lib.talks_report import build_monthly_view


class TestBuildMonthlyView:
    def test_merges_meta(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "language": ["jp"],
            "page": ["/jp/company/talk/20260101.html"],
            "pv": [100],
            "sessions": [80],
            "nav_clicks": [10],
            "nav_rate": [0.125],
            "entrances": [60],
            "total_users": [70],
            "new_users": [30],
            "bounces": [10],
            "footer_views": [5],
        })
        df_meta = pd.DataFrame({
            "URL": ["/jp/company/talk/20260101.html"],
            "Title": ["テスト記事"],
            "Language": ["JP"],
            "Tag": ["t"],
            "Date": ["2026-01-01"],
        })
        result = build_monthly_view(df_page, df_meta)
        assert "Title" in result.columns
        assert "published_date" in result.columns
        assert result.iloc[0]["Title"] == "テスト記事"
        assert result.iloc[0]["language"] == "JP"

    def test_no_meta_match(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "language": ["en"],
            "page": ["/en/company/talk/nomatch.html"],
            "pv": [50],
            "sessions": [40],
            "nav_clicks": [0],
            "nav_rate": [0.0],
            "entrances": [30],
            "total_users": [35],
            "new_users": [15],
            "bounces": [5],
            "footer_views": [2],
        })
        df_meta = pd.DataFrame(columns=["URL", "Title", "Language", "Tag", "Date"])
        result = build_monthly_view(df_page, df_meta)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["Title"])
