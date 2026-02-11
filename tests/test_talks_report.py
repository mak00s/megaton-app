"""Tests for megaton_lib.talks_report."""

import pandas as pd

from megaton_lib.talks_report import build_article_sheet, build_monthly_view


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


class TestBuildArticleSheet:
    """Tests for build_article_sheet (ARTICLE cumulative from _article-m)."""

    def _make_article_m(self) -> pd.DataFrame:
        """Create sample _article-m data spanning 2 months."""
        return pd.DataFrame({
            "month": ["202601", "202601", "202602", "202602"],
            "page": [
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
            ],
            "language": ["jp", "en", "jp", "en"],
            "pv": [100, 50, 120, 60],
            "sessions": [80, 40, 90, 45],
            "nav_clicks": [10, 5, 12, 6],
            "total_users": [70, 35, 85, 42],
            "new_users": [30, 15, 25, 10],
            "footer_views": [20, 10, 24, 12],
        })

    def _make_meta(self) -> pd.DataFrame:
        return pd.DataFrame({
            "URL": [
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
            ],
            "Title": ["JP記事", "EN Article"],
            "Language": ["JP", "EN"],
            "Tag": ["interview", "report"],
            "Date": ["2026-01-01", "2026-01-15"],
        })

    def test_aggregates_across_months(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert len(result) == 2
        # JP article: pv=100+120=220, sessions=80+90=170
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        assert jp["nav_clicks"] == 22  # 10+12
        assert jp["uu_total"] == 155   # 70+85 (sum of total_users)
        assert jp["nav_rate"] == round(22 / 170, 6)

    def test_read_rate(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        # footer_views=44, pv=220
        assert jp["read_rate"] == round(44 / 220, 6)

    def test_sort_jp_first(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert result.iloc[0]["lang"] == "JP"
        assert result.iloc[1]["lang"] == "EN"

    def test_empty_article_m(self):
        result = build_article_sheet(
            pd.DataFrame(columns=["month", "page", "pv", "sessions", "nav_clicks", "total_users", "new_users", "footer_views"]),
            self._make_meta(),
        )
        assert len(result) == 0

    def test_excludes_top_pages(self):
        """Top pages in _article-m should not contribute metrics to ARTICLE."""
        df = pd.concat([
            self._make_article_m(),
            pd.DataFrame({
                "month": ["202601"],
                "page": ["/jp/company/talk/"],
                "language": ["jp"],
                "pv": [500], "sessions": [400],
                "nav_clicks": [0], "total_users": [300],
                "new_users": [100], "footer_views": [0],
            }),
        ], ignore_index=True)
        result = build_article_sheet(df, self._make_meta())
        # Top page should not appear in output
        assert "/jp/company/talk/" not in result["page"].values
        # Article pages should still be present
        assert len(result) == 2

    def test_meta_columns(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert {"lang", "published_date", "tag", "title", "page", "uu_total"}.issubset(result.columns)
        assert "total_users" not in result.columns  # uu_total に統合済み
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        assert jp["title"] == "JP記事"
        assert jp["tag"] == "interview"
        assert jp["published_date"] == "2026-01-01"
