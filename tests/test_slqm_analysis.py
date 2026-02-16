"""Tests for megaton_lib.slqm_analysis."""

import types
from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from megaton_lib.slqm_analysis import (
    _date_col,
    _run,
    fetch_channel_breakdown,
    fetch_daily_metrics,
    fetch_landing_pages,
    fetch_new_vs_returning,
    fetch_page_metrics,
    fetch_page_transitions,
    fetch_session_quality,
    fetch_source_medium,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mg(df: pd.DataFrame | None = None):
    """Create a mock Megaton instance that returns *df* from report.run().

    mg.report.run() returns a ReportResult-like object with .df attribute.
    If *df* is None, run() returns None (simulating empty result).
    """
    mg = MagicMock()
    if df is not None:
        result = types.SimpleNamespace(df=df)
        mg.report.run.return_value = result
    else:
        mg.report.run.return_value = None
    return mg


def _make_mg_multi(dfs: list[pd.DataFrame | None]):
    """Create a mock mg that returns successive DataFrames for each run() call.

    Useful for functions like fetch_page_metrics that call run() twice.
    """
    mg = MagicMock()
    results = []
    for df in dfs:
        if df is not None:
            results.append(types.SimpleNamespace(df=df))
        else:
            results.append(None)
    mg.report.run.side_effect = results
    return mg


# ---------------------------------------------------------------------------
# _date_col
# ---------------------------------------------------------------------------


class TestDateCol:
    """Tests for _date_col()."""

    def test_converts_date_column(self):
        df = pd.DataFrame({"date": ["2026-02-04", "2026-02-05"], "uu": [10, 20]})
        result = _date_col(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        # original should be untouched
        assert df["date"].dtype == object

    def test_no_date_column(self):
        df = pd.DataFrame({"page": ["/a"], "uu": [10]})
        result = _date_col(df)
        assert "page" in result.columns

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = _date_col(df)
        assert result.empty


# ---------------------------------------------------------------------------
# _run
# ---------------------------------------------------------------------------


class TestRun:
    """Tests for _run()."""

    def test_returns_df_from_result(self):
        expected = pd.DataFrame({"sessions": [42]})
        mg = _make_mg(expected)
        df = _run(mg, ["date"], ["sessions"], filter_d="x==1", sort="date")
        assert len(df) == 1
        assert df["sessions"].iloc[0] == 42
        mg.report.run.assert_called_once_with(
            d=["date"], m=["sessions"],
            filter_d="x==1", sort="date", show=False,
        )

    def test_returns_empty_on_none(self):
        mg = _make_mg(None)
        df = _run(mg, ["date"], ["sessions"])
        assert df.empty

    def test_limit_passed_to_run(self):
        mg = _make_mg(pd.DataFrame({"sessions": [1]}))
        _run(mg, ["date"], ["sessions"], limit=50)
        mg.report.run.assert_called_once_with(
            d=["date"], m=["sessions"],
            filter_d=None, sort=None, show=False,
            limit=50,
        )

    def test_limit_none_not_passed(self):
        """limit=None → kwargs に limit が含まれない（後方互換）。"""
        mg = _make_mg(pd.DataFrame({"sessions": [1]}))
        _run(mg, ["date"], ["sessions"])
        call_kwargs = mg.report.run.call_args.kwargs
        assert "limit" not in call_kwargs


# ---------------------------------------------------------------------------
# fetch_daily_metrics
# ---------------------------------------------------------------------------


class TestFetchDailyMetrics:
    """Tests for fetch_daily_metrics()."""

    def test_basic(self):
        raw = pd.DataFrame({
            "date": ["2026-02-04", "2026-02-05"],
            "uu": [100, 200],
            "sessions": [120, 250],
            "pv": [300, 500],
        })
        mg = _make_mg(raw)
        df = fetch_daily_metrics(mg, "2026-02-01", "2026-02-15")

        mg.report.set.dates.assert_called_once_with("2026-02-01", "2026-02-15")
        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert len(df) == 2

    def test_empty_result(self):
        mg = _make_mg(None)
        df = fetch_daily_metrics(mg, "2026-01-01", "2026-01-31")
        assert df.empty

    def test_custom_hostname(self):
        mg = _make_mg(pd.DataFrame({"date": ["2026-01-01"], "uu": [1], "sessions": [1], "pv": [1]}))
        fetch_daily_metrics(mg, "2026-01-01", "2026-01-31", hostname="example.com")
        call_kwargs = mg.report.run.call_args
        assert "example.com" in call_kwargs.kwargs["filter_d"]


# ---------------------------------------------------------------------------
# fetch_page_metrics
# ---------------------------------------------------------------------------


class TestFetchPageMetrics:
    """Tests for fetch_page_metrics()."""

    def test_merge_and_read_rate(self):
        df_pv = pd.DataFrame({
            "page": ["/slqm/jp/70th/", "/slqm/jp/top/"],
            "uu": [100, 50],
            "pv": [200, 80],
        })
        df_ft = pd.DataFrame({
            "page": ["/slqm/jp/70th/"],
            "footer_views": [37],
        })
        mg = _make_mg_multi([df_pv, df_ft])
        df = fetch_page_metrics(mg, "2026-02-01", "2026-02-15")

        assert len(df) == 2
        assert "read_rate" in df.columns
        # /70th/ has footer 37 / uu 100 = 0.37
        row_70th = df.loc[df["page"] == "/slqm/jp/70th/"]
        assert row_70th["read_rate"].iloc[0] == pytest.approx(0.37)
        # /top/ has no footer → 0
        row_top = df.loc[df["page"] == "/slqm/jp/top/"]
        assert row_top["footer_views"].iloc[0] == 0
        assert row_top["read_rate"].iloc[0] == 0.0

    def test_sorted_by_uu_desc(self):
        df_pv = pd.DataFrame({
            "page": ["/a", "/b", "/c"],
            "uu": [10, 50, 30],
            "pv": [20, 100, 60],
        })
        df_ft = pd.DataFrame({"page": [], "footer_views": []})
        mg = _make_mg_multi([df_pv, df_ft])
        df = fetch_page_metrics(mg, "2026-01-01", "2026-01-31")
        assert df["uu"].tolist() == [50, 30, 10]

    def test_empty_pv_returns_empty(self):
        mg = _make_mg_multi([pd.DataFrame(), pd.DataFrame()])
        df = fetch_page_metrics(mg, "2026-01-01", "2026-01-31")
        assert df.empty

    def test_pv_none_returns_empty(self):
        mg = _make_mg_multi([None, None])
        df = fetch_page_metrics(mg, "2026-01-01", "2026-01-31")
        assert df.empty


# ---------------------------------------------------------------------------
# fetch_channel_breakdown
# ---------------------------------------------------------------------------


class TestFetchChannelBreakdown:
    """Tests for fetch_channel_breakdown()."""

    def test_without_landing_pattern(self):
        mg = _make_mg(pd.DataFrame({
            "channel": ["Organic Search", "Direct"],
            "uu": [100, 50],
            "sessions": [120, 60],
        }))
        df = fetch_channel_breakdown(mg, "2026-02-01", "2026-02-15")
        assert len(df) == 2
        flt = mg.report.run.call_args.kwargs["filter_d"]
        assert "pagePath" in flt
        assert "landingPage" not in flt

    def test_with_landing_pattern(self):
        mg = _make_mg(pd.DataFrame({
            "channel": ["Social"],
            "uu": [200],
            "sessions": [250],
        }))
        df = fetch_channel_breakdown(
            mg, "2026-02-01", "2026-02-15",
            landing_pattern=r"/slqm/(en|jp)/70th",
        )
        flt = mg.report.run.call_args.kwargs["filter_d"]
        assert "landingPage" in flt
        assert "70th" in flt


# ---------------------------------------------------------------------------
# fetch_source_medium
# ---------------------------------------------------------------------------


class TestFetchSourceMedium:
    """Tests for fetch_source_medium()."""

    def test_basic(self):
        mg = _make_mg(pd.DataFrame({
            "source_medium": ["google / organic", "facebook / social"],
            "uu": [80, 60],
            "sessions": [100, 70],
        }))
        df = fetch_source_medium(mg, "2026-02-01", "2026-02-15")
        assert len(df) == 2

    def test_landing_pattern_filter(self):
        mg = _make_mg(pd.DataFrame({
            "source_medium": ["google / organic"],
            "uu": [10],
            "sessions": [12],
        }))
        fetch_source_medium(mg, "2026-02-01", "2026-02-15", landing_pattern=r"/70th")
        flt = mg.report.run.call_args.kwargs["filter_d"]
        assert "landingPage" in flt
        assert "session_start" in flt

    def test_limit_passed(self):
        mg = _make_mg(pd.DataFrame({
            "source_medium": ["google / organic"],
            "uu": [10],
            "sessions": [12],
        }))
        fetch_source_medium(mg, "2026-02-01", "2026-02-15", limit=5)
        assert mg.report.run.call_args.kwargs["limit"] == 5


# ---------------------------------------------------------------------------
# fetch_landing_pages
# ---------------------------------------------------------------------------


class TestFetchLandingPages:
    """Tests for fetch_landing_pages()."""

    def test_basic(self):
        mg = _make_mg(pd.DataFrame({
            "landing": ["/slqm/jp/top/", "/slqm/jp/70th/"],
            "uu": [50, 30],
            "sessions": [60, 35],
        }))
        df = fetch_landing_pages(mg, "2026-02-01", "2026-02-15")
        assert len(df) == 2
        assert "landing" in df.columns

    def test_limit_passed(self):
        mg = _make_mg(pd.DataFrame({
            "landing": ["/slqm/jp/top/"],
            "uu": [50],
            "sessions": [60],
        }))
        fetch_landing_pages(mg, "2026-02-01", "2026-02-15", limit=10)
        assert mg.report.run.call_args.kwargs["limit"] == 10


# ---------------------------------------------------------------------------
# fetch_session_quality
# ---------------------------------------------------------------------------


class TestFetchSessionQuality:
    """Tests for fetch_session_quality()."""

    def test_numeric_conversion(self):
        mg = _make_mg(pd.DataFrame({
            "landing": ["/slqm/jp/top/"],
            "uu": [100],
            "sessions": [120],
            "avg_duration": ["1769.5"],  # returned as string by GA4 API
            "pages_per_session": ["3.2"],
        }))
        df = fetch_session_quality(mg, "2026-02-01", "2026-02-15")
        assert pd.api.types.is_float_dtype(df["avg_duration"])
        assert pd.api.types.is_float_dtype(df["pages_per_session"])
        assert df["avg_duration"].iloc[0] == pytest.approx(1769.5)

    def test_empty_result(self):
        mg = _make_mg(None)
        df = fetch_session_quality(mg, "2026-01-01", "2026-01-31")
        assert df.empty


# ---------------------------------------------------------------------------
# fetch_new_vs_returning
# ---------------------------------------------------------------------------


class TestFetchNewVsReturning:
    """Tests for fetch_new_vs_returning()."""

    def test_basic(self):
        mg = _make_mg(pd.DataFrame({
            "user_type": ["new", "returning"],
            "uu": [800, 200],
            "sessions": [800, 300],
        }))
        df = fetch_new_vs_returning(mg, "2026-02-01", "2026-02-15")
        assert len(df) == 2
        assert set(df["user_type"]) == {"new", "returning"}


# ---------------------------------------------------------------------------
# fetch_page_transitions
# ---------------------------------------------------------------------------


class TestFetchPageTransitions:
    """Tests for fetch_page_transitions()."""

    def test_basic_with_from_short(self):
        mg = _make_mg(pd.DataFrame({
            "from_page": [
                "https://corp.shiseido.com/slqm/jp/70th/?utm_source=fb",
                "https://corp.shiseido.com/slqm/jp/70th/history.html",
            ],
            "to_page": ["/slqm/jp/top/", "/slqm/jp/products/"],
            "users": [50, 30],
        }))
        df = fetch_page_transitions(
            mg, "2026-02-01", "2026-02-15",
            from_pattern=r"corp.shiseido.com/slqm/(en|jp)/70th/",
        )
        assert "from_short" in df.columns
        # UTM stripped, host stripped
        assert df["from_short"].iloc[0] == "/slqm/jp/70th/"
        assert df["from_short"].iloc[1] == "/slqm/jp/70th/history.html"

    def test_empty_result(self):
        mg = _make_mg(None)
        df = fetch_page_transitions(
            mg, "2026-01-01", "2026-01-31",
            from_pattern=r"nonexistent",
        )
        assert df.empty

    def test_filter_includes_both_patterns(self):
        mg = _make_mg(pd.DataFrame({
            "from_page": ["https://example.com/a"],
            "to_page": ["/b"],
            "users": [1],
        }))
        fetch_page_transitions(
            mg, "2026-02-01", "2026-02-15",
            from_pattern=r"from_pat",
            to_pattern=r"to_pat",
        )
        flt = mg.report.run.call_args.kwargs["filter_d"]
        assert "from_pat" in flt
        assert "to_pat" in flt
        assert "pageReferrer" in flt
