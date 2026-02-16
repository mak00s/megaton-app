"""Tests for megaton_lib.ga4_helpers."""

import types
from unittest.mock import MagicMock

import pandas as pd

from megaton_lib.ga4_helpers import (
    build_filter,
    run_report_df,
    to_datetime_col,
    to_numeric_cols,
)


def _make_mg(df: pd.DataFrame | None = None):
    mg = MagicMock()
    if df is not None:
        mg.report.run.return_value = types.SimpleNamespace(df=df)
    else:
        mg.report.run.return_value = None
    return mg


class TestRunReportDf:
    def test_returns_df_from_result(self):
        expected = pd.DataFrame({"sessions": [42]})
        mg = _make_mg(expected)
        df = run_report_df(mg, ["date"], ["sessions"], filter_d="x==1", sort="date")
        assert len(df) == 1
        assert df["sessions"].iloc[0] == 42
        mg.report.run.assert_called_once_with(
            d=["date"],
            m=["sessions"],
            filter_d="x==1",
            sort="date",
            show=False,
        )

    def test_returns_empty_on_none(self):
        mg = _make_mg(None)
        df = run_report_df(mg, ["date"], ["sessions"])
        assert df.empty

    def test_limit_passed_to_run(self):
        mg = _make_mg(pd.DataFrame({"sessions": [1]}))
        run_report_df(mg, ["date"], ["sessions"], limit=50)
        mg.report.run.assert_called_once_with(
            d=["date"],
            m=["sessions"],
            filter_d=None,
            sort=None,
            show=False,
            limit=50,
        )

    def test_limit_none_not_passed(self):
        mg = _make_mg(pd.DataFrame({"sessions": [1]}))
        run_report_df(mg, ["date"], ["sessions"])
        call_kwargs = mg.report.run.call_args.kwargs
        assert "limit" not in call_kwargs


class TestBuildFilter:
    def test_joins_non_empty_parts(self):
        out = build_filter("hostName=~corp", "", None, "eventName==page_view")
        assert out == "hostName=~corp;eventName==page_view"

    def test_returns_none_when_empty(self):
        assert build_filter("", None, "   ") is None


class TestToDatetimeCol:
    def test_converts_date_column(self):
        df = pd.DataFrame({"date": ["2026-02-04", "2026-02-05"], "uu": [10, 20]})
        result = to_datetime_col(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        assert df["date"].dtype == object

    def test_no_date_column(self):
        df = pd.DataFrame({"page": ["/a"], "uu": [10]})
        result = to_datetime_col(df)
        assert "page" in result.columns


class TestToNumericCols:
    def test_fillna_and_int(self):
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": ["x", "y", "z"]})
        result = to_numeric_cols(df, ["a"], fillna=0, as_int=True)
        assert result["a"].tolist() == [1, 0, 3]
        assert result["a"].dtype == int

    def test_float_conversion(self):
        df = pd.DataFrame({"x": ["1.2", "3.4"]})
        result = to_numeric_cols(df, ["x"])
        assert pd.api.types.is_float_dtype(result["x"])
