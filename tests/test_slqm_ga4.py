"""Tests for megaton_lib.slqm_ga4."""

import pandas as pd
import pytest

from megaton_lib.slqm_ga4 import (
    get_13month_start,
    ym_from_year_month,
    safe_merge_many,
    fillna_int,
    compute_sp_ratio,
)


# --- get_13month_start ---


class TestGet13MonthStart:
    """Tests for get_13month_start()."""

    def test_basic(self):
        assert get_13month_start("2026-01-31") == "2025-01-01"

    def test_mid_month(self):
        assert get_13month_start("2025-03-15") == "2024-03-01"

    def test_december(self):
        assert get_13month_start("2025-12-31") == "2024-12-01"

    def test_january(self):
        assert get_13month_start("2025-01-01") == "2024-01-01"


# --- ym_from_year_month ---


class TestYmFromYearMonth:
    """Tests for ym_from_year_month()."""

    def test_basic(self):
        df = pd.DataFrame({"year": ["2025"], "month": ["3"], "pv": [100]})
        result = ym_from_year_month(df)
        assert "ym" in result.columns
        assert "year" not in result.columns
        assert "month" not in result.columns
        assert result["ym"].iloc[0] == "2025/3/1"
        assert result.columns[0] == "ym"

    def test_empty(self):
        result = ym_from_year_month(pd.DataFrame())
        assert result.empty

    def test_none(self):
        result = ym_from_year_month(None)
        assert result.empty

    def test_missing_columns(self):
        df = pd.DataFrame({"year": [2025], "pv": [100]})
        with pytest.raises(ValueError, match="Missing required columns"):
            ym_from_year_month(df)


# --- safe_merge_many ---


class TestSafeMergeMany:
    """Tests for safe_merge_many()."""

    def test_two_dfs(self):
        df1 = pd.DataFrame({"ym": ["2025/1/1"], "uu": [10]})
        df2 = pd.DataFrame({"ym": ["2025/1/1"], "pv": [100]})
        result = safe_merge_many([df1, df2], on=["ym"])
        assert "uu" in result.columns
        assert "pv" in result.columns
        assert len(result) == 1

    def test_named_dfs(self):
        df1 = pd.DataFrame({"ym": ["2025/1/1"], "uu": [10]})
        df2 = pd.DataFrame({"ym": ["2025/1/1"], "pv": [100]})
        result = safe_merge_many([("base", df1), ("extra", df2)], on=["ym"])
        assert len(result) == 1

    def test_empty_list(self):
        result = safe_merge_many([])
        assert result.empty

    def test_none_df_skipped(self):
        df1 = pd.DataFrame({"ym": ["2025/1/1"], "uu": [10]})
        result = safe_merge_many([("base", df1), ("null", None)], on=["ym"])
        assert len(result) == 1
        assert "uu" in result.columns

    def test_outer_merge(self):
        df1 = pd.DataFrame({"ym": ["2025/1/1"], "uu": [10]})
        df2 = pd.DataFrame({"ym": ["2025/2/1"], "pv": [200]})
        result = safe_merge_many([df1, df2], on=["ym"], how="outer")
        assert len(result) == 2

    def test_preserve_int(self):
        df1 = pd.DataFrame({"ym": ["2025/1/1", "2025/2/1"], "uu": [10, 20]})
        df2 = pd.DataFrame({"ym": ["2025/1/1"], "pv": [100]})
        result = safe_merge_many([df1, df2], on=["ym"], how="left")
        # pv should be Int64 (nullable) because one NaN
        assert pd.api.types.is_integer_dtype(result["pv"])


# --- fillna_int ---


class TestFillnaInt:
    """Tests for fillna_int()."""

    def test_basic(self):
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": ["x", "y", "z"]})
        result = fillna_int(df, ["a"])
        assert result["a"].dtype == int
        assert result["a"].tolist() == [1, 0, 3]

    def test_missing_col_ignored(self):
        df = pd.DataFrame({"a": [1.0]})
        result = fillna_int(df, ["a", "nonexistent"])
        assert result["a"].dtype == int


# --- compute_sp_ratio ---


class TestComputeSpRatio:
    """Tests for compute_sp_ratio()."""

    def test_basic(self):
        df = pd.DataFrame({
            "ym": ["2025/1/1", "2025/1/1", "2025/1/1"],
            "device": ["mobile", "desktop", "tablet"],
            "uu": [30, 50, 20],
        })
        result = compute_sp_ratio(df, group_cols=["ym"])
        assert len(result) == 1
        assert result["sp_ratio"].iloc[0] == 0.5  # (30+20)/100

    def test_with_page(self):
        df = pd.DataFrame({
            "ym": ["2025/1/1", "2025/1/1"],
            "page": ["/a", "/a"],
            "device": ["mobile", "desktop"],
            "uu": [40, 60],
        })
        result = compute_sp_ratio(df, group_cols=["ym", "page"])
        assert len(result) == 1
        assert result["sp_ratio"].iloc[0] == 0.4
