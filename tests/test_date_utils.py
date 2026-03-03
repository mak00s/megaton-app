"""Tests for megaton_lib.date_utils."""

import datetime as dt

import pandas as pd

import megaton_lib.date_utils as date_utils
from megaton_lib.date_utils import (
    drop_current_month_rows,
    month_ranges_between,
    month_ranges_for_year,
    month_start_months_ago,
    month_suffix_months_ago,
    months_between,
    now_in_tz,
    parse_year_month_series,
    previous_month_range,
    previous_year_start,
    select_recent_months,
)


class TestMonthRangesForYear:
    def test_returns_12_months(self):
        result = month_ranges_for_year(2025)
        assert len(result) == 12

    def test_first_and_last_month(self):
        result = month_ranges_for_year(2025)
        assert result[0] == ("2025-01-01", "2025-01-31")
        assert result[11] == ("2025-12-01", "2025-12-31")

    def test_february_non_leap(self):
        result = month_ranges_for_year(2025)
        assert result[1] == ("2025-02-01", "2025-02-28")

    def test_february_leap(self):
        result = month_ranges_for_year(2024)
        assert result[1] == ("2024-02-01", "2024-02-29")


class TestMonthRangesBetween:
    def test_single_month(self):
        result = month_ranges_between("2025-03-01", "2025-03-31")
        assert result == [("2025-03-01", "2025-03-31")]

    def test_partial_months(self):
        result = month_ranges_between("2025-03-15", "2025-05-10")
        assert result == [
            ("2025-03-15", "2025-03-31"),
            ("2025-04-01", "2025-04-30"),
            ("2025-05-01", "2025-05-10"),
        ]

    def test_cross_year(self):
        result = month_ranges_between("2025-12-01", "2026-01-31")
        assert len(result) == 2
        assert result[0] == ("2025-12-01", "2025-12-31")
        assert result[1] == ("2026-01-01", "2026-01-31")

    def test_invalid_dates(self):
        assert month_ranges_between("bad", "2025-01-01") == []


class TestMonthsBetween:
    def test_basic(self):
        result = months_between("2025-11-01", "2026-01-31")
        assert result == ["202511", "202512", "202601"]

    def test_single_month(self):
        result = months_between("2025-06-15", "2025-06-20")
        assert result == ["202506"]

    def test_invalid(self):
        assert months_between("bad", "also-bad") == []


class TestTimezoneMonthHelpers:
    def test_now_in_tz_returns_aware_datetime(self):
        got = now_in_tz("Asia/Tokyo")
        assert isinstance(got, dt.datetime)
        assert got.tzinfo is not None
        assert getattr(got.tzinfo, "key", None) == "Asia/Tokyo"

    def test_previous_month_range(self):
        start, end = previous_month_range(reference=dt.date(2026, 3, 15))
        assert start == "2026-02-01"
        assert end == "2026-02-28"

    def test_previous_month_range_leap_year(self):
        start, end = previous_month_range(reference=dt.date(2024, 3, 1))
        assert start == "2024-02-01"
        assert end == "2024-02-29"

    def test_month_start_months_ago(self):
        got = month_start_months_ago(13, reference=dt.date(2026, 3, 15))
        assert got == "2025-02-01"

    def test_previous_year_start(self):
        got = previous_year_start(reference=dt.date(2026, 3, 15))
        assert got == "2025-01-01"

    def test_month_suffix_months_ago(self):
        got = month_suffix_months_ago(2, reference=dt.date(2026, 3, 15))
        assert got == "2026.01"


class TestParseYearMonthSeries:
    def test_mixed_formats(self):
        s = pd.Series([202301, "202302.0", "2023-03", "2023/4", "2023年5月", "bad"])
        got = parse_year_month_series(s)
        assert got.iloc[0] == pd.Timestamp("2023-01-01")
        assert got.iloc[1] == pd.Timestamp("2023-02-01")
        assert got.iloc[2] == pd.Timestamp("2023-03-01")
        assert got.iloc[3] == pd.Timestamp("2023-04-01")
        assert got.iloc[4] == pd.Timestamp("2023-05-01")
        assert pd.isna(got.iloc[5])

    def test_datetime_series(self):
        s = pd.Series(pd.to_datetime(["2026-03-20", "2026-03-01"]))
        got = parse_year_month_series(s)
        assert got.tolist() == [pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-01")]


class TestMonthDataframeFilters:
    def test_drop_current_month_rows_with_yyyymm_column(self, monkeypatch):
        fixed_now = dt.datetime(2026, 3, 20, tzinfo=dt.timezone(dt.timedelta(hours=9)))
        monkeypatch.setattr(date_utils, "now_in_tz", lambda _tz="Asia/Tokyo": fixed_now)

        df = pd.DataFrame({"month": ["202602", "202603"], "v": [1, 2]})
        got = drop_current_month_rows(df, month_col="month")
        assert got["month"].tolist() == ["202602"]

    def test_drop_current_month_rows_with_datetime_column(self, monkeypatch):
        fixed_now = dt.datetime(2026, 3, 20, tzinfo=dt.timezone(dt.timedelta(hours=9)))
        monkeypatch.setattr(date_utils, "now_in_tz", lambda _tz="Asia/Tokyo": fixed_now)

        df = pd.DataFrame(
            {
                "month": [pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")],
                "v": [1, 2],
            }
        )
        got = drop_current_month_rows(df, month_col="month")
        assert got["month"].tolist() == [pd.Timestamp("2026-02-01")]

    def test_select_recent_months_with_datetime_column(self):
        df = pd.DataFrame(
            {
                "month": pd.to_datetime(["2025-12-01", "2026-01-01", "2026-02-01", "2026-03-01"]),
                "v": [1, 2, 3, 4],
            }
        )
        got = select_recent_months(df, month_col="month", months=2)
        assert got["month"].tolist() == [pd.Timestamp("2026-02-01"), pd.Timestamp("2026-03-01")]

    def test_select_recent_months_with_yyyymm_numeric_column(self):
        df = pd.DataFrame({"month": [202512, 202601, 202602, 202603], "v": [1, 2, 3, 4]})
        got = select_recent_months(df, month_col="month", months=2)
        assert got["month"].tolist() == [202602, 202603]

    def test_select_recent_months_empty(self):
        df = pd.DataFrame({"month": [], "v": []})
        got = select_recent_months(df, month_col="month", months=2)
        assert got.empty
