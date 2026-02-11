"""Tests for megaton_lib.date_utils."""

from megaton_lib.date_utils import month_ranges_for_year, month_ranges_between, months_between


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
