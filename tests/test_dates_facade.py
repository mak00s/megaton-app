"""Tests for megaton_lib.dates facade and new date_template tokens."""
from datetime import date

import pytest

from megaton_lib import dates
from megaton_lib.date_template import resolve_date, resolve_month

REF = date(2026, 3, 15)


class TestPrevPrevMonthTokens:
    def test_prev_prev_month_start(self):
        assert resolve_date("prev-prev-month-start", reference=REF) == "2026-01-01"

    def test_prev_prev_month_end(self):
        assert resolve_date("prev-prev-month-end", reference=REF) == "2026-01-31"

    def test_year_boundary(self):
        assert resolve_date("prev-prev-month-start", reference=date(2026, 1, 10)) == "2025-11-01"
        assert resolve_date("prev-prev-month-end", reference=date(2026, 2, 5)) == "2025-12-31"


class TestResolveMonth:
    def test_tokens(self):
        assert resolve_month("this-month", reference=REF) == "202603"
        assert resolve_month("prev-month", reference=REF) == "202602"
        assert resolve_month("prev-prev-month", reference=REF) == "202601"

    def test_passthrough(self):
        assert resolve_month("202512", reference=REF) == "202512"

    def test_year_boundary(self):
        assert resolve_month("prev-month", reference=date(2026, 1, 10)) == "202512"
        assert resolve_month("prev-prev-month", reference=date(2026, 1, 10)) == "202511"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            resolve_month("next-month", reference=REF)


class TestDateObjectApi:
    def test_today_in_timezone_at_utc_month_boundary(self):
        # Ported from megaton-notebooks tests/test_date_periods.py:
        # GHA runners are UTC; JST must roll over to the next day/month.
        from datetime import UTC, datetime

        utc_now = datetime(2026, 5, 31, 15, 30, tzinfo=UTC)
        assert dates.today_in_timezone("UTC", now=utc_now) == date(2026, 5, 31)
        assert dates.today_in_timezone("Asia/Tokyo", now=utc_now) == date(2026, 6, 1)

    def test_resolve_period_month_compat(self):
        assert dates.resolve_period_month("prev-month", today=REF) == "202602"
        assert dates.resolve_period_month("202401", today=REF) == "202401"

    def test_previous_month_window(self):
        start, end = dates.previous_month_window(REF)
        assert (start, end) == (date(2026, 2, 1), date(2026, 2, 28))

    def test_month_before_window(self):
        start, end = dates.month_before_window(date(2026, 2, 1))
        assert (start, end) == (date(2026, 1, 1), date(2026, 1, 31))

    def test_resolve_period_date_tokens(self):
        assert dates.resolve_period_date("prev-month-start", today=REF) == date(2026, 2, 1)
        assert dates.resolve_period_date("prev-prev-month-end", today=REF) == date(2026, 1, 31)

    def test_resolve_period_date_absolute(self):
        assert dates.resolve_period_date("2026-05-09", today=REF) == date(2026, 5, 9)

    def test_previous_month_label(self):
        assert dates.previous_month_label(REF) == "2026/02/01 - 2026/02/28"


class TestResolveEffectiveMonthsAgo:
    def test_unchanged_outside_gha(self, monkeypatch):
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        assert dates.resolve_effective_months_ago(0, switch_day=4) == 0

    def test_gha_early_month_switches_to_prev(self, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setattr(dates, "now_in_tz", lambda tz: date(2026, 3, 2))
        assert dates.resolve_effective_months_ago(0, switch_day=4) == 1

    def test_gha_after_switch_day_keeps_current(self, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setattr(dates, "now_in_tz", lambda tz: date(2026, 3, 10))
        assert dates.resolve_effective_months_ago(0, switch_day=4) == 0

    def test_nonzero_target_unchanged_in_gha(self, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        assert dates.resolve_effective_months_ago(2, switch_day=4) == 2


class TestFacadeExports:
    def test_all_names_importable(self):
        for name in dates.__all__:
            assert hasattr(dates, name), name
