"""Tests for lib/date_template.py — resolve_date(), resolve_dates_in_params()."""

from datetime import date
from datetime import datetime as real_datetime
from zoneinfo import ZoneInfo

import pytest

import megaton_lib.date_template as dt_mod
from megaton_lib.date_template import resolve_date, resolve_dates_in_params


class TestResolveDate:
    """Tests for resolve_date()."""

    # Fixed reference date: 2026-02-07 (Saturday)
    REF = date(2026, 2, 7)

    def test_absolute_date_passthrough(self):
        assert resolve_date("2026-01-15", reference=self.REF) == "2026-01-15"

    def test_absolute_date_compact_yyyymmdd(self):
        assert resolve_date("20260115", reference=self.REF) == "2026-01-15"

    def test_today(self):
        assert resolve_date("today", reference=self.REF) == "2026-02-07"

    def test_today_minus_days(self):
        assert resolve_date("today-7d", reference=self.REF) == "2026-01-31"

    def test_today_minus_30d(self):
        assert resolve_date("today-30d", reference=self.REF) == "2026-01-08"

    def test_today_plus_days(self):
        assert resolve_date("today+3d", reference=self.REF) == "2026-02-10"

    def test_month_start(self):
        assert resolve_date("month-start", reference=self.REF) == "2026-02-01"

    def test_month_end(self):
        assert resolve_date("month-end", reference=self.REF) == "2026-02-28"

    def test_month_end_leap_year(self):
        ref = date(2024, 2, 15)
        assert resolve_date("month-end", reference=ref) == "2024-02-29"

    def test_year_start(self):
        assert resolve_date("year-start", reference=self.REF) == "2026-01-01"

    def test_year_end(self):
        assert resolve_date("year-end", reference=self.REF) == "2026-12-31"

    def test_prev_month_start(self):
        assert resolve_date("prev-month-start", reference=self.REF) == "2026-01-01"

    def test_prev_month_end(self):
        assert resolve_date("prev-month-end", reference=self.REF) == "2026-01-31"

    def test_prev_month_january(self):
        """For January, previous month is December."""
        ref = date(2026, 1, 15)
        assert resolve_date("prev-month-start", reference=ref) == "2025-12-01"
        assert resolve_date("prev-month-end", reference=ref) == "2025-12-31"

    def test_week_start(self):
        """2026-02-07 is Saturday -> week-start is 2026-02-02."""
        assert resolve_date("week-start", reference=self.REF) == "2026-02-02"

    def test_week_start_on_monday(self):
        """Monday -> remains Monday."""
        ref = date(2026, 2, 2)  # Monday
        assert resolve_date("week-start", reference=ref) == "2026-02-02"

    def test_whitespace_trimmed(self):
        assert resolve_date("  today  ", reference=self.REF) == "2026-02-07"

    def test_unknown_expression_raises(self):
        with pytest.raises(ValueError, match="Unknown date template"):
            resolve_date("yesterday", reference=self.REF)

    def test_invalid_expression_raises(self):
        with pytest.raises(ValueError, match="Unknown date template"):
            resolve_date("today-7", reference=self.REF)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            resolve_date("", reference=self.REF)

    def test_invalid_absolute_date_raises(self):
        with pytest.raises(ValueError, match="Invalid absolute date"):
            resolve_date("2026-13-40", reference=self.REF)

    def test_invalid_absolute_date_compact_raises(self):
        with pytest.raises(ValueError, match="Invalid absolute date"):
            resolve_date("20261340", reference=self.REF)

    def test_today_uses_configured_timezone(self, monkeypatch):
        class FakeDatetime:
            @classmethod
            def now(cls, tz=None):
                # Boundary time where UTC date is 2026-02-06 and JST date is 2026-02-07.
                base = real_datetime(2026, 2, 6, 15, 30, tzinfo=ZoneInfo("UTC"))
                return base.astimezone(tz) if tz else base

            @classmethod
            def strptime(cls, value, fmt):
                return real_datetime.strptime(value, fmt)

        monkeypatch.setattr(dt_mod, "datetime", FakeDatetime)

        monkeypatch.setenv("DATE_TEMPLATE_TZ", "UTC")
        assert resolve_date("today") == "2026-02-06"

        monkeypatch.setenv("DATE_TEMPLATE_TZ", "Asia/Tokyo")
        assert resolve_date("today") == "2026-02-07"

    def test_invalid_timezone_falls_back_to_jst(self, monkeypatch):
        class FakeDatetime:
            @classmethod
            def now(cls, tz=None):
                base = real_datetime(2026, 2, 6, 15, 30, tzinfo=ZoneInfo("UTC"))
                return base.astimezone(tz) if tz else base

            @classmethod
            def strptime(cls, value, fmt):
                return real_datetime.strptime(value, fmt)

        monkeypatch.setattr(dt_mod, "datetime", FakeDatetime)
        monkeypatch.setenv("DATE_TEMPLATE_TZ", "Invalid/Timezone")
        # invalid timezone -> fallback Asia/Tokyo
        assert resolve_date("today") == "2026-02-07"


class TestResolveDatesInParams:
    """Tests for resolve_dates_in_params()."""

    REF = date(2026, 2, 7)

    def test_template_resolved(self):
        params = {
            "source": "gsc",
            "date_range": {"start": "today-7d", "end": "today-3d"},
            "dimensions": ["query"],
        }
        # resolve_date uses global reference/date handling; no direct check here.
        # resolve_dates_in_params internally uses module date logic.
        # resolve_date itself is already tested, so this checks structure transformation only.
        result = resolve_dates_in_params(params)
        assert result["date_range"]["start"] != "today-7d"
        assert result["date_range"]["end"] != "today-3d"
        # Original dict remains unchanged
        assert params["date_range"]["start"] == "today-7d"

    def test_absolute_dates_unchanged(self):
        params = {
            "source": "ga4",
            "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
        }
        result = resolve_dates_in_params(params)
        assert result is params  # same object when unchanged

    def test_no_date_range(self):
        """When date_range is absent (e.g. bigquery), params are unchanged."""
        params = {"source": "bigquery", "sql": "SELECT 1"}
        result = resolve_dates_in_params(params)
        assert result is params

    def test_mixed_absolute_and_template(self):
        params = {
            "source": "gsc",
            "date_range": {"start": "2026-01-01", "end": "today-3d"},
        }
        result = resolve_dates_in_params(params)
        assert result["date_range"]["start"] == "2026-01-01"
        assert result["date_range"]["end"] != "today-3d"


class TestValidatorIntegration:
    """Whether validate_params() accepts date templates."""

    def test_template_dates_pass_validation(self):
        from megaton_lib.params_validator import validate_params

        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "today-30d", "end": "today-3d"},
            "dimensions": ["query"],
        }
        normalized, errors = validate_params(data)
        assert errors == []
        # Templates are resolved to YYYY-MM-DD
        assert normalized["date_range"]["start"] != "today-30d"
        assert len(normalized["date_range"]["start"]) == 10  # YYYY-MM-DD

    def test_invalid_template_rejected(self):
        from megaton_lib.params_validator import validate_params

        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "yesterday", "end": "today"},
            "dimensions": ["query"],
        }
        normalized, errors = validate_params(data)
        assert any(e["error_code"] == "INVALID_DATE" for e in errors)

    def test_invalid_absolute_date_rejected(self):
        from megaton_lib.params_validator import validate_params

        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "2026-13-40", "end": "today"},
            "dimensions": ["query"],
        }
        normalized, errors = validate_params(data)
        assert normalized is None
        assert any(e["error_code"] == "INVALID_DATE" for e in errors)

    def test_prev_month_range_passes(self):
        from megaton_lib.params_validator import validate_params

        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254800682",
            "date_range": {"start": "prev-month-start", "end": "prev-month-end"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
        }
        normalized, errors = validate_params(data)
        assert errors == []
        # Resolved dates are in expected format
        start = normalized["date_range"]["start"]
        end = normalized["date_range"]["end"]
        assert start < end  # start < end
