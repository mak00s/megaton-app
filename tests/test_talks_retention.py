"""Tests for megaton_lib.talks_retention."""

from megaton_lib.talks_retention import resolve_cohort_months

import pandas as pd


class TestResolveCohortMonths:
    def test_from_dates(self):
        result = resolve_cohort_months("2025-11-01", "2026-01-31")
        assert result == ["202511", "202512", "202601"]

    def test_from_dataframe(self):
        df = pd.DataFrame({"month": ["202601", "202602"]})
        result = resolve_cohort_months("bad", "bad", df)
        assert result == ["202601", "202602"]

    def test_empty(self):
        assert resolve_cohort_months("bad", "bad") == []
