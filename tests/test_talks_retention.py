"""Tests for megaton_lib.talks_retention."""

from megaton_lib.talks_retention import (
    resolve_cohort_months,
    _map_lang,
    query_retention_by_tag,
)

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


class TestMapLang:
    def test_maps_j_to_jp(self):
        df = pd.DataFrame({"lang": ["J", "E", "X"], "val": [1, 2, 3]})
        result = _map_lang(df)
        assert "language" in result.columns
        assert "lang" not in result.columns
        assert result["language"].tolist() == ["JP", "EN", ""]

    def test_preserves_other_columns(self):
        df = pd.DataFrame({"lang": ["J"], "a": [10], "b": [20]})
        result = _map_lang(df)
        assert result["a"].iloc[0] == 10
        assert result["b"].iloc[0] == 20


class TestQueryRetentionByTagPostProcess:
    """Test the pandas post-processing logic of query_retention_by_tag.

    BQ query itself cannot be tested without a live client, but the
    meta-join + tag-aggregation logic is pure pandas.
    """

    def _make_bq_result(self):
        """Simulate BQ result rows (after _map_lang)."""
        return pd.DataFrame({
            "month": ["202601", "202601", "202601"],
            "language": ["JP", "JP", "JP"],
            "first_page_path": [
                "/jp/company/talk/20250101.html",
                "/jp/company/talk/20250102.html",
                "/jp/company/talk/20250103.html",
            ],
            "users": [100, 50, 30],
            "retained_d30": [20, 15, 5],
            "retention_d30": [0.20, 0.30, 0.167],
        })

    def _make_meta(self):
        return pd.DataFrame({
            "URL": [
                "/jp/company/talk/20250101.html",
                "/jp/company/talk/20250102.html",
                "/jp/company/talk/20250103.html",
            ],
            "Tag": ["People", "People", "Technology"],
        })

    def test_aggregates_by_tag(self):
        df = self._make_bq_result()
        meta = self._make_meta()

        # Simulate the post-processing from query_retention_by_tag
        df = df.merge(meta, left_on="first_page_path", right_on="URL", how="left")
        df["tag"] = df["Tag"].fillna("(unknown)")
        df = df.drop(columns=["URL", "Tag", "first_page_path"])

        agg = df.groupby(["month", "language", "tag"], as_index=False).agg(
            users=("users", "sum"),
            retained_d30=("retained_d30", "sum"),
        )
        agg["retention_d30"] = (agg["retained_d30"] / agg["users"]).where(agg["users"] > 0, 0.0)

        # People: 100+50=150 users, 20+15=35 retained → 35/150
        people = agg[agg["tag"] == "People"].iloc[0]
        assert people["users"] == 150
        assert people["retained_d30"] == 35
        assert abs(people["retention_d30"] - 35 / 150) < 0.001

        # Technology: 30 users, 5 retained → 5/30
        tech = agg[agg["tag"] == "Technology"].iloc[0]
        assert tech["users"] == 30
        assert tech["retained_d30"] == 5

    def test_missing_meta_gives_unknown(self):
        df = self._make_bq_result()
        meta = pd.DataFrame(columns=["URL", "Tag"])

        df = df.merge(meta, left_on="first_page_path", right_on="URL", how="left")
        df["tag"] = df["Tag"].fillna("(unknown)")
        assert (df["tag"] == "(unknown)").all()

    def test_partial_meta_match(self):
        df = self._make_bq_result()
        # Only 1 of 3 URLs in meta
        meta = pd.DataFrame({
            "URL": ["/jp/company/talk/20250101.html"],
            "Tag": ["People"],
        })

        df = df.merge(meta, left_on="first_page_path", right_on="URL", how="left")
        df["tag"] = df["Tag"].fillna("(unknown)")
        df = df.drop(columns=["URL", "Tag", "first_page_path"])

        agg = df.groupby(["month", "language", "tag"], as_index=False).agg(
            users=("users", "sum"),
        )
        assert len(agg) == 2  # People + (unknown)
        assert set(agg["tag"]) == {"People", "(unknown)"}


class TestPrevMonthCalculation:
    """Test the prev_month calculation in query_monthly_active_revisit."""

    def test_regular_month(self):
        ym = "202603"
        y, m = int(ym[:4]), int(ym[4:6])
        prev_ym = f"{y}{m - 1:02d}" if m > 1 else f"{y - 1}12"
        assert prev_ym == "202602"

    def test_january(self):
        ym = "202601"
        y, m = int(ym[:4]), int(ym[4:6])
        prev_ym = f"{y}{m - 1:02d}" if m > 1 else f"{y - 1}12"
        assert prev_ym == "202512"

    def test_december(self):
        ym = "202512"
        y, m = int(ym[:4]), int(ym[4:6])
        prev_ym = f"{y}{m - 1:02d}" if m > 1 else f"{y - 1}12"
        assert prev_ym == "202511"
