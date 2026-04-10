"""Tests for megaton_lib.ga4_helpers."""

import types
from unittest.mock import MagicMock

import pandas as pd

from megaton_lib.ga4_helpers import (
    build_filter,
    collect_site_frames,
    fetch_named_clinic_report_data_or_empty,
    merge_dataframes,
    report_data_or_empty,
    run_report_merge,
    run_report_data_or_empty,
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


class TestMergeDataframes:
    def test_merges_in_order_and_coerces_int_columns(self):
        base = pd.DataFrame({"page": ["/a", "/b"], "pv": [10, 20]})
        footer = pd.DataFrame({"page": ["/a"], "footer_views": [3]})
        video = pd.DataFrame({"page": ["/a", "/b"], "video_views": [1, 2]})
        out = merge_dataframes(
            [base, footer, video],
            on="page",
            how="left",
            int_cols=["footer_views", "video_views"],
        )
        assert out.columns.tolist() == ["page", "pv", "footer_views", "video_views"]
        assert out["footer_views"].tolist() == [3, 0]
        assert out["video_views"].tolist() == [1, 2]

    def test_skips_empty_or_none_frames(self):
        base = pd.DataFrame({"k": [1], "v": [10]})
        out = merge_dataframes([base, None, pd.DataFrame()], on="k")
        assert out.equals(base)


class TestReportDataOrEmpty:
    def test_returns_empty_with_expected_cols_when_no_data(self):
        mg = MagicMock()
        mg.report.data = None
        df = report_data_or_empty(mg, ["month", "users"])
        assert df.columns.tolist() == ["month", "users"]
        assert df.empty

    def test_fills_missing_cols_and_reorders(self):
        mg = MagicMock()
        mg.report.data = pd.DataFrame({"month": ["202601"], "users": [10]})
        df = report_data_or_empty(mg, ["month", "clinic", "users"])
        assert df.columns.tolist() == ["month", "clinic", "users"]
        assert df.loc[0, "month"] == "202601"
        assert pd.isna(df.loc[0, "clinic"])
        assert df.loc[0, "users"] == 10


class TestCollectSiteFrames:
    def test_collects_non_empty_frames(self):
        mg = MagicMock()
        sites = pd.DataFrame(
            [
                {"clinic": "A", "ga4_property_id": "111"},
                {"clinic": "B", "ga4_property_id": "222"},
            ]
        )

        def fetch_fn(_mg, _site, clinic, _ga4):
            return pd.DataFrame({"clinic": [clinic], "x": [1]})

        frames = collect_site_frames(mg, sites, fetch_fn=fetch_fn, warn_label="_x")
        assert len(frames) == 2
        assert mg.ga["4"].property.id == "222"

    def test_skips_missing_property_and_skip_clinic(self):
        mg = MagicMock()
        sites = pd.DataFrame(
            [
                {"clinic": "A", "ga4_property_id": ""},
                {"clinic": "B", "ga4_property_id": "222"},
            ]
        )

        frames = collect_site_frames(
            mg,
            sites,
            fetch_fn=lambda *_: pd.DataFrame({"ok": [1]}),
            skip_clinics={"B"},
            warn_on_missing_property=True,
        )
        assert frames == []

    def test_continues_when_fetch_fn_raises(self):
        mg = MagicMock()
        sites = pd.DataFrame(
            [
                {"clinic": "A", "ga4_property_id": "111"},
                {"clinic": "B", "ga4_property_id": "222"},
            ]
        )

        def fetch_fn(_mg, _site, clinic, _ga4):
            if clinic == "A":
                raise RuntimeError("boom")
            return pd.DataFrame({"clinic": [clinic]})

        frames = collect_site_frames(mg, sites, fetch_fn=fetch_fn, warn_label="_test")
        assert len(frames) == 1
        assert frames[0]["clinic"].iloc[0] == "B"


class TestRunReportDataOrEmpty:
    def test_runs_report_and_returns_expected_columns(self):
        mg = MagicMock()
        mg.report.data = pd.DataFrame({"month": ["202601"], "users": [10]})

        out = run_report_data_or_empty(
            mg,
            dimensions=["month"],
            metrics=[("activeUsers", "users")],
            expected_cols=["month", "users", "cv"],
            filter_d="x==1",
        )

        mg.report.run.assert_called_once_with(
            d=["month"],
            m=[("activeUsers", "users")],
            filter_d="x==1",
            show=False,
        )
        assert out.columns.tolist() == ["month", "users", "cv"]
        assert out.loc[0, "month"] == "202601"
        assert out.loc[0, "users"] == 10


class TestFetchNamedClinicReportDataOrEmpty:
    def test_returns_empty_when_clinic_missing(self):
        mg = MagicMock()
        sites = pd.DataFrame([{"clinic": "A", "ga4_property_id": "111"}])
        out = fetch_named_clinic_report_data_or_empty(
            mg,
            sites,
            clinic_name="dentamap",
            dimensions=["month"],
            metrics=[("activeUsers", "users")],
            expected_cols=["month", "users"],
        )
        assert out.empty
        assert out.columns.tolist() == ["month", "users"]
        mg.report.run.assert_not_called()

    def test_returns_empty_when_property_missing(self):
        mg = MagicMock()
        sites = pd.DataFrame([{"clinic": "dentamap", "ga4_property_id": ""}])
        out = fetch_named_clinic_report_data_or_empty(
            mg,
            sites,
            clinic_name="dentamap",
            dimensions=["month"],
            metrics=[("activeUsers", "users")],
            expected_cols=["month", "users"],
        )
        assert out.empty
        assert out.columns.tolist() == ["month", "users"]
        mg.report.run.assert_not_called()

    def test_runs_report_and_sets_dates(self):
        mg = MagicMock()
        mg.report.data = pd.DataFrame({"month": ["202601"], "users": [10]})
        sites = pd.DataFrame([{"clinic": "dentamap", "ga4_property_id": "999"}])
        out = fetch_named_clinic_report_data_or_empty(
            mg,
            sites,
            clinic_name="dentamap",
            dimensions=["month"],
            metrics=[("activeUsers", "users")],
            expected_cols=["month", "users"],
            filter_d="x==1",
            set_dates=("2026-01-01", "2026-01-31"),
            warn_label="_test",
        )
        assert out.loc[0, "month"] == "202601"
        assert out.loc[0, "users"] == 10
        assert mg.ga["4"].property.id == "999"
        mg.report.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
        mg.report.run.assert_called_once_with(
            d=["month"],
            m=[("activeUsers", "users")],
            filter_d="x==1",
            show=False,
        )

    def test_returns_empty_when_run_fails(self):
        mg = MagicMock()
        mg.report.run.side_effect = RuntimeError("boom")
        sites = pd.DataFrame([{"clinic": "dentamap", "ga4_property_id": "999"}])
        out = fetch_named_clinic_report_data_or_empty(
            mg,
            sites,
            clinic_name="dentamap",
            dimensions=["month"],
            metrics=[("activeUsers", "users")],
            expected_cols=["month", "users"],
            warn_label="_test",
        )
        assert out.empty
        assert out.columns.tolist() == ["month", "users"]


class TestRunReportMerge:
    def test_merges_multiple_reports(self):
        mg = MagicMock()
        mg.report.run.side_effect = [None, None]
        mg.report.data = pd.DataFrame({"month": ["202601"], "channel": ["Organic"], "users": [10]})

        reports = [
            {
                "dimensions": ["month", "channel"],
                "metrics": [("activeUsers", "users")],
                "expected_cols": ["month", "channel", "users"],
            },
            {
                "dimensions": ["month", "channel"],
                "metrics": [("totalPurchasers", "cv")],
                "expected_cols": ["month", "channel", "cv"],
            },
        ]

        # emulate changing mg.report.data after each run
        def _set_data(*_args, **_kwargs):
            if mg.report.run.call_count == 1:
                mg.report.data = pd.DataFrame({"month": ["202601"], "channel": ["Organic"], "users": [10]})
            else:
                mg.report.data = pd.DataFrame({"month": ["202601"], "channel": ["Organic"], "cv": [2]})

        mg.report.run.side_effect = _set_data

        out = run_report_merge(mg, reports=reports, on=["month", "channel"], how="left")
        assert out.columns.tolist() == ["month", "channel", "users", "cv"]
        assert out.loc[0, "users"] == 10
        assert out.loc[0, "cv"] == 2
        assert mg.report.run.call_count == 2

    def test_fillna_value(self):
        mg = MagicMock()
        mg.report.data = pd.DataFrame({"month": ["202601"], "channel": ["Organic"], "users": [10]})

        def _set_data(*_args, **_kwargs):
            if mg.report.run.call_count == 1:
                mg.report.data = pd.DataFrame({"month": ["202601"], "channel": ["Organic"], "users": [10]})
            else:
                mg.report.data = pd.DataFrame(columns=["month", "channel", "cv"])

        mg.report.run.side_effect = _set_data

        out = run_report_merge(
            mg,
            reports=[
                {
                    "dimensions": ["month", "channel"],
                    "metrics": [("activeUsers", "users")],
                    "expected_cols": ["month", "channel", "users"],
                },
                {
                    "dimensions": ["month", "channel"],
                    "metrics": [("totalPurchasers", "cv")],
                    "expected_cols": ["month", "channel", "cv"],
                },
            ],
            on=["month", "channel"],
            how="left",
            fillna_value=0,
        )
        assert out.loc[0, "cv"] == 0


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
        assert not pd.api.types.is_datetime64_any_dtype(df["date"])

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
