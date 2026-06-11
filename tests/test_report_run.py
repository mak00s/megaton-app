"""Tests for megaton_lib.report_run scaffold and gsc_utils.fetch_for_sites."""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from megaton_lib.report_run import ReportRun, start_report_run


class TestStartReportRun:
    @patch("megaton_lib.report_run.init_report_tracker")
    def test_minimal_no_property(self, mock_init):
        run = start_report_run("r1")
        assert run.mg is None
        assert run.tracker is mock_init.return_value
        mock_init.assert_called_once_with("r1", logger=None, write_enabled=True)

    @patch("megaton_lib.report_run.init_report_tracker")
    def test_resolves_dates_into_window(self, mock_init):
        run = start_report_run("r1", start_date="2026-05-01", end_date="2026-05-31")
        assert (run.start_date, run.end_date) == ("2026-05-01", "2026-05-31")
        kwargs = mock_init.call_args.kwargs
        assert kwargs["report_start"] == "2026-05-01"
        assert kwargs["report_end"] == "2026-05-31"

    @patch("megaton_lib.report_run.init_report_tracker")
    def test_template_dates_resolved(self, mock_init):
        run = start_report_run("r1", start_date="prev-month-start", end_date="prev-month-end")
        assert run.start_date.endswith("-01")
        assert run.start_date < run.end_date

    @patch("megaton_lib.megaton_client.get_ga4")
    @patch("megaton_lib.report_run.init_report_tracker")
    def test_property_inits_client_and_sets_dates(self, mock_init, mock_get_ga4):
        mg = MagicMock()
        mock_get_ga4.return_value = mg
        run = start_report_run(
            "r1", property_id="123",
            start_date="2026-05-01", end_date="2026-05-31",
        )
        assert run.mg is mg
        mock_get_ga4.assert_called_once_with("123")
        mg.report.set.dates.assert_called_once_with("2026-05-01", "2026-05-31")

    @patch("megaton_lib.megaton_client.get_ga4")
    @patch("megaton_lib.report_run.init_report_tracker")
    def test_set_report_dates_false_skips(self, mock_init, mock_get_ga4):
        run = start_report_run(
            "r1", property_id="123",
            start_date="2026-05-01", end_date="2026-05-31",
            set_report_dates=False,
        )
        run.mg.report.set.dates.assert_not_called()

    @patch("megaton_lib.report_run.init_report_tracker")
    def test_extra_window_values_forwarded(self, mock_init):
        start_report_run("r1", rolling_13m_from="2025-05-01")
        assert mock_init.call_args.kwargs["rolling_13m_from"] == "2025-05-01"


class TestReportRunLifecycle:
    def _run(self):
        return ReportRun(name="r1", tracker=MagicMock(), mg=MagicMock())

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_finish_passed_by_default(self, mock_finish):
        run = self._run()
        run.note("ok")
        run.finish()
        mock_finish.assert_called_once_with(
            run.tracker, status="passed", notes=["ok"], errors=[],
        )

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_finish_failed_when_errors_collected(self, mock_finish):
        run = self._run()
        run.error("boom")
        run.finish()
        assert mock_finish.call_args.kwargs["status"] == "failed"

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_finish_idempotent(self, mock_finish):
        run = self._run()
        run.finish()
        run.finish()
        assert mock_finish.call_count == 1

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_context_manager_records_exception(self, mock_finish):
        run = self._run()
        with pytest.raises(RuntimeError):
            with run:
                raise RuntimeError("explode")
        kwargs = mock_finish.call_args.kwargs
        assert kwargs["status"] == "failed"
        assert any("explode" in e for e in kwargs["errors"])

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_on_finish_hooks_called(self, mock_finish):
        run = self._run()
        seen = []
        run.on_finish(lambda r: seen.append(r.name))
        run.finish()
        assert seen == ["r1"]

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_on_finish_failure_marks_summary_failed_and_reraises(self, mock_finish):
        run = self._run()

        def fail(_run):
            raise RuntimeError("delivery failed")

        run.on_finish(fail)
        with pytest.raises(RuntimeError, match="delivery failed"):
            run.finish()
        kwargs = mock_finish.call_args.kwargs
        assert kwargs["status"] == "failed"
        assert any("on_finish fail failed" in e for e in kwargs["errors"])

    @patch("megaton_lib.report_run.finish_report_tracker")
    def test_save_sheet_passes_mg(self, mock_finish):
        run = self._run()
        df = pd.DataFrame({"a": [1]})
        run.save_sheet(gs_url="u", sheet_name="s", df=df)
        run.tracker.save_sheet.assert_called_once_with(
            run.mg, gs_url="u", sheet_name="s", df=df,
        )


class TestFetchForSites:
    def _sites(self):
        return pd.DataFrame([
            {"clinic": "A", "url": "https://a.example/"},
            {"clinic": "B", "url": ""},
            {"clinic": "C", "url": "https://c.example/"},
        ])

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_combines_and_labels(self, mock_query):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.return_value = pd.DataFrame({
            "query": ["q1"], "page": ["https://x/p"],
            "clicks": [1], "impressions": [10], "ctr": [0.1], "position": [2.0],
        })
        out = fetch_for_sites(
            self._sites(),
            dimensions=["query", "page"],
            start_date="2026-05-01", end_date="2026-05-31",
            label_col="clinic",
            extra_columns={"month": "202605"},
        )
        assert mock_query.call_count == 2  # empty-url row skipped
        assert sorted(out["clinic"].unique()) == ["A", "C"]
        assert set(out["month"]) == {"202605"}

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_warn_skips_failures(self, mock_query, capsys):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.side_effect = [RuntimeError("nope"), pd.DataFrame({
            "query": ["q"], "page": ["p"],
            "clicks": [1], "impressions": [1], "ctr": [1.0], "position": [1.0],
        })]
        out = fetch_for_sites(
            self._sites(),
            dimensions=["query"],
            start_date="2026-05-01", end_date="2026-05-31",
            label_col="clinic",
        )
        assert out["clinic"].tolist() == ["C"]
        assert "GSC fetch failed" in capsys.readouterr().out

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_raise_mode(self, mock_query):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.side_effect = RuntimeError("nope")
        with pytest.raises(RuntimeError):
            fetch_for_sites(
                self._sites(),
                dimensions=["query"],
                start_date="2026-05-01", end_date="2026-05-31",
                on_error="raise",
            )

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_empty_results_return_empty_df(self, mock_query):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.return_value = pd.DataFrame()
        out = fetch_for_sites(
            self._sites(),
            dimensions=["query"],
            start_date="2026-05-01", end_date="2026-05-31",
        )
        assert out.empty

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_warn_mode_raises_when_all_attempted_sites_fail(self, mock_query):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.side_effect = RuntimeError("api down")
        with pytest.raises(RuntimeError, match="failed for all 2 attempted"):
            fetch_for_sites(
                self._sites(),
                dimensions=["query"],
                start_date="2026-05-01", end_date="2026-05-31",
            )

    @patch("megaton_lib.megaton_client.query_gsc")
    def test_all_failed_guard_can_be_disabled(self, mock_query):
        from megaton_lib.gsc_utils import fetch_for_sites

        mock_query.side_effect = RuntimeError("api down")
        out = fetch_for_sites(
            self._sites(),
            dimensions=["query"],
            start_date="2026-05-01", end_date="2026-05-31",
            fail_if_all_failed=False,
        )
        assert out.empty
