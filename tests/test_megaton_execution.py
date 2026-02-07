import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import lib.megaton_client as mc


def _reset_registry():
    mc._instances.clear()
    mc._property_map.clear()
    mc._site_map.clear()
    mc._registry_built = False
    mc._bq_clients.clear()


class TestMegatonExecution(unittest.TestCase):
    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    def test_query_ga4_selects_account_and_property(self):
        mg = MagicMock()
        ga4 = MagicMock()
        ga4.accounts = [
            {"id": "acc1", "properties": [{"id": "P1"}]},
            {"id": "acc2", "properties": [{"id": "P2"}]},
        ]
        mg.ga = {"4": ga4}
        mg.report.data = pd.DataFrame([{"sessions": 10}])

        with patch("lib.megaton_client.get_megaton_for_property", return_value=mg):
            df = mc.query_ga4(
                property_id="P2",
                start_date="2026-01-01",
                end_date="2026-01-31",
                dimensions=["date"],
                metrics=["sessions"],
                filter_d="country==JP",
                limit=50,
            )

        self.assertEqual(len(df), 1)
        mg.ga["4"].account.select.assert_any_call("acc2")
        mg.ga["4"].property.select.assert_any_call("P2")
        mg.report.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
        mg.report.run.assert_called_once()

    def test_query_gsc_runs_with_dimension_filter(self):
        mg = MagicMock()
        mg.search.data = pd.DataFrame([{"clicks": 1}])
        with patch("lib.megaton_client.get_megaton_for_site", return_value=mg):
            df = mc.query_gsc(
                site_url="sc-domain:example.com",
                start_date="2026-01-01",
                end_date="2026-01-31",
                dimensions=["query"],
                limit=100,
                dimension_filter=[{"dimension": "query", "operator": "contains", "expression": "seo"}],
            )
        self.assertEqual(len(df), 1)
        mg.search.use.assert_called_once_with("sc-domain:example.com")
        mg.search.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
        mg.search.run.assert_called_once()

    def test_get_bigquery_is_cached_by_project(self):
        mg = MagicMock()
        bq1 = MagicMock(name="bq1")
        bq2 = MagicMock(name="bq2")
        mg.launch_bigquery.side_effect = [bq1, bq2]
        with patch("lib.megaton_client.get_megaton", return_value=mg):
            a1 = mc.get_bigquery("proj-a")
            a2 = mc.get_bigquery("proj-a")
            b1 = mc.get_bigquery("proj-b")
        self.assertIs(a1, a2)
        self.assertIsNot(a1, b1)
        self.assertEqual(mg.launch_bigquery.call_count, 2)

    def test_query_bq_calls_run_dataframe(self):
        bq = MagicMock()
        bq.run.return_value = pd.DataFrame([{"x": 1}])
        with patch("lib.megaton_client.get_bigquery", return_value=bq):
            df = mc.query_bq("proj", "select 1")
        self.assertEqual(len(df), 1)
        bq.run.assert_called_once_with("select 1", to_dataframe=True)

    def test_save_to_bq_modes_and_invalid(self):
        fake_bq = types.SimpleNamespace()
        fake_bq.WriteDisposition = types.SimpleNamespace(
            WRITE_TRUNCATE="WRITE_TRUNCATE",
            WRITE_APPEND="WRITE_APPEND",
        )

        class FakeLoadJobConfig:
            def __init__(self, write_disposition, autodetect):
                self.write_disposition = write_disposition
                self.autodetect = autodetect

        fake_bq.LoadJobConfig = FakeLoadJobConfig

        client = MagicMock()
        client.get_table.return_value = types.SimpleNamespace(num_rows=12)
        load_job = MagicMock()
        client.load_table_from_dataframe.return_value = load_job
        bq = types.SimpleNamespace(client=client)

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bq}), patch(
            "lib.megaton_client.get_bigquery", return_value=bq
        ):
            out = mc.save_to_bq("p", "d", "t", pd.DataFrame([{"a": 1}]), mode="overwrite")
            self.assertEqual(out["table"], "p.d.t")
            self.assertEqual(out["row_count"], 12)
            self.assertEqual(client.load_table_from_dataframe.call_args.kwargs["job_config"].write_disposition, "WRITE_TRUNCATE")

            out2 = mc.save_to_bq("p", "d", "t", pd.DataFrame([{"a": 2}]), mode="append")
            self.assertEqual(out2["table"], "p.d.t")
            self.assertEqual(client.load_table_from_dataframe.call_args.kwargs["job_config"].write_disposition, "WRITE_APPEND")

            with self.assertRaises(ValueError):
                mc.save_to_bq("p", "d", "t", pd.DataFrame([{"a": 3}]), mode="upsert")

    def test_save_to_sheet_modes(self):
        mg = MagicMock()
        with patch("lib.megaton_client.get_megaton", return_value=mg):
            df = pd.DataFrame([{"a": 1}])
            mc.save_to_sheet("https://docs.google.com/spreadsheets/d/x", "data", df, mode="overwrite")
            mg.save.to.sheet.assert_called_once()

            mc.save_to_sheet("https://docs.google.com/spreadsheets/d/x", "data", df, mode="append")
            mg.append.to.sheet.assert_called_once()

            mc.save_to_sheet(
                "https://docs.google.com/spreadsheets/d/x",
                "data",
                df,
                mode="upsert",
                keys=["a"],
            )
            mg.upsert.to.sheet.assert_called_once()

            with self.assertRaises(ValueError):
                mc.save_to_sheet("https://docs.google.com/spreadsheets/d/x", "data", df, mode="upsert", keys=None)


if __name__ == "__main__":
    unittest.main()
