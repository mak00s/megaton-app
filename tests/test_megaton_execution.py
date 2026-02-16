import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import megaton_lib.megaton_client as mc


def _reset_registry():
    mc._instances.clear()
    mc._property_map.clear()
    mc._site_map.clear()
    mc._registry_built = False
    mc._bq_clients.clear()
    mc._bq_native_clients.clear()


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
        mg.report.run.return_value.df = pd.DataFrame([{"sessions": 10}])

        with patch("megaton_lib.megaton_client.get_megaton_for_property", return_value=mg):
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
        with patch("megaton_lib.megaton_client.get_megaton_for_site", return_value=mg):
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
        with patch("megaton_lib.megaton_client.get_megaton", return_value=mg):
            a1 = mc.get_bigquery("proj-a")
            a2 = mc.get_bigquery("proj-a")
            b1 = mc.get_bigquery("proj-b")
        self.assertIs(a1, a2)
        self.assertIsNot(a1, b1)
        self.assertEqual(mg.launch_bigquery.call_count, 2)

    def test_query_bq_calls_run_dataframe(self):
        bq = MagicMock()
        bq.run.return_value = pd.DataFrame([{"x": 1}])
        with patch("megaton_lib.megaton_client.get_bigquery", return_value=bq):
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
            "megaton_lib.megaton_client.get_bigquery", return_value=bq
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
        with patch("megaton_lib.megaton_client.get_megaton", return_value=mg):
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

    # ------------------------------------------------------------------
    # get_bq_client
    # ------------------------------------------------------------------

    def test_resolve_bq_creds_path_prefers_gac_env(self):
        with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": "/env/path.json"}, clear=True), \
             patch("megaton_lib.megaton_client.list_service_account_paths", return_value=["/creds/a.json"]):
            path = mc.resolve_bq_creds_path(creds_hint="corp")
        self.assertEqual(path, "/env/path.json")

    def test_resolve_bq_creds_path_uses_hint_match(self):
        with patch.dict("os.environ", {}, clear=True), \
             patch("megaton_lib.megaton_client.list_service_account_paths",
                   return_value=["/creds/with.json", "/creds/corp-main.json"]):
            path = mc.resolve_bq_creds_path(creds_hint="corp")
        self.assertEqual(path, "/creds/corp-main.json")

    def test_resolve_bq_creds_path_returns_none_if_no_candidates(self):
        with patch.dict("os.environ", {}, clear=True), \
             patch("megaton_lib.megaton_client.list_service_account_paths", return_value=[]):
            path = mc.resolve_bq_creds_path()
        self.assertIsNone(path)

    def test_ensure_bq_credentials_sets_env_when_missing(self):
        with patch.dict("os.environ", {}, clear=True), \
             patch("megaton_lib.megaton_client.list_service_account_paths", return_value=["/creds/corp.json"]):
            path = mc.ensure_bq_credentials(creds_hint="corp")
            self.assertEqual(path, "/creds/corp.json")
            self.assertEqual(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"), "/creds/corp.json")

    def test_describe_auth_context(self):
        with patch.dict("os.environ", {"MEGATON_CREDS_PATH": "/creds", "GOOGLE_APPLICATION_CREDENTIALS": "/env/path.json"}, clear=True), \
             patch("megaton_lib.megaton_client.list_service_account_paths", return_value=["/creds/a.json"]):
            info = mc.describe_auth_context(creds_hint="corp")
        self.assertEqual(info["megaton_env_var"], "MEGATON_CREDS_PATH")
        self.assertEqual(info["google_application_credentials"], "/env/path.json")
        self.assertEqual(info["resolved_bq_creds_path"], "/env/path.json")
        self.assertEqual(info["resolved_bq_source"], "GOOGLE_APPLICATION_CREDENTIALS")

    def test_get_bq_client_cached_by_project(self):
        fake_bigquery = MagicMock()
        client1 = MagicMock(name="client1")
        client2 = MagicMock(name="client2")
        fake_bigquery.Client.side_effect = [client1, client2]

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.list_service_account_paths", return_value=["/creds/corp.json"]), \
             patch.dict("os.environ", {}, clear=True):
            a1 = mc.get_bq_client("proj-a")
            a2 = mc.get_bq_client("proj-a")
            b1 = mc.get_bq_client("proj-b")

        self.assertIs(a1, a2)
        self.assertIsNot(a1, b1)
        self.assertEqual(fake_bigquery.Client.call_count, 2)

    def test_get_bq_client_creds_hint_matching(self):
        fake_bigquery = MagicMock()
        fake_bigquery.Client.return_value = MagicMock()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.list_service_account_paths",
                   return_value=["/creds/with.json", "/creds/corp.json"]), \
             patch.dict("os.environ", {}, clear=True):
            mc.get_bq_client("proj-x", creds_hint="corp")
            self.assertIn("corp", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))

    # ------------------------------------------------------------------
    # query_bq with params
    # ------------------------------------------------------------------

    def test_query_bq_without_params_uses_legacy(self):
        """params=None → 従来の get_bigquery().run() 経由"""
        bq = MagicMock()
        bq.run.return_value = pd.DataFrame([{"x": 1}])
        with patch("megaton_lib.megaton_client.get_bigquery", return_value=bq):
            df = mc.query_bq("proj", "select 1")
        self.assertEqual(len(df), 1)
        bq.run.assert_called_once_with("select 1", to_dataframe=True)

    def test_query_bq_with_params_uses_native_client(self):
        """params あり → get_bq_client() + parameterized query"""
        fake_bigquery = MagicMock()

        class FakeScalarQueryParameter:
            def __init__(self, name, type_, value):
                self.name = name
                self.type_ = type_
                self.value = value

        fake_bigquery.ScalarQueryParameter = FakeScalarQueryParameter
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_result_df = pd.DataFrame([{"user": "abc", "cnt": 5}])
        fake_client.query.return_value.to_dataframe.return_value = fake_result_df

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            df = mc.query_bq(
                "proj",
                "SELECT * FROM t WHERE month = @m",
                params={"m": "202602"},
                location="us-central1",
            )

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["user"], "abc")
        fake_client.query.assert_called_once()
        call_kwargs = fake_client.query.call_args
        self.assertEqual(call_kwargs.kwargs["location"], "us-central1")

    def test_query_bq_with_params_default_location_none(self):
        """location 未指定 → kwargs に location が含まれない"""
        fake_bigquery = MagicMock()
        fake_bigquery.ScalarQueryParameter = lambda n, t, v: (n, t, v)
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            mc.query_bq("proj", "SELECT 1", params={"x": "1"})

        call_kwargs = fake_client.query.call_args.kwargs
        self.assertNotIn("location", call_kwargs)

    def test_query_bq_with_empty_params_uses_native(self):
        """params={} (空dict) → native client 経由（None とは区別）"""
        fake_bigquery = MagicMock()
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            df = mc.query_bq("proj", "SELECT 1", params={})

        self.assertTrue(df.empty)
        fake_client.query.assert_called_once()


if __name__ == "__main__":
    unittest.main()
