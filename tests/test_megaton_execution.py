import os
import sys
import types
import tomllib
import unittest
from pathlib import Path
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
    def test_dependency_requires_credential_store_capable_core(self):
        config = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
        from packaging.requirements import Requirement

        requirement = next(Requirement(item) for item in config["project"]["dependencies"]
                           if Requirement(item).name == "megaton")
        self.assertNotIn("2.5.0", requirement.specifier)
        self.assertIn("2.6.0", requirement.specifier)

    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    def test_query_ga4_selects_account_and_property(self):
        mg = MagicMock()
        mg.use_property.return_value = mg
        mg.report.run.return_value.df = pd.DataFrame([{"sessions": 10}])
        mg.report.run.return_value.df.attrs["megaton_query_completeness"] = {
            "status": "complete", "fetched_rows": 1, "row_limit": 50, "total_rows": 1, "discarded_rows": 0,
        }

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
        mg.use_property.assert_called_once_with("P2")
        mg.report.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
        mg.report.run.assert_called_once()

    def test_query_ga4_invalid_dimensions_type(self):
        with self.assertRaises(TypeError):
            mc.query_ga4(
                property_id="P2",
                start_date="2026-01-01",
                end_date="2026-01-31",
                dimensions="date",  # type: ignore[arg-type]
                metrics=["sessions"],
            )

    @patch("google.analytics.data_v1beta.BetaAnalyticsDataClient")
    @patch("megaton_lib.google_workspace.build_service_account_credentials")
    def test_query_ga4_with_explicit_credentials(self, mock_build_creds, mock_client_cls):
        response = types.SimpleNamespace(
            row_count=1,
            rows=[
                types.SimpleNamespace(
                    dimension_values=[types.SimpleNamespace(value="Organic Search")],
                    metric_values=[types.SimpleNamespace(value="12")],
                )
            ]
        )
        mock_client_cls.return_value.run_report.return_value = response

        df = mc.query_ga4_with_credentials(
            property_id="P2",
            start_date="2026-01-01",
            end_date="2026-01-31",
            dimensions=[("sessionDefaultChannelGroup", "channel")],
            metrics=["sessions"],
            credentials_path="/creds/sa.json",
            filter_d="landingPagePlusQueryString==/news/example/",
        )

        mock_build_creds.assert_called_once()
        self.assertEqual(df.to_dict("records"), [{"channel": "Organic Search", "sessions": 12}])
        request = mock_client_cls.return_value.run_report.call_args.args[0]
        self.assertEqual(request.property, "properties/P2")
        self.assertEqual(request.dimension_filter.filter.field_name, "landingPagePlusQueryString")

    def test_query_ga4_with_explicit_credentials_rejects_bad_filter(self):
        with self.assertRaises(ValueError):
            mc.query_ga4_with_credentials(
                property_id="P2",
                start_date="2026-01-01",
                end_date="2026-01-31",
                dimensions=["date"],
                metrics=["sessions"],
                credentials_path="/creds/sa.json",
                filter_d="country contains JP",
            )

    @patch("google.analytics.data_v1beta.BetaAnalyticsDataClient")
    @patch("megaton_lib.google_workspace.build_service_account_credentials")
    def test_explicit_ga4_filter_preserves_operators_in_values(self, build_creds, client_cls):
        client_cls.return_value.run_report.return_value = types.SimpleNamespace(row_count=0, rows=[])
        cases = [
            ("pagePath=~^/search/.*q!=.*$", "pagePath", "FULL_REGEXP", "^/search/.*q!=.*$", False),
            ("pagePath==/search?q=~value!=other", "pagePath", "EXACT", "/search?q=~value!=other", False),
            ("pagePath!=/search?q=~value==other", "pagePath", "EXACT", "/search?q=~value==other", True),
            ("customEvent:site == example.com", "customEvent:site", "EXACT", "example.com", False),
        ]
        for clause, field, match_type, value, negated in cases:
            with self.subTest(clause=clause):
                mc.query_ga4_with_credentials(
                    "123", "2026-01-01", "2026-01-02", ["pagePath"], ["sessions"],
                    credentials_path="/creds/sa.json", filter_d=clause)
                expression = client_cls.return_value.run_report.call_args.args[0].dimension_filter
                self.assertEqual(bool(expression.not_expression), negated)
                actual = expression.not_expression.filter if negated else expression.filter
                self.assertEqual(actual.field_name, field)
                self.assertEqual(actual.string_filter.match_type.name, match_type)
                self.assertEqual(actual.string_filter.value, value)

    @patch("google.analytics.data_v1beta.BetaAnalyticsDataClient")
    @patch("megaton.auth.store.CredentialStore")
    def test_query_ga4_with_user_oauth_and_quota(self, store_cls, client_cls):
        store = store_cls.return_value
        store.resolve_email.return_value = "reader@example.com"
        store.identity.return_value = "reader@example.com"
        client_cls.return_value.run_report.return_value = types.SimpleNamespace(row_count=0, rows=[])
        mc.query_ga4_with_credentials(
            "123", "2026-01-01", "2026-01-02", ["hostName"], ["sessions"],
            user_account="reader@example.com", quota_project="example-project",
            filter_d="hostName=~^(old|new)\\.example\\.com$",
        )
        store.user_credentials.assert_called_once_with(
            "reader@example.com", scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        store.user_credentials.return_value.with_quota_project.assert_called_once_with("example-project")
        request = client_cls.return_value.run_report.call_args.args[0]
        self.assertEqual(request.dimension_filter.filter.string_filter.match_type.name, "FULL_REGEXP")

    @patch("megaton.auth.store.CredentialStore")
    def test_query_ga4_user_oauth_rejects_wrong_identity(self, store_cls):
        store_cls.return_value.resolve_email.return_value = "reader@example.com"
        store_cls.return_value.identity.return_value = "other@example.com"
        with self.assertRaises(PermissionError):
            mc.query_ga4_with_credentials(
                "123", "2026-01-01", "2026-01-02", ["date"], ["sessions"],
                user_account="reader@example.com")

    @patch("megaton_lib.megaton_client._checked_query_frame")
    @patch("megaton_lib.megaton_client.run_query")
    @patch("megaton_lib.megaton_client.start.Megaton")
    @patch("megaton.auth.store.CredentialStore")
    def test_query_gsc_user_oauth_and_quota(self, store_cls, megaton_cls, run, checked):
        store = store_cls.return_value
        store.resolve_email.return_value = "reader@example.com"
        store.identity.return_value = "reader@example.com"
        mc.query_gsc("https://example.com/", "2026-01-01", "2026-01-02", ["date"],
                     user_account="reader@example.com", quota_project="example-project")
        store.user_credentials.assert_called_once_with(
            "reader@example.com", scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
        self.assertIs(megaton_cls.return_value.creds,
                      store.user_credentials.return_value.with_quota_project.return_value)
        self.assertIs(run.call_args.kwargs["client"], megaton_cls.return_value)
        checked.assert_called_once_with(run.return_value, allow_incomplete=False)

    @patch("megaton_lib.megaton_client.start.Megaton")
    @patch("megaton.auth.store.CredentialStore")
    def test_query_gsc_rejects_wrong_identity(self, store_cls, megaton_cls):
        store_cls.return_value.resolve_email.return_value = "reader@example.com"
        store_cls.return_value.identity.return_value = "other@example.com"
        with self.assertRaises(PermissionError):
            mc.query_gsc("https://example.com/", "2026-01-01", "2026-01-02", ["date"],
                         user_account="reader@example.com")
        megaton_cls.assert_not_called()

    def test_query_gsc_quota_requires_explicit_account(self):
        with self.assertRaises(ValueError):
            mc.query_gsc("https://example.com/", "2026-01-01", "2026-01-02", ["date"],
                         quota_project="example-project")

    def test_query_gsc_runs_with_dimension_filter(self):
        mg = MagicMock()
        mg.search.run.return_value.df = pd.DataFrame([{"clicks": 1}])
        mg.search.run.return_value.df.attrs["megaton_query_completeness"] = {
            "status": "complete", "fetched_rows": 1, "row_limit": 100, "total_rows": None, "discarded_rows": 0,
        }
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

    def test_query_gsc_invalid_dimension_filter_shape(self):
        with self.assertRaises(ValueError):
            mc.query_gsc(
                site_url="sc-domain:example.com",
                start_date="2026-01-01",
                end_date="2026-01-31",
                dimensions=["query"],
                dimension_filter=[{"dimension": "query", "operator": "contains"}],  # expression missing
            )

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
        """params=None -> use legacy get_bigquery().run() path."""
        bq = MagicMock()
        bq.run.return_value = pd.DataFrame([{"x": 1}])
        with patch("megaton_lib.megaton_client.get_bigquery", return_value=bq):
            df = mc.query_bq("proj", "select 1")
        self.assertEqual(len(df), 1)
        bq.run.assert_called_once_with("select 1", to_dataframe=True)

    def test_query_bq_with_params_uses_native_client(self):
        """With params -> use get_bq_client() + parameterized query."""
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

    def test_query_bq_with_non_string_param_values_are_stringified(self):
        fake_bigquery = MagicMock()

        class FakeScalarQueryParameter:
            def __init__(self, name, type_, value):
                self.name = name
                self.type_ = type_
                self.value = value

        fake_bigquery.ScalarQueryParameter = FakeScalarQueryParameter
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            mc.query_bq("proj", "SELECT @n AS n", params={"n": 123})

        qp = fake_bigquery.QueryJobConfig.call_args.kwargs["query_parameters"][0]
        self.assertEqual(qp.value, "123")

    def test_query_bq_with_none_param_value_preserved(self):
        fake_bigquery = MagicMock()

        class FakeScalarQueryParameter:
            def __init__(self, name, type_, value):
                self.name = name
                self.type_ = type_
                self.value = value

        fake_bigquery.ScalarQueryParameter = FakeScalarQueryParameter
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            mc.query_bq("proj", "SELECT @x AS x", params={"x": None})

        qp = fake_bigquery.QueryJobConfig.call_args.kwargs["query_parameters"][0]
        self.assertIsNone(qp.value)

    def test_query_bq_with_params_default_location_none(self):
        """Without location, kwargs should not include location."""
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

    def test_query_bq_invalid_params_type(self):
        with self.assertRaises(TypeError):
            mc.query_bq("proj", "SELECT 1", params=[("x", 1)])  # type: ignore[arg-type]

    def test_query_bq_with_empty_params_uses_native(self):
        """params={} (empty dict) -> native client path (distinct from None)."""
        fake_bigquery = MagicMock()
        fake_bigquery.QueryJobConfig = MagicMock()

        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame()

        with patch.dict(sys.modules, {"google.cloud.bigquery": fake_bigquery}), \
             patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client):
            df = mc.query_bq("proj", "SELECT 1", params={})

        self.assertTrue(df.empty)
        fake_client.query.assert_called_once()

    def test_query_bq_force_native_without_params(self):
        """force_native=True should bypass legacy wrapper even without params."""
        fake_client = MagicMock()
        fake_client.query.return_value.to_dataframe.return_value = pd.DataFrame([{"x": 1}])

        with patch("megaton_lib.megaton_client.get_bq_client", return_value=fake_client) as mocked_client, \
             patch("megaton_lib.megaton_client.get_bigquery") as mocked_legacy:
            df = mc.query_bq(
                "proj",
                "SELECT 1 AS x",
                location="asia-northeast1",
                force_native=True,
            )

        self.assertEqual(len(df), 1)
        mocked_legacy.assert_not_called()
        mocked_client.assert_called_once_with("proj", creds_hint="corp")
        fake_client.query.assert_called_once()
        self.assertEqual(
            fake_client.query.call_args.kwargs["location"],
            "asia-northeast1",
        )


if __name__ == "__main__":
    unittest.main()
