import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

import app.main as app_main
from app.schemas import SAMPLE_GA4_JSON, SAMPLE_GSC_JSON


class TestAppMain(unittest.TestCase):
    def setUp(self):
        app_main._ga4_module = None
        app_main._gsc_module = None
        app_main._viz_module = None
        app_main._properties_cache = None
        app_main._sites_cache = None
        app_main._last_result_df = None

    def test_execute_query_ga4_success(self):
        params = SimpleNamespace(source="ga4", visualization=SimpleNamespace(type="line", x="x", y="y"))
        df = pd.DataFrame([{"x": 1, "y": 2}])
        ga4_mod = MagicMock()
        ga4_mod.execute_ga4_query.return_value = df
        viz_mod = MagicMock()
        viz_mod.create_chart.return_value = "chart_obj"
        viz_mod.format_dataframe.return_value = pd.DataFrame([{"x": "1", "y": "2"}])

        with patch("app.main.QueryParams.from_json", return_value=params), patch(
            "app.main._get_ga4_module", return_value=ga4_mod
        ), patch("app.main._get_viz_module", return_value=viz_mod):
            table, chart, msg = app_main.execute_query('{"schema_version":"1.0"}')

        self.assertEqual(chart, "chart_obj")
        self.assertIn("1 行", msg)
        self.assertIsNotNone(app_main._last_result_df)
        self.assertEqual(len(table), 1)

    def test_execute_query_gsc_empty(self):
        params = SimpleNamespace(source="gsc", visualization=None)
        gsc_mod = MagicMock()
        gsc_mod.execute_gsc_query.return_value = pd.DataFrame()
        with patch("app.main.QueryParams.from_json", return_value=params), patch(
            "app.main._get_gsc_module", return_value=gsc_mod
        ):
            table, chart, msg = app_main.execute_query('{"schema_version":"1.0"}')
        self.assertIsNone(table)
        self.assertIsNone(chart)
        self.assertIn("データが取得できませんでした", msg)

    def test_execute_query_json_parse_error(self):
        table, chart, msg = app_main.execute_query("{bad json")
        self.assertIsNone(table)
        self.assertIsNone(chart)
        self.assertIn("JSONパースエラー", msg)

    def test_execute_query_generic_error(self):
        with patch("app.main.QueryParams.from_json", side_effect=ValueError("bad")):
            table, chart, msg = app_main.execute_query('{"schema_version":"1.0"}')
        self.assertIsNone(table)
        self.assertIsNone(chart)
        self.assertIn("エラー:", msg)

    def test_save_to_csv(self):
        self.assertEqual(app_main.save_to_csv()[0], None)

        with tempfile.TemporaryDirectory() as tmp:
            app_main.OUTPUT_DIR = tmp
            app_main._last_result_df = pd.DataFrame([{"a": 1}])
            fixed_dt = MagicMock()
            fixed_dt.now.return_value.strftime.return_value = "20260207_123000"
            with patch("app.main.datetime", fixed_dt):
                path, msg = app_main.save_to_csv()
            self.assertTrue(Path(path).exists())
            self.assertIn("保存完了", msg)

    def test_samples_and_choices(self):
        self.assertEqual(app_main.load_ga4_sample(), SAMPLE_GA4_JSON)
        self.assertEqual(app_main.load_gsc_sample(), SAMPLE_GSC_JSON)

        ga4_mod = MagicMock()
        ga4_mod.list_ga4_properties.return_value = [{"property_name": "P", "property_id": "1"}]
        with patch("app.main._get_ga4_module", return_value=ga4_mod):
            text = app_main.get_properties_list()
            self.assertIn("P", text)
            choices1 = app_main.get_property_choices()
            choices2 = app_main.get_property_choices()
            self.assertEqual(choices1, choices2)

        gsc_mod = MagicMock()
        gsc_mod.list_gsc_sites.return_value = ["sc-domain:example.com"]
        with patch("app.main._get_gsc_module", return_value=gsc_mod):
            text = app_main.get_sites_list()
            self.assertIn("example.com", text)
            choices = app_main.get_site_choices()
            self.assertEqual(choices[0][0], "sc-domain:example.com")

    def test_choices_fallback(self):
        with patch("app.main._get_ga4_module", side_effect=RuntimeError("x")):
            choices = app_main.get_property_choices()
            self.assertTrue(len(choices) >= 1)
        app_main._sites_cache = None
        with patch("app.main._get_gsc_module", side_effect=RuntimeError("x")):
            choices = app_main.get_site_choices()
            self.assertTrue(len(choices) >= 1)

    def test_update_json_and_sync(self):
        raw = json.dumps({"source": "ga4", "property_id": "1", "date_range": {"start": "a", "end": "b"}})
        out = app_main.update_json_from_ui(raw, "gsc", "1", "sc-domain:example.com", "2026-01-01", "2026-01-31")
        parsed = json.loads(out)
        self.assertEqual(parsed["source"], "gsc")
        self.assertNotIn("property_id", parsed)
        self.assertEqual(parsed["site_url"], "sc-domain:example.com")

        s = app_main.sync_ui_from_json(out)
        self.assertEqual(s[0], "gsc")
        bad = app_main.sync_ui_from_json("{bad")
        self.assertEqual(bad[0], "ga4")

    def test_on_source_change(self):
        ga4 = app_main.on_source_change("ga4", "{}")
        gsc = app_main.on_source_change("gsc", "{}")
        self.assertEqual(ga4[0], SAMPLE_GA4_JSON)
        self.assertEqual(gsc[0], SAMPLE_GSC_JSON)


if __name__ == "__main__":
    unittest.main()
