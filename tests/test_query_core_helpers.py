import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import scripts.query as query_cli


class TestParseGscFilter(unittest.TestCase):
    def test_parse_none_when_empty(self):
        self.assertIsNone(query_cli.parse_gsc_filter(""))

    def test_parse_valid(self):
        got = query_cli.parse_gsc_filter("query:contains:seo;page:equals:/blog")
        self.assertEqual(
            got,
            [
                {"dimension": "query", "operator": "contains", "expression": "seo"},
                {"dimension": "page", "operator": "equals", "expression": "/blog"},
            ],
        )

    def test_parse_invalid_raises(self):
        with self.assertRaises(ValueError):
            query_cli.parse_gsc_filter("bad-format")


class TestLoadParams(unittest.TestCase):
    def test_file_not_found(self):
        params, err = query_cli.load_params("input/not_exists_abc123.json")
        self.assertIsNone(params)
        self.assertEqual(err["error_code"], "PARAMS_FILE_NOT_FOUND")

    def test_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text("{invalid json", encoding="utf-8")
            params, err = query_cli.load_params(str(path))
            self.assertIsNone(params)
            self.assertEqual(err["error_code"], "INVALID_JSON")

    def test_validation_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ok.json"
            path.write_text(json.dumps({"schema_version": "1.0"}), encoding="utf-8")
            with patch.object(query_cli, "validate_params", return_value=(None, [{"error_code": "E"}])):
                params, err = query_cli.load_params(str(path))
            self.assertIsNone(params)
            self.assertEqual(err["error_code"], "PARAMS_VALIDATION_FAILED")
            self.assertIn("details", err)

    def test_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ok.json"
            raw = {"schema_version": "1.0", "source": "ga4"}
            path.write_text(json.dumps(raw), encoding="utf-8")
            validated = {"schema_version": "1.0", "source": "ga4", "x": 1}
            with patch.object(query_cli, "validate_params", return_value=(validated, [])):
                params, err = query_cli.load_params(str(path))
            self.assertIsNone(err)
            self.assertEqual(params, validated)


class TestExecuteQueryFromParams(unittest.TestCase):
    def test_ga4_branch(self):
        params = {
            "source": "ga4",
            "property_id": "p1",
            "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "filter_d": "pagePath=@/blog",
            "limit": 123,
        }
        df = pd.DataFrame([{"date": "2026-01-01", "sessions": 1}])
        with patch.object(query_cli, "query_ga4", return_value=df) as mock_q:
            out_df, headers = query_cli.execute_query_from_params(params)
        self.assertEqual(len(out_df), 1)
        self.assertIn("プロパティ: p1", headers)
        mock_q.assert_called_once()

    def test_gsc_branch_with_sort_and_filter(self):
        params = {
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
            "dimensions": ["query"],
            "filter": "query:contains:seo",
            "limit": 100,
        }
        df = pd.DataFrame([{"query": "a", "clicks": 1}, {"query": "b", "clicks": 3}])
        with patch.object(query_cli, "query_gsc", return_value=df) as mock_q:
            out_df, headers = query_cli.execute_query_from_params(params)
        self.assertEqual(int(out_df.iloc[0]["clicks"]), 3)
        self.assertTrue(any("フィルタ:" in h for h in headers))
        mock_q.assert_called_once()

    def test_gsc_branch_without_clicks(self):
        params = {
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
            "dimensions": ["query"],
        }
        df = pd.DataFrame([{"query": "a"}])
        with patch.object(query_cli, "query_gsc", return_value=df):
            out_df, _ = query_cli.execute_query_from_params(params)
        self.assertEqual(list(out_df.columns), ["query"])

    def test_bigquery_branch(self):
        params = {"source": "bigquery", "project_id": "p", "sql": "select 1"}
        df = pd.DataFrame([{"x": 1}])
        with patch.object(query_cli, "query_bq", return_value=df) as mock_q:
            out_df, headers = query_cli.execute_query_from_params(params)
        self.assertEqual(len(out_df), 1)
        self.assertIn("プロジェクト: p", headers)
        mock_q.assert_called_once_with("p", "select 1")

    def test_unknown_source_raises(self):
        with self.assertRaises(ValueError):
            query_cli.execute_query_from_params({"source": "unknown"})


class TestExecuteSave(unittest.TestCase):
    def test_csv_overwrite_and_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            df1 = pd.DataFrame([{"a": 1}])
            df2 = pd.DataFrame([{"a": 2}])
            r1 = query_cli.execute_save(df1, {"to": "csv", "path": str(path), "mode": "overwrite"})
            r2 = query_cli.execute_save(df2, {"to": "csv", "path": str(path), "mode": "append"})
            self.assertEqual(r1["mode"], "overwrite")
            self.assertEqual(r2["mode"], "append")
            saved = pd.read_csv(path)
            self.assertEqual(len(saved), 2)

    def test_sheets(self):
        df = pd.DataFrame([{"a": 1}])
        with patch.object(query_cli, "save_to_sheet") as mock_save:
            result = query_cli.execute_save(
                df,
                {
                    "to": "sheets",
                    "sheet_url": "https://docs.google.com/spreadsheets/d/x",
                    "sheet_name": "data",
                    "mode": "upsert",
                    "keys": ["a"],
                },
            )
        self.assertEqual(result["saved_to"], "sheets")
        mock_save.assert_called_once()

    def test_bigquery(self):
        df = pd.DataFrame([{"a": 1}])
        with patch.object(query_cli, "save_to_bq", return_value={"table": "p.d.t"}) as mock_save:
            result = query_cli.execute_save(
                df,
                {"to": "bigquery", "project_id": "p", "dataset": "d", "table": "t", "mode": "overwrite"},
            )
        self.assertEqual(result["saved_to"], "bigquery")
        self.assertEqual(result["table"], "p.d.t")
        mock_save.assert_called_once()

    def test_unknown_target_raises(self):
        with self.assertRaises(ValueError):
            query_cli.execute_save(pd.DataFrame([{"a": 1}]), {"to": "s3"})


if __name__ == "__main__":
    unittest.main()
