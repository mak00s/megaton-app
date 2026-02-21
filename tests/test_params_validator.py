import unittest

from megaton_lib.params_validator import validate_params


class TestParamsValidator(unittest.TestCase):
    def test_valid_ga4(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["sessionDefaultChannelGroup"],
            "metrics": ["totalUsers"],
            "limit": 1000,
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertEqual(normalized["source"], "ga4")

    def test_valid_gsc_with_default_limit(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertEqual(normalized["limit"], 1000)

    def test_valid_bigquery(self):
        data = {
            "schema_version": "1.0",
            "source": "bigquery",
            "project_id": "my-project",
            "sql": "SELECT 1",
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertEqual(normalized["source"], "bigquery")

    def test_missing_schema_version(self):
        data = {
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "INVALID_SCHEMA_VERSION" for err in errors))

    def test_reject_unknown_field(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "unexpected": "x",
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "UNKNOWN_FIELD" for err in errors))

    def test_invalid_date(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026/02/01", "end": "2026-02-03"},
            "dimensions": ["query"],
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "INVALID_DATE" for err in errors))

    def test_limit_out_of_range(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "limit": 100001,
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "OUT_OF_RANGE" for err in errors))


    # --- pipeline ---
    def test_valid_ga4_with_pipeline(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "pipeline": {
                "transform": "date:date_format",
                "where": "sessions > 10",
                "sort": "sessions DESC",
                "head": 20,
            },
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertIn("pipeline", normalized)

    def test_valid_gsc_with_pipeline(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query", "page"],
            "pipeline": {
                "transform": "page:url_decode,page:strip_qs,page:path_only",
                "group_by": "page",
                "aggregate": "sum:clicks,sum:impressions",
                "sort": "sum_clicks DESC",
            },
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])

    def test_valid_bq_with_pipeline(self):
        data = {
            "schema_version": "1.0",
            "source": "bigquery",
            "project_id": "my-project",
            "sql": "SELECT 1",
            "pipeline": {"columns": "col1,col2"},
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])

    def test_pipeline_unknown_field(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "pipeline": {"filter": "bad"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "UNKNOWN_FIELD" for err in errors))

    def test_pipeline_group_by_without_aggregate(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "pipeline": {"group_by": "query"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "INVALID_PIPELINE" for err in errors))

    def test_pipeline_invalid_type(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "pipeline": {"head": "not_int"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "INVALID_TYPE" for err in errors))

    def test_pipeline_head_out_of_range(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "pipeline": {"head": 0},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "OUT_OF_RANGE" for err in errors))

    def test_pipeline_head_bool_rejected(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "254477007",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "pipeline": {"head": True},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(err["error_code"] == "INVALID_TYPE" for err in errors))

    # --- save ---
    def test_valid_save_csv(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "csv", "path": "output/report.csv"},
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertEqual(normalized["save"]["to"], "csv")

    def test_valid_save_csv_append(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "csv", "path": "output/report.csv", "mode": "append"},
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])

    def test_valid_save_sheets(self):
        data = {
            "schema_version": "1.0",
            "source": "ga4",
            "property_id": "123",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
            "save": {
                "to": "sheets",
                "sheet_url": "https://docs.google.com/spreadsheets/d/xxx",
                "sheet_name": "data",
                "mode": "upsert",
                "keys": ["date"],
            },
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])

    def test_valid_save_bq(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {
                "to": "bigquery",
                "project_id": "my-proj",
                "dataset": "analytics",
                "table": "gsc_data",
            },
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])

    def test_save_csv_missing_path(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "csv"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "MISSING_REQUIRED" for e in errors))

    def test_save_csv_upsert_rejected(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "csv", "path": "x.csv", "mode": "upsert"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "INVALID_SAVE_MODE" for e in errors))

    def test_save_bq_upsert_rejected(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {
                "to": "bigquery",
                "project_id": "p",
                "dataset": "d",
                "table": "t",
                "mode": "upsert",
            },
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "INVALID_SAVE_MODE" for e in errors))

    def test_save_sheets_upsert_requires_keys(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "sheets", "sheet_url": "https://example.com/sheet", "mode": "upsert"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "MISSING_REQUIRED" for e in errors))

    def test_save_bq_missing_fields(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "bigquery", "project_id": "x"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        # dataset and table missing
        missing_errors = [e for e in errors if e["error_code"] == "MISSING_REQUIRED"]
        self.assertGreaterEqual(len(missing_errors), 2)

    def test_save_unknown_field(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "csv", "path": "x.csv", "unknown": "x"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "UNKNOWN_FIELD" for e in errors))

    def test_save_invalid_target(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "s3"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "INVALID_SAVE_TARGET" for e in errors))

    def test_save_sheets_missing_url(self):
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "save": {"to": "sheets"},
        }
        normalized, errors = validate_params(data)
        self.assertIsNone(normalized)
        self.assertTrue(any(e["error_code"] == "MISSING_REQUIRED" for e in errors))

    def test_save_with_pipeline(self):
        """Both save and pipeline can be specified together."""
        data = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
            "dimensions": ["query"],
            "pipeline": {"where": "clicks > 10", "sort": "clicks DESC"},
            "save": {"to": "csv", "path": "output/report.csv"},
        }
        normalized, errors = validate_params(data)
        self.assertEqual(errors, [])
        self.assertIn("pipeline", normalized)
        self.assertIn("save", normalized)


if __name__ == "__main__":
    unittest.main()
