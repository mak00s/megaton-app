import unittest
from datetime import date

import pandas as pd

from app.ui.query_builders import (
    AGG_NONE,
    build_agent_params,
    build_pipeline_kwargs,
    build_transform_expression,
    detect_url_columns,
    parse_gsc_filter,
)


class TestParseGscFilter(unittest.TestCase):
    def test_empty_returns_none(self):
        self.assertIsNone(parse_gsc_filter(""))

    def test_valid_returns_list(self):
        self.assertEqual(
            parse_gsc_filter("query:contains:seo;page:equals:/blog"),
            [
                {"dimension": "query", "operator": "contains", "expression": "seo"},
                {"dimension": "page", "operator": "equals", "expression": "/blog"},
            ],
        )

    def test_invalid_parts_are_ignored(self):
        self.assertEqual(
            parse_gsc_filter("bad;query:contains:seo"),
            [{"dimension": "query", "operator": "contains", "expression": "seo"}],
        )


class TestPipelineBuilders(unittest.TestCase):
    def test_detect_url_columns(self):
        df = pd.DataFrame(
            {
                "page": ["https://example.com/a", "/local/path"],
                "title": ["hello", "world"],
            }
        )
        self.assertEqual(detect_url_columns(df), ["page"])

    def test_build_transform_expression(self):
        expr = build_transform_expression(
            has_date_col=True,
            url_cols=["page"],
            tf_date=True,
            tf_url_decode=True,
            tf_strip_qs=True,
            keep_params="id,ref",
            tf_path_only=True,
        )
        self.assertEqual(expr, "date:date_format,page:url_decode,page:strip_qs:id,ref,page:path_only")

    def test_build_pipeline_kwargs(self):
        kwargs, derived = build_pipeline_kwargs(
            transform_expr="page:path_only",
            where_expr="clicks > 10",
            selected_cols=["page", "clicks"],
            group_cols=["page"],
            agg_map={"clicks": "sum", "ctr": "mean", "position": AGG_NONE},
            head_val=20,
        )
        self.assertEqual(kwargs["transform"], "page:path_only")
        self.assertEqual(kwargs["where"], "clicks > 10")
        self.assertEqual(kwargs["columns"], "page,clicks")
        self.assertEqual(kwargs["group_by"], "page")
        self.assertEqual(kwargs["aggregate"], "sum:clicks,mean:ctr")
        self.assertEqual(kwargs["head"], 20)
        self.assertEqual(derived, ["sum_clicks", "mean_ctr"])


class TestAgentParams(unittest.TestCase):
    def test_build_ga4_params(self):
        params = build_agent_params(
            source="GA4",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            limit=100,
            property_id="123",
            dimensions=["date"],
            metrics=["sessions"],
            filter_d="country==JP",
        )
        self.assertEqual(params["source"], "ga4")
        self.assertEqual(params["property_id"], "123")
        self.assertEqual(params["date_range"]["start"], "2026-01-01")

    def test_build_gsc_params(self):
        params = build_agent_params(
            source="GSC",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            limit=500,
            site_url="sc-domain:example.com",
            dimensions=["query"],
            gsc_filter="query:contains:seo",
        )
        self.assertEqual(params["source"], "gsc")
        self.assertEqual(params["site_url"], "sc-domain:example.com")
        self.assertEqual(params["filter"], "query:contains:seo")

    def test_build_bigquery_params(self):
        params = build_agent_params(source="BigQuery", bq_project="p1", sql="select 1")
        self.assertEqual(params, {"source": "bigquery", "project_id": "p1", "sql": "select 1"})


if __name__ == "__main__":
    unittest.main()
