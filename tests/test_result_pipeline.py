import unittest

import pandas as pd

from megaton_lib.result_inspector import (
    apply_transform,
    apply_where,
    apply_sort,
    apply_columns,
    apply_group_aggregate,
    apply_pipeline,
    parse_transforms,
)


class TestResultPipeline(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "page": ["/blog/a", "/blog/a", "/blog/b", "/blog/b", "/products/x", "/products/x"],
                "query": ["seo tips", "seo guide", "python tutorial", "python basics", "buy widget", "widget price"],
                "clicks": [100, 50, 200, 30, 80, 120],
                "impressions": [1000, 800, 3000, 500, 600, 2000],
                "ctr": [0.10, 0.0625, 0.0667, 0.06, 0.1333, 0.06],
                "position": [3.2, 5.1, 2.8, 8.4, 4.0, 6.5],
            }
        )

    # --- apply_where ---
    def test_where_basic(self):
        out = apply_where(self.df, "clicks > 100")
        self.assertEqual(len(out), 2)

    def test_where_and(self):
        out = apply_where(self.df, "impressions >= 1000 and ctr < 0.07")
        self.assertEqual(len(out), 2)

    def test_where_string_contains(self):
        out = apply_where(self.df, "page.str.contains('/blog/')")
        self.assertEqual(len(out), 4)

    def test_where_invalid_raises(self):
        with self.assertRaises(ValueError):
            apply_where(self.df, "unknown_col > 10")

    # --- apply_sort ---
    def test_sort_single_desc(self):
        out = apply_sort(self.df, "clicks DESC")
        self.assertEqual(out.iloc[0]["clicks"], 200)

    def test_sort_multiple(self):
        out = apply_sort(self.df, "page ASC,clicks DESC")
        blog_a = out[out["page"] == "/blog/a"]
        self.assertEqual(blog_a.iloc[0]["clicks"], 100)

    def test_sort_default_asc(self):
        out = apply_sort(self.df, "clicks")
        self.assertEqual(out.iloc[0]["clicks"], 30)

    def test_sort_invalid_column(self):
        with self.assertRaises(ValueError):
            apply_sort(self.df, "not_exists DESC")

    # --- apply_columns ---
    def test_columns_select(self):
        out = apply_columns(self.df, "query,clicks,impressions")
        self.assertEqual(list(out.columns), ["query", "clicks", "impressions"])

    def test_columns_invalid(self):
        with self.assertRaises(ValueError):
            apply_columns(self.df, "query,not_exists")

    # --- apply_group_aggregate ---
    def test_group_sum(self):
        out = apply_group_aggregate(self.df, "page", "sum:clicks")
        self.assertIn("sum_clicks", out.columns)
        self.assertEqual(int(out[out["page"] == "/blog/a"]["sum_clicks"].iloc[0]), 150)

    def test_group_multiple_agg(self):
        out = apply_group_aggregate(self.df, "page", "sum:clicks,mean:ctr,max:position")
        self.assertIn("sum_clicks", out.columns)
        self.assertIn("mean_ctr", out.columns)
        self.assertIn("max_position", out.columns)

    def test_group_invalid_func(self):
        with self.assertRaises(ValueError):
            apply_group_aggregate(self.df, "page", "invalid:clicks")

    # --- apply_pipeline ---
    def test_pipeline_where_sort_head(self):
        out = apply_pipeline(
            self.df,
            where="impressions >= 800",
            sort="clicks DESC",
            head=2,
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(int(out.iloc[0]["clicks"]), 200)

    def test_pipeline_group_then_sort(self):
        out = apply_pipeline(
            self.df,
            group_by="page",
            aggregate="sum:clicks,sum:impressions",
            sort="sum_clicks DESC",
        )
        self.assertEqual(out.iloc[0]["page"], "/blog/b")

    def test_pipeline_all(self):
        out = apply_pipeline(
            self.df,
            where="impressions >= 800",
            group_by="page",
            aggregate="sum:clicks,mean:ctr",
            sort="sum_clicks DESC",
            columns="page,sum_clicks,mean_ctr",
            head=2,
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(list(out.columns), ["page", "sum_clicks", "mean_ctr"])


class TestTransform(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "date": ["20260101", "20260102", "20260103"],
                "page": [
                    "https://example.com/blog/a?utm_source=google&id=1",
                    "https://example.com/blog/b?utm_source=twitter&id=2&ref=top",
                    "https://example.com/%E3%83%96%E3%83%AD%E3%82%B0?id=3",
                ],
                "clicks": [100, 200, 50],
            }
        )

    # --- parse_transforms ---
    def test_parse_basic(self):
        result = parse_transforms("date:date_format")
        self.assertEqual(result, [("date", "date_format", None)])

    def test_parse_multiple(self):
        result = parse_transforms("date:date_format,page:url_decode")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ("date", "date_format", None))
        self.assertEqual(result[1], ("page", "url_decode", None))

    def test_parse_strip_qs_with_args(self):
        result = parse_transforms("page:strip_qs:id,ref")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("page", "strip_qs", "id,ref"))

    def test_parse_empty_raises(self):
        with self.assertRaises(ValueError):
            parse_transforms("")

    def test_parse_invalid_func_raises(self):
        with self.assertRaises(ValueError):
            parse_transforms("page:nonexistent")

    # --- apply_transform: date_format ---
    def test_date_format(self):
        out = apply_transform(self.df, "date:date_format")
        self.assertEqual(out["date"].iloc[0], "2026-01-01")
        self.assertEqual(out["date"].iloc[1], "2026-01-02")
        self.assertEqual(out["date"].iloc[2], "2026-01-03")

    # --- apply_transform: url_decode ---
    def test_url_decode(self):
        out = apply_transform(self.df, "page:url_decode")
        self.assertIn("ブログ", out["page"].iloc[2])

    # --- apply_transform: path_only ---
    def test_path_only(self):
        out = apply_transform(self.df, "page:path_only")
        self.assertEqual(out["page"].iloc[0], "/blog/a")
        self.assertEqual(out["page"].iloc[1], "/blog/b")

    # --- apply_transform: strip_qs ---
    def test_strip_qs_all(self):
        out = apply_transform(self.df, "page:strip_qs")
        self.assertNotIn("?", out["page"].iloc[0])
        self.assertNotIn("utm_source", out["page"].iloc[0])
        self.assertNotIn("id=", out["page"].iloc[0])

    def test_strip_qs_keep_one(self):
        out = apply_transform(self.df, "page:strip_qs:id")
        # id should be kept
        self.assertIn("id=1", out["page"].iloc[0])
        # utm_source should be removed
        self.assertNotIn("utm_source", out["page"].iloc[0])

    def test_strip_qs_keep_multiple(self):
        out = apply_transform(self.df, "page:strip_qs:id,ref")
        # Row 1: has id and ref
        self.assertIn("id=2", out["page"].iloc[1])
        self.assertIn("ref=top", out["page"].iloc[1])
        # utm_source should be removed
        self.assertNotIn("utm_source", out["page"].iloc[1])

    # --- chained transforms ---
    def test_chained_transforms(self):
        out = apply_transform(self.df, "page:strip_qs,page:path_only")
        # After strip_qs + path_only, should be just the path
        self.assertEqual(out["page"].iloc[0], "/blog/a")
        self.assertEqual(out["page"].iloc[1], "/blog/b")

    # --- error cases ---
    def test_transform_invalid_column(self):
        with self.assertRaises(ValueError):
            apply_transform(self.df, "nonexistent:date_format")

    def test_transform_invalid_function(self):
        with self.assertRaises(ValueError):
            apply_transform(self.df, "page:bogus_func")

    def test_transform_empty(self):
        with self.assertRaises(ValueError):
            apply_transform(self.df, "")

    # --- pipeline integration ---
    def test_pipeline_transform_then_where(self):
        out = apply_pipeline(
            self.df,
            transform="date:date_format",
            where="date == '2026-01-01'",
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["clicks"], 100)


if __name__ == "__main__":
    unittest.main()
