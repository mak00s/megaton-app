import unittest

import pandas as pd

from lib.result_inspector import (
    apply_where,
    apply_sort,
    apply_columns,
    apply_group_aggregate,
    apply_pipeline,
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


if __name__ == "__main__":
    unittest.main()
