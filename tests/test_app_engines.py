import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from app.engine import ga4, gsc, visualize
from app.schemas import DateRange, Filter, QueryParams, Visualization


class TestGa4Engine(unittest.TestCase):
    def test_execute_ga4_query_with_property_resolution_and_filters(self):
        mg = MagicMock()
        mg.ga = {
            "4": MagicMock(
                accounts=[
                    {"id": "acc1", "properties": [{"id": "p1"}]},
                    {"id": "acc2", "properties": [{"id": "p2"}]},
                ]
            )
        }
        mg.report.data = pd.DataFrame([{"sessions": 10}])

        params = QueryParams(
            schema_version="1.0",
            source="ga4",
            date_range=DateRange(start="2026-01-01", end="2026-01-31"),
            dimensions=["date"],
            metrics=["sessions"],
            filters=[
                Filter(field="country", op="==", value="Japan"),
                Filter(field="deviceCategory", op="!=", value="desktop"),
            ],
            property_id="p2",
            limit=100,
        )

        with patch("app.engine.ga4.get_megaton", return_value=mg):
            df = ga4.execute_ga4_query(params)

        self.assertEqual(len(df), 1)
        mg.ga["4"].account.select.assert_called_once_with("acc2")
        mg.ga["4"].property.select.assert_called_once_with("p2")
        mg.report.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
        mg.report.run.assert_called_once()
        self.assertEqual(mg.report.run.call_args.kwargs["filter_d"], "country==Japan;deviceCategory!=desktop")

    def test_execute_ga4_query_fallback_default_account(self):
        mg = MagicMock()
        mg.ga = {"4": MagicMock(accounts=[{"id": "acc1", "properties": [{"id": "p1"}]}])}
        mg.report.data = pd.DataFrame([{"sessions": 1}])
        params = QueryParams(
            schema_version="1.0",
            source="ga4",
            date_range=DateRange(start="2026-01-01", end="2026-01-31"),
            dimensions=["date"],
            metrics=["sessions"],
            property_id="unknown",
        )
        with patch("app.engine.ga4.get_megaton", return_value=mg):
            ga4.execute_ga4_query(params)
        mg.ga["4"].account.select.assert_called_once_with(ga4.DEFAULT_GA4_ACCOUNT)
        mg.ga["4"].property.select.assert_called_once_with("unknown")

    def test_list_ga4_properties(self):
        mg = MagicMock()
        mg.ga = {
            "4": MagicMock(
                accounts=[
                    {
                        "id": "a1",
                        "name": "Account 1",
                        "properties": [{"id": "p1", "name": "Prop 1"}],
                    }
                ]
            )
        }
        with patch("app.engine.ga4.get_megaton", return_value=mg):
            props = ga4.list_ga4_properties()
        self.assertEqual(props[0]["property_id"], "p1")
        self.assertIn("Prop 1", props[0]["display"])


class TestGscEngine(unittest.TestCase):
    def test_execute_gsc_query_with_filters(self):
        mg = MagicMock()
        mg.search.data = pd.DataFrame(
            [
                {"query": "seo tips", "page": "/blog/a"},
                {"query": "ads", "page": "/blog/b"},
            ]
        )
        params = QueryParams(
            schema_version="1.0",
            source="gsc",
            site_url="sc-domain:example.com",
            date_range=DateRange(start="2026-01-01", end="2026-01-31"),
            dimensions=["query", "page"],
            metrics=["clicks"],
            filters=[
                Filter(field="query", op="contains", value="seo"),
                Filter(field="page", op="not_contains", value="z"),
            ],
            limit=200,
        )
        with patch("app.engine.gsc.get_megaton", return_value=mg):
            df = gsc.execute_gsc_query(params)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["query"], "seo tips")
        mg.search.use.assert_called_once_with("sc-domain:example.com")
        mg.search.run.assert_called_once()

    def test_list_gsc_sites(self):
        mg = MagicMock()
        mg.search.get.sites.return_value = ["sc-domain:a.com", "sc-domain:b.com"]
        with patch("app.engine.gsc.get_megaton", return_value=mg):
            sites = gsc.list_gsc_sites()
        self.assertEqual(len(sites), 2)


class TestVisualize(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"x": list(range(30)), "y": list(range(30))})

    def test_create_chart_none_for_table_or_none(self):
        self.assertIsNone(visualize.create_chart(self.df, None))
        self.assertIsNone(visualize.create_chart(self.df, Visualization(type="table")))

    def test_create_chart_line_bar_pie(self):
        line = visualize.create_chart(self.df, Visualization(type="line", x="x", y="y", title="L"))
        bar = visualize.create_chart(self.df, Visualization(type="bar", x="x", y="y", title="B"))
        pie = visualize.create_chart(self.df, Visualization(type="pie", x="x", y="y", title="P"))
        self.assertIsNotNone(line)
        self.assertIsNotNone(bar)
        self.assertIsNotNone(pie)

    def test_create_chart_unknown_returns_none(self):
        viz = Visualization(type="line", x="x", y="y")
        viz.type = "unknown"  # runtime override for branch test
        self.assertIsNone(visualize.create_chart(self.df, viz))

    def test_format_dataframe_formats_numeric(self):
        df = pd.DataFrame(
            {
                "clicks": [1000],
                "ctr": [0.125],
                "position": [3.14159],
                "value": [1234.567],
            }
        )
        out = visualize.format_dataframe(df)
        self.assertEqual(out.iloc[0]["clicks"], "1,000")
        self.assertEqual(out.iloc[0]["ctr"], "12.50%")
        self.assertEqual(out.iloc[0]["position"], "3.1")
        self.assertEqual(out.iloc[0]["value"], "1,234.57")


if __name__ == "__main__":
    unittest.main()
