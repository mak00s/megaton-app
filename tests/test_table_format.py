import unittest

import pandas as pd

from app.ui.table_format import _format_number, _parse_date_like_series, build_table_view_df


class TestTableFormatHelpers(unittest.TestCase):
    def test_parse_date_like_series(self):
        series = pd.Series(["2026-03-12", "Mar 13, 2026", "x"])
        parsed = _parse_date_like_series(series)
        self.assertEqual(str(parsed.iloc[0].date()), "2026-03-12")
        self.assertEqual(str(parsed.iloc[1].date()), "2026-03-13")
        self.assertTrue(pd.isna(parsed.iloc[2]))

    def test_format_number_percent_ratio_and_value(self):
        self.assertEqual(_format_number(0.25, kind="percent", decimals=1, thousands_sep=False), "25.0%")
        self.assertEqual(_format_number(1.5, kind="percent", decimals=1, thousands_sep=False), "150.0%")
        self.assertEqual(_format_number(25.5, kind="percent", decimals=1, thousands_sep=False), "25.5%")

    def test_build_table_view_df_formats_hint_columns(self):
        df = pd.DataFrame(
            {
                "date": ["Mar 13, 2026", "Mar 14, 2026"],
                "orders": [1000, 25],
                "rate": [0.25, 1.5],
                "revenue": [1234.5, 9.0],
            }
        )
        out = build_table_view_df(
            df,
            date_format="%Y-%m-%d",
            thousands_sep=True,
            decimals=1,
            column_types={"date": "date", "orders": "int", "rate": "percent", "revenue": "currency"},
        )
        self.assertEqual(out.loc[0, "date"], "2026-03-13")
        self.assertEqual(out.loc[0, "orders"], "1,000")
        self.assertEqual(out.loc[0, "rate"], "25.0%")
        self.assertEqual(out.loc[1, "rate"], "150.0%")
        self.assertEqual(out.loc[0, "revenue"], "1,234.5")


if __name__ == "__main__":
    unittest.main()
