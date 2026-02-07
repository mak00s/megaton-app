import unittest

import pandas as pd

from app.ui.params_utils import (
    has_effective_params_update,
    parse_ga4_filter_to_df,
    parse_gsc_filter_to_df,
    serialize_ga4_filter_from_df,
    serialize_gsc_filter_from_df,
)


class TestFilterHelpers(unittest.TestCase):
    def test_parse_ga4_filter_to_df_empty(self):
        df = parse_ga4_filter_to_df("")
        self.assertEqual(list(df.columns), ["対象", "演算子", "値"])
        self.assertEqual(len(df), 0)

    def test_parse_ga4_filter_to_df(self):
        df = parse_ga4_filter_to_df("sessions>=100;pagePath=@/blog")
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0].to_dict(), {"対象": "sessions", "演算子": ">=", "値": "100"})
        self.assertEqual(df.iloc[1].to_dict(), {"対象": "pagePath", "演算子": "=@", "値": "/blog"})

    def test_serialize_ga4_filter_from_df(self):
        df = pd.DataFrame(
            [
                {"対象": "sessions", "演算子": ">=", "値": "100"},
                {"対象": "pagePath", "演算子": "=@", "値": "/blog"},
            ]
        )
        out = serialize_ga4_filter_from_df(df)
        self.assertEqual(out, "sessions>=100;pagePath=@/blog")

    def test_parse_gsc_filter_to_df(self):
        df = parse_gsc_filter_to_df("query:contains:seo;page:equals:/blog")
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0].to_dict(), {"対象": "query", "演算子": "contains", "値": "seo"})
        self.assertEqual(df.iloc[1].to_dict(), {"対象": "page", "演算子": "equals", "値": "/blog"})

    def test_serialize_gsc_filter_from_df(self):
        df = pd.DataFrame(
            [
                {"対象": "query", "演算子": "contains", "値": "seo"},
                {"対象": "page", "演算子": "equals", "値": "/blog"},
            ]
        )
        out = serialize_gsc_filter_from_df(df)
        self.assertEqual(out, "query:contains:seo;page:equals:/blog")


class TestUpdateDecision(unittest.TestCase):
    def test_no_update_when_mtime_not_advanced(self):
        self.assertFalse(has_effective_params_update(100.0, 100.0, "a", "b"))

    def test_update_when_invalid_json_and_mtime_advanced(self):
        self.assertTrue(has_effective_params_update(101.0, 100.0, None, "old"))

    def test_no_update_when_canonical_same(self):
        self.assertFalse(has_effective_params_update(101.0, 100.0, '{"a":1}', '{"a":1}'))

    def test_update_when_canonical_changed(self):
        self.assertTrue(has_effective_params_update(101.0, 100.0, '{"a":2}', '{"a":1}'))


if __name__ == "__main__":
    unittest.main()
