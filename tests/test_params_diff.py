import json
import unittest

from lib.params_diff import canonicalize_json


class TestParamsDiff(unittest.TestCase):
    def test_canonicalize_ignores_whitespace_and_indent(self):
        a = json.loads(
            """
            {
              "source": "ga4",
              "limit": 1000,
              "date_range": { "start": "2026-02-01", "end": "2026-02-03" }
            }
            """
        )
        b = json.loads('{"source":"ga4","limit":1000,"date_range":{"start":"2026-02-01","end":"2026-02-03"}}')
        self.assertEqual(canonicalize_json(a), canonicalize_json(b))

    def test_canonicalize_ignores_key_order(self):
        a = {"a": 1, "b": 2, "c": {"x": 1, "y": 2}}
        b = {"c": {"y": 2, "x": 1}, "b": 2, "a": 1}
        self.assertEqual(canonicalize_json(a), canonicalize_json(b))

    def test_canonicalize_detects_real_change(self):
        a = {"source": "ga4", "limit": 1000}
        b = {"source": "ga4", "limit": 500}
        self.assertNotEqual(canonicalize_json(a), canonicalize_json(b))


if __name__ == "__main__":
    unittest.main()
