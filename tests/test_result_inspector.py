import tempfile
import unittest
from pathlib import Path

import pandas as pd

from lib.result_inspector import read_head, build_summary


class TestResultInspector(unittest.TestCase):
    def _create_csv(self, path: Path):
        df = pd.DataFrame(
            {
                "channel": ["Organic Search", "Direct", "Referral", "Direct"],
                "users": [100, 50, 25, 75],
                "rate": [0.1, 0.2, 0.3, 0.4],
            }
        )
        df.to_csv(path, index=False, encoding="utf-8-sig")

    def test_read_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sample.csv"
            self._create_csv(csv_path)
            head = read_head(csv_path, 2)
            self.assertEqual(len(head), 2)
            self.assertEqual(head.iloc[0]["channel"], "Organic Search")

    def test_read_head_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sample.csv"
            self._create_csv(csv_path)
            with self.assertRaises(ValueError):
                read_head(csv_path, 0)

    def test_build_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "sample.csv"
            self._create_csv(csv_path)
            summary = build_summary(csv_path)
            self.assertEqual(summary["row_count"], 4)
            self.assertEqual(summary["column_count"], 3)
            self.assertIn("users", summary["numeric_summary"])
            self.assertIn("channel", summary["top_values"])


if __name__ == "__main__":
    unittest.main()
