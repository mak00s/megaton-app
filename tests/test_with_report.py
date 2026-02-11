from __future__ import annotations

import pandas as pd

from megaton_lib.with_report import build_month_sheet_df


def test_build_month_sheet_df_basic_shape_and_sort():
    df_articles = pd.DataFrame(
        [
            {"month": "202501", "article_id": "a1", "region": "GHQ", "pv": 10, "uu": 5, "vstart": 1},
            {"month": "202501", "article_id": "a1", "region": "JPN", "pv": 20, "uu": 8, "vstart": 2},
            {"month": "202501", "article_id": "a2", "region": "GHQ", "pv": 3, "uu": 2, "vstart": 0},
        ]
    )
    df_meta = pd.DataFrame(
        [
            {
                "article_id": "a1",
                "article_title": "t1",
                "article_category": "c1",
                "article_date": "2025-01-10",
                "language": "jp",
                "likes": 1,
            },
            {
                "article_id": "a2",
                "article_title": "t2",
                "article_category": "c2",
                "article_date": "2025-01-11",
                "language": "jp",
                "likes": 2,
            },
        ]
    )

    out = build_month_sheet_df(df_articles, df_meta, ["202501"])

    assert len(out) == 2
    assert out.columns[0] == "No"
    assert "PV_合計" in out.columns
    assert out.iloc[0]["article_id"] == "a1"  # higher PV total first

