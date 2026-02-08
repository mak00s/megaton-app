from __future__ import annotations

import pandas as pd

from megaton_lib.articles import aggregate_article_meta


def test_aggregate_article_meta_picks_pv_max_fields_and_formats_date():
    df = pd.DataFrame(
        [
            # Same article, two langs/titles; JP has more PV
            {
                "article_id": "a1",
                "article_title": "JP title",
                "article_category": "cat1",
                "article_date": "2025/11/3",
                "likes": "119",
                "language": "jp",
                "pv": 10,
            },
            {
                "article_id": "a1",
                "article_title": "EN title",
                "article_category": "(not set)",
                "article_date": "Nov. 3, 2025",
                "likes": "120.0",
                "language": "en",
                "pv": 5,
            },
            # Noise row with 1900-date: should be ignored for article_date
            {
                "article_id": "a1",
                "article_title": "(not set)",
                "article_category": "",
                "article_date": "1/13",  # parses to 1900-01-13
                "likes": None,
                "language": "jp",
                "pv": 999,
            },
        ]
    )

    out = aggregate_article_meta(df)
    assert list(out.columns) == [
        "article_id",
        "article_title",
        "article_category",
        "article_date",
        "likes",
        "language",
    ]
    row = out.iloc[0].to_dict()
    assert row["article_id"] == "a1"
    assert row["article_title"] == "JP title / EN title"
    assert row["article_category"] == "cat1"
    assert row["article_date"] == "2025/11/3"
    assert row["likes"] == 120
    assert row["language"] == "JP/EN"

def test_aggregate_article_meta_allows_multiline_title_joiner():
    df = pd.DataFrame(
        [
            {
                "article_id": "a1",
                "article_title": "JP title",
                "article_category": "cat1",
                "article_date": "2025/11/3",
                "likes": "0",
                "language": "jp",
                "pv": 10,
            },
            {
                "article_id": "a1",
                "article_title": "EN title",
                "article_category": "cat1",
                "article_date": "2025/11/3",
                "likes": "0",
                "language": "en",
                "pv": 9,
            },
        ]
    )
    out = aggregate_article_meta(df, title_joiner="\n")
    assert out.loc[0, "article_title"] == "JP title\nEN title"


def test_aggregate_article_meta_falls_back_to_article_id_when_no_valid_title():
    df = pd.DataFrame(
        [
            {
                "article_id": "a2",
                "article_title": "(not set)",
                "article_category": "cat",
                "article_date": "",
                "likes": "",
                "language": "jp",
                "pv": 1,
            }
        ]
    )
    out = aggregate_article_meta(df)
    assert out.loc[0, "article_title"] == "a2"
    assert out.loc[0, "article_date"] == ""
    assert out.loc[0, "likes"] == 0
