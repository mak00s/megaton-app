import pandas as pd
import pytest

from megaton_lib.gsc_utils import (
    aggregate_search_console_data,
    deduplicate_queries,
    filter_by_clinic_thresholds,
    force_text_on_numeric_column,
)


def test_aggregate_search_console_data_weighted_position_and_clean_page():
    df = pd.DataFrame(
        {
            "query": ["q1", "q1"],
            "page": ["https://x.com/Page?a=1", "https://x.com/Page#f"],
            "clicks": [1, 2],
            "impressions": [10, 20],
            "position": [2.0, 4.0],
        }
    )
    out = aggregate_search_console_data(df)
    assert len(out) == 1
    assert out.loc[0, "page"] == "https://x.com/page"
    assert out.loc[0, "clicks"] == 3
    assert out.loc[0, "impressions"] == 30
    assert out.loc[0, "position"] == pytest.approx((2 * 10 + 4 * 20) / 30)


def test_deduplicate_queries_merges_space_variants():
    df = pd.DataFrame(
        {
            "month": ["202601", "202601"],
            "clinic": ["渋谷", "渋谷"],
            "query": ["abc", "a bc"],
            "page": ["/x", "/x"],
            "impressions": [100, 20],
            "clicks": [10, 2],
            "position": [2.0, 4.0],
        }
    )
    out = deduplicate_queries(df)
    assert len(out) == 1
    assert out.loc[0, "query"] == "abc"
    assert out.loc[0, "impressions"] == 120


def test_filter_by_clinic_thresholds_drops_low_value_rows():
    df = pd.DataFrame(
        {
            "clinic": ["渋谷", "渋谷", "新宿"],
            "clicks": [0, 1, 0],
            "impressions": [5, 5, 1],
            "position": [80, 80, 99],
        }
    )
    thresholds = pd.DataFrame(
        {
            "clinic": ["渋谷", "新宿"],
            "min_impressions": [10, 10],
            "max_position": [50, 50],
        }
    )
    out = filter_by_clinic_thresholds(df, thresholds)
    assert len(out) == 1
    assert out.iloc[0]["clinic"] == "渋谷"
    assert out.iloc[0]["clicks"] == 1


def test_filter_by_clinic_thresholds_keeps_only_defined_clinics():
    df = pd.DataFrame(
        {
            "clinic": ["渋谷", "未定義"],
            "clicks": [1, 1],
            "impressions": [100, 100],
            "position": [5, 5],
        }
    )
    thresholds = pd.DataFrame(
        {
            "clinic": ["渋谷"],
            "min_impressions": [10],
            "max_position": [50],
        }
    )
    out = filter_by_clinic_thresholds(df, thresholds)
    assert out["clinic"].tolist() == ["渋谷"]


def test_force_text_on_numeric_column_prefixes_numeric_values():
    df = pd.DataFrame({"query": ["0123", "abc"]})
    out = force_text_on_numeric_column(df, column="query")
    assert out["query"].tolist() == ["'0123", "abc"]
