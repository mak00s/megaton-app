import pandas as pd

from megaton_lib.table_utils import apply_pattern_map, classify_by_pattern_map


def test_apply_pattern_map_keeps_unmatched_by_default():
    df = pd.DataFrame({"query": ["abc", "xyz"]})
    out = apply_pattern_map(df, "query", {r"abc": "mapped"})
    assert out["query"].tolist() == ["mapped", "xyz"]


def test_apply_pattern_map_uses_default_unmatched_when_set():
    df = pd.DataFrame({"page": ["/a", "/z"]})
    out = apply_pattern_map(df, "page", {r"^/a": "A"}, output_col="cat", default_unmatched="other")
    assert out["cat"].tolist() == ["A", "other"]


def test_classify_by_pattern_map_assigns_default_label():
    df = pd.DataFrame({"page": ["/news/1", "/x"]})
    out = classify_by_pattern_map(
        df,
        {r"/news/": "news"},
        source_col="page",
        output_col="page_category",
        default_label="other",
    )
    assert out["page_category"].tolist() == ["news", "other"]
