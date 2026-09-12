import pandas as pd
import pytest
from megaton_lib.query_normalization import QueryPolicy, group_queries

POLICY = QueryPolicy(reorder_terms=("サンプルブランド", "工場見学"))


def data(queries):
    return pd.DataFrame(
        {
            "query": queries,
            "clicks": range(1, len(queries) + 1),
            "impressions": [100] * len(queries),
        }
    )


def test_surface_order_and_no_character_anagrams():
    r = group_queries(
        data(
            [
                "サンプルブランド 工場見学",
                "工場見学 サンプルブランド",
                "サンプルブランド工場見学",
                "ＥＸＡＭＰＬＥ－ＢＲＡＮＤ",
                "example-brand",
                "abc",
                "cba",
            ]
        ),
        policy=POLICY,
    )
    assert len(r.grouped) == 4
    assert r.grouped.clicks.sum() == 28
    assert r.grouped.impressions.sum() == 700
    assert r.grouped.set_index("query").loc["サンプルブランド工場見学", "clicks"] == 6


def test_protected_scopes_and_intents():
    r = group_queries(
        data(
            [
                "工場見学",
                "工場見学 予約",
                "株価",
                "配当",
                "サンプルブランド 2025",
                "サンプルブランド 2026",
                "cream skin",
                "skin cream",
                "now here",
                "nowhere",
            ]
        )
    )
    assert len(r.grouped) == 10
    f = data(["サンプルブランド", "シセイドウ"])
    f["scope"] = ["brand", "nonbrand"]
    r = group_queries(
        f,
        partitions=("scope",),
        policy=QueryPolicy(replacements=(("シセイドウ", "サンプルブランド"),)),
    )
    assert len(r.grouped) == 2


def test_joint_period_mapping_and_ctr():
    f = pd.DataFrame(
        [
            ["工場見学 サンプルブランド", 90, 100, "07"],
            ["サンプルブランド 工場見学", 2, 10, "08"],
            ["工場見学 サンプルブランド", 1, 20, "08"],
        ],
        columns=["query", "clicks", "impressions", "month"],
    )
    r = group_queries(f, period="month", current_period="08", policy=POLICY)
    assert set(r.grouped["query"]) == {"サンプルブランド 工場見学"}
    assert r.grouped.set_index("month").loc["08", "ctr"] == 0.1
    pd.testing.assert_frame_equal(
        r.grouped,
        group_queries(
            f.sample(frac=1, random_state=2),
            period="month",
            current_period="08",
            policy=POLICY,
        ).grouped,
    )


def test_duplicate_input_index_and_unapproved_order():
    f = pd.concat([data(["サンプルブランド 工場見学"]), data(["工場見学 サンプルブランド"])])
    assert len(group_queries(f, policy=POLICY).grouped) == 1
    assert len(group_queries(f).grouped) == 2
    assert (
        len(group_queries(data(["東京 大阪", "大阪 東京"]), policy=POLICY).grouped) == 2
    )


def test_empty_and_nonempty_outputs_share_column_order():
    f = data(["サンプルブランド"])
    f["month"] = "08"
    f["scope"] = "brand"
    kwargs = dict(partitions=("scope",), period="month")
    nonempty = group_queries(f, **kwargs)
    empty = group_queries(f.iloc[:0], **kwargs)
    assert list(empty.grouped.columns) == list(nonempty.grouped.columns)
    assert list(empty.audit.columns) == list(nonempty.audit.columns)


def test_reject_null_or_bad_metrics():
    for queries in [[None], [""]]:
        with pytest.raises(ValueError):
            group_queries(data(queries))
    f = data(["ok"])
    f["clicks"] = -1
    with pytest.raises(ValueError):
        group_queries(f)
