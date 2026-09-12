"""Auditable lexical query grouping, without character sorting or fuzzy merging.

Callers own vocabulary and scope. Aggregates are conserved per period and
partition; CTR is recomputed, never summed. No site-specific policy lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

import pandas as pd


@dataclass(frozen=True)
class QueryPolicy:
    replacements: tuple[tuple[str, str], ...] = ()
    guards: tuple[str, ...] = ()
    reorder_terms: tuple[str, ...] = ()


@dataclass
class QueryGroups:
    grouped: pd.DataFrame
    audit: pd.DataFrame


def normalize_query(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("query must be a non-empty string")
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value).casefold()).strip()


def _keys(value: str, policy: QueryPolicy) -> set[tuple]:
    text = normalize_query(value)
    guard = tuple(bool(re.search(p, text, re.I)) for p in policy.guards)
    numbers = tuple(re.findall(r"\d+", text))
    for pattern, replacement in policy.replacements:
        text = re.sub(pattern, replacement, text)
    # Only join whitespace between Japanese characters, not English word boundaries.
    compact = re.sub(r"(?<=[ぁ-んァ-ヶ一-龠ー])\s+(?=[ぁ-んァ-ヶ一-龠ー])", "", text)
    keys = {("surface", text, guard, numbers), ("surface", compact, guard, numbers)}
    tokens = text.split()
    # Quoted queries, operators and natural-language clauses are order-sensitive.
    if (
        len(tokens) > 1
        and all(t in policy.reorder_terms for t in tokens)
        and re.search(r"[ぁ-んァ-ヶ一-龠]", text)
        and not re.search(
            r'["「」:<>+\-]|\b(?:not|or|and|to|from|before|after)\b', text
        )
        and not any(
            t in {"の", "を", "が", "は", "へ", "に", "と", "より", "から", "まで"}
            for t in tokens
        )
    ):
        keys.add(("terms", tuple(sorted(tokens)), guard, tuple(sorted(numbers))))
    return keys


def group_queries(
    frame: pd.DataFrame,
    *,
    policy: QueryPolicy = QueryPolicy(),
    partitions: tuple[str, ...] = (),
    period: str | None = None,
    current_period: str | None = None,
) -> QueryGroups:
    """Group query/clicks/impressions using a joint-period representative mapping.

    Additional dimensions such as language, page or brand scope must be supplied
    explicitly as partitions. Output includes all rows, even zero-click queries.
    """
    dims = list(partitions) + ([period] if period else [])
    if len(set(dims)) != len(dims):
        raise ValueError("Duplicate grouping dimensions")
    required = {"query", "clicks", "impressions", *dims}
    if missing := required - set(frame.columns):
        raise ValueError(f"Missing columns: {sorted(missing)}")
    work = (
        frame[list(dict.fromkeys([*dims, "query", "clicks", "impressions"]))]
        .copy()
        .reset_index(drop=True)
    )
    if dims and work[dims].isna().any().any():
        raise ValueError("Null grouping dimension")
    for metric in ("clicks", "impressions"):
        work[metric] = pd.to_numeric(work[metric], errors="raise")
        if (
            work[metric].isna().any()
            or (~work[metric].map(lambda n: 0 <= n < float("inf"))).any()
        ):
            raise ValueError(f"Invalid {metric}")
    if work.empty:
        return QueryGroups(
            pd.DataFrame(
                columns=[*dims, "query", "clicks", "impressions", "variants", "ctr"]
            ),
            pd.DataFrame(
                columns=[
                    *dims,
                    "original_query",
                    "query",
                    "clicks",
                    "impressions",
                    "reason",
                ]
            ),
        )
    work["normalized"] = work["query"].map(normalize_query)
    work["representative"] = ""
    work["reason"] = ""
    blocks = (
        work.groupby(list(partitions), sort=True, dropna=False)
        if partitions
        else [(None, work)]
    )
    for _, block in blocks:
        forms = sorted(block["query"].unique())
        parent = {v: v for v in forms}

        def find(v):
            while parent[v] != v:
                parent[v] = parent[parent[v]]
                v = parent[v]
            return v

        owners = {}
        for value in forms:
            for key in sorted(_keys(value, policy), key=repr):
                if key in owners:
                    a, b = find(value), find(owners[key])
                    parent[max(a, b)] = min(a, b)
                else:
                    owners[key] = value
        score = block.groupby("query")[["clicks", "impressions"]].sum()
        cur = (
            block[block[period].eq(current_period)]
            if period and current_period is not None
            else block
        )
        cur_score = (
            cur.groupby("query")[["clicks", "impressions"]]
            .sum()
            .reindex(forms, fill_value=0)
        )
        clusters = {}
        for value in forms:
            clusters.setdefault(find(value), []).append(value)
        scores = score.to_dict("index")
        current_scores = cur_score.to_dict("index")
        representatives, reasons = {}, {}
        for values in clusters.values():
            representative = min(
                values,
                key=lambda v: (
                    -current_scores[v]["clicks"],
                    -current_scores[v]["impressions"],
                    -scores[v]["clicks"],
                    -scores[v]["impressions"],
                    len(v),
                    v,
                ),
            )
            for value in values:
                representatives[value] = representative
                reasons[value] = (
                    "unchanged" if len(values) == 1 else "surface_or_term_order"
                )
        work.loc[block.index, "representative"] = block["query"].map(representatives)
        work.loc[block.index, "reason"] = block["query"].map(reasons)
    audit = work.rename(
        columns={"query": "original_query", "representative": "query"}
    )[[*dims, "original_query", "query", "clicks", "impressions", "reason"]]
    group_dims = [*dims, "representative"]
    grouped = (
        work.groupby(group_dims, as_index=False, dropna=False)
        .agg(
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
            variants=("query", "nunique"),
        )
        .rename(columns={"representative": "query"})
    )
    grouped["ctr"] = (grouped.clicks / grouped.impressions).where(
        grouped.impressions.gt(0), 0.0
    )
    before = (
        work.groupby(dims, dropna=False)[["clicks", "impressions"]].sum()
        if dims
        else work[["clicks", "impressions"]].sum()
    )
    after = (
        grouped.groupby(dims, dropna=False)[["clicks", "impressions"]].sum()
        if dims
        else grouped[["clicks", "impressions"]].sum()
    )
    if dims:
        pd.testing.assert_frame_equal(before, after)
    else:
        pd.testing.assert_series_equal(before, after)
    return QueryGroups(
        grouped.sort_values(
            ["clicks", "impressions", "query"], ascending=[False, False, True]
        ).reset_index(drop=True),
        audit.reset_index(drop=True),
    )
