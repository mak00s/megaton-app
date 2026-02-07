"""Streamlit params helpers with pure, testable logic."""
from __future__ import annotations

import pandas as pd

# GA4 operators
GA4_OPERATORS = ["==", "!=", "=@", "!@", "=~", "!~", ">", ">=", "<", "<="]
GA4_OPERATOR_LABELS = {
    "==": "等しい",
    "!=": "等しくない",
    "=@": "含む",
    "!@": "含まない",
    "=~": "正規表現一致",
    "!~": "正規表現不一致",
    ">": "より大きい",
    ">=": "以上",
    "<": "より小さい",
    "<=": "以下",
}

# GSC operators
GSC_OPERATORS = ["contains", "notContains", "equals", "notEquals", "includingRegex", "excludingRegex"]
GSC_OPERATOR_LABELS = {
    "contains": "含む",
    "notContains": "含まない",
    "equals": "等しい",
    "notEquals": "等しくない",
    "includingRegex": "正規表現一致",
    "excludingRegex": "正規表現不一致",
}


def parse_ga4_filter_to_df(filter_str: str) -> pd.DataFrame:
    """GA4フィルタ文字列をDataFrameにパース."""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=["対象", "演算子", "値"])

    rows = []
    for part in filter_str.split(";"):
        part = part.strip()
        if not part:
            continue
        # Try longer operators first.
        for op in sorted(GA4_OPERATORS, key=len, reverse=True):
            if op in part:
                idx = part.index(op)
                field = part[:idx]
                value = part[idx + len(op) :]
                rows.append({"対象": field, "演算子": op, "値": value})
                break

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["対象", "演算子", "値"])


def serialize_ga4_filter_from_df(df: pd.DataFrame) -> str:
    """DataFrameからGA4フィルタ文字列を生成."""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row["対象"] and row["演算子"] and row["値"]:
            parts.append(f"{row['対象']}{row['演算子']}{row['値']}")
    return ";".join(parts)


def parse_gsc_filter_to_df(filter_str: str) -> pd.DataFrame:
    """GSCフィルタ文字列をDataFrameにパース."""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=["対象", "演算子", "値"])

    rows = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) == 3:
            rows.append({"対象": parts[0], "演算子": parts[1], "値": parts[2]})

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["対象", "演算子", "値"])


def serialize_gsc_filter_from_df(df: pd.DataFrame) -> str:
    """DataFrameからGSCフィルタ文字列を生成."""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row["対象"] and row["演算子"] and row["値"]:
            parts.append(f"{row['対象']}:{row['演算子']}:{row['値']}")
    return ";".join(parts)


def has_effective_params_update(
    current_mtime: float,
    last_mtime: float,
    canonical: str | None,
    last_canonical: str | None,
) -> bool:
    """mtime + canonical diff based update decision."""
    if current_mtime <= last_mtime:
        return False
    if canonical is None:
        return True
    return canonical != last_canonical
