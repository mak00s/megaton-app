"""Streamlit params helpers with pure, testable logic."""
from __future__ import annotations

import pandas as pd

# Internal column names (language-independent)
COL_FIELD = "field"
COL_OPERATOR = "operator"
COL_VALUE = "value"
FILTER_COLUMNS = [COL_FIELD, COL_OPERATOR, COL_VALUE]

# GA4 operators
GA4_OPERATORS = ["==", "!=", "=@", "!@", "=~", "!~", ">", ">=", "<", "<="]

# GSC operators
GSC_OPERATORS = ["contains", "notContains", "equals", "notEquals", "includingRegex", "excludingRegex"]


def parse_ga4_filter_to_df(filter_str: str) -> pd.DataFrame:
    """Parse GA4 filter string into a DataFrame."""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=FILTER_COLUMNS)

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
                rows.append({COL_FIELD: field, COL_OPERATOR: op, COL_VALUE: value})
                break

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=FILTER_COLUMNS)


def serialize_ga4_filter_from_df(df: pd.DataFrame) -> str:
    """Serialize DataFrame back to GA4 filter string."""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row[COL_FIELD] and row[COL_OPERATOR] and row[COL_VALUE]:
            parts.append(f"{row[COL_FIELD]}{row[COL_OPERATOR]}{row[COL_VALUE]}")
    return ";".join(parts)


def parse_gsc_filter_to_df(filter_str: str) -> pd.DataFrame:
    """Parse GSC filter string into a DataFrame."""
    if not filter_str or not filter_str.strip():
        return pd.DataFrame(columns=FILTER_COLUMNS)

    rows = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) == 3:
            rows.append({COL_FIELD: parts[0], COL_OPERATOR: parts[1], COL_VALUE: parts[2]})

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=FILTER_COLUMNS)


def serialize_gsc_filter_from_df(df: pd.DataFrame) -> str:
    """Serialize DataFrame back to GSC filter string."""
    if df is None or df.empty:
        return ""
    parts = []
    for _, row in df.iterrows():
        if row[COL_FIELD] and row[COL_OPERATOR] and row[COL_VALUE]:
            parts.append(f"{row[COL_FIELD]}:{row[COL_OPERATOR]}:{row[COL_VALUE]}")
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
