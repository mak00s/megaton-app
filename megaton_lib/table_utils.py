"""DataFrame text/pattern mapping helpers."""

from __future__ import annotations

import re

import pandas as pd


def apply_pattern_map(
    df: pd.DataFrame,
    column: str,
    mapping: dict[str, str],
    *,
    output_col: str | None = None,
    default_unmatched: str | None = None,
) -> pd.DataFrame:
    """Apply regex mapping to ``column``.

    Args:
        df: Source DataFrame.
        column: Input column to match against patterns.
        mapping: ``{pattern: mapped_value}`` dict.
        output_col: Output column name. Defaults to ``column``.
        default_unmatched: Value used when no pattern matches.
            ``None`` keeps the original value.
    """
    if df.empty or not mapping or column not in df.columns:
        return df

    def map_value(value: object) -> str:
        text = str(value or "")
        for pattern, mapped in mapping.items():
            try:
                if re.search(pattern, text):
                    return str(mapped)
            except re.error as exc:
                print(f"[warn] invalid regex pattern in map: {pattern} ({exc})")
        if default_unmatched is None:
            return text
        return str(default_unmatched)

    out = df.copy()
    target_col = output_col or column
    out[target_col] = out[column].astype(str).apply(map_value)
    return out


def classify_by_pattern_map(
    df: pd.DataFrame,
    mapping: dict[str, str],
    *,
    source_col: str,
    output_col: str = "category",
    default_label: str = "other",
) -> pd.DataFrame:
    """Classify rows by regex map with explicit fallback label."""
    out = apply_pattern_map(
        df,
        source_col,
        mapping,
        output_col=output_col,
        default_unmatched=default_label,
    )
    if output_col not in out.columns:
        out = out.copy()
        out[output_col] = default_label
    return out
