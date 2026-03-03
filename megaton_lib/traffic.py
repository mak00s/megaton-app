"""Traffic/source normalization and channel classification helpers."""

from __future__ import annotations

import re
from typing import Mapping, Sequence

import pandas as pd


def normalize_domain(value: str) -> str:
    """Normalize domain text for grouping/compare."""
    v = str(value).strip().lower()
    v = re.sub(r"^https?://", "", v)
    v = v.split("/")[0]
    return v.replace("www.", "")


def apply_source_normalization(
    df: pd.DataFrame,
    source_map: Mapping[str, str],
    *,
    source_col: str = "source",
) -> pd.DataFrame:
    """Normalize source column with regex map.

    Input source values are lowercased before matching.
    """
    if source_col not in df.columns:
        return df

    def normalize(value: object) -> str:
        src = str(value or "").lower().strip()
        for pattern, normalized in source_map.items():
            try:
                if re.search(str(pattern), src):
                    return str(normalized)
            except re.error as exc:
                print(f"[warn] invalid regex pattern in source_map: {pattern} ({exc})")
        return src

    out = df.copy()
    out[source_col] = out[source_col].apply(normalize)
    return out


def classify_channel(
    row: pd.Series | dict,
    *,
    group_domains: set[str] | None = None,
    ai_keywords: Sequence[str] | None = None,
    referral_search_keywords: Sequence[str] | None = None,
    referral_social_keywords: Sequence[str] | None = None,
) -> str:
    """Reclassify GA default channel using source/medium heuristics."""
    group_domains = group_domains or set()
    ai_keywords = ai_keywords or ["bard", "chatgpt", "claude", "copilot", "gemini", "perplexity"]
    referral_search_keywords = referral_search_keywords or ["search", "docomo.ne.jp", ".jword.jp", "jp.hao123.com"]
    referral_social_keywords = referral_social_keywords or ["threads.net", "threads"]

    ch = str(row.get("channel", "") or "")
    med = str(row.get("medium", "") or "").lower()
    src = str(row.get("source", "") or "").lower().replace("www.", "")

    if any(k in src for k in ai_keywords) or any(k in med for k in ai_keywords):
        return "AI"
    if med == "map" or re.search(r"(^|\.)maps?\.", src):
        return "Map"

    if ch == "Referral":
        if any(k in src for k in referral_search_keywords):
            return "Organic Search"
        if any(k in src for k in referral_social_keywords):
            return "Organic Social"
        if any(d in src for d in group_domains):
            return "Group"

    return ch
