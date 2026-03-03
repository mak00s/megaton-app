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


def ensure_trailing_slash(path: str, *, preserve_suffixes: tuple[str, ...] = (".html", "/")) -> str:
    """Append ``/`` unless path already ends with known suffixes.

    Args:
        path: URL path text.
        preserve_suffixes: suffixes considered already-normalized.
    """
    text = str(path or "")
    if text.endswith(preserve_suffixes):
        return text
    return text + "/"


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


def reclassify_source_channel(
    row: pd.Series | dict,
    *,
    channel_col: str = "channel",
    source_col: str = "source",
    medium_col: str = "medium",
    ai_patterns: Mapping[str, str] | None = None,
    internal_pattern: str | None = None,
    organic_search_pattern: str | None = None,
    social_patterns: Mapping[str, str] | None = None,
    referral_search_pattern: str | None = None,
) -> tuple[str, str]:
    """Reclassify source+channel together with configurable regex rules."""
    ai_patterns = ai_patterns or {
        "ChatGPT": r"(chatgpt|chat\.openai\.com)",
        "Copilot": r"(copilot|bing\.com|microsoftcopilot)",
        "Gemini": r"(gemini|bard|aistudio\.google\.com|makersuite\.google\.com)",
        "Claude": r"(claude|anthropic\.com)",
        "Perplexity": r"(perplexity|pplx\.ai)",
    }
    internal_pattern = internal_pattern or (
        r"(extra\.shiseido\.co\.jp|(spark|international|intra)\.shiseido\.co\.jp"
        r"|office\.net|sharepoint|teams|basement\.jp|yammer)"
    )
    organic_search_pattern = organic_search_pattern or r"(service\.smt\.docomo\.ne\.jp|search|jp\.hao123\.com|\.jword\.jp)"
    social_patterns = social_patterns or {
        "Twitter": r"(t\.co|twitter)",
        "Instagram": r"instagram",
        "Facebook": r"facebook",
        "Threads": r"threads",
        "TikTok": r"tiktok",
    }
    referral_search_pattern = referral_search_pattern or r"search"

    ch = str(row.get(channel_col, "") or "")
    src_raw = str(row.get(source_col, "") or "")
    src = src_raw.lower().replace("www.", "")
    med = str(row.get(medium_col, "") or "").lower()

    for ai_name, pattern in ai_patterns.items():
        if re.search(pattern, src) or re.search(pattern, med):
            return ai_name, "AI"

    if re.search(internal_pattern, src):
        return src, "Shiseido Internal"

    if re.search(organic_search_pattern, src):
        return src, "Organic Search"

    for social_name, pattern in social_patterns.items():
        if re.search(pattern, src):
            return social_name, "Organic Social"

    if ch == "Referral" and re.search(referral_search_pattern, src):
        return src, "Organic Search"

    return src_raw, ch
