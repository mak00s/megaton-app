"""Traffic/source channel classification helpers (row-level, business rules).

Generic primitives (``normalize_domain``, ``source_host``,
``is_non_public_dev_source``, ``ensure_trailing_slash``,
``apply_source_normalization``) moved to ``megaton.transform.traffic`` and are
re-exported here for compatibility. For DataFrame-level channel
classification with custom channels, prefer
``megaton.transform.classify_source_channel`` / ``classify_channel``.
"""

from __future__ import annotations

import re
from typing import Mapping, Sequence

import pandas as pd

from megaton.transform.traffic import (  # noqa: F401
    apply_source_normalization,
    ensure_trailing_slash,
    is_non_public_dev_source,
    normalize_domain,
    source_host,
)

# Backward-compatible private alias (pre-promotion name).
_source_host = source_host


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

    if is_non_public_dev_source(src):
        return "Direct"
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
    internal_label: str = "Internal",
    organic_search_pattern: str | None = None,
    social_patterns: Mapping[str, str] | None = None,
    referral_search_pattern: str | None = None,
) -> tuple[str, str]:
    """Reclassify source+channel together with configurable regex rules.

    ``internal_pattern`` / ``internal_label`` classify intranet/corp hosts.
    Both default to generic values; pass an org-specific ``internal_pattern``
    (e.g. your intranet domains) and ``internal_label`` to tag them — keep
    company-specific host lists in the calling repo, not here.
    """
    ai_patterns = ai_patterns or {
        "ChatGPT": r"(chatgpt|chat\.openai\.com)",
        "Copilot": r"(copilot|bing\.com|microsoftcopilot)",
        "Gemini": r"(gemini|bard|aistudio\.google\.com|makersuite\.google\.com)",
        "Claude": r"(claude|anthropic\.com)",
        "Perplexity": r"(perplexity|pplx\.ai)",
    }
    internal_pattern = internal_pattern or r"(office\.net|sharepoint|teams|yammer)"
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
        return src, internal_label

    if re.search(organic_search_pattern, src):
        return src, "Organic Search"

    for social_name, pattern in social_patterns.items():
        if re.search(pattern, src):
            return social_name, "Organic Social"

    if ch == "Referral" and re.search(referral_search_pattern, src):
        return src, "Organic Search"

    return src_raw, ch
