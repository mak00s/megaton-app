"""DEI Lab GA4 helpers.

Functions specific to the DEI Lab report that are reusable across notebooks.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


def classify_source_channel(row: dict[str, Any] | pd.Series) -> tuple[str, str]:
    """Classify a traffic source into (source, channel).

    Returns a (source, channel) tuple.  Used to reclassify GA4's default
    channel grouping for AI chatbots, internal access, social networks, etc.

    Args:
        row: dict-like with at least ``channel`` and ``source`` keys.
              ``medium`` is optional (used by some callers).

    Returns:
        (source, channel) tuple.
    """
    ch = str(row.get("channel", ""))
    src = str(row.get("source", "")).lower().replace("www.", "")

    # --- AI channels ---
    ai_patterns: dict[str, str] = {
        "ChatGPT": r"(chatgpt|chat\.openai\.com)",
        "Copilot": r"(copilot|bing\.com|microsoftcopilot)",
        "Gemini": r"(gemini|bard|aistudio\.google\.com|makersuite\.google\.com)",
        "Claude": r"(claude|anthropic\.com)",
        "Perplexity": r"(perplexity|pplx\.ai)",
    }
    med = str(row.get("medium", "")).lower()
    for ai_name, pattern in ai_patterns.items():
        if re.search(pattern, src) or re.search(pattern, med):
            return ai_name, "AI"

    # --- Internal access ---
    if re.search(
        r"(extra\.shiseido\.co\.jp|(spark|international|intra)\.shiseido\.co\.jp"
        r"|office\.net|sharepoint|teams|basement\.jp|yammer)",
        src,
    ):
        return src, "Shiseido Internal"

    # --- Organic Search ---
    if re.search(
        r"(service\.smt\.docomo\.ne\.jp|search|jp\.hao123\.com|\.jword\.jp)", src
    ):
        return src, "Organic Search"

    # --- Social ---
    if re.search(r"(t\.co|twitter)", src):
        return "Twitter", "Organic Social"
    if "instagram" in src:
        return "Instagram", "Organic Social"
    if "facebook" in src:
        return "Facebook", "Organic Social"
    if "threads" in src:
        return "Threads", "Organic Social"
    if "tiktok" in src:
        return "TikTok", "Organic Social"

    # --- Referral → Organic Search reclassification ---
    if ch == "Referral" and "search" in src:
        return src, "Organic Search"

    # fallback
    return row.get("source", ""), ch


def ensure_trailing_slash(path: str) -> str:
    """Append ``/`` to *path* unless it already ends with ``/`` or ``.html``.

    >>> ensure_trailing_slash("/deilab/page1")
    '/deilab/page1/'
    >>> ensure_trailing_slash("/deilab/page1.html")
    '/deilab/page1.html'
    >>> ensure_trailing_slash("/deilab/page1/")
    '/deilab/page1/'
    """
    if path.endswith(("/", ".html")):
        return path
    return path + "/"


def build_page_metrics(
    dfs: list[tuple[str, pd.DataFrame]],
    *,
    merge_on: str | list[str] = "page",
    int_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Merge multiple metric DataFrames by left-joining on a common key.

    Args:
        dfs: list of (label, DataFrame) pairs.  First is the base.
        merge_on: column(s) to join on.
        int_cols: columns to coerce to int after merge (fillna 0).

    Returns:
        Merged DataFrame.
    """
    if not dfs:
        return pd.DataFrame()

    _, base = dfs[0]
    result = base.copy()

    for _label, df in dfs[1:]:
        if df is None or df.empty:
            continue
        result = result.merge(df, on=merge_on, how="left")

    if int_cols:
        for col in int_cols:
            if col in result.columns:
                result[col] = (
                    pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)
                )

    return result
