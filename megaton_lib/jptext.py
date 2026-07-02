"""Number parsing for Japanese financial text.

Scrapers reading broker/bank pages and Sheets cells keep re-deriving the same
regexes ("1,234円", "年 0.35%", "¥20,000") and the same "strip currency
symbols then to_numeric" dance. This module is the single home for those
primitives so every pipeline parses amounts the same way.

pandas is imported lazily so regex-only callers don't pay for it.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

# First signed decimal number in a string, commas allowed: "1,234.5株" → "1,234.5".
NUM_RE = re.compile(r"[-+]?[\d,]*\.?\d+")

# Yen amount with the 円 suffix: "残高 1,234,567 円" → group(1) = "1,234,567".
MONEY_RE = re.compile(r"([\d,]+)\s*円")

# Annual interest rate: "年 0.35%" / "年0.35％" → group(1) = "0.35".
YEAR_RATE_RE = re.compile(r"年\s*([\d.]+)\s*[%％]")

# Amount and rate on one line (time-deposit rows): "1,000,000円 0.35%" →
# group(1) = amount, group(2) = rate.
AMT_RATE_RE = re.compile(r"([\d,]+)\s*円\s*([\d.]+)\s*[%％]")

# Currency symbols / separators / percent to strip before ``to_numeric``:
# covers cells read back from formatted Sheets columns ("¥20,000", "$2,720.39",
# "€66.94", "0.350%") and scraped text with full-width variants.
CURRENCY_STRIP_RE = re.compile(r"[¥￥$€£,%％\s]")


def parse_number(text: object) -> float | None:
    """Return the first number in ``text`` as a float, or None.

    The shared form of the per-adapter ``_NUM_RE.search`` + comma-strip
    helper: ``parse_number("1,234株") == 1234.0``, ``parse_number("--") is
    None``.
    """
    m = NUM_RE.search(str(text or ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def parse_money(text: object) -> float | None:
    """Return the first "N円" amount in ``text`` as a float, or None."""
    m = MONEY_RE.search(str(text or ""))
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def strip_currency(text: object) -> str:
    """Strip currency symbols, commas, percent signs, and whitespace."""
    return CURRENCY_STRIP_RE.sub("", str(text or ""))


def coerce_numeric(df: "pd.DataFrame", cols) -> "pd.DataFrame":
    """Force the given DataFrame columns numeric (text → number, else NaN).

    Sheets reads come back as strings; without this a re-written amount lands
    as a leading-apostrophe text value instead of a real number. Currency
    symbols/commas/% are stripped first so formatted cells ("¥20,000",
    "$2,720.39", "0.350%") don't silently NaN out on the round-trip.
    """
    import pandas as pd

    if df is None or df.empty:
        return df
    out = df.copy()
    for c in cols:
        if c in out.columns:
            cleaned = out[c].astype(str).str.replace(CURRENCY_STRIP_RE, "", regex=True)
            out[c] = pd.to_numeric(cleaned, errors="coerce")
    return out
