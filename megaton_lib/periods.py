"""Period parsing helpers.

Small utilities for notebook parameters that describe month windows/summaries.
"""

from __future__ import annotations

import re
from datetime import date, datetime

from dateutil.relativedelta import relativedelta


_QUARTER_MONTHS: dict[str, list[str]] = {
    "Q1": ["01", "02", "03"],
    "Q2": ["04", "05", "06"],
    "Q3": ["07", "08", "09"],
    "Q4": ["10", "11", "12"],
}


def parse_summary_tokens(
    tokens_str: str,
    *,
    reference: date | datetime | None = None,
) -> list[tuple[str, list[str]]]:
    """Parse a comma-separated token string into (sheet_name, [yyyymm...]) entries.

    Supported token formats:
    - N: single month N months ago relative to reference ("0" = this month)
    - this-year: current year based on reference ("2026" => 202601..202612)
    - YYYY: whole year ("2025" => 202501..202512)
    - YYYYQn: quarter ("2025Q1" => 202501..202503)
    """
    if tokens_str is None:
        raise ValueError("tokens_str must be a non-empty string")

    tokens_str = str(tokens_str).strip()
    if not tokens_str:
        raise ValueError("tokens_str must be a non-empty string")

    if reference is None:
        ref_dt = datetime.now()
    elif isinstance(reference, datetime):
        ref_dt = reference
    else:
        ref_dt = datetime(reference.year, reference.month, reference.day)

    result: list[tuple[str, list[str]]] = []
    for token in tokens_str.split(","):
        token = token.strip()
        if not token:
            continue

        m_q = re.match(r"^(\d{4})(Q[1-4])$", token, re.IGNORECASE)
        if m_q:
            year, q = m_q.group(1), m_q.group(2).upper()
            months = [f"{year}{mm}" for mm in _QUARTER_MONTHS[q]]
            result.append((f"{year}{q}", months))
            continue

        if re.match(r"^\d{4}$", token):
            months = [f"{token}{mm:02d}" for mm in range(1, 13)]
            result.append((token, months))
            continue

        if token.lower() == "this-year":
            year = ref_dt.strftime("%Y")
            months = [f"{year}{mm:02d}" for mm in range(1, 13)]
            result.append((year, months))
            continue

        # Relative month token
        n = int(token)
        dt = ref_dt - relativedelta(months=n)
        ym = dt.strftime("%Y%m")
        result.append((ym, [ym]))

    return result
