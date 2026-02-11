"""Month-range utilities for batch processing and backfill."""

from __future__ import annotations

import calendar
import datetime as dt

import pandas as pd


def month_ranges_for_year(year: int) -> list[tuple[str, str]]:
    """Return ``(start, end)`` ISO-date pairs for each month of *year*.

    >>> month_ranges_for_year(2025)[0]
    ('2025-01-01', '2025-01-31')
    """
    out: list[tuple[str, str]] = []
    for m in range(1, 13):
        last = calendar.monthrange(year, m)[1]
        out.append((dt.date(year, m, 1).isoformat(), dt.date(year, m, last).isoformat()))
    return out


def month_ranges_between(start: str, end: str) -> list[tuple[str, str]]:
    """Return ``(start, end)`` ISO-date pairs for each month in the range.

    Partial months are clamped to *start* / *end*.

    >>> month_ranges_between("2025-03-15", "2025-05-10")
    [('2025-03-15', '2025-03-31'), ('2025-04-01', '2025-04-30'), ('2025-05-01', '2025-05-10')]
    """
    sd = pd.to_datetime(start, errors="coerce")
    ed = pd.to_datetime(end, errors="coerce")
    if pd.isna(sd) or pd.isna(ed):
        return []
    sd_date = sd.date()
    ed_date = ed.date()
    out: list[tuple[str, str]] = []
    cur = dt.date(sd_date.year, sd_date.month, 1)
    end_first = dt.date(ed_date.year, ed_date.month, 1)
    while cur <= end_first:
        last = calendar.monthrange(cur.year, cur.month)[1]
        m_start = max(cur, sd_date)
        m_end = min(dt.date(cur.year, cur.month, last), ed_date)
        out.append((m_start.isoformat(), m_end.isoformat()))
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out


def months_between(start, end) -> list[str]:
    """Return ``yyyymm`` strings for each month between two dates.

    >>> months_between("2025-11-01", "2026-01-31")
    ['202511', '202512', '202601']
    """
    sd = pd.to_datetime(start, errors="coerce")
    ed = pd.to_datetime(end, errors="coerce")
    if pd.isna(sd) or pd.isna(ed):
        return []
    out: list[str] = []
    y, m = sd.year, sd.month
    while dt.date(y, m, 1) <= ed.date():
        out.append(f"{y:04d}{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out
