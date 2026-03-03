"""Month-range utilities for batch processing and backfill."""

from __future__ import annotations

import calendar
import datetime as dt
from zoneinfo import ZoneInfo

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


def _resolve_tz(tz: str) -> ZoneInfo:
    """Resolve timezone name with Asia/Tokyo fallback."""
    try:
        return ZoneInfo(str(tz).strip() or "Asia/Tokyo")
    except Exception:
        return ZoneInfo("Asia/Tokyo")


def _as_datetime(value: dt.datetime | dt.date | None, *, tz: str) -> dt.datetime:
    """Normalize date/datetime into timezone-aware datetime."""
    if value is None:
        return dt.datetime.now(_resolve_tz(tz))
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=_resolve_tz(tz))
        return value.astimezone(_resolve_tz(tz))
    return dt.datetime.combine(value, dt.time.min, tzinfo=_resolve_tz(tz))


def _add_months(base: dt.datetime, months_delta: int) -> dt.datetime:
    """Return month-shifted datetime while keeping day clipped to month end."""
    month_index = base.year * 12 + (base.month - 1) + int(months_delta)
    year = month_index // 12
    month = (month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(base.day, last_day)
    return base.replace(year=year, month=month, day=day)


def now_in_tz(tz: str = "Asia/Tokyo") -> dt.datetime:
    """Return current timezone-aware datetime."""
    return dt.datetime.now(_resolve_tz(tz))


def previous_month_range(
    *,
    reference: dt.datetime | dt.date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> tuple[str, str]:
    """Return previous month start/end as formatted strings."""
    ref = _as_datetime(reference, tz=tz)
    first_this_month = ref.replace(day=1)
    prev_month_end = first_this_month - dt.timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    return prev_month_start.strftime(out_fmt), prev_month_end.strftime(out_fmt)


def month_start_months_ago(
    months_ago: int,
    *,
    reference: dt.datetime | dt.date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> str:
    """Return month-start string for N months ago."""
    ref = _as_datetime(reference, tz=tz).replace(day=1)
    target = _add_months(ref, -int(months_ago))
    return target.strftime(out_fmt)


def previous_year_start(
    *,
    reference: dt.datetime | dt.date | None = None,
    tz: str = "Asia/Tokyo",
    out_fmt: str = "%Y-%m-%d",
) -> str:
    """Return Jan 1 of previous year as formatted string."""
    ref = _as_datetime(reference, tz=tz)
    return ref.replace(year=ref.year - 1, month=1, day=1).strftime(out_fmt)


def month_suffix_months_ago(
    months_ago: int,
    *,
    reference: dt.datetime | dt.date | None = None,
    tz: str = "Asia/Tokyo",
    fmt: str = "%Y.%m",
) -> str:
    """Return month suffix (e.g. ``YYYY.MM``) for N months ago."""
    ref = _as_datetime(reference, tz=tz)
    target = _add_months(ref, -int(months_ago))
    return target.strftime(fmt)


def parse_year_month_series(series: pd.Series) -> pd.Series:
    """Parse mixed month formats into month-start datetime.

    Accepted values include: ``202301``, ``202301.0``, ``2023-01``,
    ``2023/01``, ``2023年1月``.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series).dt.to_period("M").dt.to_timestamp()

    s = series.astype("string").str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)

    # Pick the first year(4d) + month(1-2d) pair from flexible formats such as:
    # 202301 / 20231 / 2023-01 / 2023/1 / 2023年1月 / 2023-01-15
    ym = s.str.extract(r"^\D*(\d{4})\D*?(\d{1,2})(?:\D|$)", expand=True)
    y = ym[0]
    m = ym[1]
    yyyymm = y.str.cat(m.str.zfill(2), na_rep="")
    yyyymm = yyyymm.where(y.notna() & m.notna(), pd.NA)
    return pd.to_datetime(yyyymm, format="%Y%m", errors="coerce")


def _to_month_start_series(series: pd.Series) -> pd.Series:
    """Normalize values to month-start ``Timestamp`` series."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()

    parsed = parse_year_month_series(series)
    if parsed.notna().any():
        return parsed
    return pd.to_datetime(series, errors="coerce").dt.to_period("M").dt.to_timestamp()


def drop_current_month_rows(
    df: pd.DataFrame,
    *,
    month_col: str,
    tz: str = "Asia/Tokyo",
) -> pd.DataFrame:
    """Drop rows that belong to current month in timezone."""
    month_series = _to_month_start_series(df[month_col])
    current_month = pd.Timestamp(now_in_tz(tz).date().replace(day=1))
    keep = month_series.isna() | (month_series != current_month)
    return df[keep].copy()


def select_recent_months(
    df: pd.DataFrame,
    *,
    month_col: str,
    months: int = 13,
) -> pd.DataFrame:
    """Filter DataFrame to recent N months from max(month_col)."""
    if df.empty:
        return df.copy()
    months = int(months)
    if months <= 0:
        return df.iloc[0:0].copy()

    month_series = _to_month_start_series(df[month_col])
    valid = month_series.dropna()
    if valid.empty:
        return df.iloc[0:0].copy()

    max_month = valid.max()
    start_month = (max_month - pd.DateOffset(months=months - 1)).replace(day=1)
    keep = month_series.notna() & (month_series >= start_month)
    return df[keep].copy()
