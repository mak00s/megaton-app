"""Date template resolver.

Allows relative date expressions in ``params.json`` date_range.start/end.

Supported expressions:
  today                  -> execution date
  today-Nd               -> N days ago
  today+Nd               -> N days later
  month-start            -> first day of current month
  month-end              -> last day of current month
  year-start             -> Jan 1 of current year
  year-end               -> Dec 31 of current year
  prev-month-start       -> first day of previous month
  prev-month-end         -> last day of previous month
  prev-prev-month-start  -> first day of the month before previous
  prev-prev-month-end    -> last day of the month before previous
  week-start             -> Monday of current week (ISO: Monday=0)
  YYYY-MM-DD             -> pass through (absolute date)
  YYYYMMDD               -> normalized to YYYY-MM-DD

Month expressions (``resolve_month``):
  this-month / prev-month / prev-prev-month / YYYYMM -> "YYYYMM"
"""

from __future__ import annotations

import calendar
import os
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from megaton_lib.tz_utils import resolve_timezone


# today±Nd pattern
_RELATIVE_RE = re.compile(r"^today([+-])(\d+)d$")


def _resolve_timezone() -> ZoneInfo:
    """Resolve DATE_TEMPLATE_TZ (fallback to Asia/Tokyo on invalid value)."""
    return resolve_timezone(os.getenv("DATE_TEMPLATE_TZ"))


def _current_date_in_configured_tz() -> date:
    """Return current date in configured timezone."""
    return datetime.now(_resolve_timezone()).date()


def resolve_date(expr: str, *, reference: date | None = None) -> str:
    """Resolve a date template expression to YYYY-MM-DD.

    Args:
        expr: Date expression (e.g. "today-7d", "prev-month-start", "2026-01-01").
        reference: Reference date (default: execution date).

    Returns:
        Date string in "YYYY-MM-DD" format.

    Raises:
        ValueError: Unknown date expression.
    """
    ref = reference or _current_date_in_configured_tz()
    expr = expr.strip()

    # Absolute dates (YYYY-MM-DD / YYYYMMDD): validate and return.
    if re.match(r"^\d{4}-\d{2}-\d{2}$", expr):
        try:
            datetime.strptime(expr, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid absolute date: '{expr}'") from e
        return expr

    if re.match(r"^\d{8}$", expr):
        try:
            dt = datetime.strptime(expr, "%Y%m%d")
        except ValueError as e:
            raise ValueError(f"Invalid absolute date: '{expr}'") from e
        return dt.strftime("%Y-%m-%d")

    if expr == "today":
        return ref.isoformat()

    m = _RELATIVE_RE.match(expr)
    if m:
        sign, days = m.group(1), int(m.group(2))
        delta = timedelta(days=days if sign == "+" else -days)
        return (ref + delta).isoformat()

    if expr == "month-start":
        return ref.replace(day=1).isoformat()

    if expr == "month-end":
        last_day = calendar.monthrange(ref.year, ref.month)[1]
        return ref.replace(day=last_day).isoformat()

    if expr == "year-start":
        return ref.replace(month=1, day=1).isoformat()

    if expr == "year-end":
        return ref.replace(month=12, day=31).isoformat()

    if expr == "prev-month-start":
        first = ref.replace(day=1)
        prev = first - timedelta(days=1)
        return prev.replace(day=1).isoformat()

    if expr == "prev-month-end":
        first = ref.replace(day=1)
        return (first - timedelta(days=1)).isoformat()

    if expr == "prev-prev-month-start":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        return (prev_first - timedelta(days=1)).replace(day=1).isoformat()

    if expr == "prev-prev-month-end":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        return (prev_first - timedelta(days=1)).isoformat()

    if expr == "week-start":
        # ISO: Monday = 0
        return (ref - timedelta(days=ref.weekday())).isoformat()

    raise ValueError(
        f"Unknown date template: '{expr}'. "
        "Use today, today±Nd, month-start, month-end, year-start, year-end, "
        "prev-month-start, prev-month-end, prev-prev-month-start, "
        "prev-prev-month-end, week-start, or YYYY-MM-DD."
    )


def resolve_month(expr: str, *, reference: date | None = None) -> str:
    """Resolve a month expression to "YYYYMM".

    Args:
        expr: "this-month", "prev-month", "prev-prev-month", or "YYYYMM".
        reference: Reference date (default: execution date).

    Raises:
        ValueError: Unknown month expression.
    """
    ref = reference or _current_date_in_configured_tz()
    token = str(expr).strip()

    if token == "this-month":
        return f"{ref:%Y%m}"
    if token == "prev-month":
        prev_end = ref.replace(day=1) - timedelta(days=1)
        return f"{prev_end:%Y%m}"
    if token == "prev-prev-month":
        prev_first = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
        prev_prev_end = prev_first - timedelta(days=1)
        return f"{prev_prev_end:%Y%m}"
    if len(token) == 6 and token.isdigit():
        return token
    raise ValueError(
        f"Unknown month expression: '{expr}'. "
        "Use this-month, prev-month, prev-prev-month, or YYYYMM."
    )


def resolve_dates_in_params(params: dict) -> dict:
    """Resolve template dates in ``params['date_range'].start/end``.

    Returns a new dict without mutating the original.
    If date_range is absent (e.g. bigquery), returns params as-is.
    """
    date_range = params.get("date_range")
    if not date_range:
        return params

    start = date_range.get("start", "")
    end = date_range.get("end", "")

    resolved_start = resolve_date(start)
    resolved_end = resolve_date(end)

    if resolved_start == start and resolved_end == end:
        return params  # unchanged

    new_params = dict(params)
    new_params["date_range"] = {
        "start": resolved_start,
        "end": resolved_end,
    }
    return new_params
