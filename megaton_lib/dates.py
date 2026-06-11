"""Single entry point for all date/period helpers.

Notebooks and scripts should import dates ONLY from this module::

    from megaton_lib.dates import resolve_date, previous_month_window, now_in_tz

Implementation modules (``date_template``, ``date_utils``, ``periods``,
``tz_utils``) stay importable, but new code should not import them directly.

String API (templates -> "YYYY-MM-DD" / "YYYYMM"):
    resolve_date("prev-month-start"), resolve_month("prev-prev-month")

Date-object API (tuples of ``datetime.date``):
    today_in_timezone(), previous_month_window(), month_before_window(),
    resolve_period_date(), previous_month_label()

Month-range / DataFrame helpers (pandas):
    month_ranges_for_year, month_ranges_between, months_between,
    previous_month_range, month_start_months_ago, previous_year_start,
    month_suffix_months_ago, parse_year_month_series,
    drop_current_month_rows, select_recent_months

Summary tokens ("3", "this-year", "2024", "2024Q3" -> yyyymm list):
    parse_summary_tokens
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from megaton_lib.date_template import (  # noqa: F401
    resolve_date,
    resolve_dates_in_params,
    resolve_month,
)
from megaton_lib.date_utils import (  # noqa: F401
    drop_current_month_rows,
    month_ranges_between,
    month_ranges_for_year,
    month_start_months_ago,
    month_suffix_months_ago,
    months_between,
    now_in_tz,
    parse_year_month_series,
    previous_month_range,
    previous_year_start,
    select_recent_months,
)
from megaton_lib.periods import parse_summary_tokens  # noqa: F401
from megaton_lib.tz_utils import resolve_timezone  # noqa: F401

__all__ = [
    # string API
    "resolve_date",
    "resolve_month",
    "resolve_dates_in_params",
    # date-object API
    "today_in_timezone",
    "previous_month_window",
    "month_before_window",
    "resolve_period_date",
    "resolve_period_month",
    "previous_month_label",
    # month-range / DataFrame helpers
    "month_ranges_for_year",
    "month_ranges_between",
    "months_between",
    "previous_month_range",
    "month_start_months_ago",
    "previous_year_start",
    "month_suffix_months_ago",
    "parse_year_month_series",
    "drop_current_month_rows",
    "select_recent_months",
    "now_in_tz",
    # summary tokens
    "parse_summary_tokens",
    # tz
    "resolve_timezone",
    # ops helpers
    "resolve_effective_months_ago",
]


# ----------------------------------------------------------------------
# Date-object API (migrated from megaton-notebooks lib/date_periods.py)
# ----------------------------------------------------------------------

def today_in_timezone(timezone: str = "Asia/Tokyo", *, now: datetime | None = None) -> date:
    """Return today's date in the given timezone."""
    current = now or datetime.now(ZoneInfo(timezone))
    return current.astimezone(ZoneInfo(timezone)).date()


def previous_month_window(today: date | None = None, *, timezone: str = "Asia/Tokyo") -> tuple[date, date]:
    """Return (first day, last day) of the previous month as date objects."""
    base = today or today_in_timezone(timezone)
    first_this_month = base.replace(day=1)
    end = first_this_month - timedelta(days=1)
    return end.replace(day=1), end


def month_before_window(month_start: date) -> tuple[date, date]:
    """Return (first day, last day) of the month before *month_start*."""
    end = month_start - timedelta(days=1)
    return end.replace(day=1), end


def resolve_period_date(value: str, *, today: date | None = None, timezone: str = "Asia/Tokyo") -> date:
    """Resolve a period token (or ISO date) to a ``datetime.date``.

    Date-object counterpart of :func:`resolve_date`. Supports
    prev-month-start/end, prev-prev-month-start/end, and YYYY-MM-DD.
    """
    token = str(value).strip()
    if token in {"prev-month-start", "prev-month-end",
                 "prev-prev-month-start", "prev-prev-month-end"}:
        reference = today or today_in_timezone(timezone)
        return date.fromisoformat(resolve_date(token, reference=reference))
    return date.fromisoformat(token)


def previous_month_label(today: date | None = None, *, timezone: str = "Asia/Tokyo") -> str:
    """Return 'YYYY/MM/DD - YYYY/MM/DD' label for the previous month."""
    start, end = previous_month_window(today, timezone=timezone)
    return f"{start:%Y/%m/%d} - {end:%Y/%m/%d}"


def resolve_period_month(value: str, *, today: date | None = None, timezone: str = "Asia/Tokyo") -> str:
    """Resolve a month token to "YYYYMM" (date-object-API counterpart of resolve_month)."""
    reference = today or today_in_timezone(timezone)
    return resolve_month(value, reference=reference)


# ----------------------------------------------------------------------
# Ops helpers (promoted from megaton-notebooks lib/notebook_paths.py)
# ----------------------------------------------------------------------

def resolve_effective_months_ago(
    target_months_ago: int,
    *,
    switch_day: int,
    tz: str = "Asia/Tokyo",
) -> int:
    """Return *effective* months-ago, adjusting for early-month scheduled runs.

    When running inside GitHub Actions with ``target_months_ago == 0``,
    switch to the previous month (1) if today's day-of-month is before
    *switch_day*. Outside GitHub Actions the value is returned unchanged.
    """
    effective = int(target_months_ago)
    is_gha = os.environ.get("GITHUB_ACTIONS", "").strip().lower() == "true"
    if is_gha and effective == 0:
        today = now_in_tz(tz)
        effective = 0 if int(today.day) >= int(switch_day) else 1
    return effective
