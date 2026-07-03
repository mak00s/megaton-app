"""Single entry point for all date/period helpers.

Notebooks and scripts should import dates ONLY from this module::

    from megaton_lib.dates import resolve_date, previous_month_window, now_in_tz

Since megaton 1.5.0 the generic vocabulary lives in ``megaton.dates`` and is
re-exported here; this module keeps only the app-specific pieces
(``resolve_effective_months_ago`` for GHA scheduling, ``parse_summary_tokens``
for the report-summary DSL, params handling, and the DATE_TEMPLATE_TZ compat
wrappers).

String API (templates -> "YYYY-MM-DD" / "YYYYMM"):
    resolve_date("prev-month-start"), resolve_month("prev-prev-month")

Date-object API (tuples of ``datetime.date``):
    today_in_timezone(), previous_month_window(), month_before_window(),
    resolve_period_date(), resolve_period_month(), previous_month_label()

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

# App wrappers (honor DATE_TEMPLATE_TZ) around the megaton 1.5 vocabulary.
from megaton_lib.date_template import (  # noqa: F401
    resolve_date,
    resolve_dates_in_params,
    resolve_month,
)

# Generic vocabulary promoted into megaton 1.5.0.
from megaton.dates import (  # noqa: F401
    drop_current_month_rows,
    month_before_window,
    month_ranges_between,
    month_ranges_for_year,
    month_start_months_ago,
    month_suffix_months_ago,
    months_between,
    now_in_tz,
    parse_year_month_series,
    previous_month_label,
    previous_month_range,
    previous_month_window,
    previous_year_start,
    resolve_period_date,
    resolve_period_month,
    select_recent_months,
    today_in_timezone,
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
    # ops helpers (app-specific)
    "resolve_effective_months_ago",
]


# ----------------------------------------------------------------------
# Ops helpers (app-specific: GHA scheduling logic, stays out of megaton)
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
