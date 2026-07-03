"""Month-range utilities — re-exported from ``megaton.dates`` (since megaton 1.5).

Implementations were promoted into the megaton package so all repos share
one date stack. This module remains as a compatibility import surface.
New code should import from :mod:`megaton_lib.dates`.
"""

from __future__ import annotations

from megaton.dates import (  # noqa: F401
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

__all__ = [
    "month_ranges_for_year",
    "month_ranges_between",
    "months_between",
    "now_in_tz",
    "previous_month_range",
    "month_start_months_ago",
    "previous_year_start",
    "month_suffix_months_ago",
    "parse_year_month_series",
    "drop_current_month_rows",
    "select_recent_months",
]
