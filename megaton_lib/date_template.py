"""Date template resolver — delegates to ``megaton.dates`` (since megaton 1.5).

The vocabulary previously implemented here (calendar tokens such as
``prev-month-start``) was merged into megaton 1.5.0, together with the
GA-style tokens (``today`` / ``yesterday`` / ``NdaysAgo``) — so this module
is now a thin compatibility wrapper that keeps the historical
``DATE_TEMPLATE_TZ`` environment override working. New code should import
from :mod:`megaton_lib.dates` (or ``megaton.dates`` directly).
"""

from __future__ import annotations

import os
from datetime import date

from megaton.dates import resolve_date as _resolve_date
from megaton.dates import resolve_month as _resolve_month


def _tz() -> str | None:
    """DATE_TEMPLATE_TZ compat; None lets megaton resolve MEGATON_TZ -> Asia/Tokyo."""
    return (os.getenv("DATE_TEMPLATE_TZ") or "").strip() or None


def resolve_date(expr: str, *, reference: date | None = None) -> str:
    """Resolve a date template expression to YYYY-MM-DD (see megaton.dates)."""
    return _resolve_date(expr, reference=reference, tz=_tz())


def resolve_month(expr: str, *, reference: date | None = None) -> str:
    """Resolve a month expression to "YYYYMM" (see megaton.dates)."""
    return _resolve_month(expr, reference=reference, tz=_tz())


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
