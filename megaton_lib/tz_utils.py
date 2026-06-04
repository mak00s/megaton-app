"""Shared timezone resolution for date helpers.

Stdlib-only on purpose: both ``date_template`` (lightweight, pulled in by
params validation) and ``date_utils`` (which also imports pandas) depend on
this, so it must not add heavy imports to the lighter module's chain.
"""

from __future__ import annotations

from zoneinfo import ZoneInfo

DEFAULT_TZ = "Asia/Tokyo"


def resolve_timezone(name: str | None = None) -> ZoneInfo:
    """Return a ``ZoneInfo`` for ``name``, falling back to ``Asia/Tokyo``.

    An empty/blank/None name, or an unknown timezone, resolves to the
    default rather than raising — callers treat timezone config as a soft
    preference, not a hard requirement.
    """
    candidate = (str(name).strip() if name is not None else "") or DEFAULT_TZ
    try:
        return ZoneInfo(candidate)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)
