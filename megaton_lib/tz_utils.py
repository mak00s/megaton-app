"""Shared timezone resolution for date helpers.

Stdlib-only on purpose: both ``date_template`` (lightweight, pulled in by
params validation) and ``date_utils`` (which also imports pandas) depend on
this, so it must not add heavy imports to the lighter module's chain.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

DEFAULT_TZ = "Asia/Tokyo"

# Fixed +09:00 offset for scrapers/pipelines that stamp and compare JST
# datetimes. A fixed offset (not ZoneInfo) keeps equality/pickling trivial and
# is exactly what consumer repos historically defined inline (unnamed, so
# tzname()/%Z output stays identical for them); Japan has no DST so ZoneInfo
# and this offset are interchangeable in practice.
JST = timezone(timedelta(hours=9))


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
