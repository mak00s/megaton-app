"""JSON-file cache for scraped data with per-entry TTL.

Designed for ``actions/cache`` persistence across CI runs. Falls back to an
empty cache on missing/corrupt files. Each entry carries a ``fetched_at`` ISO
string used for TTL checks (timestamps default to JST).

The cache is a single dict keyed by caller-chosen strings; values are
open-ended dicts so callers can stash whatever they need.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from megaton_lib.tz_utils import JST

logger = logging.getLogger(__name__)


def load_cache(path: str | os.PathLike[str]) -> dict[str, dict]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            logger.warning("cache at %s is not a dict; ignoring", p)
            return {}
        return data
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("cache load failed (%s); starting fresh: %s", p, exc)
        return {}


def save_cache(path: str | os.PathLike[str], cache: dict[str, dict]) -> None:
    """Write atomically (tmp then rename) so an interrupted run never corrupts."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False, indent=0, default=str)
    tmp.replace(p)


def is_fresh(
    entry: dict | None,
    ttl_hours: float,
    *,
    now: datetime | None = None,
    field: str = "fetched_at",
) -> bool:
    """Whether ``entry[field]`` is within ``ttl_hours``.

    ``field`` lets a single entry track multiple ages — e.g. ``fetched_at`` for
    fast-moving data and ``fundamentals_fetched_at`` for slow data. Falls back
    to ``fetched_at`` when the requested field is absent.
    """
    if not entry:
        return False
    ts = entry.get(field) or entry.get("fetched_at")
    if not ts:
        return False
    try:
        fetched_at = datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return False
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=JST)
    now_ = now or datetime.now(JST)
    return now_ - fetched_at <= timedelta(hours=ttl_hours)


def stamp(
    payload: dict[str, Any], *, now: datetime | None = None, extra: dict | None = None
) -> dict[str, Any]:
    """Wrap ``payload`` with a ``fetched_at`` timestamp (plus ``extra`` keys)."""
    now_ = now or datetime.now(JST)
    out = {"payload": payload, "fetched_at": now_.isoformat(timespec="seconds")}
    if extra:
        out.update(extra)
    return out
