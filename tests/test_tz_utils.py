from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from megaton_lib.tz_utils import DEFAULT_TZ, JST, resolve_timezone

pytestmark = pytest.mark.unit


def test_jst_is_unnamed_fixed_plus_9_offset():
    # Consumers historically defined timezone(timedelta(hours=9)) inline;
    # the shared constant must compare equal and format identically.
    assert JST == timezone(timedelta(hours=9))
    assert datetime(2026, 7, 2, 12, 0, tzinfo=JST).isoformat() == "2026-07-02T12:00:00+09:00"


def test_resolve_timezone_defaults_and_falls_back():
    assert resolve_timezone(None) == ZoneInfo(DEFAULT_TZ)
    assert resolve_timezone("  ") == ZoneInfo(DEFAULT_TZ)
    assert resolve_timezone("No/Such_Zone") == ZoneInfo(DEFAULT_TZ)
    assert resolve_timezone("UTC") == ZoneInfo("UTC")
