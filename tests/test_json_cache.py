from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from megaton_lib.json_cache import is_fresh, load_cache, save_cache, stamp
from megaton_lib.tz_utils import JST

pytestmark = pytest.mark.unit


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "sub" / "cache.json"  # parent dir auto-created
    save_cache(path, {"7203": {"fetched_at": "2026-07-01T10:00:00+09:00", "v": 1}})
    assert load_cache(path)["7203"]["v"] == 1


def test_load_missing_and_corrupt_files_fall_back_to_empty(tmp_path):
    assert load_cache(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    assert load_cache(bad) == {}
    non_dict = tmp_path / "list.json"
    non_dict.write_text("[1, 2]", encoding="utf-8")
    assert load_cache(non_dict) == {}


def test_save_is_atomic_leaves_no_tmp(tmp_path):
    path = tmp_path / "cache.json"
    save_cache(path, {"a": {}})
    assert not path.with_suffix(".json.tmp").exists()


def test_is_fresh_within_and_beyond_ttl():
    now = datetime(2026, 7, 2, 12, 0, tzinfo=JST)
    entry = stamp({"x": 1}, now=now - timedelta(hours=2))
    assert is_fresh(entry, ttl_hours=3, now=now) is True
    assert is_fresh(entry, ttl_hours=1, now=now) is False


def test_is_fresh_handles_missing_and_naive_timestamps():
    now = datetime(2026, 7, 2, 12, 0, tzinfo=JST)
    assert is_fresh(None, 1, now=now) is False
    assert is_fresh({}, 1, now=now) is False
    assert is_fresh({"fetched_at": "garbage"}, 1, now=now) is False
    # naive ISO string is interpreted as JST
    assert is_fresh({"fetched_at": "2026-07-02T11:30:00"}, 1, now=now) is True


def test_is_fresh_alt_field_falls_back_to_fetched_at():
    now = datetime(2026, 7, 2, 12, 0, tzinfo=JST)
    entry = {
        "fetched_at": (now - timedelta(hours=5)).isoformat(),
        "slow_fetched_at": (now - timedelta(hours=1)).isoformat(),
    }
    assert is_fresh(entry, 2, now=now, field="slow_fetched_at") is True
    assert is_fresh(entry, 2, now=now, field="absent") is False  # falls back to 5h-old


def test_stamp_wraps_payload_with_extra():
    now = datetime(2026, 7, 2, 12, 0, tzinfo=JST)
    out = stamp({"p": 1}, now=now, extra={"source": "yahoo"})
    assert out["payload"] == {"p": 1}
    assert out["fetched_at"] == "2026-07-02T12:00:00+09:00"
    assert out["source"] == "yahoo"
