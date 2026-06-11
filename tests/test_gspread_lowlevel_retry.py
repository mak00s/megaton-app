"""Tests for retry behavior in gspread_lowlevel.call_with_retry."""
from unittest.mock import MagicMock

import pytest

from megaton_lib import gspread_lowlevel as gl


class _FakeAPIError(Exception):
    """Stand-in shaped like gspread APIError (has .response.status_code)."""

    def __init__(self, status):
        super().__init__(f"http {status}")
        self.response = MagicMock(status_code=status)


@pytest.fixture
def _patch_gspread_exc(monkeypatch):
    """Make call_with_retry's exception tuple catch our fake error."""
    import gspread

    monkeypatch.setattr(gspread.exceptions, "APIError", _FakeAPIError)
    return _FakeAPIError


def test_retries_on_429_with_quota_floor(_patch_gspread_exc):
    sleeps = []
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _FakeAPIError(429)
        return "ok"

    result = gl.call_with_retry("op", flaky, sleep=sleeps.append)
    assert result == "ok"
    assert calls["n"] == 3
    # each 429 retry adds a quota-floor extra sleep, then the backoff sleep
    assert sum(sleeps) >= 2 * gl._QUOTA_FLOOR_WAIT


def test_does_not_retry_on_4xx_other_than_429(_patch_gspread_exc):
    calls = {"n": 0}

    def denied():
        calls["n"] += 1
        raise _FakeAPIError(403)

    with pytest.raises(_FakeAPIError):
        gl.call_with_retry("op", denied, sleep=lambda *_: None)
    assert calls["n"] == 1


def test_retries_on_503_without_floor(_patch_gspread_exc):
    sleeps = []
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise _FakeAPIError(503)
        return "ok"

    assert gl.call_with_retry("op", flaky, sleep=sleeps.append) == "ok"
    assert sum(sleeps) < gl._QUOTA_FLOOR_WAIT


def test_raises_after_max_retries(_patch_gspread_exc):
    calls = {"n": 0}

    def always_quota():
        calls["n"] += 1
        raise _FakeAPIError(429)

    with pytest.raises(_FakeAPIError):
        gl.call_with_retry("op", always_quota, max_retries=3, sleep=lambda *_: None)
    assert calls["n"] == 3


def test_retries_requests_transport_errors():
    import requests

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise requests.exceptions.ConnectionError("reset")
        return "ok"

    assert gl.call_with_retry("op", flaky, sleep=lambda *_: None) == "ok"
    assert calls["n"] == 2
