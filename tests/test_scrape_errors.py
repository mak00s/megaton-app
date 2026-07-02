from __future__ import annotations

import pytest

from megaton_lib.scrape_errors import ScrapeError

pytestmark = pytest.mark.unit


def test_scrape_error_carries_kind():
    error = ScrapeError("blocked", kind="rejected")

    assert str(error) == "blocked"
    assert error.kind == "rejected"


def test_scrape_error_defaults_to_unknown():
    assert ScrapeError("failed").kind == "unknown"
