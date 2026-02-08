from __future__ import annotations

from datetime import date, datetime

import pytest

from megaton_lib.periods import parse_summary_tokens


def test_parse_relative_months_reference_datetime():
    ref = datetime(2026, 2, 8)
    assert parse_summary_tokens("0,1", reference=ref) == [
        ("202602", ["202602"]),
        ("202601", ["202601"]),
    ]


def test_parse_relative_months_reference_date():
    ref = date(2026, 2, 8)
    assert parse_summary_tokens("0", reference=ref) == [("202602", ["202602"])]


def test_parse_year():
    got = parse_summary_tokens("2025", reference=datetime(2026, 2, 8))
    assert got[0][0] == "2025"
    assert got[0][1][0] == "202501"
    assert got[0][1][-1] == "202512"
    assert len(got[0][1]) == 12


def test_parse_quarter_case_insensitive():
    assert parse_summary_tokens("2025q1", reference=datetime(2026, 2, 8)) == [
        ("2025Q1", ["202501", "202502", "202503"])
    ]


def test_parse_ignores_empty_tokens():
    assert parse_summary_tokens("0,,1,", reference=datetime(2026, 2, 8)) == [
        ("202602", ["202602"]),
        ("202601", ["202601"]),
    ]


def test_empty_string_raises():
    with pytest.raises(ValueError):
        parse_summary_tokens("")

