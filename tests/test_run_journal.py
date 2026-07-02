from __future__ import annotations

import datetime as dt

import pytest

from megaton_lib.run_journal import (
    ensure_header,
    ensure_header_and_migrate,
    old_row_delete_count,
    trim_after_append,
    trim_rows_older_than,
)
from megaton_lib.tz_utils import JST

pytestmark = pytest.mark.unit


class FakeWorksheet:
    def __init__(self, rows=None, *, title: str = "journal", col_count: int = 3):
        self.title = title
        self.rows = rows or []
        self.col_count = col_count
        self.updated = []
        self.deleted = []
        self.resizes = []
        self.cleared = []

    def get_all_values(self):
        return self.rows

    def row_values(self, index):
        return self.rows[index - 1] if len(self.rows) >= index else []

    def update(self, *, values, range_name, value_input_option):
        self.updated.append((values, range_name, value_input_option))
        self.rows = values

    def resize(self, *, cols):
        self.resizes.append(cols)
        self.col_count = cols

    def batch_clear(self, ranges):
        self.cleared.extend(ranges)

    def col_values(self, index):
        idx = index - 1
        return [row[idx] for row in self.rows if len(row) > idx]

    def delete_rows(self, start, end):
        self.deleted.append((start, end))
        del self.rows[start - 1:end]


def test_ensure_header_and_migrate_converts_legacy_rows():
    ws = FakeWorksheet(
        [
            ["date", "status"],
            ["2026-07-01", "ok"],
        ],
        col_count=4,
    )

    changed = ensure_header_and_migrate(
        ws,
        header=["date", "status", "memo"],
        legacy_headers=[["date", "status"]],
        migrate_row=lambda row, _header: [row[0], row[1], ""],
        clear_extra_range="D1:D{row_count}",
    )

    assert changed is True
    assert ws.updated == [([["date", "status", "memo"], ["2026-07-01", "ok", ""]], "A1:C2", "RAW")]
    assert ws.cleared == ["D1:D2"]
    assert ws.resizes == [3]


def test_ensure_header_returns_false_for_unknown_header():
    ws = FakeWorksheet([["unexpected"], ["x"]])

    assert ensure_header_and_migrate(
        ws,
        header=["date", "status"],
        legacy_headers=[["old_date", "old_status"]],
        migrate_row=lambda row, _header: row,
    ) is False
    assert ws.updated == []


def test_ensure_header_updates_empty_or_mismatched_header():
    ws = FakeWorksheet([], col_count=1)

    ensure_header(ws, header=["date", "status"])

    assert ws.resizes == [2]
    assert ws.updated == [([["date", "status"]], "A1", "USER_ENTERED")]


def test_trim_after_append_deletes_old_rows_when_over_max():
    rows = [["h"], *[[str(i)] for i in range(1, 8)]]
    ws = FakeWorksheet(rows)

    trim_after_append(
        ws,
        {"updates": {"updatedRange": "journal!A8:A8"}},
        max_rows=5,
        keep_rows=3,
        enabled=True,
    )

    assert ws.deleted == [(2, 5)]
    assert ws.rows == [["h"], ["5"], ["6"], ["7"]]


def test_trim_after_append_skips_disabled_and_boundary_max():
    rows = [["h"], ["1"], ["2"], ["3"], ["4"]]
    ws = FakeWorksheet(rows)

    trim_after_append(ws, {"updates": {"updatedRange": "journal!A5:A5"}}, max_rows=5, keep_rows=3, enabled=True)
    trim_after_append(ws, {"updates": {"updatedRange": "journal!A6:A6"}}, max_rows=5, keep_rows=3, enabled=False)

    assert ws.deleted == []


def test_old_row_delete_count_stops_at_cutoff_or_blank():
    rows = [
        ["date"],
        ["2026-06-29 01:00"],
        ["2026-06-30 01:00"],
        ["2026-07-01 01:00"],
        ["2026-06-28 01:00"],
    ]

    assert old_row_delete_count(rows, "2026-07-01") == 2


def test_trim_rows_older_than_deletes_only_leading_old_rows():
    rows = [
        ["date", "message"],
        ["2026-06-23 01:00", "old"],
        ["2026-06-24 01:00", "keep-cutoff"],
        ["2026-06-22 01:00", "later-old-but-not-leading"],
    ]
    ws = FakeWorksheet(rows)

    trim_rows_older_than(
        ws,
        date_col=1,
        max_age_days=7,
        now=dt.datetime(2026, 7, 1, 12, 0, tzinfo=JST),
    )

    assert ws.deleted == [(2, 2)]
    assert ws.rows[1][1] == "keep-cutoff"


def test_trim_rows_older_than_handles_empty_sheet():
    ws = FakeWorksheet([])

    trim_rows_older_than(ws, date_col=1, max_age_days=7, now=dt.datetime(2026, 7, 1, tzinfo=JST))

    assert ws.deleted == []
