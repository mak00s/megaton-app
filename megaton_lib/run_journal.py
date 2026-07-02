"""Generic worksheet journal maintenance helpers.

Consumer repos own their business schema, row construction, and migration
content. This module only provides the mechanics: ensure a worksheet header,
drive legacy row migration, trim old rows after appends, and remove rows older
than a date retention cutoff.
"""

from __future__ import annotations

import datetime as dt
import contextlib
import re
from collections.abc import Callable
from typing import Any

from megaton_lib.gspread_lowlevel import call_with_retry, column_label
from megaton_lib.tz_utils import JST


def ensure_header_and_migrate(
    ws,
    *,
    header: list[str],
    legacy_headers: list[list[str]],
    migrate_row: Callable[[list[str], list[str]], list[Any]],
    clear_extra_range: str | None = None,
    value_input_option: str = "RAW",
) -> bool:
    """Ensure ``header`` and migrate legacy rows when a known old header exists.

    Returns True when the worksheet is already on ``header`` or a known legacy
    header was migrated. Returns False for an unknown existing header so callers
    can decide whether to overwrite it.
    """
    rows = call_with_retry(f"{ws.title}.get_all_values", ws.get_all_values)
    if not rows:
        return False

    current_header = rows[0]
    if current_header[:len(header)] == header:
        migrated = [header]
        changed = False
        for row in rows[1:]:
            migrated_row = migrate_row(row, header)
            if migrated_row != [*row, *[""] * max(0, len(header) - len(row))][:len(header)]:
                changed = True
            migrated.append(migrated_row)
        if changed:
            _write_migrated(ws, migrated, header, rows, clear_extra_range, value_input_option)
        _shrink_extra_columns(ws, len(header))
        return True

    matched_header = next((legacy for legacy in legacy_headers if current_header[:len(legacy)] == legacy), None)
    if matched_header is None:
        return False

    migrated = [header]
    for row in rows[1:]:
        migrated.append(migrate_row(row, current_header))
    if ws.col_count < len(header):
        call_with_retry(f"{ws.title}.resize", lambda: ws.resize(cols=len(header)))
    _write_migrated(ws, migrated, header, rows, clear_extra_range, value_input_option)
    _shrink_extra_columns(ws, len(header))
    return True


def ensure_header(
    ws,
    *,
    header: list[str],
    value_input_option: str = "USER_ENTERED",
) -> None:
    if ws.col_count < len(header):
        call_with_retry(f"{ws.title}.resize", lambda: ws.resize(cols=len(header)))
    current_header = call_with_retry(f"{ws.title}.row_values", lambda: ws.row_values(1))
    if current_header[:len(header)] != header:
        call_with_retry(
            f"{ws.title}.update",
            lambda: ws.update(values=[header], range_name="A1", value_input_option=value_input_option),
        )


def trim_after_append(
    ws,
    append_response,
    *,
    max_rows: int,
    keep_rows: int,
    enabled: bool,
) -> None:
    """Trim old rows after append when enabled and the used row count exceeds max.

    The caller owns concurrency policy. This function intentionally has no
    environment-variable knowledge, locks, or business constants.
    """
    if not enabled:
        return
    try:
        rng = append_response["updates"]["updatedRange"]  # e.g. "log!A312:L312"
        last_row = int(re.search(r"(\d+)$", rng.split("!")[-1].split(":")[-1]).group(1))
    except Exception:
        return
    if last_row <= max_rows:
        return
    try:
        used = len(call_with_retry(f"{ws.title}.col_values", lambda: ws.col_values(1)))
    except Exception:
        return
    if used <= max_rows:
        return
    delete_end = used - keep_rows
    if delete_end < 2:
        return
    with contextlib.suppress(Exception):
        call_with_retry(f"{ws.title}.delete_rows", lambda: ws.delete_rows(2, delete_end))


def old_row_delete_count(rows: list, cutoff: str, *, date_col: int = 1) -> int:
    """Count contiguous leading data rows older than ``cutoff`` (YYYY-MM-DD)."""
    n = 0
    idx = date_col - 1
    for row in rows[1:]:
        day = (row[idx] or "")[:10] if len(row) > idx else ""
        if not day or day >= cutoff:
            break
        n += 1
    return n


def trim_rows_older_than(
    ws,
    *,
    date_col: int,
    max_age_days: int,
    now: dt.datetime | None = None,
) -> None:
    """Delete leading rows whose date column is older than ``max_age_days``."""
    base = (now or dt.datetime.now(JST)).astimezone(JST)
    cutoff = (base.date() - dt.timedelta(days=max_age_days)).isoformat()
    try:
        rows = call_with_retry(f"{ws.title}.get_all_values", ws.get_all_values)
    except Exception:
        return
    count = old_row_delete_count(rows, cutoff, date_col=date_col)
    if count > 0:
        with contextlib.suppress(Exception):
            call_with_retry(f"{ws.title}.delete_rows", lambda: ws.delete_rows(2, count + 1))


def _write_migrated(
    ws,
    migrated: list[list[Any]],
    header: list[str],
    rows: list[list[str]],
    clear_extra_range: str | None,
    value_input_option: str,
) -> None:
    end_col = column_label(len(header))
    call_with_retry(
        f"{ws.title}.update",
        lambda: ws.update(
            values=migrated,
            range_name=f"A1:{end_col}{len(migrated)}",
            value_input_option=value_input_option,
        ),
    )
    if clear_extra_range:
        with contextlib.suppress(Exception):
            call_with_retry(
                f"{ws.title}.batch_clear",
                lambda: ws.batch_clear([clear_extra_range.format(row_count=len(rows))]),
            )


def _shrink_extra_columns(ws, col_count: int) -> None:
    if ws.col_count > col_count:
        with contextlib.suppress(Exception):
            call_with_retry(f"{ws.title}.resize", lambda: ws.resize(cols=col_count))

