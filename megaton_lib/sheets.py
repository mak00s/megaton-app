"""Google Sheets helpers for notebooks.

This module intentionally wraps a few common Megaton patterns so notebooks can
stay short and avoid accidental destructive operations (e.g., clearing sheets
when the DataFrame is empty).
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd


def upsert_or_skip(mg, name: str, df: pd.DataFrame, *, keys: list, sort_by: list | None = None, **kwargs) -> bool:
    """Upsert DataFrame to a worksheet, or print skip message when empty.

    This is a thin convenience wrapper around ``mg.upsert.to.sheet()`` that
    notebooks use to avoid repeating the if/else empty-check pattern.

    Args:
        mg: Megaton instance with a spreadsheet already opened.
        name: worksheet name.
        df: DataFrame to upsert.
        keys: column(s) used for deduplication.
        sort_by: column(s) to sort the final result. Defaults to *keys*.
        **kwargs: extra arguments forwarded to ``mg.upsert.to.sheet()``.

    Returns:
        True if upsert was performed, False if skipped.
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty) or len(df) == 0:
        print(f"[skip] {name}: no rows")
        return False
    mg.upsert.to.sheet(name, df, keys=keys, sort_by=sort_by or keys, **kwargs)
    return True


def save_sheet_from_template(
    mg,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    start_row: int = 3,
    create_if_missing: bool = True,
    template_sheet: Optional[str] = None,
    template_regex: str = r"^\d{6}$",
    skip_if_empty: bool = True,
    clear_after_duplicate: bool = True,
    save_kwargs: Optional[dict] = None,
) -> bool:
    """Save a DataFrame to a worksheet, duplicating a template when missing.

    Intended usage:
    - Preserve headers/formatting in the first rows (use ``start_row>=2``).
    - Create missing sheet by duplicating a recent template sheet to keep style.
    - Do nothing when ``df`` is empty, to avoid clearing existing sheet data.

    Args:
        mg: Megaton instance (from ``megaton_lib.megaton_client.get_ga4`` etc.)
        sheet_name: target worksheet name.
        df: DataFrame to write.
        start_row: 1-based row number where headers are written.
        create_if_missing: when True, duplicate a template sheet if missing.
        template_sheet: explicit template worksheet name (must exist).
        template_regex: used when template_sheet is omitted; the last matching
            sheet is used as template (fallback: the last sheet).
        skip_if_empty: when True and df is empty, skip write/duplication.

    Returns:
        True if we attempted to write, False if skipped.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    if skip_if_empty and df.empty:
        print(f"[skip] {sheet_name}: df is empty (no clear/write)")
        return False

    if not getattr(mg, "gs", None) or not getattr(mg.gs, "_driver", None):
        raise ValueError("Google Sheets is not opened. Call mg.open.sheet(url) first.")

    sheets = list(mg.gs.sheets)
    if not sheets:
        raise ValueError("No worksheets found in the opened spreadsheet.")

    if sheet_name not in sheets:
        if not create_if_missing:
            print(f"[skip] {sheet_name}: worksheet missing and create_if_missing=False")
            return False

        if template_sheet is None:
            candidates = [s for s in sheets if re.match(template_regex, s)]
            if candidates:
                # Prefer the most recent-looking name (e.g., "202502" > "202501").
                # Fallback to sheet order if names aren't numeric.
                numeric = [s for s in candidates if str(s).isdigit()]
                template_sheet = max(numeric, key=lambda x: int(str(x))) if numeric else candidates[-1]
            else:
                # No suitable template exists; create an empty worksheet instead of
                # duplicating an unrelated sheet.
                mg.gs.sheet.create(sheet_name)
                mg.save.to.sheet(sheet_name, df, start_row=start_row, **(save_kwargs or {}))
                return True
        elif template_sheet not in sheets:
            raise ValueError(f"Template worksheet not found: {template_sheet}")

        if not mg.gs.sheet.select(template_sheet):
            raise RuntimeError(f"Failed to select template worksheet: {template_sheet}")

        src_id = getattr(mg.gs.sheet, "id", None)
        if not src_id:
            raise RuntimeError("Failed to read template worksheet id")

        mg.gs._driver.duplicate_sheet(src_id, new_sheet_name=sheet_name)

        # Clear values in the duplicated sheet so the old template contents don't remain.
        # This keeps formatting while ensuring the new write is clean.
        if clear_after_duplicate:
            try:
                mg.gs.sheet.select(sheet_name)
                if start_row <= 1:
                    mg.gs.sheet.clear()
                else:
                    ws = getattr(mg.gs.sheet, "_driver", None)
                    if ws and hasattr(ws, "batch_clear") and hasattr(ws, "row_count"):
                        ws.batch_clear([f"{start_row}:{ws.row_count}"])
                    else:
                        # Fallback: clear everything (format is preserved; values are removed)
                        mg.gs.sheet.clear()
            except Exception as e:
                print(f"[warn] {sheet_name}: failed to clear duplicated sheet values: {e}")

    mg.save.to.sheet(sheet_name, df, start_row=start_row, **(save_kwargs or {}))
    return True
