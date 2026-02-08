"""Google Sheets helpers for notebooks.

This module intentionally wraps a few common Megaton patterns so notebooks can
stay short and avoid accidental destructive operations (e.g., clearing sheets
when the DataFrame is empty).
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd


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
            template_sheet = candidates[-1] if candidates else sheets[-1]
        elif template_sheet not in sheets:
            raise ValueError(f"Template worksheet not found: {template_sheet}")

        if not mg.gs.sheet.select(template_sheet):
            raise RuntimeError(f"Failed to select template worksheet: {template_sheet}")

        src_id = getattr(mg.gs.sheet, "id", None)
        if not src_id:
            raise RuntimeError("Failed to read template worksheet id")

        mg.gs._driver.duplicate_sheet(src_id, new_sheet_name=sheet_name)

    mg.save.to.sheet(sheet_name, df, start_row=start_row)
    return True

