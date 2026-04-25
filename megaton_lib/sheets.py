"""Google Sheets helpers for notebooks.

This module intentionally wraps a few common Megaton patterns so notebooks can
stay short and avoid accidental destructive operations (e.g., clearing sheets
when the DataFrame is empty).
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from .gspread_lowlevel import (
    DEFAULT_SHEETS_SCOPES,
    add_sheet_request,
    append_dimension_request,
    append_gspread_rows,
    append_rows,
    auto_resize_dimensions_request,
    batch_update_gspread_spreadsheet,
    batch_update_spreadsheet,
    delete_gspread_sheet_if_exists,
    delete_sheet_if_exists,
    delete_sheet_request,
    ensure_gspread_min_dimensions,
    ensure_gspread_sheet_exists,
    ensure_min_dimensions,
    ensure_sheet_exists,
    fetch_gspread_sheet_properties,
    fetch_sheet_properties,
    get_gspread_sheet_id,
    get_or_create_gspread_worksheet,
    get_or_create_worksheet,
    get_sheet_id,
    open_gspread_spreadsheet,
    open_spreadsheet,
    overwrite_gspread_worksheet,
    overwrite_worksheet,
    set_frozen_columns,
    set_frozen_rows,
    set_gspread_frozen_columns,
    set_gspread_frozen_rows,
    update_grid_properties_request,
    update_sheet_properties_request,
)

__all__ = [
    "read_sheet_table",
    "load_pattern_map",
    "upsert_or_skip",
    "replace_sheet_by_group_keys",
    "update_cells",
    "save_sheet_table",
    "duplicate_sheet_into",
    "save_sheet_from_template",
    "write_sheet_blocks",
]


def read_sheet_table(
    mg,
    *,
    sheet_url: str,
    sheet_name: str,
    header_row: int = 0,
) -> pd.DataFrame:
    """Read worksheet values into a DataFrame.

    Returns empty DataFrame when the worksheet has no rows.
    """
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")
    mg.gs.sheet.select(sheet_name)

    header_row = int(header_row)
    if header_row < 0:
        raise ValueError("header_row must be >= 0")

    if header_row == 0:
        data = mg.gs.sheet.data or []
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df.columns = [str(c).strip() for c in df.columns]
        return df.dropna(how="all")

    worksheet = mg.gs._driver.worksheet(sheet_name)
    values = worksheet.get_all_values()
    if not values or header_row >= len(values):
        return pd.DataFrame()

    headers = [str(c).strip() for c in values[header_row]]
    rows = values[header_row + 1 :]
    df = pd.DataFrame(rows, columns=headers)
    if df.empty:
        return df
    return df.dropna(how="all")


def load_pattern_map(
    mg,
    *,
    sheet_url: str,
    sheet_name: str,
    key_col: str,
    value_col: str,
) -> dict[str, str]:
    """Load regex mapping table from a worksheet as ``{pattern: value}``."""
    df = read_sheet_table(mg, sheet_url=sheet_url, sheet_name=sheet_name)
    if df.empty:
        print(f"[warn] {sheet_name} is empty")
        return {}

    required = {key_col, value_col}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{sheet_name}: missing columns {missing}")

    df = df[[key_col, value_col]].dropna(subset=[key_col])
    return {str(k): str(v) for k, v in zip(df[key_col], df[value_col])}


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


def replace_sheet_by_group_keys(
    mg,
    *,
    sheet_url: str,
    sheet_name: str,
    df_new: pd.DataFrame,
    remove_group_keys: list[str],
    sort_by: list[str],
    columns: list[str],
) -> pd.DataFrame:
    """Replace existing rows matching group keys, then overwrite the sheet.

    Useful when a monthly batch should fully refresh a group (e.g. month+clinic)
    while keeping older groups untouched.
    """
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")

    if sheet_name not in mg.gs.sheets:
        try:
            mg.gs.sheet.create(sheet_name)
        except Exception:
            mg.gs._driver.add_worksheet(title=sheet_name, rows=2000, cols=30)

    mg.gs.sheet.select(sheet_name)
    existing = pd.DataFrame(mg.gs.sheet.data or [])

    incoming = df_new.copy()
    for col in columns:
        if col not in incoming.columns:
            incoming[col] = pd.NA
    incoming = incoming[columns].copy()

    if existing.empty:
        out = incoming.copy()
        mg.save.to.sheet(df=out, sheet_name=sheet_name)
        return out

    for col in columns:
        if col not in existing.columns:
            existing[col] = pd.NA

    existing = existing[columns].copy()

    for key in remove_group_keys:
        existing[key] = existing[key].astype(str).str.strip()
        incoming[key] = incoming[key].astype(str).str.strip()

    keys_to_remove = set(tuple(r) for r in incoming[remove_group_keys].drop_duplicates().values.tolist())
    if keys_to_remove:
        mask = existing[remove_group_keys].apply(tuple, axis=1).isin(keys_to_remove)
        existing = existing[~mask].copy()

    out = pd.concat([existing, incoming], ignore_index=True).sort_values(sort_by).reset_index(drop=True)
    mg.save.to.sheet(df=out, sheet_name=sheet_name)
    return out


def update_cells(mg, *, sheet_url: str, sheet_name: str, values: dict[str, str]) -> None:
    """Update multiple A1 cells in a worksheet."""
    if not values:
        return
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")
    mg.gs.sheet.select(sheet_name)
    for cell, value in values.items():
        mg.gs.sheet._driver.update_acell(cell, value)


def save_sheet_table(
    mg,
    *,
    sheet_url: str,
    sheet_name: str,
    df: pd.DataFrame,
    sort_by: str | list[str] | None = None,
    sort_desc: bool = True,
    create_if_missing: bool = True,
    auto_width: bool = True,
    freeze_header: bool = True,
    start_row: int | None = None,
    min_rows: int | None = None,
    min_cols: int | None = None,
    hide_gridlines: bool | None = None,
    tab_color: str | dict | None = None,
) -> bool:
    """Open a workbook and overwrite a worksheet with a DataFrame.

    Optional visual/grid formatting delegates to the public ``mg.sheet.*``
    helpers provided by megaton. Direct-gspread helpers remain available for
    low-level batchUpdate use cases.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")

    if start_row is None:
        mg._sheets.save_sheet(
            sheet_name,
            df,
            sort_by=sort_by,
            sort_desc=sort_desc,
            create_if_missing=create_if_missing,
            auto_width=auto_width,
            freeze_header=freeze_header,
        )
    else:
        mg.save.to.sheet(
            sheet_name,
            df,
            start_row=start_row,
            freeze_header=freeze_header,
            auto_width=auto_width,
        )

    if any(value is not None for value in (min_rows, min_cols, hide_gridlines, tab_color)):
        missing = []
        sheets = getattr(mg, "sheets", None)
        if sheets is None:
            missing.append("mg.sheets")
        elif not hasattr(sheets, "select"):
            missing.append("mg.sheets.select")

        sheet = getattr(mg, "sheet", None)
        if sheet is None:
            missing.append("mg.sheet")
        elif min_rows is not None or min_cols is not None:
            if not hasattr(sheet, "resize"):
                missing.append("mg.sheet.resize")

        if sheet is not None and hide_gridlines is not None:
            gridlines = getattr(sheet, "gridlines", None)
            if gridlines is None:
                missing.append("mg.sheet.gridlines")
            elif hide_gridlines is True and not hasattr(gridlines, "hide"):
                missing.append("mg.sheet.gridlines.hide")
            elif hide_gridlines is False and not hasattr(gridlines, "show"):
                missing.append("mg.sheet.gridlines.show")

        if sheet is not None and tab_color is not None:
            tab = getattr(sheet, "tab", None)
            if tab is None:
                missing.append("mg.sheet.tab")
            elif not hasattr(tab, "color"):
                missing.append("mg.sheet.tab.color")

        if missing:
            raise RuntimeError(
                "megaton sheet formatting helpers are unavailable: "
                + ", ".join(missing)
            )

        # mg.sheet.* acts on the current worksheet, so select explicitly after writing.
        mg.sheets.select(sheet_name)
        if min_rows is not None or min_cols is not None:
            mg.sheet.resize(rows=min_rows, cols=min_cols)
        if hide_gridlines is True:
            mg.sheet.gridlines.hide()
        elif hide_gridlines is False:
            mg.sheet.gridlines.show()
        if tab_color is not None:
            mg.sheet.tab.color(tab_color)
    return True


def duplicate_sheet_into(
    mg,
    *,
    sheet_url: str,
    source_sheet_name: str,
    new_sheet_name: str,
    cell_update: dict[str, str] | None = None,
) -> bool:
    """Duplicate a worksheet inside the target workbook."""
    if not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")

    duplicate_fn = getattr(mg.sheets, "duplicate", None)
    if callable(duplicate_fn):
        return bool(
            duplicate_fn(
                source_sheet_name,
                new_sheet_name,
                cell_update=cell_update,
            )
        )

    source_worksheet = mg.gs._driver.worksheet(source_sheet_name)
    mg.gs._driver.duplicate_sheet(
        source_worksheet.id,
        new_sheet_name=new_sheet_name,
    )
    duplicated = mg.sheets.select(new_sheet_name)
    if duplicated and cell_update:
        mg.sheet.cell.set(cell_update["cell"], cell_update["value"])
    return bool(duplicated)


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


def write_sheet_blocks(
    mg,
    *,
    sheet_name: str,
    blocks: list[tuple[str, list[list]]],
    sheet_url: str | None = None,
    create_if_missing: bool = True,
    clear_sheet: bool = False,
) -> bool:
    """Write one or more rectangular blocks to a worksheet.

    Each block is a tuple of ``(a1, values)`` where ``values`` is the 2-D list
    accepted by gspread ``Worksheet.update``.
    """
    if sheet_url and not mg.open.sheet(sheet_url):
        raise RuntimeError(f"Could not open sheet URL: {sheet_url}")
    if not getattr(mg, "gs", None) or not getattr(mg.gs, "_driver", None):
        raise ValueError("Google Sheets is not opened. Call mg.open.sheet(url) first.")

    sheets = list(getattr(mg.gs, "sheets", []) or [])
    if sheet_name not in sheets:
        if not create_if_missing:
            print(f"[skip] {sheet_name}: worksheet missing and create_if_missing=False")
            return False
        mg.gs.sheet.create(sheet_name)

    select_fn = getattr(getattr(mg, "sheets", None), "select", None) or getattr(mg.gs.sheet, "select", None)
    if not callable(select_fn) or not select_fn(sheet_name):
        raise RuntimeError(f"Failed to select worksheet: {sheet_name}")

    ws = getattr(mg.gs.sheet, "_driver", None)
    if not (ws and hasattr(ws, "update")):
        print(f"[skip] {sheet_name}: worksheet driver does not support update()")
        return False

    if clear_sheet:
        try:
            if hasattr(ws, "clear"):
                ws.clear()
            else:
                mg.gs.sheet.clear()
        except Exception:
            pass

    wrote = False
    for a1, values in blocks:
        if not values:
            continue
        ws.update(a1, values)
        wrote = True
    return wrote
