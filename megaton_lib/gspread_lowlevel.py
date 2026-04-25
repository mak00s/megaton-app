"""Low-level gspread and Google Sheets batchUpdate helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DEFAULT_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

__all__ = [
    "DEFAULT_SHEETS_SCOPES",
    "open_spreadsheet",
    "get_or_create_worksheet",
    "overwrite_worksheet",
    "append_rows",
    "batch_update_spreadsheet",
    "fetch_sheet_properties",
    "get_sheet_id",
    "add_sheet_request",
    "delete_sheet_request",
    "update_sheet_properties_request",
    "update_grid_properties_request",
    "append_dimension_request",
    "auto_resize_dimensions_request",
    "ensure_sheet_exists",
    "delete_sheet_if_exists",
    "set_frozen_rows",
    "set_frozen_columns",
    "ensure_min_dimensions",
]


def open_spreadsheet(
    *,
    spreadsheet_id: str,
    credentials_path: str | Path,
    scopes: list[str] | tuple[str, ...] | None = None,
):
    """Open a Google spreadsheet with a service-account JSON file."""
    import gspread
    from google.oauth2.service_account import Credentials

    path = Path(credentials_path)
    if not path.exists():
        raise FileNotFoundError(f"service account JSON not found: {path}")
    creds = Credentials.from_service_account_file(
        str(path),
        scopes=list(scopes or DEFAULT_SHEETS_SCOPES),
    )
    return gspread.authorize(creds).open_by_key(spreadsheet_id)


def get_or_create_worksheet(
    spreadsheet,
    sheet_name: str,
    *,
    rows: int = 100,
    cols: int = 20,
):
    """Return a worksheet, creating it when missing."""
    import gspread

    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=sheet_name,
            rows=max(int(rows), 1),
            cols=max(int(cols), 1),
        )


def overwrite_worksheet(
    spreadsheet,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    min_rows: int = 100,
    extra_cols: int = 2,
    value_input_option: str = "USER_ENTERED",
    freeze_header: bool = True,
    dry_run: bool = False,
) -> int:
    """Clear and overwrite a worksheet with a DataFrame.

    Returns the number of data rows written.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if dry_run:
        print(f"would_write_sheet={sheet_name} rows={len(df)}")
        return int(len(df))

    ws = get_or_create_worksheet(
        spreadsheet,
        sheet_name,
        rows=max(len(df) + 10, min_rows),
        cols=max(len(df.columns) + extra_cols, 1),
    )
    ws.clear()
    values = [df.columns.tolist()] + df.astype(str).replace("nan", "").values.tolist()
    ws.update(values, value_input_option=value_input_option)
    if freeze_header:
        ws.freeze(rows=1)
    return int(len(df))


def append_rows(
    spreadsheet,
    sheet_name: str,
    rows: list[list[object]],
    *,
    value_input_option: str = "USER_ENTERED",
    create_if_missing: bool = True,
    min_rows: int = 100,
    min_cols: int = 20,
    dry_run: bool = False,
) -> int:
    """Append rows to a worksheet and return the number of rows appended."""
    if dry_run:
        print(f"would_append_sheet={sheet_name} rows={len(rows)}")
        return len(rows)
    if create_if_missing:
        ws = get_or_create_worksheet(
            spreadsheet,
            sheet_name,
            rows=max(len(rows) + 10, min_rows),
            cols=min_cols,
        )
    else:
        ws = spreadsheet.worksheet(sheet_name)
    if rows:
        ws.append_rows(rows, value_input_option=value_input_option)
    return len(rows)


def batch_update_spreadsheet(
    spreadsheet,
    requests: list[dict],
    *,
    dry_run: bool = False,
) -> dict:
    """Run a Sheets API batchUpdate request through a gspread spreadsheet."""
    if dry_run:
        return {"dry_run": True, "requests": requests}
    if not requests:
        return {"replies": []}
    return spreadsheet.batch_update({"requests": requests})


def fetch_sheet_properties(spreadsheet) -> dict[str, dict]:
    """Return worksheet properties keyed by title."""
    if hasattr(spreadsheet, "fetch_sheet_metadata"):
        metadata = spreadsheet.fetch_sheet_metadata(
            params={"fields": "sheets.properties"}
        )
        sheets = metadata.get("sheets", [])
    else:
        sheets = [
            {"properties": {"title": ws.title, "sheetId": ws.id}}
            for ws in spreadsheet.worksheets()
        ]
    return {
        str(sheet.get("properties", {}).get("title", "")): sheet.get("properties", {})
        for sheet in sheets
        if sheet.get("properties", {}).get("title")
    }


def get_sheet_id(spreadsheet, sheet_name: str) -> int:
    """Resolve a worksheet title to its numeric sheetId."""
    props = fetch_sheet_properties(spreadsheet).get(sheet_name)
    if not props:
        raise KeyError(f"sheet not found: {sheet_name}")
    return int(props["sheetId"])


def add_sheet_request(
    title: str,
    *,
    rows: int | None = None,
    cols: int | None = None,
    frozen_rows: int | None = None,
    frozen_cols: int | None = None,
    hide_gridlines: bool | None = None,
    tab_color: dict | None = None,
    index: int | None = None,
) -> dict:
    """Build an addSheet request."""
    properties: dict[str, object] = {"title": title}
    grid: dict[str, int] = {}
    if rows is not None:
        grid["rowCount"] = int(rows)
    if cols is not None:
        grid["columnCount"] = int(cols)
    if frozen_rows is not None:
        grid["frozenRowCount"] = int(frozen_rows)
    if frozen_cols is not None:
        grid["frozenColumnCount"] = int(frozen_cols)
    if hide_gridlines is not None:
        grid["hideGridlines"] = bool(hide_gridlines)
    if grid:
        properties["gridProperties"] = grid
    if tab_color is not None:
        properties["tabColor"] = tab_color
    if index is not None:
        properties["index"] = int(index)
    return {"addSheet": {"properties": properties}}


def delete_sheet_request(sheet_id: int) -> dict:
    """Build a deleteSheet request."""
    return {"deleteSheet": {"sheetId": int(sheet_id)}}


def update_sheet_properties_request(
    sheet_id: int,
    *,
    title: str | None = None,
    index: int | None = None,
    hidden: bool | None = None,
    tab_color: dict | None = None,
) -> dict:
    """Build an updateSheetProperties request for non-grid properties."""
    properties: dict[str, object] = {"sheetId": int(sheet_id)}
    fields: list[str] = []
    if title is not None:
        properties["title"] = title
        fields.append("title")
    if index is not None:
        properties["index"] = int(index)
        fields.append("index")
    if hidden is not None:
        properties["hidden"] = bool(hidden)
        fields.append("hidden")
    if tab_color is not None:
        properties["tabColor"] = tab_color
        fields.append("tabColor")
    if not fields:
        raise ValueError("at least one sheet property must be provided")
    return {
        "updateSheetProperties": {
            "properties": properties,
            "fields": ",".join(fields),
        }
    }


def update_grid_properties_request(
    sheet_id: int,
    *,
    frozen_rows: int | None = None,
    frozen_cols: int | None = None,
    row_count: int | None = None,
    column_count: int | None = None,
) -> dict:
    """Build an updateSheetProperties request for grid properties."""
    grid: dict[str, int] = {}
    fields: list[str] = []
    if frozen_rows is not None:
        grid["frozenRowCount"] = int(frozen_rows)
        fields.append("gridProperties.frozenRowCount")
    if frozen_cols is not None:
        grid["frozenColumnCount"] = int(frozen_cols)
        fields.append("gridProperties.frozenColumnCount")
    if row_count is not None:
        grid["rowCount"] = int(row_count)
        fields.append("gridProperties.rowCount")
    if column_count is not None:
        grid["columnCount"] = int(column_count)
        fields.append("gridProperties.columnCount")
    if not fields:
        raise ValueError("at least one grid property must be provided")
    return {
        "updateSheetProperties": {
            "properties": {"sheetId": int(sheet_id), "gridProperties": grid},
            "fields": ",".join(fields),
        }
    }


def append_dimension_request(sheet_id: int, *, dimension: str, length: int) -> dict:
    """Build an appendDimension request."""
    return {
        "appendDimension": {
            "sheetId": int(sheet_id),
            "dimension": dimension,
            "length": int(length),
        }
    }


def auto_resize_dimensions_request(
    sheet_id: int,
    *,
    dimension: str = "COLUMNS",
    start_index: int = 0,
    end_index: int | None = None,
) -> dict:
    """Build an autoResizeDimensions request."""
    dim_range = {
        "sheetId": int(sheet_id),
        "dimension": dimension,
        "startIndex": int(start_index),
    }
    if end_index is not None:
        dim_range["endIndex"] = int(end_index)
    return {"autoResizeDimensions": {"dimensions": dim_range}}


def ensure_sheet_exists(
    spreadsheet,
    sheet_name: str,
    *,
    rows: int = 100,
    cols: int = 20,
    dry_run: bool = False,
) -> bool:
    """Create a worksheet when it does not exist. Returns True if created."""
    if sheet_name in fetch_sheet_properties(spreadsheet):
        return False
    batch_update_spreadsheet(
        spreadsheet,
        [add_sheet_request(sheet_name, rows=rows, cols=cols)],
        dry_run=dry_run,
    )
    return True


def delete_sheet_if_exists(
    spreadsheet,
    sheet_name: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Delete a worksheet when it exists. Returns True if deleted."""
    props = fetch_sheet_properties(spreadsheet).get(sheet_name)
    if not props:
        return False
    batch_update_spreadsheet(
        spreadsheet,
        [delete_sheet_request(int(props["sheetId"]))],
        dry_run=dry_run,
    )
    return True


def set_frozen_rows(
    spreadsheet,
    sheet_name: str,
    count: int,
    *,
    dry_run: bool = False,
) -> None:
    """Set frozen row count for a worksheet."""
    sheet_id = get_sheet_id(spreadsheet, sheet_name)
    batch_update_spreadsheet(
        spreadsheet,
        [update_grid_properties_request(sheet_id, frozen_rows=count)],
        dry_run=dry_run,
    )


def set_frozen_columns(
    spreadsheet,
    sheet_name: str,
    count: int,
    *,
    dry_run: bool = False,
) -> None:
    """Set frozen column count for a worksheet."""
    sheet_id = get_sheet_id(spreadsheet, sheet_name)
    batch_update_spreadsheet(
        spreadsheet,
        [update_grid_properties_request(sheet_id, frozen_cols=count)],
        dry_run=dry_run,
    )


def ensure_min_dimensions(
    spreadsheet,
    sheet_name: str,
    *,
    min_rows: int | None = None,
    min_cols: int | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Append rows/columns until a worksheet reaches minimum dimensions."""
    props = fetch_sheet_properties(spreadsheet).get(sheet_name)
    if not props:
        raise KeyError(f"sheet not found: {sheet_name}")
    sheet_id = int(props["sheetId"])
    grid = props.get("gridProperties", {})
    requests: list[dict] = []
    if min_rows is not None:
        current_rows = int(grid.get("rowCount", 0))
        if current_rows < min_rows:
            requests.append(
                append_dimension_request(
                    sheet_id,
                    dimension="ROWS",
                    length=int(min_rows) - current_rows,
                )
            )
    if min_cols is not None:
        current_cols = int(grid.get("columnCount", 0))
        if current_cols < min_cols:
            requests.append(
                append_dimension_request(
                    sheet_id,
                    dimension="COLUMNS",
                    length=int(min_cols) - current_cols,
                )
            )
    batch_update_spreadsheet(spreadsheet, requests, dry_run=dry_run)
    return requests


# Backward-compatible aliases for the initial direct-gspread helper names.
open_gspread_spreadsheet = open_spreadsheet
get_or_create_gspread_worksheet = get_or_create_worksheet
overwrite_gspread_worksheet = overwrite_worksheet
append_gspread_rows = append_rows
batch_update_gspread_spreadsheet = batch_update_spreadsheet
fetch_gspread_sheet_properties = fetch_sheet_properties
get_gspread_sheet_id = get_sheet_id
ensure_gspread_sheet_exists = ensure_sheet_exists
delete_gspread_sheet_if_exists = delete_sheet_if_exists
set_gspread_frozen_rows = set_frozen_rows
set_gspread_frozen_columns = set_frozen_columns
ensure_gspread_min_dimensions = ensure_min_dimensions
