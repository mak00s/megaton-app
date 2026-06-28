"""Low-level gspread and Google Sheets batchUpdate helpers.

All network calls in this module are wrapped with :func:`call_with_retry`
(exponential backoff via ``megaton.retry_utils.expo_retry``; HTTP 429 quota
errors wait at least 30 seconds before the next attempt).
"""

from __future__ import annotations

import datetime as _dt
import logging
import math
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_QUOTA_FLOOR_WAIT = 30.0  # HTTP 429 needs at least this many seconds


def _get_status_code(exc: BaseException):
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def call_with_retry(
    op: str,
    func,
    *,
    max_retries: int = 4,
    backoff_factor: float = 2.0,
    sleep=time.sleep,
):
    """Run a gspread/Sheets network call with exponential-backoff retry.

    Retries gspread ``APIError`` with status 429/5xx and ``requests``
    transport errors. HTTP 429 quota retries add extra sleep so the wait
    before the next attempt is at least 30 seconds. ``op`` is the log label.
    """
    import gspread
    import requests

    from megaton import retry_utils

    def _is_retryable(exc: BaseException) -> bool:
        if isinstance(exc, requests.exceptions.RequestException):
            return True
        return _get_status_code(exc) in _RETRYABLE_STATUS_CODES

    def _on_retry(attempt_no: int, max_attempts: int, wait: float, exc: BaseException) -> None:
        logger.warning(
            "%s failed; retrying in %.1fs (%s/%s): %s",
            op, wait, attempt_no, max_attempts, exc,
        )
        if _get_status_code(exc) == 429 and wait < _QUOTA_FLOOR_WAIT:
            extra = _QUOTA_FLOOR_WAIT - wait
            logger.info("Quota error: adding %.1fs extra wait", extra)
            sleep(extra)

    return retry_utils.expo_retry(
        func,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        exceptions=(gspread.exceptions.APIError, requests.exceptions.RequestException),
        is_retryable=_is_retryable,
        on_retry=_on_retry,
        sleep=sleep,
    )


__all__ = [
    "DEFAULT_SHEETS_SCOPES",
    "call_with_retry",
    "open_spreadsheet",
    "get_or_create_worksheet",
    "overwrite_worksheet",
    "append_rows",
    "fetch_worksheet_values",
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
    "column_label",
    "gs_serial_to_date",
    "dimension_requests",
    "copy_format_request",
    "cell_data",
    "contiguous_runs",
    "dataframe_update_cells_rows",
    "atomic_replace_dataframe_requests",
]

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?([+-]\d{2}:?\d{2}|Z)?$"
)
_SHEETS_EPOCH_DATE = _dt.date(1899, 12, 30)


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _cell_value(value: Any) -> dict:
    """Build ``userEnteredValue`` for a Python value.

    Strings intentionally stay strings so IDs/codes with leading zeroes are not
    coerced to numbers by Sheets.
    """
    if _is_missing_scalar(value):
        return {"stringValue": ""}
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"numberValue": float(value)}
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return {"stringValue": ""}
        return {"numberValue": value}
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return {"stringValue": ""}
        if s.startswith("="):
            return {"formulaValue": s}
        return {"stringValue": s}
    return {"stringValue": str(value)}


def _datetime_serial(value: str) -> float | None:
    try:
        dt = _dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    # Timezone-aware strings are left as text. Callers that care about timezone
    # semantics should normalize before writing; silently stripping offsets can
    # shift the displayed date.
    if dt.tzinfo is not None:
        return None
    epoch = _dt.datetime(1899, 12, 30)
    return (dt - epoch).total_seconds() / 86400.0


def cell_data(
    value: Any,
    *,
    date_format: str = "yyyy-mm-dd",
    datetime_format: str = "yyyy-mm-dd hh:mm",
) -> dict:
    """Build Sheets API ``CellData`` for a value.

    ISO date and naive ISO datetime strings are written as real Sheets serials
    with a display format. Other strings stay strings.
    """
    if isinstance(value, str):
        s = value.strip()
        if _DATETIME_RE.match(s):
            serial = _datetime_serial(s)
            if serial is not None:
                return {
                    "userEnteredValue": {"numberValue": serial},
                    "userEnteredFormat": {
                        "numberFormat": {"type": "DATE_TIME", "pattern": datetime_format}
                    },
                }
        elif _DATE_RE.match(s):
            try:
                serial = (_dt.date.fromisoformat(s) - _SHEETS_EPOCH_DATE).days
            except ValueError:
                return {"userEnteredValue": _cell_value(value)}
            return {
                "userEnteredValue": {"numberValue": float(serial)},
                "userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": date_format}},
            }
    return {"userEnteredValue": _cell_value(value)}


def contiguous_runs(values: list[Any]):
    """Yield ``(value, start, end)`` for maximal contiguous equal-value runs."""
    i, n = 0, len(values)
    while i < n:
        j = i + 1
        while j < n and values[j] == values[i]:
            j += 1
        yield values[i], i, j
        i = j


def dataframe_update_cells_rows(df: pd.DataFrame | None) -> list[dict]:
    """Convert a DataFrame to Sheets API row records including the header row."""
    if df is None:
        values: list[list[Any]] = [[]]
    elif df.empty:
        values = [[str(c) for c in df.columns]]
    else:
        values = [[str(c) for c in df.columns]]
        values.extend([list(row) for row in df.itertuples(index=False, name=None)])
    return [{"values": [cell_data(v) for v in row]} for row in values]


def atomic_replace_dataframe_requests(
    sheet_id: int,
    df: pd.DataFrame | None,
    *,
    number_formats: dict[int, dict] | None = None,
    cell_number_formats: dict[int, list[dict | None]] | None = None,
    cell_format: dict | None = None,
    column_widths: list[int] | None = None,
    clear_format: bool = True,
    header_format: dict | None = None,
    freeze_header: bool = True,
) -> list[dict]:
    """Build batchUpdate requests for atomic DataFrame sheet replacement.

    The request sequence is clear → resize → write values → durable formats.
    It is intentionally low-level and side-effect free so application-specific
    workbook facades can use it without adopting megaton's higher-level sheet
    abstractions.
    """
    rows = dataframe_update_cells_rows(df)
    n_rows = max(len(rows), 2)
    n_cols = max(
        max((len(r.get("values", [])) for r in rows), default=1),
        1,
    )
    clear_fields = "userEnteredValue,userEnteredFormat" if clear_format else "userEnteredValue"
    requests: list[dict] = [
        {
            "updateCells": {
                "range": {"sheetId": int(sheet_id)},
                "fields": clear_fields,
            }
        },
        update_grid_properties_request(
            int(sheet_id),
            row_count=n_rows,
            column_count=n_cols,
        ),
        {
            "updateCells": {
                "start": {"sheetId": int(sheet_id), "rowIndex": 0, "columnIndex": 0},
                "rows": rows,
                "fields": "userEnteredValue,userEnteredFormat",
            }
        },
    ]

    if header_format is None:
        header_format = {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.94, "green": 0.94, "blue": 0.97},
        }
    if header_format:
        fields = "userEnteredFormat(" + ",".join(header_format.keys()) + ")"
        requests.append({
            "repeatCell": {
                "range": {"sheetId": int(sheet_id), "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": header_format},
                "fields": fields,
            }
        })

    if freeze_header:
        requests.append(update_grid_properties_request(int(sheet_id), frozen_rows=1))
    else:
        requests.append(update_grid_properties_request(int(sheet_id), frozen_rows=0))

    for col, nf in (number_formats or {}).items():
        if 0 <= col < n_cols:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": int(sheet_id),
                        "startRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": nf}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            })
    for col, row_formats in (cell_number_formats or {}).items():
        if not (0 <= col < n_cols):
            continue
        for nf, start, end in contiguous_runs(row_formats):
            if nf is None:
                continue
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": int(sheet_id),
                        "startRowIndex": 1 + start,
                        "endRowIndex": 1 + end,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": nf}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            })

    numeric_cols = set(number_formats or {}) | set(cell_number_formats or {})
    for col in sorted(numeric_cols):
        if 0 <= col < n_cols:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": int(sheet_id),
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}},
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            })

    if cell_format:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": int(sheet_id),
                    "startRowIndex": 0,
                    "endRowIndex": n_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": n_cols,
                },
                "cell": {"userEnteredFormat": cell_format},
                "fields": "userEnteredFormat(" + ",".join(cell_format.keys()) + ")",
            }
        })
    for col, px in enumerate(column_widths or []):
        if 0 <= col < n_cols and px:
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": int(sheet_id),
                        "dimension": "COLUMNS",
                        "startIndex": col,
                        "endIndex": col + 1,
                    },
                    "properties": {"pixelSize": int(px)},
                    "fields": "pixelSize",
                }
            })
    return requests


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
    client = gspread.authorize(creds)
    return call_with_retry(
        f"open_spreadsheet {spreadsheet_id}",
        lambda: client.open_by_key(spreadsheet_id),
    )


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
        return call_with_retry(
            f"worksheet {sheet_name}",
            lambda: spreadsheet.worksheet(sheet_name),
        )
    except gspread.exceptions.WorksheetNotFound:
        return call_with_retry(
            f"add_worksheet {sheet_name}",
            lambda: spreadsheet.add_worksheet(
                title=sheet_name,
                rows=max(int(rows), 1),
                cols=max(int(cols), 1),
            ),
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
    call_with_retry(f"clear {sheet_name}", ws.clear)
    values = [df.columns.tolist()] + df.astype(str).replace("nan", "").values.tolist()
    call_with_retry(
        f"update {sheet_name}",
        lambda: ws.update(values, value_input_option=value_input_option),
    )
    if freeze_header:
        call_with_retry(f"freeze {sheet_name}", lambda: ws.freeze(rows=1))
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
        ws = call_with_retry(
            f"worksheet {sheet_name}",
            lambda: spreadsheet.worksheet(sheet_name),
        )
    if rows:
        call_with_retry(
            f"append_rows {sheet_name}",
            lambda: ws.append_rows(rows, value_input_option=value_input_option),
        )
    return len(rows)


def fetch_worksheet_values(
    spreadsheet,
    sheet_name: str,
    *,
    missing_ok: bool = False,
) -> list[list[str]]:
    """Return all values of a worksheet as a list of string rows.

    The read-side counterpart to :func:`overwrite_worksheet` /
    :func:`append_rows`, so callers don't reach past this module to call
    ``worksheet(...).get_all_values()`` directly.

    Args:
        missing_ok: When the worksheet does not exist, return ``[]``
            instead of raising ``WorksheetNotFound``.
    """
    import gspread

    try:
        ws = call_with_retry(
            f"worksheet {sheet_name}",
            lambda: spreadsheet.worksheet(sheet_name),
        )
    except gspread.exceptions.WorksheetNotFound:
        if missing_ok:
            return []
        raise
    return call_with_retry(f"get_all_values {sheet_name}", ws.get_all_values)


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
    return call_with_retry(
        "batch_update",
        lambda: spreadsheet.batch_update({"requests": requests}),
    )


def fetch_sheet_properties(spreadsheet) -> dict[str, dict]:
    """Return worksheet properties keyed by title."""
    if hasattr(spreadsheet, "fetch_sheet_metadata"):
        metadata = call_with_retry(
            "fetch_sheet_metadata",
            lambda: spreadsheet.fetch_sheet_metadata(
                params={"fields": "sheets.properties"}
            ),
        )
        sheets = metadata.get("sheets", [])
    else:
        sheets = [
            {"properties": {"title": ws.title, "sheetId": ws.id}}
            for ws in call_with_retry("worksheets", spreadsheet.worksheets)
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


# --- A1 / serial-date helpers (promoted from megaton-notebooks lib/sheets_utils.py) ---

GS_EPOCH = _dt.datetime(1899, 12, 30)


def column_label(index: int) -> str:
    """1-based column index -> A1 letter (1 -> A, 27 -> AA)."""
    if index < 1:
        raise ValueError("Column index must be >= 1")
    out = ""
    n = index
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def gs_serial_to_date(value: object) -> "_dt.datetime | None":
    """Google Sheets serial number -> datetime (None for empty/invalid)."""
    if value in ("", None):
        return None
    try:
        return GS_EPOCH + _dt.timedelta(days=float(value))
    except (TypeError, ValueError):
        return None


# --- batchUpdate request builders (promoted from lib/sheets_requests.py) ---

def dimension_requests(
    sheet_id: int,
    *,
    col_widths: list[int],
    row_count: int,
    row_height_px: int | None = None,
) -> list[dict]:
    """Column-width (and optional row-height) updateDimensionProperties requests."""
    requests: list[dict] = []
    for idx, width in enumerate(col_widths):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )
    if row_height_px is not None:
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": row_count,
                    },
                    "properties": {"pixelSize": row_height_px},
                    "fields": "pixelSize",
                }
            }
        )
    return requests


def copy_format_request(
    source_sheet_id: int,
    target_sheet_id: int,
    *,
    src_start_row: int,
    src_end_row: int,
    src_start_col: int,
    src_end_col: int,
    dst_start_row: int,
    dst_end_row: int,
    dst_start_col: int,
    dst_end_col: int,
) -> dict:
    """PASTE_FORMAT copyPaste request between sheet ranges."""
    return {
        "copyPaste": {
            "source": {
                "sheetId": source_sheet_id,
                "startRowIndex": src_start_row,
                "endRowIndex": src_end_row,
                "startColumnIndex": src_start_col,
                "endColumnIndex": src_end_col,
            },
            "destination": {
                "sheetId": target_sheet_id,
                "startRowIndex": dst_start_row,
                "endRowIndex": dst_end_row,
                "startColumnIndex": dst_start_col,
                "endColumnIndex": dst_end_col,
            },
            "pasteType": "PASTE_FORMAT",
            "pasteOrientation": "NORMAL",
        }
    }
