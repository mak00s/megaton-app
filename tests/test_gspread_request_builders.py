from __future__ import annotations

import pandas as pd

from megaton_lib.gspread_lowlevel import (
    atomic_replace_dataframe_requests,
    cell_data,
    contiguous_runs,
    copy_format_request,
    dimension_requests,
)


def test_cell_data_preserves_string_codes() -> None:
    assert cell_data("03311187") == {"userEnteredValue": {"stringValue": "03311187"}}


def test_cell_data_writes_iso_dates_as_sheet_serials() -> None:
    assert cell_data("2026-06-23") == {
        "userEnteredValue": {"numberValue": 46196.0},
        "userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}},
    }


def test_cell_data_writes_naive_iso_datetimes_as_sheet_serials() -> None:
    result = cell_data("2026-06-23 19:05")

    assert result["userEnteredFormat"] == {
        "numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm"}
    }
    assert result["userEnteredValue"]["numberValue"] == 46196.79513888889


def test_dataframe_update_cells_rows_blanks_pandas_missing_values() -> None:
    df = pd.DataFrame({
        "nullable_int": pd.Series([pd.NA], dtype="Int64"),
        "timestamp": [pd.NaT],
    })

    rows = atomic_replace_dataframe_requests(123, df)[2]["updateCells"]["rows"]

    assert rows[1]["values"] == [
        {"userEnteredValue": {"stringValue": ""}},
        {"userEnteredValue": {"stringValue": ""}},
    ]


def test_contiguous_runs_groups_equal_adjacent_values() -> None:
    assert list(contiguous_runs(["a", "a", "b", None, None, "a"])) == [
        ("a", 0, 2),
        ("b", 2, 3),
        (None, 3, 5),
        ("a", 5, 6),
    ]


def test_atomic_replace_dataframe_requests_builds_core_sequence() -> None:
    df = pd.DataFrame({"code": ["03311187"], "amount": [1000]})

    requests = atomic_replace_dataframe_requests(123, df)

    assert [next(iter(r.keys())) for r in requests] == [
        "updateCells",
        "updateSheetProperties",
        "updateCells",
        "repeatCell",
        "updateSheetProperties",
    ]
    assert requests[0]["updateCells"]["fields"] == "userEnteredValue,userEnteredFormat"
    assert requests[1]["updateSheetProperties"]["properties"]["gridProperties"] == {
        "rowCount": 2,
        "columnCount": 2,
    }
    rows = requests[2]["updateCells"]["rows"]
    assert rows[0]["values"][0] == {"userEnteredValue": {"stringValue": "code"}}
    assert rows[1]["values"][0] == {"userEnteredValue": {"stringValue": "03311187"}}
    assert rows[1]["values"][1] == {"userEnteredValue": {"numberValue": 1000.0}}


def test_atomic_replace_dataframe_requests_can_preserve_existing_formats() -> None:
    requests = atomic_replace_dataframe_requests(
        123,
        pd.DataFrame({"summary": ["long text"]}),
        clear_format=False,
    )

    assert requests[0]["updateCells"]["fields"] == "userEnteredValue"


def test_atomic_replace_dataframe_requests_can_clear_header_freeze() -> None:
    requests = atomic_replace_dataframe_requests(
        123,
        pd.DataFrame({"summary": ["long text"]}),
        freeze_header=False,
    )

    assert requests[4]["updateSheetProperties"]["properties"]["gridProperties"] == {
        "frozenRowCount": 0,
    }


def test_atomic_replace_dataframe_requests_applies_durable_formats() -> None:
    currency = {"type": "CURRENCY", "pattern": "[$¥]#,##0"}
    percent = {"type": "PERCENT", "pattern": "0.0%"}
    wrap = {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}

    requests = atomic_replace_dataframe_requests(
        123,
        pd.DataFrame({"name": ["a", "b"], "amount": [1000, 2000], "ratio": [0.1, 0.2]}),
        number_formats={1: currency},
        cell_number_formats={2: [percent, percent]},
        cell_format=wrap,
        column_widths=[80, 120],
    )

    numeric_header_alignments = [
        r["repeatCell"]["range"]["startColumnIndex"]
        for r in requests
        if "repeatCell" in r
        and r["repeatCell"].get("cell", {}).get("userEnteredFormat", {}).get("horizontalAlignment")
        == "RIGHT"
    ]
    widths = [
        r["updateDimensionProperties"]["properties"]["pixelSize"]
        for r in requests
        if "updateDimensionProperties" in r
    ]

    assert numeric_header_alignments == [1, 2]
    assert widths == [80, 120]
    assert any(
        r.get("repeatCell", {}).get("cell", {}).get("userEnteredFormat", {}) == wrap
        for r in requests
    )


def test_dimension_requests_builds_column_and_row_dimensions() -> None:
    requests = dimension_requests(123, col_widths=[80, 120], row_count=10, row_height_px=21)

    assert requests == [
        {
            "updateDimensionProperties": {
                "range": {"sheetId": 123, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 80},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": 123, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 120},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": 123, "dimension": "ROWS", "startIndex": 0, "endIndex": 10},
                "properties": {"pixelSize": 21},
                "fields": "pixelSize",
            }
        },
    ]


def test_dimension_requests_can_skip_row_height_update() -> None:
    assert len(dimension_requests(123, col_widths=[80], row_count=10)) == 1


def test_copy_format_request_builds_paste_format_request() -> None:
    request = copy_format_request(
        1,
        2,
        src_start_row=3,
        src_end_row=4,
        src_start_col=5,
        src_end_col=6,
        dst_start_row=7,
        dst_end_row=8,
        dst_start_col=9,
        dst_end_col=10,
    )

    assert request == {
        "copyPaste": {
            "source": {
                "sheetId": 1,
                "startRowIndex": 3,
                "endRowIndex": 4,
                "startColumnIndex": 5,
                "endColumnIndex": 6,
            },
            "destination": {
                "sheetId": 2,
                "startRowIndex": 7,
                "endRowIndex": 8,
                "startColumnIndex": 9,
                "endColumnIndex": 10,
            },
            "pasteType": "PASTE_FORMAT",
            "pasteOrientation": "NORMAL",
        }
    }
