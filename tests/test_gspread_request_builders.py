from __future__ import annotations

from megaton_lib.gspread_lowlevel import copy_format_request, dimension_requests


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
