from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton_lib import gspread_lowlevel, sheets as sheets_module
from megaton_lib.gspread_lowlevel import (
    add_sheet_request,
    auto_resize_dimensions_request,
    delete_sheet_if_exists,
    ensure_min_dimensions,
    ensure_sheet_exists,
    get_sheet_id,
    set_frozen_rows,
    update_grid_properties_request,
    update_sheet_properties_request,
)
from megaton_lib.sheets import duplicate_sheet_into, save_sheet_table, write_sheet_blocks


def _save_mg():
    save_sheet_mock = Mock()
    resize_mock = Mock()
    hide_gridlines_mock = Mock()
    show_gridlines_mock = Mock()
    tab_color_mock = Mock()
    mg = SimpleNamespace(
        open=SimpleNamespace(sheet=Mock(return_value=True)),
        sheets=SimpleNamespace(select=Mock()),
        sheet=SimpleNamespace(
            resize=resize_mock,
            gridlines=SimpleNamespace(
                hide=hide_gridlines_mock,
                show=show_gridlines_mock,
            ),
            tab=SimpleNamespace(color=tab_color_mock),
        ),
        _sheets=SimpleNamespace(save_sheet=save_sheet_mock),
        save=SimpleNamespace(to=SimpleNamespace(sheet=Mock())),
    )
    return mg, save_sheet_mock


def _duplicate_mg(*, has_duplicate: bool = True):
    duplicate_mock = Mock(return_value=True)
    select_mock = Mock(return_value=True)
    driver = SimpleNamespace(
        worksheet=Mock(return_value=SimpleNamespace(id=101)),
        duplicate_sheet=Mock(),
    )
    mg = SimpleNamespace(
        open=SimpleNamespace(sheet=Mock(return_value=True)),
        sheets=SimpleNamespace(
            duplicate=duplicate_mock if has_duplicate else None,
            select=select_mock,
        ),
        gs=SimpleNamespace(_driver=driver),
        sheet=SimpleNamespace(cell=SimpleNamespace(set=Mock())),
    )
    return mg, duplicate_mock, driver, select_mock


def test_save_sheet_table_opens_sheet_and_saves():
    mg, save_mock = _save_mg()
    df = pd.DataFrame({"a": [1]})

    saved = save_sheet_table(
        mg,
        sheet_url="https://docs.google.com/spreadsheets/d/test",
        sheet_name="_data",
        df=df,
        sort_by="a",
    )

    assert saved is True
    mg.open.sheet.assert_called_once_with("https://docs.google.com/spreadsheets/d/test")
    save_mock.assert_called_once_with(
        "_data",
        df,
        sort_by="a",
        sort_desc=True,
        create_if_missing=True,
        auto_width=True,
        freeze_header=True,
    )
    mg.sheets.select.assert_not_called()
    mg.sheet.resize.assert_not_called()
    mg.sheet.gridlines.hide.assert_not_called()
    mg.sheet.tab.color.assert_not_called()


def test_save_sheet_table_applies_megaton_sheet_formatting():
    mg, _save_mock = _save_mg()
    df = pd.DataFrame({"a": [1]})

    saved = save_sheet_table(
        mg,
        sheet_url="https://docs.google.com/spreadsheets/d/test",
        sheet_name="_data",
        df=df,
        min_rows=100,
        min_cols=8,
        hide_gridlines=True,
        tab_color="#2f80ed",
    )

    assert saved is True
    mg.sheets.select.assert_called_once_with("_data")
    mg.sheet.resize.assert_called_once_with(rows=100, cols=8)
    mg.sheet.gridlines.hide.assert_called_once_with()
    mg.sheet.gridlines.show.assert_not_called()
    mg.sheet.tab.color.assert_called_once_with("#2f80ed")


def test_save_sheet_table_reports_missing_megaton_formatting_helpers():
    mg, _save_mock = _save_mg()
    delattr(mg.sheet, "resize")
    df = pd.DataFrame({"a": [1]})

    with pytest.raises(RuntimeError, match="mg\\.sheet\\.resize"):
        save_sheet_table(
            mg,
            sheet_url="https://docs.google.com/spreadsheets/d/test",
            sheet_name="_data",
            df=df,
            min_rows=100,
        )


def test_save_sheet_table_reports_missing_megaton_sheet_namespace():
    mg, _save_mock = _save_mg()
    delattr(mg, "sheet")
    df = pd.DataFrame({"a": [1]})

    with pytest.raises(RuntimeError) as exc:
        save_sheet_table(
            mg,
            sheet_url="https://docs.google.com/spreadsheets/d/test",
            sheet_name="_data",
            df=df,
            min_rows=100,
            hide_gridlines=True,
            tab_color="#2f80ed",
        )

    message = str(exc.value)
    assert "mg.sheet" in message
    assert "mg.sheet.resize" not in message
    assert "mg.sheet.gridlines" not in message
    assert "mg.sheet.tab" not in message


def test_duplicate_sheet_into_uses_native_duplicate_when_available():
    mg, duplicate_mock, driver, select_mock = _duplicate_mg(has_duplicate=True)

    duplicated = duplicate_sheet_into(
        mg,
        sheet_url="https://docs.google.com/spreadsheets/d/test",
        source_sheet_name="202501",
        new_sheet_name="202502",
        cell_update={"cell": "B1", "value": "202502"},
    )

    assert duplicated is True
    mg.open.sheet.assert_called_once_with("https://docs.google.com/spreadsheets/d/test")
    duplicate_mock.assert_called_once_with(
        "202501",
        "202502",
        cell_update={"cell": "B1", "value": "202502"},
    )
    driver.duplicate_sheet.assert_not_called()
    select_mock.assert_not_called()


def test_duplicate_sheet_into_falls_back_to_driver_duplicate():
    mg, _, driver, select_mock = _duplicate_mg(has_duplicate=False)

    duplicated = duplicate_sheet_into(
        mg,
        sheet_url="https://docs.google.com/spreadsheets/d/test",
        source_sheet_name="202501",
        new_sheet_name="202502",
        cell_update={"cell": "B1", "value": "202502"},
    )

    assert duplicated is True
    driver.worksheet.assert_called_once_with("202501")
    driver.duplicate_sheet.assert_called_once_with(101, new_sheet_name="202502")
    select_mock.assert_called_once_with("202502")
    mg.sheet.cell.set.assert_called_once_with("B1", "202502")


def test_write_sheet_blocks_creates_selects_and_updates():
    worksheet = SimpleNamespace(update=Mock(), clear=Mock())
    sheet = SimpleNamespace(create=Mock(), select=Mock(return_value=True), _driver=worksheet)
    mg = SimpleNamespace(
        open=SimpleNamespace(sheet=Mock(return_value=True)),
        gs=SimpleNamespace(sheets=["config"], sheet=sheet, _driver=SimpleNamespace()),
        sheets=SimpleNamespace(select=Mock(return_value=True)),
    )

    wrote = write_sheet_blocks(
        mg,
        sheet_url="https://docs.google.com/spreadsheets/d/test",
        sheet_name="MONTHLY",
        blocks=[("A1", [["Title"]]), ("A2", [["2026"]])],
        clear_sheet=True,
    )

    assert wrote is True
    mg.open.sheet.assert_called_once_with("https://docs.google.com/spreadsheets/d/test")
    sheet.create.assert_called_once_with("MONTHLY")
    mg.sheets.select.assert_called_once_with("MONTHLY")
    worksheet.clear.assert_called_once()
    worksheet.update.assert_any_call("A1", [["Title"]])
    worksheet.update.assert_any_call("A2", [["2026"]])


class _BatchSpreadsheet:
    def __init__(self):
        self.requests = []
        self.metadata = {
            "sheets": [
                {
                    "properties": {
                        "title": "Data",
                        "sheetId": 10,
                        "gridProperties": {"rowCount": 100, "columnCount": 5},
                    }
                }
            ]
        }

    def fetch_sheet_metadata(self, params=None):
        self.params = params
        return self.metadata

    def batch_update(self, body):
        self.requests.append(body)
        return {"replies": [{} for _ in body.get("requests", [])]}


def test_request_builders():
    assert add_sheet_request(
        "New",
        rows=10,
        cols=3,
        frozen_rows=1,
        hide_gridlines=True,
        tab_color={"red": 1.0},
    ) == {
        "addSheet": {
            "properties": {
                "title": "New",
                "gridProperties": {
                    "rowCount": 10,
                    "columnCount": 3,
                    "frozenRowCount": 1,
                    "hideGridlines": True,
                },
                "tabColor": {"red": 1.0},
            }
        }
    }
    assert update_sheet_properties_request(10, title="Renamed", index=2) == {
        "updateSheetProperties": {
            "properties": {"sheetId": 10, "title": "Renamed", "index": 2},
            "fields": "title,index",
        }
    }
    assert update_grid_properties_request(10, frozen_rows=1) == {
        "updateSheetProperties": {
            "properties": {"sheetId": 10, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }
    }
    assert auto_resize_dimensions_request(10, end_index=4) == {
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": 10,
                "dimension": "COLUMNS",
                "startIndex": 0,
                "endIndex": 4,
            }
        }
    }


def test_sheets_module_reexports_lowlevel_gspread_helpers():
    assert sheets_module.add_sheet_request is add_sheet_request
    assert sheets_module.ensure_min_dimensions is ensure_min_dimensions
    assert sheets_module.open_gspread_spreadsheet is sheets_module.open_spreadsheet
    assert "open_spreadsheet" not in sheets_module.__all__
    assert "open_spreadsheet" in gspread_lowlevel.__all__
    assert "open_gspread_spreadsheet" not in gspread_lowlevel.__all__


def test_batch_helpers_resolve_sheet_id_and_create_delete_freeze_resize():
    spreadsheet = _BatchSpreadsheet()

    assert get_sheet_id(spreadsheet, "Data") == 10
    assert ensure_sheet_exists(spreadsheet, "Data") is False
    assert ensure_sheet_exists(spreadsheet, "New", rows=20, cols=4) is True
    set_frozen_rows(spreadsheet, "Data", 2)
    requests = ensure_min_dimensions(spreadsheet, "Data", min_rows=100, min_cols=8)
    assert len(requests) == 1
    assert delete_sheet_if_exists(spreadsheet, "Data") is True

    sent = [req for call in spreadsheet.requests for req in call["requests"]]
    assert sent[0]["addSheet"]["properties"]["title"] == "New"
    assert sent[1]["updateSheetProperties"]["properties"]["gridProperties"] == {
        "frozenRowCount": 2
    }
    assert sent[2]["appendDimension"] == {
        "sheetId": 10,
        "dimension": "COLUMNS",
        "length": 3,
    }
    assert sent[3]["deleteSheet"]["sheetId"] == 10
