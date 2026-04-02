from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from megaton_lib.sheets import duplicate_sheet_into, save_sheet_table, write_sheet_blocks


def _save_mg():
    save_sheet_mock = Mock()
    mg = SimpleNamespace(
        open=SimpleNamespace(sheet=Mock(return_value=True)),
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
