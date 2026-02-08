from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton_lib.sheets import save_sheet_from_template


@dataclass
class _FakeSheet:
    ids: dict[str, int]
    id: int | None = None

    def select(self, name: str) -> bool:
        if name not in self.ids:
            return False
        self.id = self.ids[name]
        return True


def _fake_mg(*, sheets: list[str], ids: dict[str, int]):
    driver = SimpleNamespace(duplicate_sheet=Mock())
    gs = SimpleNamespace(
        _driver=driver,
        sheet=_FakeSheet(ids=ids),
        sheets=sheets,
    )
    save = SimpleNamespace(to=SimpleNamespace(sheet=Mock()))
    return SimpleNamespace(gs=gs, save=save)


def test_skip_empty_df_does_not_duplicate_or_write():
    mg = _fake_mg(sheets=["202501"], ids={"202501": 1})
    df = pd.DataFrame()

    wrote = save_sheet_from_template(mg, "202502", df, start_row=3)

    assert wrote is False
    assert mg.gs._driver.duplicate_sheet.call_count == 0
    assert mg.save.to.sheet.call_count == 0


def test_duplicate_when_missing_then_write():
    mg = _fake_mg(sheets=["202501", "misc"], ids={"202501": 101, "misc": 9})
    df = pd.DataFrame({"a": [1]})

    wrote = save_sheet_from_template(mg, "202502", df, start_row=3)

    assert wrote is True
    mg.gs._driver.duplicate_sheet.assert_called_once_with(101, new_sheet_name="202502")
    mg.save.to.sheet.assert_called_once()


def test_no_duplicate_when_exists_write_only():
    mg = _fake_mg(sheets=["202502"], ids={"202502": 202})
    df = pd.DataFrame({"a": [1]})

    wrote = save_sheet_from_template(mg, "202502", df, start_row=3)

    assert wrote is True
    assert mg.gs._driver.duplicate_sheet.call_count == 0
    mg.save.to.sheet.assert_called_once_with("202502", df, start_row=3)


def test_template_sheet_not_found_raises():
    mg = _fake_mg(sheets=["202501"], ids={"202501": 1})
    df = pd.DataFrame({"a": [1]})

    with pytest.raises(ValueError, match="Template worksheet not found"):
        save_sheet_from_template(mg, "202502", df, template_sheet="missing")


def test_create_if_missing_false_skips():
    mg = _fake_mg(sheets=["202501"], ids={"202501": 1})
    df = pd.DataFrame({"a": [1]})

    wrote = save_sheet_from_template(mg, "202502", df, create_if_missing=False)

    assert wrote is False
    assert mg.gs._driver.duplicate_sheet.call_count == 0
    assert mg.save.to.sheet.call_count == 0

