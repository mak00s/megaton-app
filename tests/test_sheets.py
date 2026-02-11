from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton_lib.sheets import save_sheet_from_template, upsert_or_skip


@dataclass
class _FakeSheet:
    ids: dict[str, int]
    id: int | None = None

    def select(self, name: str) -> bool:
        # In real usage, the sheet may be created/duplicated just before select.
        if name not in self.ids:
            self.ids[name] = max(self.ids.values() or [0]) + 1
        self.id = self.ids[name]
        return True

    def clear(self):
        return True

    _driver = None


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


def test_create_when_no_template_matches_regex():
    mg = _fake_mg(sheets=["_article-m"], ids={"_article-m": 1})
    # Provide a create method to emulate gsheet API
    mg.gs.sheet.create = Mock()
    df = pd.DataFrame({"a": [1]})

    wrote = save_sheet_from_template(mg, "202502", df, template_regex=r"^\\d{6}$")

    assert wrote is True
    assert mg.gs._driver.duplicate_sheet.call_count == 0
    mg.gs.sheet.create.assert_called_once_with("202502")
    mg.save.to.sheet.assert_called_once()


# ── upsert_or_skip ──────────────────────────────────────────────


def _upsert_mg():
    """Create a minimal mg mock for upsert_or_skip tests."""
    upsert_mock = Mock()
    mg = SimpleNamespace(upsert=SimpleNamespace(to=SimpleNamespace(sheet=upsert_mock)))
    return mg, upsert_mock


def test_upsert_or_skip_calls_upsert_when_data_present():
    mg, mock = _upsert_mg()
    df = pd.DataFrame({"month": ["2024-01"], "page": ["/a"], "pv": [10]})

    result = upsert_or_skip(mg, "_article-m", df, keys=["month", "page"])

    assert result is True
    mock.assert_called_once_with(
        "_article-m", df, keys=["month", "page"], sort_by=["month", "page"],
    )


def test_upsert_or_skip_skips_empty_dataframe():
    mg, mock = _upsert_mg()
    df = pd.DataFrame()

    result = upsert_or_skip(mg, "_article-m", df, keys=["month", "page"])

    assert result is False
    mock.assert_not_called()


def test_upsert_or_skip_skips_none():
    mg, mock = _upsert_mg()

    result = upsert_or_skip(mg, "_article-m", None, keys=["month"])

    assert result is False
    mock.assert_not_called()


def test_upsert_or_skip_custom_sort_by():
    mg, mock = _upsert_mg()
    df = pd.DataFrame({"a": [1], "b": [2]})

    upsert_or_skip(mg, "sheet", df, keys=["a"], sort_by=["b", "a"])

    mock.assert_called_once_with("sheet", df, keys=["a"], sort_by=["b", "a"])


def test_upsert_or_skip_forwards_extra_kwargs():
    mg, mock = _upsert_mg()
    df = pd.DataFrame({"a": [1], "link": ["x"], "ts": ["now"]})

    upsert_or_skip(mg, "_link", df,
                   keys=["a", "link"], columns=["a", "link", "ts"])

    mock.assert_called_once_with(
        "_link", df,
        keys=["a", "link"], sort_by=["a", "link"],
        columns=["a", "link", "ts"],
    )
