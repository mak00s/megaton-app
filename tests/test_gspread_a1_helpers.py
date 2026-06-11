from __future__ import annotations

from datetime import datetime

import pytest

from megaton_lib.gspread_lowlevel import column_label, gs_serial_to_date


def test_column_label_converts_one_based_indexes() -> None:
    assert column_label(1) == "A"
    assert column_label(26) == "Z"
    assert column_label(27) == "AA"
    assert column_label(52) == "AZ"


def test_column_label_rejects_non_positive_indexes() -> None:
    with pytest.raises(ValueError, match="Column index must be >= 1"):
        column_label(0)


def test_gs_serial_to_date_handles_google_sheets_serials() -> None:
    assert gs_serial_to_date(1) == datetime(1899, 12, 31)
    assert gs_serial_to_date("45000") == datetime(2023, 3, 15)
    assert gs_serial_to_date("") is None
    assert gs_serial_to_date(None) is None
    assert gs_serial_to_date("not-a-date") is None
