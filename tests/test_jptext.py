from __future__ import annotations

import pandas as pd
import pytest

from megaton_lib.jptext import (
    AMT_RATE_RE,
    NUM_RE,
    YEAR_RATE_RE,
    coerce_numeric,
    parse_money,
    parse_number,
    strip_currency,
)

pytestmark = pytest.mark.unit


def test_num_re_finds_signed_decimal_with_commas():
    assert NUM_RE.search("評価額 -1,234.5 円").group(0) == "-1,234.5"


def test_parse_number_common_broker_patterns():
    assert parse_number("1,234株") == 1234.0
    assert parse_number("+5.5%") == 5.5
    assert parse_number("--") is None
    assert parse_number(None) is None
    assert parse_number("") is None


def test_parse_money_yen_suffix():
    assert parse_money("残高 1,234,567 円") == 1234567.0
    assert parse_money("残高なし") is None


def test_year_rate_re_full_and_half_width_percent():
    assert YEAR_RATE_RE.search("年 0.35%").group(1) == "0.35"
    assert YEAR_RATE_RE.search("年0.02％").group(1) == "0.02"


def test_amt_rate_re_time_deposit_row():
    m = AMT_RATE_RE.search("1,000,000円 0.35%")
    assert m.group(1) == "1,000,000"
    assert m.group(2) == "0.35"


def test_strip_currency_symbols_commas_percent():
    assert strip_currency("¥20,000") == "20000"
    assert strip_currency("$2,720.39") == "2720.39"
    assert strip_currency("€66.94") == "66.94"
    assert strip_currency("0.350%") == "0.350"
    assert strip_currency("￥1,000 ％") == "1000"
    assert strip_currency(None) == ""


def test_coerce_numeric_round_trips_formatted_sheet_cells():
    df = pd.DataFrame(
        {
            "amount": ["¥20,000", "$2,720.39", "€66.94", ""],
            "rate": ["0.350%", "1.2", "", "x"],
            "name": ["a", "b", "c", "d"],
        }
    )
    out = coerce_numeric(df, ("amount", "rate", "missing_col"))
    assert out["amount"].tolist()[:3] == [20000.0, 2720.39, 66.94]
    assert pd.isna(out["amount"].iloc[3])
    assert out["rate"].tolist()[:2] == [0.35, 1.2]
    assert pd.isna(out["rate"].iloc[2]) and pd.isna(out["rate"].iloc[3])
    assert out["name"].tolist() == ["a", "b", "c", "d"]
    # original untouched
    assert df["amount"].iloc[0] == "¥20,000"


def test_coerce_numeric_empty_df_passthrough():
    df = pd.DataFrame()
    assert coerce_numeric(df, ("amount",)) is df
    assert coerce_numeric(None, ("amount",)) is None
