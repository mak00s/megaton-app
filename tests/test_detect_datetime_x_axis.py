"""Tests for detect_datetime_x_axis (defined in app/streamlit_app.py).

The function is pure (no Streamlit dependency) but lives in a module with
heavy Streamlit imports.  Rather than mocking the entire import chain, we
extract the function source at import time.
"""

import importlib.util
import textwrap
import types

import pandas as pd


def _load_detect_datetime_x_axis():
    """Extract detect_datetime_x_axis from streamlit_app.py without importing the module."""
    import ast
    from pathlib import Path

    src = (Path(__file__).parent.parent / "app" / "streamlit_app.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "detect_datetime_x_axis":
            func_src = textwrap.dedent(ast.get_source_segment(src, node))
            break
    else:
        raise RuntimeError("detect_datetime_x_axis not found in streamlit_app.py")

    ns: dict = {"pd": pd}
    exec(compile(func_src, "<detect_datetime_x_axis>", "exec"), ns)
    return ns["detect_datetime_x_axis"]


detect_datetime_x_axis = _load_detect_datetime_x_axis()


# --- empty / trivial inputs ---

def test_empty_dataframe_returns_none():
    df = pd.DataFrame()
    col, series = detect_datetime_x_axis(df)
    assert col is None
    assert series is None


def test_single_numeric_column_returns_none():
    df = pd.DataFrame({"value": [1, 2, 3]})
    col, series = detect_datetime_x_axis(df)
    assert col is None
    assert series is None


# --- "date" column priority ---

def test_date_column_is_prioritised():
    df = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-02"],
        "event_time": pd.to_datetime(["2026-03-01", "2026-03-02"]),
    })
    col, series = detect_datetime_x_axis(df)
    assert col == "date"


def test_date_column_with_all_invalid_falls_through():
    """If 'date' column exists but contains no valid dates, skip it."""
    df = pd.DataFrame({
        "date": ["not-a-date", "also-not"],
        "created_at": pd.to_datetime(["2026-01-01", "2026-01-02"]),
    })
    col, series = detect_datetime_x_axis(df)
    assert col == "created_at"


# --- native datetime64 columns ---

def test_native_datetime_column_detected():
    df = pd.DataFrame({
        "id": [1, 2],
        "created_at": pd.to_datetime(["2026-01-01", "2026-02-01"]),
    })
    col, series = detect_datetime_x_axis(df)
    assert col == "created_at"


# --- string heuristic with 80% threshold ---

def test_string_dates_above_threshold_detected():
    """8 out of 10 valid → 80% → detected."""
    values = [f"2026-01-{i:02d}" for i in range(1, 9)] + ["bad", "also-bad"]
    df = pd.DataFrame({"event_date": values})
    col, series = detect_datetime_x_axis(df)
    assert col == "event_date"
    assert series is not None


def test_string_dates_below_threshold_not_detected():
    """7 out of 10 valid → 70% → not detected."""
    values = [f"2026-01-{i:02d}" for i in range(1, 8)] + ["x", "y", "z"]
    df = pd.DataFrame({"event_date": values})
    col, series = detect_datetime_x_axis(df)
    assert col is None
    assert series is None


def test_threshold_boundary_exactly_80_percent():
    """Exactly 80% → detected (>= 0.8)."""
    values = ["2026-01-01"] * 4 + ["bad"]  # 4/5 = 0.8
    df = pd.DataFrame({"day": values})
    col, series = detect_datetime_x_axis(df)
    assert col == "day"


# --- hinted column names are tried first ---

def test_hinted_name_preferred_over_unhinted():
    """A column named 'month' should be tried before 'category'."""
    df = pd.DataFrame({
        "category": ["2026-01-01", "2026-02-01", "2026-03-01"],
        "month": ["2026-04", "2026-05", "2026-06"],
    })
    col, _series = detect_datetime_x_axis(df)
    assert col == "month"


# --- numeric columns not mistakenly detected ---

def test_numeric_int_column_skipped():
    """Integer columns like IDs should not be parsed as dates."""
    df = pd.DataFrame({"user_id": [20260301, 20260302, 20260303]})
    col, series = detect_datetime_x_axis(df)
    assert col is None


def test_numeric_float_column_skipped():
    df = pd.DataFrame({"score": [1.5, 2.5, 3.5]})
    col, series = detect_datetime_x_axis(df)
    assert col is None


# --- returned series is correctly parsed ---

def test_returned_series_is_datetime():
    df = pd.DataFrame({"date": ["2026-01-01", "2026-01-02"]})
    col, series = detect_datetime_x_axis(df)
    assert col == "date"
    assert pd.api.types.is_datetime64_any_dtype(series)
    assert len(series) == 2
