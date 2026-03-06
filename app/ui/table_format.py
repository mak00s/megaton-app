"""Display formatting helpers for tabular query results."""
from __future__ import annotations

import math
import warnings

import pandas as pd


def _parse_date_like_series(series: pd.Series) -> pd.Series:
    """Parse common date-like text formats without noisy fallback warnings."""
    text = series.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    is_ymd = text.str.match(r"^\d{4}-\d{1,2}-\d{1,2}$", na=False)
    is_mdy = text.str.match(r"^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}$", na=False)
    if is_ymd.any():
        parsed.loc[is_ymd] = pd.to_datetime(text.loc[is_ymd], errors="coerce", format="%Y-%m-%d")
    if is_mdy.any():
        parsed.loc[is_mdy] = pd.to_datetime(text.loc[is_mdy], errors="coerce", format="%b %d, %Y")
    return parsed


def _normalize_percent_value(num: float) -> float:
    """Auto-normalize ratio-like values to percent values."""
    abs_num = abs(num)
    if abs_num <= 1.0:
        return num * 100.0
    # Treat fractional values in (1, 2) as likely ratio-like (e.g. 1.5 -> 150%).
    if 1.0 < abs_num < 2.0 and not math.isclose(abs_num, round(abs_num), abs_tol=1e-9):
        return num * 100.0
    return num


def _format_number(value: object, *, kind: str, decimals: int, thousands_sep: bool) -> str:
    if pd.isna(value):
        return ""
    try:
        num = float(value)
    except Exception:
        return str(value)

    if kind == "int":
        return f"{int(round(num)):,}" if thousands_sep else str(int(round(num)))
    if kind == "percent":
        percent_val = _normalize_percent_value(num)
        fmt = f"{{:,.{decimals}f}}" if thousands_sep else f"{{:.{decimals}f}}"
        return f"{fmt.format(percent_val)}%"
    if kind == "currency":
        fmt = f"{{:,.{decimals}f}}" if thousands_sep else f"{{:.{decimals}f}}"
        return fmt.format(num)
    fmt = f"{{:,.{decimals}f}}" if thousands_sep else f"{{:.{decimals}f}}"
    return fmt.format(num)


def build_table_view_df(
    df: pd.DataFrame,
    *,
    date_format: str = "%Y-%m-%d",
    thousands_sep: bool = True,
    decimals: int = 2,
    column_types: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Build a display-only DataFrame with configurable formatting."""
    out = df.copy()
    hints = {str(k): str(v).lower() for k, v in (column_types or {}).items()}

    # Date formatting (hinted or auto-detected).
    for col in out.columns:
        series = out[col]
        hint = hints.get(str(col), "")
        if hint == "text":
            out[col] = series.astype(str)
            continue
        if pd.api.types.is_datetime64_any_dtype(series):
            out[col] = pd.to_datetime(series, errors="coerce").dt.strftime(date_format).fillna("")
            continue
        if pd.api.types.is_numeric_dtype(series):
            continue
        parsed = _parse_date_like_series(series)
        if hint == "date":
            out[col] = parsed.dt.strftime(date_format).where(parsed.notna(), series.astype(str))
            continue
        if parsed.notna().any() and float(parsed.notna().mean()) >= 0.8:
            out[col] = parsed.dt.strftime(date_format).where(parsed.notna(), series.astype(str))

    # Numeric formatting (hinted first, then auto integer formatting).
    for col in out.columns:
        hint = hints.get(str(col), "")
        base_series = pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(dtype="float64")
        if hint in {"int", "float", "currency", "percent"}:
            out[col] = base_series.map(
                lambda v: _format_number(v, kind=hint, decimals=decimals, thousands_sep=thousands_sep)
            )
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        non_na = base_series.dropna()
        if non_na.empty:
            continue
        if ((non_na - non_na.round()).abs() < 1e-9).all():
            if thousands_sep:
                out[col] = base_series.map(lambda v: f"{int(round(v)):,}" if pd.notna(v) else "")
            else:
                out[col] = base_series.map(lambda v: str(int(round(v))) if pd.notna(v) else "")

    return out


def detect_datetime_x_axis(df: pd.DataFrame) -> tuple[str | None, pd.Series | None]:
    """Pick a datetime-like column for chart X-axis when available."""

    def safe_to_datetime(series: pd.Series) -> pd.Series:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            try:
                return pd.to_datetime(series, errors="coerce", format="mixed")
            except TypeError:
                return pd.to_datetime(series, errors="coerce")

    if df.empty:
        return None, None

    if "date" in df.columns:
        parsed = safe_to_datetime(df["date"])
        if parsed.notna().any():
            return "date", parsed

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            parsed = safe_to_datetime(df[col])
            if parsed.notna().any():
                return col, parsed

    time_name_hints = ("date", "day", "time", "month", "week")
    hinted = [c for c in df.columns if any(h in str(c).lower() for h in time_name_hints)]
    others = [c for c in df.columns if c not in hinted]

    for col in [*hinted, *others]:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            continue
        parsed = safe_to_datetime(series)
        if len(parsed) == 0:
            continue
        if float(parsed.notna().mean()) >= 0.8:
            return col, parsed

    return None, None
