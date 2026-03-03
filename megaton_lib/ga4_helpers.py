"""Shared GA4 helper functions for report execution and DataFrame conversion."""

from __future__ import annotations

from collections.abc import Callable
from typing import Iterable

import pandas as pd


def run_report_df(
    mg,
    dimensions,
    metrics,
    *,
    filter_d=None,
    sort=None,
    limit=None,
) -> pd.DataFrame:
    """Execute ``mg.report.run`` and return a DataFrame.

    Returns an empty DataFrame when the report result is ``None``.
    """
    kwargs: dict = {
        "d": dimensions,
        "m": metrics,
        "filter_d": filter_d,
        "sort": sort,
        "show": False,
    }
    if limit is not None:
        kwargs["limit"] = limit

    result = mg.report.run(**kwargs)
    if result is None:
        return pd.DataFrame()
    return result.df


def merge_dataframes(
    frames: list[pd.DataFrame | None],
    *,
    on: str | list[str],
    how: str = "left",
    int_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Merge multiple DataFrames in order, skipping ``None``/empty frames."""
    if not frames:
        return pd.DataFrame()
    base = frames[0]
    if base is None:
        return pd.DataFrame()
    merged = base.copy()

    for df in frames[1:]:
        if df is None or len(df) == 0:
            continue
        merged = merged.merge(df, on=on, how=how)

    if int_cols:
        for col in int_cols:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).astype(int)
    return merged


def collect_site_frames(
    mg,
    sites_df: pd.DataFrame,
    *,
    fetch_fn: Callable[[object, pd.Series, str, str], pd.DataFrame | None],
    clinic_col: str = "clinic",
    property_col: str = "ga4_property_id",
    skip_clinics: set[str] | None = None,
    warn_label: str = "ga4",
    warn_on_missing_property: bool = False,
) -> list[pd.DataFrame]:
    """Collect per-site DataFrames with shared GA4 property loop.

    Args:
        mg: Megaton instance.
        sites_df: Site settings table.
        fetch_fn: Callback called as ``fetch_fn(mg, site, clinic, ga4_id)``.
        clinic_col: Clinic column name.
        property_col: GA4 property ID column name.
        skip_clinics: Clinic names to skip.
        warn_label: Prefix for warning logs.
        warn_on_missing_property: Print warning when GA4 property is empty.
    """
    frames: list[pd.DataFrame] = []
    skip = set(skip_clinics or set())

    for _, site in sites_df.iterrows():
        clinic = str(site.get(clinic_col, "") or "").strip()
        if clinic in skip:
            continue

        ga4_id = str(site.get(property_col, "") or "").strip()
        if not ga4_id:
            if warn_on_missing_property:
                print(f"[warn] skip clinic={clinic}: {property_col} missing")
            continue

        try:
            mg.ga["4"].property.id = ga4_id
            df = fetch_fn(mg, site, clinic, ga4_id)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception as exc:
            print(f"[warn] {warn_label} clinic failed: {clinic} ({ga4_id}) {exc}")

    return frames


def fetch_named_clinic_report_data_or_empty(
    mg,
    sites_df: pd.DataFrame,
    *,
    clinic_name: str,
    dimensions,
    metrics,
    expected_cols: list[str],
    filter_d: str | None = None,
    clinic_col: str = "clinic",
    property_col: str = "ga4_property_id",
    set_dates: tuple[str, str] | None = None,
    warn_label: str = "ga4",
) -> pd.DataFrame:
    """Run a report for one clinic row in ``sites_df`` and return shaped data.

    Returns an empty DataFrame with ``expected_cols`` when:
    - clinic row is missing
    - GA4 property is missing
    - report execution fails
    """
    if clinic_col not in sites_df.columns:
        return pd.DataFrame(columns=expected_cols)

    row_df = sites_df[sites_df[clinic_col].astype(str).str.strip() == clinic_name]
    if row_df.empty:
        return pd.DataFrame(columns=expected_cols)

    row = row_df.iloc[0]
    ga4_id = str(row.get(property_col, "") or "").strip()
    if not ga4_id:
        print(f"[warn] {warn_label} clinic={clinic_name}: {property_col} missing")
        return pd.DataFrame(columns=expected_cols)

    try:
        mg.ga["4"].property.id = ga4_id
        if set_dates is not None:
            mg.report.set.dates(set_dates[0], set_dates[1])
        return run_report_data_or_empty(
            mg,
            dimensions=dimensions,
            metrics=metrics,
            expected_cols=expected_cols,
            filter_d=filter_d,
        )
    except Exception as exc:
        print(f"[warn] {warn_label} clinic failed: {clinic_name} ({ga4_id}) {exc}")
        return pd.DataFrame(columns=expected_cols)


def report_data_or_empty(mg, expected_cols: list[str]) -> pd.DataFrame:
    """Return ``mg.report.data`` as DataFrame with guaranteed columns.

    Args:
        mg: Megaton instance whose ``report.run`` was executed.
        expected_cols: Column names to guarantee and order.
    """
    df = getattr(getattr(mg, "report", None), "data", None)
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=expected_cols)
    out = pd.DataFrame(df).copy()
    for col in expected_cols:
        if col not in out.columns:
            out[col] = pd.NA
    return out[expected_cols]


def run_report_data_or_empty(
    mg,
    *,
    dimensions,
    metrics,
    expected_cols: list[str],
    filter_d: str | None = None,
) -> pd.DataFrame:
    """Run ``mg.report.run`` then return ``report.data`` with expected columns."""
    mg.report.run(
        d=dimensions,
        m=metrics,
        filter_d=filter_d,
        show=False,
    )
    return report_data_or_empty(mg, expected_cols)


def run_report_merge(
    mg,
    *,
    reports: list[dict],
    on: list[str],
    how: str = "outer",
    fillna_value=None,
) -> pd.DataFrame:
    """Run multiple reports and merge outputs on shared keys.

    Each element in ``reports`` must include:
    - ``dimensions``
    - ``metrics``
    - ``expected_cols``
    - optional ``filter_d``
    """
    merged: pd.DataFrame | None = None
    for spec in reports:
        df_part = run_report_data_or_empty(
            mg,
            dimensions=spec["dimensions"],
            metrics=spec["metrics"],
            expected_cols=spec["expected_cols"],
            filter_d=spec.get("filter_d"),
        )
        merged = df_part if merged is None else merged.merge(df_part, on=on, how=how)

    if merged is None:
        merged = pd.DataFrame()
    if fillna_value is not None and not merged.empty:
        merged = merged.where(merged.notna(), fillna_value)
    return merged


def build_filter(*parts: str | None) -> str | None:
    """Build GA4 filter string by joining non-empty parts with ``;``."""
    cleaned = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return ";".join(cleaned) if cleaned else None


def to_datetime_col(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Return a copy with ``col`` converted to datetime when present."""
    if df.empty or col not in df.columns:
        return df
    out = df.copy()
    out[col] = pd.to_datetime(out[col])
    return out


def to_numeric_cols(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    fillna: int | float | None = None,
    as_int: bool = False,
) -> pd.DataFrame:
    """Return a copy with selected columns converted to numeric."""
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        series = pd.to_numeric(out[col], errors="coerce")
        if fillna is not None:
            series = series.fillna(fillna)
        if as_int:
            series = series.astype(int)
        out[col] = series
    return out
