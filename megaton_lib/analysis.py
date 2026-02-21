"""Analysis utilities for AI agents.

Display helpers designed to reduce context usage.
- show(): row-limited display + optional CSV save
- properties() / sites(): list available resources

Usage:
    from megaton_lib.megaton_client import query_ga4
    from megaton_lib.analysis import show

    df = query_ga4(...)          # not added to context
    df = df[df["col"] != "X"]    # not added to context
    show(df, n=10, save="output/result.csv")  # only this output is shown
"""

import pandas as pd
from pathlib import Path

from megaton_lib.megaton_client import get_ga4_properties, get_gsc_sites


def show(
    df: pd.DataFrame,
    n: int = 20,
    save: str | None = None,
) -> None:
    """Display a DataFrame with a row limit.

    Args:
        df: DataFrame to display.
        n: Maximum number of rows to show (default: 20).
        save: CSV path. If set, saves full data and prints only top n rows.
    """
    if n <= 0:
        raise ValueError("n must be >= 1")

    if save:
        Path(save).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save, index=False)

    total = len(df)
    if total <= n:
        print(df.to_string(index=False))
    else:
        print(df.head(n).to_string(index=False))
        print(f"... ({total - n} more rows)")

    print(f"\n[{total} rows x {len(df.columns)} cols]", end="")
    if save:
        print(f" → saved: {save}", end="")
    print()


def properties() -> None:
    """Display GA4 properties."""
    props = get_ga4_properties()
    for p in props:
        print(f"  {p['id']:>12}  {p['name']}")
    print(f"\n[{len(props)} properties]")


def sites() -> None:
    """Display GSC sites."""
    site_list = get_gsc_sites()
    for s in site_list:
        print(f"  {s}")
    print(f"\n[{len(site_list)} sites]")
