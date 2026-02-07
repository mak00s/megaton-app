"""AI Agent 用の分析ユーティリティ

context を浪費しないための表示ヘルパー。
- show(): 行数制限付き表示 + 任意でCSV保存
- properties() / sites(): 一覧表示

使い方:
    from lib.megaton_client import query_ga4
    from lib.analysis import show

    df = query_ga4(...)          # contextに載らない
    df = df[df["col"] != "X"]   # contextに載らない
    show(df, n=10, save="output/result.csv")  # ここだけ制限付き出力
"""

import pandas as pd
from pathlib import Path

from lib.megaton_client import get_ga4_properties, get_gsc_sites


def show(
    df: pd.DataFrame,
    n: int = 20,
    save: str | None = None,
) -> None:
    """DataFrameを行数制限付きで表示。

    Args:
        df: 表示するDataFrame
        n: 表示する最大行数（デフォルト20）
        save: CSVパス（指定時はファイル保存し、表示は先頭n行のみ）
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
    """GA4プロパティ一覧を表示。"""
    props = get_ga4_properties()
    for p in props:
        print(f"  {p['id']:>12}  {p['name']}")
    print(f"\n[{len(props)} properties]")


def sites() -> None:
    """GSCサイト一覧を表示。"""
    site_list = get_gsc_sites()
    for s in site_list:
        print(f"  {s}")
    print(f"\n[{len(site_list)} sites]")
