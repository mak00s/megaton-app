"""params.json の実質差分判定ユーティリティ"""
from __future__ import annotations

import json
from typing import Any


def canonicalize_json(data: Any) -> str:
    """JSON互換オブジェクトを正規化文字列に変換する。

    インデント・空白・キー順の差分を吸収する。
    """
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
