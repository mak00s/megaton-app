"""Notebook 初期化ヘルパー

notebooks/ 配下から lib を使うための共通セットアップ。
セットアップセルで `from megaton_lib.notebook import init` → `init()` するだけで:
- sys.path にプロジェクトルートを追加
- MEGATON_CREDS_PATH を設定
- lib モジュールを最新版にリロード
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def init() -> Path:
    """プロジェクトルートを検出し、パス・環境変数・モジュールを初期化する。

    Returns:
        プロジェクトルートの Path
    """
    root = _find_project_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    os.environ["MEGATON_CREDS_PATH"] = str(root / "credentials")

    _reload_lib()

    return root


def _find_project_root() -> Path:
    """CWD から上方向に lib/ ディレクトリを探してプロジェクトルートを返す。"""
    d = Path.cwd()
    while d != d.parent:
        if (d / "lib").is_dir():
            return d
        d = d.parent
    raise RuntimeError("Project root not found (no 'lib/' directory in parents)")


def _reload_lib() -> None:
    """lib 配下のモジュールを依存順にリロードする。"""
    import megaton_lib.credentials
    import megaton_lib.megaton_client
    import megaton_lib.analysis

    importlib.reload(lib.credentials)
    importlib.reload(lib.megaton_client)
    importlib.reload(lib.analysis)

    from megaton_lib.megaton_client import reset_registry
    reset_registry()
