"""Jupytext percent format ノートブックを CLI から実行する。

パラメータセル（tags=["parameters"]）の変数を外から上書きでき、
GitHub Actions での定期実行に対応。matplotlib は Agg バックエンドで
plt.show() を無効化する。

Usage:
    python scripts/run_notebook.py <notebook.py> [-p KEY=VALUE ...]

Examples:
    # デフォルトパラメータで実行
    python scripts/run_notebook.py notebooks/reports/yokohama_cv.py

    # 日付テンプレートで上書き
    python scripts/run_notebook.py notebooks/reports/yokohama_cv.py \\
      -p START_DATE=today-30d -p END_DATE=today
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# --- セルパース ----------------------------------------------------------

_CELL_MARKER = re.compile(r"^# %%(.*)$")
_PARAMS_TAG = re.compile(r'tags\s*=\s*\[.*"parameters".*\]')
_HEADER_END = re.compile(r"^# ---\s*$")


def extract_cells(source: str) -> list[dict]:
    """Jupytext percent format の .py をセルのリストに分割する。

    Returns:
        list of {"marker": str, "source": str, "is_params": bool}
        先頭の YAML ヘッダー（# --- で囲まれた部分）は除外する。
    """
    lines = source.splitlines(keepends=True)
    cells: list[dict] = []

    # --- YAML ヘッダーをスキップ ---
    i = 0
    if lines and lines[0].strip() == "# ---":
        i = 1
        while i < len(lines):
            if _HEADER_END.match(lines[i].strip()) and i > 0:
                i += 1
                break
            i += 1

    # --- セルに分割 ---
    current_marker = ""
    current_lines: list[str] = []

    while i < len(lines):
        m = _CELL_MARKER.match(lines[i].rstrip())
        if m:
            # 前のセルを保存
            if current_marker or current_lines:
                cells.append(_make_cell(current_marker, current_lines))
            current_marker = lines[i].rstrip()
            current_lines = []
        else:
            current_lines.append(lines[i])
        i += 1

    # 最後のセル
    if current_marker or current_lines:
        cells.append(_make_cell(current_marker, current_lines))

    return cells


def _make_cell(marker: str, lines: list[str]) -> dict:
    source = "".join(lines)
    is_params = bool(_PARAMS_TAG.search(marker))
    return {"marker": marker, "source": source, "is_params": is_params}


# --- パラメータ注入 ------------------------------------------------------

_ASSIGN_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)\s*=\s*(.+)$", re.MULTILINE)


def _resolve_value(raw: str) -> str:
    """値が日付テンプレートなら解決し、そうでなければそのまま返す。"""
    try:
        from lib.date_template import resolve_date
        return resolve_date(raw)
    except (ValueError, ImportError):
        return raw


def _format_value(value: str) -> str:
    """Python リテラルとしてフォーマットする。

    数値はそのまま、それ以外は文字列として引用符で囲む。
    """
    # int / float 判定
    try:
        int(value)
        return value
    except ValueError:
        pass
    try:
        float(value)
        return value
    except ValueError:
        pass
    # 文字列（安全にエスケープ）
    return repr(value)


def inject_params(cells: list[dict], overrides: dict[str, str]) -> list[dict]:
    """parameters セルの変数を overrides で上書きする。

    日付テンプレート（today-7d 等）は自動的に解決される。
    overrides に含まれるがセルに存在しないキーは無視する。
    """
    if not overrides:
        return cells

    result = []
    for cell in cells:
        if not cell["is_params"]:
            result.append(cell)
            continue

        new_source = cell["source"]
        for key, raw_value in overrides.items():
            resolved = _resolve_value(raw_value)
            formatted = _format_value(resolved)
            # KEY = ... の行を置換
            pattern = re.compile(
                rf"^({re.escape(key)}\s*=\s*)(.+)$", re.MULTILINE
            )
            new_source = pattern.sub(rf"\g<1>{formatted}", new_source)

        result.append({**cell, "source": new_source})
    return result


# --- 実行 ----------------------------------------------------------------


def run(notebook_path: str, overrides: dict[str, str]) -> None:
    """ノートブックをスクリプトとして実行する。"""
    os.environ.setdefault("MPLBACKEND", "Agg")

    nb = Path(notebook_path).resolve()
    if not nb.exists():
        raise FileNotFoundError(f"Notebook not found: {nb}")

    source = nb.read_text(encoding="utf-8")
    cells = extract_cells(source)
    cells = inject_params(cells, overrides)

    # markdown セルを除外してスクリプトを組み立て
    code_parts = []
    for cell in cells:
        if "# %% [markdown]" in cell["marker"]:
            continue
        code_parts.append(cell["source"])

    script = "\n".join(code_parts)

    # CWD をノートブックのディレクトリに設定
    original_cwd = os.getcwd()
    os.chdir(nb.parent)
    try:
        compiled = compile(script, str(nb), "exec")
        exec_globals = {"__name__": "__main__", "__file__": str(nb)}
        exec(compiled, exec_globals)
    finally:
        os.chdir(original_cwd)


# --- CLI -----------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a jupytext percent-format notebook from CLI",
    )
    parser.add_argument("notebook", help="Path to .py notebook")
    parser.add_argument(
        "-p", "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a parameter (repeatable). Date templates supported.",
    )
    return parser.parse_args(argv)


def _parse_param_pairs(params: list[str]) -> dict[str, str]:
    """['-p', 'K=V', ...] 形式のリストを dict に変換する。"""
    result = {}
    for p in params:
        if "=" not in p:
            raise ValueError(f"Invalid param format (expected KEY=VALUE): {p}")
        key, value = p.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        overrides = _parse_param_pairs(args.param)
        run(args.notebook, overrides)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
