"""バッチ実行エンジン

configsディレクトリ内のJSONを順番に実行する。

使い方:
    python scripts/query.py --batch configs/weekly/
    python scripts/query.py --batch configs/weekly/ --json
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from lib.params_validator import validate_params


def collect_configs(batch_path: str) -> list[Path]:
    """バッチ対象のJSONファイルをファイル名順で収集。

    Args:
        batch_path: ディレクトリパス or 単一JSONファイルパス

    Returns:
        ソート済みのPathリスト

    Raises:
        FileNotFoundError: パスが存在しない
        ValueError: JSONファイルが見つからない
    """
    p = Path(batch_path)
    if not p.exists():
        raise FileNotFoundError(f"Batch path not found: {batch_path}")

    if p.is_file():
        if p.suffix != ".json":
            raise ValueError(f"Not a JSON file: {batch_path}")
        return [p]

    configs = sorted(p.glob("*.json"))
    if not configs:
        raise ValueError(f"No JSON files found in: {batch_path}")

    return configs


def _load_and_validate(config_path: Path) -> tuple[dict | None, list[dict]]:
    """1つのconfigファイルを読み込み・検証。"""
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return None, [{"error_code": "INVALID_JSON", "message": str(e)}]

    params, errors = validate_params(raw)
    if errors:
        return None, errors

    return params, []


def run_batch(
    batch_path: str,
    *,
    execute_fn: Callable[[dict, Path], dict[str, Any]],
    on_progress: Callable[[str, int, int, dict], None] | None = None,
) -> dict[str, Any]:
    """バッチ実行。

    Args:
        batch_path: configs ディレクトリまたは単一JSONファイル
        execute_fn: 1つのconfig を実行する関数。
            (params: dict, config_path: Path) -> {"status": "ok", ...} or {"status": "error", ...}
        on_progress: 各config完了時のコールバック(config_name, index, total, result)

    Returns:
        バッチ実行結果サマリ:
        {
            "total": N,
            "succeeded": N,
            "failed": N,
            "skipped": N,
            "results": [{config, status, ...}, ...],
            "elapsed_sec": float,
        }
    """
    configs = collect_configs(batch_path)
    total = len(configs)
    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    t0 = time.monotonic()

    for i, config_path in enumerate(configs):
        config_name = config_path.name
        params, errors = _load_and_validate(config_path)

        if errors:
            entry = {
                "config": config_name,
                "status": "skipped",
                "errors": errors,
            }
            skipped += 1
            results.append(entry)
            if on_progress:
                on_progress(config_name, i + 1, total, entry)
            continue

        try:
            result = execute_fn(params, config_path)
            status = result.get("status", "ok")
            entry = {
                "config": config_name,
                "status": status,
                **result,
            }
            if status == "ok":
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            entry = {
                "config": config_name,
                "status": "error",
                "error": str(e),
            }
            failed += 1

        results.append(entry)
        if on_progress:
            on_progress(config_name, i + 1, total, entry)

    elapsed = round(time.monotonic() - t0, 2)

    return {
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "results": results,
        "elapsed_sec": elapsed,
    }
