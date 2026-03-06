"""Output helpers for audit reports."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_report_json(
    report: dict[str, Any],
    *,
    output_dir: str | Path,
    file_stem: str,
) -> Path:
    """Write report payload as UTF-8 JSON and return its path."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{file_stem}_{date.today().strftime('%Y%m%d')}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_dict_csv(
    values: dict[str, Any],
    *,
    output_dir: str | Path,
    file_stem: str,
    key_name: str,
    value_name: str,
) -> Path:
    """Write dict payload as 2-column CSV and return its path."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{file_stem}_{date.today().strftime('%Y%m%d')}.csv"
    df = pd.DataFrame(
        [{key_name: k, value_name: v} for k, v in values.items()],
        columns=[key_name, value_name],
    )
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
