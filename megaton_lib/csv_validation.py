"""Validation of generated analytics CSVs against their source snapshots."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd


def _fingerprint(values: list[str]) -> str:
    h = hashlib.sha256()
    for value in sorted(values):
        h.update(value.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def validate_generated_import_csv(
    *,
    df_expected: pd.DataFrame,
    csv_path: Path,
    key_column: str,
    required_headers: list[str] | None = None,
    compare_all_columns_by_key: bool = False,
) -> dict[str, object]:
    """Validate a UTF-8 (optionally BOM-prefixed) analytics CSV snapshot.

    Header order, row count and unique nonblank keys must match. Row order
    is ignored. Value comparison is opt-in; expected missing values use the
    existing ``(not set)`` convention. No files are changed and no API is called.
    Raises RuntimeError for mismatches; pandas read/parse errors propagate.
    """
    if not csv_path.exists():
        raise RuntimeError(f"Generated CSV not found: {csv_path}")

    df_actual = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8")
    expected_headers = [str(c) for c in df_expected.columns.tolist()]
    actual_headers = [str(c) for c in df_actual.columns.tolist()]

    if required_headers is not None:
        required = [str(c) for c in required_headers]
        if actual_headers != required:
            raise RuntimeError(
                f"Generated CSV header mismatch. actual={actual_headers} required={required}"
            )

    if actual_headers != expected_headers:
        raise RuntimeError(
            f"Generated CSV header mismatch against source DataFrame. "
            f"actual={actual_headers} expected={expected_headers}"
        )

    expected_count = int(len(df_expected))
    actual_count = int(len(df_actual))
    if actual_count != expected_count:
        raise RuntimeError(
            f"Generated CSV row count mismatch. actual={actual_count} expected={expected_count}"
        )

    if key_column not in df_expected.columns or key_column not in df_actual.columns:
        raise RuntimeError(f"Key column missing in generated CSV validation: {key_column}")

    expected_keys = df_expected[key_column].astype(str).str.strip()
    actual_keys = df_actual[key_column].astype(str).str.strip()

    if expected_keys.eq("").any() or actual_keys.eq("").any():
        raise RuntimeError(f"Generated CSV has empty key values in column: {key_column}")

    if expected_keys.duplicated().any() or actual_keys.duplicated().any():
        raise RuntimeError(f"Generated CSV has duplicate keys in column: {key_column}")

    expected_fingerprint = _fingerprint(expected_keys.tolist())
    actual_fingerprint = _fingerprint(actual_keys.tolist())
    if actual_fingerprint != expected_fingerprint:
        raise RuntimeError(
            "Generated CSV key fingerprint mismatch. "
            "CSV content does not match in-memory source snapshot."
        )

    mismatch_rows = 0
    if compare_all_columns_by_key:
        df_e = df_expected.copy().fillna("(not set)").astype(str)
        df_a = df_actual.copy().fillna("(not set)").astype(str)

        # Align rows on the same stripped keys the fingerprint check validated.
        df_e[key_column] = expected_keys.to_numpy()
        df_a[key_column] = actual_keys.to_numpy()
        e = df_e.set_index(key_column).sort_index()
        a = df_a.set_index(key_column).sort_index()
        if list(e.columns) != list(a.columns):
            raise RuntimeError(
                f"Generated CSV value-columns mismatch: expected={list(e.columns)} actual={list(a.columns)}"
            )
        diff_mask = (e != a).any(axis=1)
        mismatch_rows = int(diff_mask.sum())
        if mismatch_rows > 0:
            sample_keys = diff_mask[diff_mask].index.tolist()[:5]
            raise RuntimeError(
                f"Generated CSV differs from expected source values. "
                f"mismatch_rows={mismatch_rows} sample_keys={sample_keys}"
            )

    return {
        "file": str(csv_path),
        "row_count": actual_count,
        "key_column": key_column,
        "key_fingerprint": actual_fingerprint,
        "mismatch_rows": mismatch_rows,
    }
