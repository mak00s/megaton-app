"""Utility for effective-diff checks on params.json."""
from __future__ import annotations

import json
from typing import Any


def canonicalize_json(data: Any) -> str:
    """Convert a JSON-compatible object into a canonical string.

    Absorbs differences in indentation, whitespace, and key order.
    """
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
