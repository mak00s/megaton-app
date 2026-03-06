"""Adobe Target Feeds export.

Mirrors ``export_target_feeds.sh`` from at-recs.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from megaton_lib.audit.providers.target.client import AdobeTargetClient

# Sensitive keys whose values are redacted during export
_SENSITIVE_KEYS = re.compile(
    r"(password|passwd|secret|token|api_key|apikey|auth|credential|username)",
    re.IGNORECASE,
)
_REDACTED = "***REDACTED***"


def export_feeds(
    client: AdobeTargetClient,
    output_root: str | Path,
    feed_names: list[str],
) -> dict[str, Any]:
    """Export selected Target feeds to local files.

    Parameters
    ----------
    client : AdobeTargetClient
    output_root : directory for output
    feed_names : exact feed names to export (case-sensitive)

    Returns
    -------
    dict with ``exported`` count and ``feeds`` list.
    """
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    # Fetch all feeds
    all_feeds = client.get_all("/feeds")

    # Filter by name (exact match)
    name_set = set(feed_names)
    matched = [f for f in all_feeds if f.get("name") in name_set]

    index_entries: list[dict[str, Any]] = []
    for feed in matched:
        feed_id = feed.get("id", "unknown")
        name = feed.get("name", str(feed_id))

        # Fetch detail
        try:
            detail = client.get(f"/feeds/{feed_id}")
        except RuntimeError:
            detail = feed

        if isinstance(detail, list):
            detail = feed

        # Redact sensitive fields
        detail = _redact_sensitive(detail)

        # Save
        (root / f"{feed_id}.json").write_text(
            json.dumps(detail, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        index_entries.append({"id": feed_id, "name": name})

    (root / "index.json").write_text(
        json.dumps(index_entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return {"exported": len(matched), "feeds": index_entries}


def _redact_sensitive(obj: Any) -> Any:
    """Recursively redact sensitive fields in a JSON-like structure."""
    if isinstance(obj, dict):
        return {
            k: (_REDACTED if _SENSITIVE_KEYS.search(k) and isinstance(v, str) else _redact_sensitive(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_sensitive(item) for item in obj]
    return obj
