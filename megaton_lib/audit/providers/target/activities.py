"""Adobe Target activities export helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from megaton_lib.audit.providers.target.client import AdobeTargetClient


def load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def parse_ids(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def resolve_activity_ids(index_path: Path, raw_ids: str = "") -> list[int]:
    if raw_ids:
        return parse_ids(raw_ids)

    index_data = load_json(index_path)
    if isinstance(index_data, dict):
        activities = index_data.get("activities", [])
        if isinstance(activities, list):
            ids: list[int] = []
            for item in activities:
                if isinstance(item, dict) and isinstance(item.get("id"), int):
                    ids.append(item["id"])
            if ids:
                return ids

    raise ValueError(
        f"No activity IDs found. Pass raw_ids or create {index_path}"
    )


def fetch_activity(client: AdobeTargetClient, tenant_id: str, activity_id: int) -> dict[str, Any]:
    url = f"{client.config.base_url.rstrip('/')}/{tenant_id}/target/activities/ab/{activity_id}"
    result = client._request("GET", url)
    if not isinstance(result, dict):
        raise RuntimeError(f"Unexpected non-object response for activity {activity_id}")
    return result


def export_activities(
    client: AdobeTargetClient,
    tenant_id: str,
    output_root: str | Path,
    activity_ids: list[int],
) -> dict[str, Any]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    index_items: list[dict[str, object]] = []
    for activity_id in activity_ids:
        activity = fetch_activity(client, tenant_id, activity_id)
        (root / f"{activity_id}.json").write_text(
            json.dumps(activity, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        index_items.append(
            {
                "id": activity_id,
                "file": f"{activity_id}.json",
                "name": activity.get("name", ""),
            }
        )

    index_payload = {
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "activities": index_items,
    }
    (root / "index.json").write_text(
        json.dumps(index_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return index_payload
