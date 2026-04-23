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


def _v3_activity_client(client: AdobeTargetClient) -> AdobeTargetClient:
    """Return a Target client configured for Admin API v3 activity endpoints."""
    return client.with_accept_header("application/vnd.adobe.target.v3+json")


def fetch_activity(client: AdobeTargetClient, tenant_id: str, activity_id: int) -> dict[str, Any]:
    """Fetch one activity detail JSON, supporting both AB and XT/options activities.

    Activity detail endpoints require Admin API v3.  Older v1 defaults return
    ``409 Cannot access activity with options in this version of API`` for XT /
    options-backed activities.
    """
    v3_client = _v3_activity_client(client)
    base_url = v3_client.config.base_url.rstrip("/")
    last_error: Exception | None = None

    for activity_type in ("ab", "xt"):
        url = f"{base_url}/{tenant_id}/target/activities/{activity_type}/{activity_id}"
        try:
            result = v3_client._request("GET", url)
        except RuntimeError as exc:
            last_error = exc
            message = str(exc)
            if "HTTP 404" in message:
                continue
            raise
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected non-object response for activity {activity_id}")
        return result

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Activity not found: {activity_id}")


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
