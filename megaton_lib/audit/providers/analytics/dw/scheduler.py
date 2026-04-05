"""Higher-level helpers for Data Warehouse template discovery and cloning."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from .api import AdobeDataWarehouseClient


def _strip_nulls(obj: Any) -> Any:
    """Recursively remove keys with None values from dicts.

    Adobe DW API distinguishes between an explicit ``null`` and an absent key;
    sending ``null`` for optional fields in create payloads causes 400 errors.
    Apply this to the final payload before POST to avoid those issues.
    """
    if isinstance(obj, dict):
        return {k: _strip_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_nulls(item) for item in obj]
    return obj


def _deep_get(data: dict[str, Any], *keys: str) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_sortable_datetime(value: Any) -> tuple[int, str]:
    text = _normalize_text(value)
    if not text:
        return (0, "")
    try:
        return (1, datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat())
    except ValueError:
        return (1, text)


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_scheduled_request_uuid(item: dict[str, Any]) -> str:
    return _normalize_text(_deep_get(item, "metadata", "scheduledRequestUUID"))


def _extract_updated_datetime(item: dict[str, Any]) -> datetime | None:
    return _parse_iso_datetime(_deep_get(item, "metadata", "updatedDate"))


def _safe_scheduled_request_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("scheduledRequests", [])
    if not isinstance(items, list):
        raise RuntimeError(f"Unexpected scheduled requests payload: {payload}")
    return [item for item in items if isinstance(item, dict)]


def _collect_same_updated_date_requests(
    client: AdobeDataWarehouseClient,
    *,
    rsid: str,
    updated_at: datetime,
    created_after: str | None = None,
    created_before: str | None = None,
    status: str | None = None,
    batch_limit: int = 100,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    updated_at_text = _to_utc_z(updated_at)
    payload = client.list_scheduled_requests(
        rsid=rsid,
        updated_after=updated_at_text,
        updated_before=updated_at_text,
        created_after=created_after,
        created_before=created_before,
        status=status,
        sort="updatedDate:asc",
        limit=batch_limit,
    )
    items = _safe_scheduled_request_items(payload)
    unique_items: dict[str, dict[str, Any]] = {}
    for item in items:
        uuid = _extract_scheduled_request_uuid(item)
        if uuid:
            unique_items[uuid] = item

    total_hint = payload.get("total")
    if isinstance(total_hint, int):
        has_more = total_hint > len(unique_items)
    else:
        has_more = len(items) >= batch_limit
    if has_more:
        raise RuntimeError(
            "Adobe Data Warehouse scheduled request pagination is ambiguous for "
            f"updatedDate={updated_at_text}: the API only exposes limit-based listing, "
            "so requests sharing the same timestamp beyond one batch cannot be collected safely."
        )
    return list(unique_items.values()), payload


def _normalize_report_range(report_range: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(report_range, dict):
        raise TypeError("report_range must be an object")

    preset = _normalize_text(report_range.get("preset"))
    start = report_range.get("startDateTime")
    end = report_range.get("endDateTime")

    if not start and report_range.get("dateFrom"):
        start = _coerce_datetime(report_range["dateFrom"], end_of_day=False)
    if not end and report_range.get("dateTo"):
        end = _coerce_datetime(report_range["dateTo"], end_of_day=True)

    if not preset and not (start and end):
        raise ValueError("report_range requires either preset or startDateTime+endDateTime")

    result: dict[str, Any] = {}
    if preset:
        result["preset"] = preset
    if start or end:
        result["startDateTime"] = _normalize_text(start) or None
        result["endDateTime"] = _normalize_text(end) or None
    elif preset:
        result["startDateTime"] = None
        result["endDateTime"] = None
    return result


def _coerce_datetime(value: Any, *, end_of_day: bool) -> str:
    text = _normalize_text(value)
    if not text:
        raise ValueError("date value is required")
    if "T" in text:
        return text
    suffix = "23:59:59Z" if end_of_day else "00:00:00Z"
    return f"{text}T{suffix}"


def _extract_segment_ids(detail: dict[str, Any]) -> set[str]:
    segment_list = _deep_get(detail, "request", "reportParameters", "segmentList") or []
    ids: set[str] = set()
    if isinstance(segment_list, list):
        for item in segment_list:
            if isinstance(item, dict):
                segment_id = _normalize_text(item.get("id"))
                if segment_id:
                    ids.add(segment_id)
    return ids


def _extract_field_ids(
    detail: dict[str, Any],
    field_name: str,
) -> list[str]:
    raw_items = _deep_get(detail, "request", "reportParameters", field_name) or []
    out: list[str] = []
    if not isinstance(raw_items, list):
        return out
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        value = _normalize_text(item.get("id"))
        if value:
            out.append(value)
    return out


def _summarize_request(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _deep_get(item, "request", "name") or "",
        "rsid": _deep_get(item, "request", "rsid") or "",
        "scheduled_request_uuid": _deep_get(item, "metadata", "scheduledRequestUUID") or "",
        "status": _deep_get(item, "metadata", "status") or "",
        "created_date": _deep_get(item, "metadata", "createdDate") or "",
        "updated_date": _deep_get(item, "metadata", "updatedDate") or "",
        "owner_login": _deep_get(item, "metadata", "ownerInfo", "login") or "",
        "raw": item,
    }


def _status_matches(value: str, accepted: list[str] | None) -> bool:
    if not accepted:
        return True
    value_norm = value.strip().lower()
    accepted_norm = {item.strip().lower() for item in accepted if item.strip()}
    return value_norm in accepted_norm


def collect_scheduled_requests(
    client: AdobeDataWarehouseClient,
    *,
    rsid: str,
    updated_after: str | None = None,
    updated_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    status: str | None = None,
    limit: int = 100,
    max_windows: int = 1000,
) -> dict[str, Any]:
    """Collect scheduled requests even when the API ignores the page parameter."""
    batch_limit = 100
    current_updated_before = _normalize_text(updated_before) or None
    seen: dict[str, dict[str, Any]] = {}
    batches: list[dict[str, Any]] = []

    for _ in range(max_windows):
        payload = client.list_scheduled_requests(
            rsid=rsid,
            updated_after=updated_after,
            updated_before=current_updated_before,
            created_after=created_after,
            created_before=created_before,
            status=status,
            limit=batch_limit,
        )
        items = _safe_scheduled_request_items(payload)

        before_count = len(seen)
        oldest_dt: datetime | None = None
        for item in items:
            uuid = _extract_scheduled_request_uuid(item)
            if not uuid:
                continue
            seen[uuid] = item
            item_updated_dt = _extract_updated_datetime(item)
            if item_updated_dt is not None and (oldest_dt is None or item_updated_dt < oldest_dt):
                oldest_dt = item_updated_dt

        total_hint = payload.get("total")
        boundary_payload: dict[str, Any] | None = None
        boundary_added = 0
        if (
            len(items) >= batch_limit
            and oldest_dt is not None
            and (not isinstance(total_hint, int) or len(seen) < total_hint)
        ):
            boundary_items, boundary_payload = _collect_same_updated_date_requests(
                client,
                rsid=rsid,
                updated_at=oldest_dt,
                created_after=created_after,
                created_before=created_before,
                status=status,
                batch_limit=batch_limit,
            )
            boundary_before_count = len(seen)
            for item in boundary_items:
                uuid = _extract_scheduled_request_uuid(item)
                if uuid:
                    seen[uuid] = item
            boundary_added = len(seen) - boundary_before_count

        batches.append(
            {
                "updated_before": current_updated_before,
                "returned": len(items),
                "total": total_hint,
                "added": len(seen) - before_count,
                "boundary_probe": {
                    "updated_at": _to_utc_z(oldest_dt),
                    "returned": len(boundary_payload.get("scheduledRequests", [])) if boundary_payload else 0,
                    "total": boundary_payload.get("total") if boundary_payload else None,
                    "added": boundary_added,
                }
                if boundary_payload is not None
                else None,
            }
        )

        if len(items) < batch_limit and (
            not isinstance(total_hint, int) or len(seen) >= total_hint
        ):
            break
        if len(seen) == before_count:
            break
        if oldest_dt is None:
            break

        next_updated_before = _to_utc_z(oldest_dt - timedelta(seconds=1))
        if next_updated_before == current_updated_before:
            break
        current_updated_before = next_updated_before

    rows = sorted(
        seen.values(),
        key=lambda item: _parse_sortable_datetime(_deep_get(item, "metadata", "updatedDate")),
        reverse=True,
    )
    return {
        "total": len(rows),
        "totalReturned": len(rows),
        "scheduledRequests": rows,
        "pagination": {
            "mode": "updated_before_window",
            "windows": batches,
        },
    }


def find_template_requests(
    client: AdobeDataWarehouseClient,
    *,
    rsid: str,
    name_contains: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    status: list[str] | None = None,
    owner_login: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List scheduled request summaries and filter them for template discovery."""
    api_status = None
    if status and len(status) == 1:
        api_status = status[0]

    data = collect_scheduled_requests(
        client,
        rsid=rsid,
        updated_after=updated_after,
        updated_before=updated_before,
        created_after=created_after,
        created_before=created_before,
        status=api_status,
        limit=limit,
    )

    accepted_name = _normalize_text(name_contains).lower()
    accepted_owner = _normalize_text(owner_login).lower()

    items = data.get("scheduledRequests", [])
    if not isinstance(items, list):
        raise RuntimeError(f"Unexpected scheduled requests payload: {data}")

    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        summary = _summarize_request(item)
        if accepted_name and accepted_name not in summary["name"].lower():
            continue
        if accepted_owner and accepted_owner != summary["owner_login"].lower():
            continue
        if not _status_matches(summary["status"], status):
            continue
        out.append(summary)

    out.sort(key=lambda item: _parse_sortable_datetime(item["updated_date"]), reverse=True)
    return out[: max(1, limit)]


def resolve_template_request(
    client: AdobeDataWarehouseClient,
    *,
    rsid: str = "",
    scheduled_request_uuid: str | None = None,
    name_contains: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    output_file_basename: str | None = None,
    segment_id: str | None = None,
    owner_login: str | None = None,
    status: list[str] | None = None,
    require_unique: bool = True,
    limit: int = 100,
) -> dict[str, Any]:
    """Resolve one template request detail by UUID or filtered search."""
    if _normalize_text(scheduled_request_uuid):
        detail = client.get_scheduled_request(_normalize_text(scheduled_request_uuid))
        resolved_rsid = _normalize_text(_deep_get(detail, "request", "rsid"))
        expected_rsid = _normalize_text(rsid)
        if expected_rsid and resolved_rsid and resolved_rsid != expected_rsid:
            raise ValueError(
                f"Template rsid mismatch: expected {rsid!r}, got {resolved_rsid!r}"
            )
        return detail

    candidates = find_template_requests(
        client,
        rsid=rsid,
        name_contains=name_contains,
        updated_after=updated_after,
        updated_before=updated_before,
        created_after=created_after,
        created_before=created_before,
        status=status,
        owner_login=owner_login,
        limit=limit,
    )
    if not candidates:
        raise LookupError(f"No template scheduled requests found for rsid={rsid!r}")

    basename_filter = _normalize_text(output_file_basename)
    segment_filter = _normalize_text(segment_id)
    details: list[dict[str, Any]] = []
    for candidate in candidates:
        detail = client.get_scheduled_request(candidate["scheduled_request_uuid"])
        if basename_filter:
            basename = _normalize_text(
                _deep_get(detail, "request", "outputFile", "outputFileBasename")
            )
            if basename != basename_filter:
                continue
        if segment_filter and segment_filter not in _extract_segment_ids(detail):
            continue
        details.append(detail)

    if not details:
        raise LookupError(
            "No template scheduled request matched the requested detail filters"
        )

    details.sort(
        key=lambda item: _parse_sortable_datetime(_deep_get(item, "metadata", "updatedDate")),
        reverse=True,
    )
    if require_unique and len(details) != 1:
        uuids = [
            _normalize_text(_deep_get(item, "metadata", "scheduledRequestUUID"))
            for item in details
        ]
        raise ValueError(f"Expected exactly one template request, found {len(details)}: {uuids}")
    return details[0]


def summarize_template_detail(detail: dict[str, Any]) -> dict[str, Any]:
    """Return a compact, human-scannable summary for one template detail payload."""
    return {
        "scheduled_request_uuid": _normalize_text(
            _deep_get(detail, "metadata", "scheduledRequestUUID")
        ),
        "name": _normalize_text(_deep_get(detail, "request", "name")),
        "rsid": _normalize_text(_deep_get(detail, "request", "rsid")),
        "owner_login": _normalize_text(_deep_get(detail, "metadata", "ownerInfo", "login")),
        "status": _normalize_text(_deep_get(detail, "metadata", "status")),
        "created_date": _normalize_text(_deep_get(detail, "metadata", "createdDate")),
        "updated_date": _normalize_text(_deep_get(detail, "metadata", "updatedDate")),
        "schedule_at": _normalize_text(_deep_get(detail, "schedule", "scheduleAt")),
        "schedule_frequency": _normalize_text(
            _deep_get(detail, "schedule", "periodSettings", "frequency")
        ),
        "output_file_basename": _normalize_text(
            _deep_get(detail, "request", "outputFile", "outputFileBasename")
        ),
        "file_format": _normalize_text(_deep_get(detail, "request", "outputFile", "fileFormat")),
        "compression_format": _normalize_text(
            _deep_get(detail, "request", "outputFile", "compressionFormat")
        ),
        "export_location_uuid": _normalize_text(
            _deep_get(detail, "delivery", "exportLocationUUID")
        ),
        "dimension_ids": _extract_field_ids(detail, "dimensionList"),
        "metric_ids": _extract_field_ids(detail, "metricList"),
        "segment_ids": sorted(_extract_segment_ids(detail)),
        "report_range": deepcopy(_deep_get(detail, "request", "reportParameters", "reportRange") or {}),
        "date_granularity": _normalize_text(
            _deep_get(detail, "request", "reportParameters", "dateGranularity")
        ),
        "number_of_rows_in_table": _deep_get(
            detail, "request", "reportParameters", "numberOfRowsInTable"
        ),
    }


def build_cloned_request_body(
    *,
    template_detail: dict[str, Any],
    name: str,
    schedule_at: str,
    report_range: dict[str, Any],
    output_file_basename: str | None = None,
    sharing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a create/update payload from a template request detail payload."""
    template_request = deepcopy(_deep_get(template_detail, "request") or {})
    template_schedule = deepcopy(_deep_get(template_detail, "schedule") or {})
    export_location_uuid = _normalize_text(
        _deep_get(template_detail, "delivery", "exportLocationUUID")
    )
    if not export_location_uuid:
        raise ValueError("template_detail.delivery.exportLocationUUID is required")

    body_request = {
        "name": _normalize_text(name),
        "sharing": deepcopy(sharing) if sharing is not None else deepcopy(template_request.get("sharing") or {}),
        "outputFile": deepcopy(template_request.get("outputFile") or {}),
        "reportParameters": deepcopy(template_request.get("reportParameters") or {}),
        "rsid": _normalize_text(template_request.get("rsid")),
    }
    if not body_request["name"]:
        raise ValueError("name is required")
    if not body_request["rsid"]:
        raise ValueError("template request rsid is required")

    if output_file_basename is not None:
        body_request["outputFile"]["outputFileBasename"] = _normalize_text(output_file_basename)

    body_request["reportParameters"]["reportRange"] = _normalize_report_range(report_range)

    schedule_at_text = _normalize_text(schedule_at)
    if not schedule_at_text:
        raise ValueError("schedule_at is required")

    schedule: dict[str, Any] = {
        "scheduleAt": schedule_at_text,
        "periodSettings": deepcopy(template_schedule.get("periodSettings") or {}),
        "cancelSettings": deepcopy(template_schedule.get("cancelSettings")),
    }

    # Build delivery: preserve email block from template (required by API),
    # override exportLocationUUID with the resolved value.
    delivery = deepcopy(_deep_get(template_detail, "delivery") or {})
    delivery["exportLocationUUID"] = export_location_uuid

    # _strip_nulls removes all None values recursively; Adobe DW API
    # rejects explicit nulls on create but accepts absent keys.
    return _strip_nulls({
        "schedule": schedule,
        "request": body_request,
        "delivery": delivery,
    })


def create_request_from_template(
    client: AdobeDataWarehouseClient,
    *,
    template_detail: dict[str, Any],
    name: str,
    schedule_at: str,
    report_range: dict[str, Any],
    output_file_basename: str | None = None,
    sharing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create one scheduled request from a template detail payload."""
    body = build_cloned_request_body(
        template_detail=template_detail,
        name=name,
        schedule_at=schedule_at,
        report_range=report_range,
        output_file_basename=output_file_basename,
        sharing=sharing,
    )
    return client.create_scheduled_request(body)


def bulk_create_requests_from_template(
    client: AdobeDataWarehouseClient,
    *,
    template_detail: dict[str, Any],
    requests: list[dict[str, Any]],
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Create or preview multiple scheduled requests from one template."""
    out: list[dict[str, Any]] = []
    for item in requests:
        if not isinstance(item, dict):
            raise TypeError("requests must contain objects")
        body = build_cloned_request_body(
            template_detail=template_detail,
            name=_normalize_text(item.get("name")),
            schedule_at=_normalize_text(item.get("schedule_at") or item.get("scheduleAt")),
            report_range=item.get("report_range") or item.get("reportRange") or {},
            output_file_basename=item.get("output_file_basename") or item.get("outputFileBasename"),
            sharing=item.get("sharing"),
        )
        if dry_run:
            out.append(
                {
                    "name": _deep_get(body, "request", "name"),
                    "scheduled_request_uuid": None,
                    "dry_run": True,
                    "body": body,
                }
            )
            continue
        created = client.create_scheduled_request(body)
        out.append(
            {
                "name": _deep_get(created, "request", "name") or _deep_get(body, "request", "name"),
                "scheduled_request_uuid": _deep_get(created, "metadata", "scheduledRequestUUID"),
                "dry_run": False,
                "response": created,
            }
        )
    return out
