"""Correlate collection events with their own HTTP outcomes, without payloads."""
from __future__ import annotations

import json
from urllib.parse import parse_qs


class CollectionReceiptError(AssertionError):
    """A required event was missing, pending, failed, or rejected by HTTP."""


def named_events(body: str | None, names: set[str]) -> list[str]:
    """Read known event fields from JSON, nested JSON strings, or form bodies.

    Unknown/encoded payloads return no evidence; they must not imply delivery.
    Only the allowlisted names are returned, never identity values or payloads.
    """
    found: set[str] = set()
    event_keys = {"event", "event_name", "eventName", "type"}

    def walk(value, depth=0):
        if depth > 12:
            return
        if isinstance(value, dict):
            for key, child in value.items():
                if key in event_keys:
                    candidates = child if isinstance(child, list) else [child]
                    found.update(item for item in candidates if isinstance(item, str) and item in names)
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                walk(child, depth + 1)
        elif isinstance(value, str):
            if value.startswith(("{", "[")):
                try:
                    walk(json.loads(value), depth + 1)
                except ValueError:
                    pass

    try:
        walk(json.loads(body or "{}"))
    except ValueError:
        walk(parse_qs(body or ""))
    return sorted(found)


def require_event_receipts(records: list[dict], events, *, stream=None) -> None:
    """Require every matching request to have a successful response.

    A different event's success, a queued API call, or a missing response is
    insufficient. A batched request may establish multiple event receipts.
    """
    expected = set(events)
    if not expected:
        raise CollectionReceiptError("required_events_empty")
    for event in expected:
        matching = [r for r in records if event in r["events"]
                    and (stream is None or r.get("stream") == stream)]
        if not matching:
            raise CollectionReceiptError("missing_event_receipt:" + event)
        for row in matching:
            if row.get("failed"):
                raise CollectionReceiptError("collection_request_failed:" + event)
            if row.get("status") is None:
                raise CollectionReceiptError("collection_response_pending:" + event)
            if not 200 <= row["status"] < 300:
                raise CollectionReceiptError("collection_http_error:" + event)


class CollectionReceipts:
    """Keep opaque request correlations in memory and safe evidence in records."""

    def __init__(self):
        self.records: list[dict] = []
        self._requests: dict[object, dict] = {}

    def request(self, request, events, *, stream=None) -> dict:
        if request not in self._requests:
            record = {"requestId": len(self.records) + 1,
                      "events": sorted(set(events)), "stream": stream,
                      "status": None, "failed": False}
            self._requests[request] = record
            self.records.append(record)
        return self._requests[request]

    def response(self, response) -> dict | None:
        record = self._requests.get(response.request)
        if record is not None:
            record["status"] = response.status
        return record

    def failure(self, request) -> None:
        record = self._requests.get(request)
        if record is not None:
            record["failed"] = True

    def require(self, events, *, stream=None) -> None:
        require_event_receipts(self.records, events, stream=stream)

    def received(self, events, *, stream=None) -> bool:
        try:
            self.require(events, stream=stream)
        except CollectionReceiptError:
            return False
        return True
