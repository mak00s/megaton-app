"""Tests for Target Recommendations feed export."""

from __future__ import annotations

import json

from megaton_lib.audit.providers.target.feeds import export_feeds


class _MockClient:
    def __init__(self) -> None:
        self.get_all_calls: list[str] = []
        self.get_calls: list[str] = []

    def get_all(self, endpoint: str) -> list[dict]:
        self.get_all_calls.append(endpoint)
        return [
            {"id": 101, "name": "Target Feed"},
            {"id": 202, "name": "Other Feed"},
        ]

    def get(self, endpoint: str) -> dict:
        self.get_calls.append(endpoint)
        return {
            "id": 101,
            "name": "Target Feed",
            "authToken": "secret-token",
            "configuration": {"username": "feed-user", "path": "/items.csv"},
        }


def test_export_feeds_uses_recs_endpoints_and_writes_files(tmp_path):
    client = _MockClient()

    summary = export_feeds(client, tmp_path, ["Target Feed"])

    assert summary == {
        "exported": 1,
        "feeds": [{"id": 101, "name": "Target Feed"}],
    }
    assert client.get_all_calls == ["/recs/feeds"]
    assert client.get_calls == ["/recs/feeds/101"]

    detail = json.loads((tmp_path / "101.json").read_text(encoding="utf-8"))
    assert detail["name"] == "Target Feed"
    assert detail["authToken"] == "***REDACTED***"
    assert detail["configuration"]["username"] == "***REDACTED***"
    assert detail["configuration"]["path"] == "/items.csv"

    index = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
    assert index == [{"id": 101, "name": "Target Feed"}]
