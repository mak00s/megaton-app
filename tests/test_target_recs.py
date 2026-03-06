"""Tests for Target Recommendations export, apply, and getoffer scope."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from megaton_lib.audit.providers.target.getoffer_scope import (
    detect_getoffer_scope,
    export_getoffer_scope,
)
from megaton_lib.audit.providers.target.recs import (
    RESOURCE_TYPES,
    _strip_metadata,
    apply_recs,
    export_recs,
)


class _MockClient:
    """Minimal mock for AdobeTargetClient."""

    def __init__(self, list_data: dict[str, list], detail_data: dict[str, dict] | None = None):
        self._list = list_data
        self._detail = detail_data or {}
        self.patched: list[tuple[str, dict]] = []

    def get_all(self, endpoint: str, **kw) -> list[dict]:
        resource = endpoint.strip("/").split("/")[0]
        return list(self._list.get(resource, []))

    def get(self, endpoint: str, **kw) -> dict:
        # /criteria/123 → try detail, fallback to item in list
        parts = endpoint.strip("/").split("/")
        key = "/".join(parts)
        if key in self._detail:
            return self._detail[key]
        # Return basic item
        resource = parts[0]
        item_id = int(parts[1]) if len(parts) > 1 else None
        for item in self._list.get(resource, []):
            if item.get("id") == item_id:
                return item
        raise RuntimeError(f"Not found: {endpoint}")

    def patch(self, endpoint: str, payload: dict) -> dict:
        self.patched.append((endpoint, payload))
        return payload


# ---- export tests ----


def test_export_creates_files(tmp_path):
    client = _MockClient({
        "criteria": [{"id": 101, "name": "Crit A"}, {"id": 102, "name": "Crit B"}],
    })

    summary = export_recs(client, tmp_path, resources=["criteria"])

    assert summary["criteria"] == 2
    assert (tmp_path / "criteria" / "index.json").exists()
    assert (tmp_path / "criteria" / "101.json").exists()
    assert (tmp_path / "criteria" / "102.json").exists()

    index = json.loads((tmp_path / "criteria" / "index.json").read_text())
    assert len(index) == 2
    assert index[0]["id"] == 101


def test_export_name_regex_filter(tmp_path):
    client = _MockClient({
        "criteria": [
            {"id": 1, "name": "CSK Demo: test"},
            {"id": 2, "name": "Other criterion"},
        ],
    })

    summary = export_recs(client, tmp_path, resources=["criteria"], name_regex="^CSK")
    assert summary["criteria"] == 1


def test_export_id_list_filter(tmp_path):
    client = _MockClient({
        "criteria": [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
            {"id": 3, "name": "C"},
        ],
    })

    summary = export_recs(client, tmp_path, resources=["criteria"], id_list=[1, 3])
    assert summary["criteria"] == 2


def test_export_max_items(tmp_path):
    client = _MockClient({
        "criteria": [{"id": i, "name": f"C{i}"} for i in range(10)],
    })

    summary = export_recs(client, tmp_path, resources=["criteria"], max_items=3)
    assert summary["criteria"] == 3


def test_export_designs_extracts_script(tmp_path):
    client = _MockClient(
        {"designs": [{"id": 201, "name": "JSON99"}]},
        detail_data={"designs/201": {"id": 201, "name": "JSON99", "content": '{"items": []}'}},
    )

    export_recs(client, tmp_path, resources=["designs"])
    # Content starts with '{', so should be .vtl
    assert (tmp_path / "designs" / "201.vtl").exists()


def test_export_per_resource_filters(tmp_path):
    """Per-resource dict filters should work."""
    client = _MockClient({
        "criteria": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
        "designs": [{"id": 3, "name": "D1"}, {"id": 4, "name": "D2"}],
    })

    summary = export_recs(
        client, tmp_path,
        resources=["criteria", "designs"],
        name_regex={"criteria": "^A$"},  # only filter criteria
    )
    assert summary["criteria"] == 1
    assert summary["designs"] == 2


# ---- apply tests ----


def test_apply_dry_run_reports_changes(tmp_path):
    # Write local file
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    local = {"id": 101, "name": "Updated Crit"}
    (crit_dir / "101.json").write_text(json.dumps(local))

    # Remote has different name
    client = _MockClient(
        {"criteria": [{"id": 101, "name": "Old Crit"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=True)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is False
    assert len(client.patched) == 0


def test_apply_sends_patch_when_changed(tmp_path):
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    local = {"id": 101, "name": "Updated"}
    (crit_dir / "101.json").write_text(json.dumps(local))

    client = _MockClient(
        {"criteria": [{"id": 101, "name": "Old"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    assert len(client.patched) == 1
    assert client.patched[0][1] == local


def test_apply_skips_unchanged(tmp_path):
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    data = {"id": 101, "name": "Same"}
    (crit_dir / "101.json").write_text(json.dumps(data))

    client = _MockClient({"criteria": [data]})

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    assert changes[0]["changed"] is False
    assert len(client.patched) == 0


def test_strip_metadata():
    obj = {"id": 1, "name": "x", "lastModifiedAt": "2025-01-01", "modifiedBy": "user"}
    clean = _strip_metadata(obj)
    assert "lastModifiedAt" not in clean
    assert "modifiedBy" not in clean
    assert clean["id"] == 1


def test_strip_metadata_actual_api_keys():
    """Verify metadata keys from real Target API responses are stripped."""
    obj = {
        "id": 1,
        "name": "x",
        "lastModified": "2025-12-19T11:00:01.000Z",
        "lastModifiersEmail": "user@example.com",
        "lastModifiersName": "Test User",
    }
    clean = _strip_metadata(obj)
    assert "lastModified" not in clean
    assert "lastModifiersEmail" not in clean
    assert "lastModifiersName" not in clean
    assert clean == {"id": 1, "name": "x"}


def test_apply_merges_design_sidecar(tmp_path):
    """apply_recs reads .vtl sidecar and merges content into JSON before PATCH."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    local_json = {"id": 201, "name": "D1", "content": "old template"}
    (des_dir / "201.json").write_text(json.dumps(local_json))
    # Sidecar with updated template
    (des_dir / "201.vtl").write_text("new template from vtl")

    client = _MockClient(
        {"designs": [{"id": 201, "name": "D1", "content": "old template"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    # Verify the PATCH payload has merged sidecar content
    patched_payload = client.patched[0][1]
    assert patched_payload["content"] == "new template from vtl"


# ---- getoffer scope tests ----


def test_detect_getoffer_scope_reads_response_body(tmp_path):
    """Scope detection traverses call.response.body (actual capture structure)."""
    delivery = [
        {
            "request": {"url": "https://example.com"},
            "mboxes": [],
            "response": {
                "status": 200,
                "body": {
                    "status": 200,
                    "execute": {
                        "mboxes": [
                            {
                                "name": "top-mbox",
                                "options": [
                                    {
                                        "responseTokens": {
                                            "activity.id": 12345,
                                            "activity.name": "Test Activity",
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                },
            },
        },
    ]
    (tmp_path / "delivery-calls.json").write_text(json.dumps(delivery))

    scope = detect_getoffer_scope(tmp_path)
    assert 12345 in scope["activity_ids"]
    assert "Test Activity" in scope["criteria_names"]


def test_detect_getoffer_scope_flat_structure(tmp_path):
    """Also support flat structure (prefetch/execute at top level)."""
    delivery = [
        {
            "execute": {
                "mboxes": [
                    {
                        "name": "mbox1",
                        "options": [
                            {"responseTokens": {"activity.id": 999}},
                        ],
                    },
                ],
            },
        },
    ]
    (tmp_path / "delivery-calls.json").write_text(json.dumps(delivery))

    scope = detect_getoffer_scope(tmp_path)
    assert 999 in scope["activity_ids"]


def test_detect_getoffer_scope_mboxes_object(tmp_path):
    """Extract mbox names from mboxes: { A: "name" } object format."""
    code = '''var CONFIG = {
  mboxes: {
    A: "CSK-A",
    B: "CSK-B"
  },
  collectionByMbox: {
    "CSK-A": "essence-master",
    "CSK-B": "web-seminars"
  }
};'''
    code_path = tmp_path / "getoffer.custom-code.js"
    code_path.write_text(code)
    (tmp_path / "delivery-calls.json").write_text("[]")

    scope = detect_getoffer_scope(tmp_path, code_path)
    assert "CSK-A" in scope["mboxes"]
    assert "CSK-B" in scope["mboxes"]
    assert "essence-master" in scope["collection_names"]
    assert "web-seminars" in scope["collection_names"]


def test_export_getoffer_scope_passes_only_scoped_resources(monkeypatch, tmp_path):
    """export_getoffer_scope should export only resources with filters."""
    code = '''var CONFIG = {
  collectionByMbox: {
    "CSK-A": "essence-master"
  }
};'''
    code_path = tmp_path / "getoffer.custom-code.js"
    code_path.write_text(code)
    delivery = [
        {
            "response": {
                "body": {
                    "execute": {
                        "mboxes": [
                            {
                                "name": "CSK-A",
                                "options": [
                                    {"responseTokens": {"activity.name": "Criteria A"}},
                                ],
                            },
                        ],
                    },
                },
            },
        },
    ]
    (tmp_path / "delivery-calls.json").write_text(json.dumps(delivery))

    captured: dict[str, object] = {}

    def _fake_export_recs(client, output_root, resources=None, name_regex=None):
        captured["resources"] = resources
        captured["name_regex"] = name_regex
        return {"criteria": 1, "collections": 1}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.getoffer_scope.export_recs",
        _fake_export_recs,
    )

    result = export_getoffer_scope(
        _MockClient({}),
        tmp_path / "out",
        tmp_path,
        code_path,
    )

    assert captured["resources"] == ["criteria", "collections"]
    assert isinstance(captured["name_regex"], dict)
    assert "criteria" in captured["name_regex"]
    assert "collections" in captured["name_regex"]
    assert "designs" not in captured["resources"]
    assert result["export_summary"] == {"criteria": 1, "collections": 1}


def test_export_getoffer_scope_fallbacks_to_criteria_only(monkeypatch, tmp_path):
    """When no scope filters exist, export_getoffer_scope still scopes to criteria."""
    (tmp_path / "delivery-calls.json").write_text("[]")

    captured: dict[str, object] = {}

    def _fake_export_recs(client, output_root, resources=None, name_regex=None):
        captured["resources"] = resources
        captured["name_regex"] = name_regex
        return {"criteria": 0}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.getoffer_scope.export_recs",
        _fake_export_recs,
    )

    result = export_getoffer_scope(
        _MockClient({}),
        tmp_path / "out",
        tmp_path,
        None,
    )

    assert captured["resources"] == ["criteria"]
    assert captured["name_regex"] is None
    assert result["export_summary"] == {"criteria": 0}


# ---- config loader oauth test ----


def test_config_loader_parses_adobe_tags_oauth():
    """_parse_tag_source passes oauth config to AdobeTagsConfig."""
    from megaton_lib.audit.config import _parse_tag_source

    node = {
        "source": "adobe_tags",
        "adobe_tags": {
            "property_id": "PR123",
            "oauth": {
                "client_id_env": "MY_CLIENT_ID",
                "scopes": "openid,AdobeID",
            },
        },
    }
    result = _parse_tag_source(node)
    assert result.adobe_tags is not None
    assert result.adobe_tags.oauth is not None
    assert result.adobe_tags.oauth.client_id_env == "MY_CLIENT_ID"
    assert result.adobe_tags.oauth.scopes == "openid,AdobeID"


def test_config_loader_adobe_tags_oauth_true_shorthand():
    """oauth: true uses defaults."""
    from megaton_lib.audit.config import _parse_tag_source

    node = {
        "source": "adobe_tags",
        "adobe_tags": {
            "property_id": "PR456",
            "oauth": True,
        },
    }
    result = _parse_tag_source(node)
    assert result.adobe_tags is not None
    assert result.adobe_tags.oauth is not None
    assert result.adobe_tags.oauth.client_id_env == "ADOBE_CLIENT_ID"


def test_config_loader_adobe_tags_no_oauth():
    """Without oauth key, oauth is None."""
    from megaton_lib.audit.config import _parse_tag_source

    node = {
        "source": "adobe_tags",
        "adobe_tags": {
            "property_id": "PR789",
        },
    }
    result = _parse_tag_source(node)
    assert result.adobe_tags is not None
    assert result.adobe_tags.oauth is None
