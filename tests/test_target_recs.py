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
    _CRITERIA_GROUP_TO_SUBTYPE,
    _criteria_detail_endpoint,
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
        self.putted: list[tuple[str, dict]] = []

    def get_all(self, endpoint: str, **kw) -> list[dict]:
        resource = endpoint.strip("/").split("/")[0]
        return list(self._list.get(resource, []))

    def get(self, endpoint: str, **kw) -> dict:
        # Try exact detail key first (e.g. "criteria/popularity/101")
        parts = endpoint.strip("/").split("/")
        key = "/".join(parts)
        if key in self._detail:
            return self._detail[key]
        # For sub-type criteria paths like /criteria/popularity/101,
        # also try the generic key /criteria/101
        resource = parts[0]
        if resource == "criteria" and len(parts) == 3:
            generic_key = f"criteria/{parts[2]}"
            if generic_key in self._detail:
                return self._detail[generic_key]
            # Fallback to list lookup by ID
            item_id = int(parts[2])
            for item in self._list.get("criteria", []):
                if item.get("id") == item_id:
                    return item
            raise RuntimeError(f"Not found: {endpoint}")
        # Return basic item by ID
        item_id = int(parts[1]) if len(parts) > 1 else None
        for item in self._list.get(resource, []):
            if item.get("id") == item_id:
                return item
        raise RuntimeError(f"Not found: {endpoint}")

    def patch(self, endpoint: str, payload: dict) -> dict:
        self.patched.append((endpoint, payload))
        return payload

    def put(self, endpoint: str, payload: dict) -> dict:
        self.putted.append((endpoint, payload))
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
    """Design script extraction works with both 'script' and 'content' fields."""
    # Real Target API uses "script" field
    client = _MockClient(
        {"designs": [{"id": 201, "name": "JSON99"}]},
        detail_data={"designs/201": {"id": 201, "name": "JSON99", "script": '{"items": []}'}},
    )

    export_recs(client, tmp_path, resources=["designs"])
    # Content starts with '{', so should be .vtl
    assert (tmp_path / "designs" / "201.vtl").exists()

    # Also works with legacy "content" field
    client2 = _MockClient(
        {"designs": [{"id": 202, "name": "HTML1"}]},
        detail_data={"designs/202": {"id": 202, "name": "HTML1", "content": '<div>test</div>'}},
    )
    export_recs(client2, tmp_path, resources=["designs"])
    assert (tmp_path / "designs" / "202.html").exists()


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
    local = {"id": 101, "name": "Updated Crit", "criteriaGroup": "POPULARITY"}
    (crit_dir / "101.json").write_text(json.dumps(local))

    # Remote has different name
    client = _MockClient(
        {"criteria": [{"id": 101, "name": "Old Crit", "criteriaGroup": "POPULARITY"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=True)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is False
    assert len(client.putted) == 0


def test_apply_criteria_uses_put_via_subtype(tmp_path):
    """Criteria uses PUT via sub-type endpoint (PATCH returns 405)."""
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    local = {"id": 101, "name": "Updated", "criteriaGroup": "POPULARITY"}
    (crit_dir / "101.json").write_text(json.dumps(local))

    client = _MockClient(
        {"criteria": [{"id": 101, "name": "Old", "criteriaGroup": "POPULARITY"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    # Criteria must use PUT (sub-type endpoints don't support PATCH)
    assert len(client.putted) == 1
    assert len(client.patched) == 0
    assert client.putted[0][0] == "/criteria/popularity/101"


def test_criteria_detail_endpoint_resolves_subtype():
    """_criteria_detail_endpoint maps criteriaGroup to sub-type endpoint."""
    client = _MockClient(
        {"criteria": [{"id": 42, "name": "Test", "criteriaGroup": "POPULARITY"}]},
    )
    ep, is_sub = _criteria_detail_endpoint(client, 42)
    assert ep == "/criteria/popularity/42"
    assert is_sub is True


def test_criteria_detail_endpoint_item_subtype():
    """ITEM criteriaGroup maps to /criteria/item/."""
    client = _MockClient(
        {"criteria": [{"id": 7, "name": "Co-viewed", "criteriaGroup": "ITEM"}]},
    )
    ep, is_sub = _criteria_detail_endpoint(client, 7)
    assert ep == "/criteria/item/7"
    assert is_sub is True


def test_criteria_detail_endpoint_unknown_group_fallback():
    """Unknown criteriaGroup falls back to generic /criteria/{id}."""
    client = _MockClient(
        {"criteria": [{"id": 9, "name": "X", "criteriaGroup": "UNKNOWN_TYPE"}]},
    )
    ep, is_sub = _criteria_detail_endpoint(client, 9)
    assert ep == "/criteria/9"
    assert is_sub is False


def test_apply_criteria_fallback_uses_patch(tmp_path):
    """When criteriaGroup is unknown, apply falls back to PATCH on generic endpoint."""
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    local = {"id": 55, "name": "New", "criteriaGroup": "FUTURE_TYPE"}
    (crit_dir / "55.json").write_text(json.dumps(local))

    client = _MockClient(
        {"criteria": [{"id": 55, "name": "Old", "criteriaGroup": "FUTURE_TYPE"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    # Fallback: generic endpoint uses PATCH, not PUT
    assert len(client.patched) == 1
    assert len(client.putted) == 0
    assert client.patched[0][0] == "/criteria/55"


def test_export_criteria_uses_subtype_detail(tmp_path):
    """export_recs fetches criteria via sub-type endpoint for full detail."""
    # Slim response (generic /criteria/101)
    slim = {"id": 101, "name": "Pop", "criteriaGroup": "POPULARITY"}
    # Full detail (sub-type /criteria/popularity/101)
    full = {
        **slim,
        "backupDisabled": True,
        "configuration": {
            "inclusionRules": [{"attribute": "diseaseField", "operation": "contains"}],
        },
    }
    client = _MockClient(
        {"criteria": [slim]},
        detail_data={"criteria/101": slim, "criteria/popularity/101": full},
    )

    summary = export_recs(client, tmp_path, resources=["criteria"])
    assert summary["criteria"] == 1

    saved = json.loads((tmp_path / "criteria" / "101.json").read_text())
    # Must contain the full detail from sub-type endpoint
    assert "configuration" in saved
    assert saved["backupDisabled"] is True


def test_apply_designs_uses_put(tmp_path):
    """Designs require PUT because PATCH ignores the script field."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    local = {"id": 99, "name": "JSON99", "script": "<new>"}
    (des_dir / "99.json").write_text(json.dumps(local))

    client = _MockClient(
        {"designs": [{"id": 99, "name": "JSON99", "script": "<old>"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    # Must use PUT, not PATCH
    assert len(client.putted) == 1
    assert len(client.patched) == 0
    assert client.putted[0][0] == "/designs/99"


def test_apply_strips_metadata_before_send(tmp_path):
    """Payload sent to API must not contain server-managed metadata keys."""
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    local = {"id": 101, "name": "New", "criteriaGroup": "ITEM",
             "lastModified": "2026-01-01", "lastModifiersEmail": "x@y"}
    (crit_dir / "101.json").write_text(json.dumps(local))

    client = _MockClient(
        {"criteria": [{"id": 101, "name": "Old", "criteriaGroup": "ITEM"}]},
    )

    apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    # Criteria uses PUT via sub-type endpoint
    sent = client.putted[0][1]
    assert "lastModified" not in sent
    assert "lastModifiersEmail" not in sent
    assert sent["name"] == "New"


def test_apply_designs_strips_metadata_before_put(tmp_path):
    """PUT payload for designs must not contain server-managed metadata keys."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    local = {"id": 99, "name": "D", "script": "new", "lastModified": "2026-01-01", "lastModifiersName": "u"}
    (des_dir / "99.json").write_text(json.dumps(local))

    client = _MockClient(
        {"designs": [{"id": 99, "name": "D", "script": "old"}]},
    )

    apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    sent = client.putted[0][1]
    assert "lastModified" not in sent
    assert "lastModifiersName" not in sent
    assert sent["script"] == "new"


def test_apply_skips_unchanged(tmp_path):
    crit_dir = tmp_path / "criteria"
    crit_dir.mkdir()
    data = {"id": 101, "name": "Same", "criteriaGroup": "ITEM"}
    (crit_dir / "101.json").write_text(json.dumps(data))

    client = _MockClient({"criteria": [data]})

    changes = apply_recs(client, tmp_path, resources=["criteria"], dry_run=False)
    assert changes[0]["changed"] is False
    assert len(client.putted) == 0


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
    """apply_recs reads .vtl sidecar and merges script into JSON before PATCH."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    # Real Target API uses "script" field for designs
    local_json = {"id": 201, "name": "D1", "script": "old template"}
    (des_dir / "201.json").write_text(json.dumps(local_json))
    # Sidecar with updated template
    (des_dir / "201.vtl").write_text("new template from vtl")

    client = _MockClient(
        {"designs": [{"id": 201, "name": "D1", "script": "old template"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    # Designs use PUT (PATCH ignores script field)
    put_payload = client.putted[0][1]
    assert put_payload["script"] == "new template from vtl"
    assert len(client.patched) == 0


def test_apply_merges_design_sidecar_code_subdir(tmp_path):
    """apply_recs reads sidecar from code/ subdirectory and normalises to 'script'."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    code_dir = des_dir / "code"
    code_dir.mkdir()

    # Legacy data may use "content" — sidecar merge always normalises to "script"
    local_json = {"id": 13628, "name": "Default Template", "content": "old"}
    (des_dir / "13628_default-template.json").write_text(json.dumps(local_json))
    # at-recs layout: code/<id>_<slug>.vtl
    (code_dir / "13628_default-template.vtl").write_text("new template from code dir")

    client = _MockClient(
        {"designs": [{"id": 13628, "name": "Default Template", "content": "old"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    assert changes[0]["applied"] is True
    put_payload = client.putted[0][1]
    # Sidecar merged into canonical "script" key; legacy "content" removed
    assert put_payload["script"] == "new template from code dir"
    assert "content" not in put_payload


def test_apply_merges_design_sidecar_id_only_in_code(tmp_path):
    """Sidecar code/<id>.vtl also works; legacy 'content' normalised to 'script'."""
    des_dir = tmp_path / "designs"
    des_dir.mkdir()
    code_dir = des_dir / "code"
    code_dir.mkdir()

    local_json = {"id": 999, "name": "Simple", "content": "old"}
    (des_dir / "999.json").write_text(json.dumps(local_json))
    (code_dir / "999.vtl").write_text("id-only template")

    client = _MockClient(
        {"designs": [{"id": 999, "name": "Simple", "content": "old"}]},
    )

    changes = apply_recs(client, tmp_path, resources=["designs"], dry_run=False)
    assert len(changes) == 1
    assert changes[0]["changed"] is True
    put_payload = client.putted[0][1]
    assert put_payload["script"] == "id-only template"
    assert "content" not in put_payload


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
                                            "recommendation.criteria.title": "Top Sellers",
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
    assert "Top Sellers" in scope["criteria_names"]


def test_detect_getoffer_scope_parses_option_content(tmp_path):
    """Scope detection parses option.content JSON for recs.activity info."""
    content_json = json.dumps({
        "recs": {
            "activity": {
                "criteria.title": "CSK Demo: 疾患が会員情報と合致",
                "algorithm.name": "CSK Demo: 項目：類似疾患",
                "campaign.id": 803269,
                "campaign.name": "CSK A：疾患が会員情報と合致",
            },
        },
    })
    delivery = [
        {
            "response": {
                "status": 200,
                "body": {
                    "execute": {
                        "mboxes": [
                            {
                                "name": "CSK-A",
                                "options": [
                                    {
                                        "responseTokens": {
                                            "activity.id": 803269,
                                            "activity.name": "CSK A：疾患が会員情報と合致",
                                        },
                                        "content": content_json,
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
    assert 803269 in scope["activity_ids"]
    assert "CSK Demo: 疾患が会員情報と合致" in scope["criteria_names"]
    assert "CSK Demo: 項目：類似疾患" in scope["criteria_names"]
    # activity.name should NOT be in criteria_names (it's not a criteria)
    assert "CSK A：疾患が会員情報と合致" not in scope["criteria_names"]


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
    content_json = json.dumps({
        "recs": {"activity": {"criteria.title": "Criteria A", "algorithm.name": "Algo A"}},
    })
    delivery = [
        {
            "response": {
                "body": {
                    "execute": {
                        "mboxes": [
                            {
                                "name": "CSK-A",
                                "options": [
                                    {
                                        "responseTokens": {"activity.id": 100},
                                        "content": content_json,
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

    assert set(captured["resources"]) == {"criteria", "collections"}
    assert isinstance(captured["name_regex"], dict)
    assert "criteria" in captured["name_regex"]
    assert "collections" in captured["name_regex"]
    assert "designs" not in captured["resources"]
    assert result["export_summary"] == {"criteria": 1, "collections": 1}


def test_export_getoffer_scope_skips_export_when_no_scope_filters(monkeypatch, tmp_path):
    """When no scope filters exist, export_getoffer_scope skips export."""
    (tmp_path / "delivery-calls.json").write_text("[]")

    def _fake_export_recs(client, output_root, resources=None, name_regex=None):
        raise AssertionError("export_recs should not be called when scope is empty")

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

    assert result["export_summary"] == {}


def test_export_getoffer_scope_include_designs(monkeypatch, tmp_path):
    """include_designs=True adds designs to scoped resources."""
    code = '''var CONFIG = {
  collectionByMbox: { "CSK-A": "essence-master" }
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
        return {"criteria": 1, "collections": 1, "designs": 2}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.getoffer_scope.export_recs",
        _fake_export_recs,
    )

    result = export_getoffer_scope(
        _MockClient({}),
        tmp_path / "out",
        tmp_path,
        code_path,
        include_designs=True,
        designs_name_regex="^(JSON99)$",
    )

    assert "designs" in captured["resources"]
    assert captured["name_regex"]["designs"] == "^(JSON99)$"
    assert result["export_summary"]["designs"] == 2


def test_export_getoffer_scope_include_designs_no_regex(monkeypatch, tmp_path):
    """include_designs=True without regex exports all designs."""
    (tmp_path / "delivery-calls.json").write_text("[]")

    captured: dict[str, object] = {}

    def _fake_export_recs(client, output_root, resources=None, name_regex=None):
        captured["resources"] = resources
        captured["name_regex"] = name_regex
        return {"criteria": 0, "designs": 5}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.getoffer_scope.export_recs",
        _fake_export_recs,
    )

    result = export_getoffer_scope(
        _MockClient({}),
        tmp_path / "out",
        tmp_path,
        None,
        include_designs=True,
    )

    assert "designs" in captured["resources"]
    # No designs_name_regex → no filter for designs
    assert captured["name_regex"] is None or "designs" not in (captured["name_regex"] or {})


def test_export_getoffer_scope_designs_regex_overrides_autodetected(monkeypatch, tmp_path):
    """Explicit designs_name_regex overrides auto-detected value from scope."""
    # Set up scope that auto-detects a designs regex (via design_names in delivery)
    code = '''var CONFIG = {
  collectionByMbox: { "CSK-A": "essence-master" }
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
        return {"criteria": 1, "collections": 1, "designs": 3}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.target.getoffer_scope.export_recs",
        _fake_export_recs,
    )

    # Even though scope detection doesn't produce designs_name_regex here,
    # test that explicit designs_name_regex is applied regardless of whether
    # designs was already in scoped_resources.
    result = export_getoffer_scope(
        _MockClient({}),
        tmp_path / "out",
        tmp_path,
        code_path,
        include_designs=True,
        designs_name_regex="^(CustomDesign)$",
    )

    assert "designs" in captured["resources"]
    assert captured["name_regex"]["designs"] == "^(CustomDesign)$"


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
