from __future__ import annotations

import json

import pytest

from megaton_lib.audit.providers.tag_config.adobe_tags import (
    _reactor_post,
    build_library,
    deploy_library,
    extract_mapping_from_settings,
    find_dirty_origin_rules,
    list_library_resources,
    list_rule_revisions,
    parse_settings_object,
    revise_library_rules,
)
from megaton_lib.audit.config import AdobeTagsConfig


def test_parse_settings_object_json_string() -> None:
    raw = '{"map":[{"key":"^/jp/","value":"JP"}]}'
    settings = parse_settings_object(raw)
    assert settings["map"][0]["key"] == "^/jp/"


def test_extract_mapping_from_settings_list() -> None:
    settings = {
        "map": [
            {"key": "^/jp/", "value": "JP"},
            {"pattern": "^/en/", "output": "EN"},
        ]
    }
    mapping = extract_mapping_from_settings(settings)
    assert mapping == {"^/jp/": "JP", "^/en/": "EN"}


def test_extract_mapping_from_settings_dict() -> None:
    settings = {
        "mappings": {
            "^/jp/": "JP",
            "^/en/": "EN",
        }
    }
    mapping = extract_mapping_from_settings(settings)
    assert mapping == {"^/jp/": "JP", "^/en/": "EN"}


# ---- test infrastructure for HTTP-calling functions ----


class _Resp:
    """Minimal requests.Response stub."""

    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        return self._payload


def _make_config() -> AdobeTagsConfig:
    return AdobeTagsConfig(
        property_id="PR123",
        api_key_env="TEST_API_KEY",
        bearer_token_env="TEST_BEARER_TOKEN",
        page_size=25,
    )


@pytest.fixture()
def tags_env(monkeypatch):
    monkeypatch.setenv("TEST_API_KEY", "test-key")
    monkeypatch.setenv("TEST_BEARER_TOKEN", "test-token")


# ---- _reactor_post tests ----


def test_reactor_post_success(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        lambda url, headers, json, timeout: _Resp(201, {"data": {"id": "BL1"}}),
    )
    result = _reactor_post(config, "/libraries/LB1/builds", {"data": {}})
    assert result["data"]["id"] == "BL1"


def test_reactor_post_error_raises(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        lambda url, headers, json, timeout: _Resp(422, {"errors": [{"detail": "bad"}]}),
    )
    with pytest.raises(RuntimeError, match="POST failed"):
        _reactor_post(config, "/libraries/LB1/builds", {"data": {}})


# ---- build_library tests ----


def test_build_library_returns_summary(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        lambda url, headers, json, timeout: _Resp(201, {
            "data": {
                "id": "BL123",
                "type": "builds",
                "attributes": {
                    "status": "pending",
                    "created_at": "2026-03-07T10:00:00Z",
                },
            },
        }),
    )
    result = build_library(config, "LB456")
    assert result["id"] == "BL123"
    assert result["status"] == "pending"
    assert result["created_at"] == "2026-03-07T10:00:00Z"


# ---- list_library_resources tests ----


def test_list_library_resources_detects_stale(tags_env, monkeypatch):
    config = _make_config()

    def mock_get(url, headers, timeout):
        if "/rules" in url:
            return _Resp(200, {
                "data": [{
                    "id": "RL1",
                    "attributes": {"name": "Rule 1", "revision_number": 2, "dirty": False},
                    "meta": {"latest_revision_number": 3},
                }],
                "meta": {"pagination": {}},
            })
        return _Resp(200, {
            "data": [{
                "id": "DE1",
                "attributes": {"name": "DE 1", "revision_number": 5, "dirty": False},
                "meta": {"latest_revision_number": 5},
            }],
            "meta": {"pagination": {}},
        })

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        mock_get,
    )
    result = list_library_resources(config, "LB789")
    assert len(result["rules"]) == 1
    assert len(result["data_elements"]) == 1
    assert len(result["stale"]) == 1
    assert result["stale"][0]["id"] == "RL1"
    assert result["stale"][0]["type"] == "rules"


def test_list_library_resources_no_stale(tags_env, monkeypatch):
    config = _make_config()

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        lambda url, headers, timeout: _Resp(200, {
            "data": [{
                "id": "RL1",
                "attributes": {"name": "Rule 1", "revision_number": 5, "dirty": False},
                "meta": {"latest_revision_number": 5},
            }],
            "meta": {"pagination": {}},
        }),
    )
    result = list_library_resources(config, "LB789")
    assert result["stale"] == []


# ---- list_rule_revisions tests ----


def test_list_rule_revisions(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        lambda url, headers, timeout: _Resp(200, {
            "data": [
                {
                    "id": "RL1-rev1",
                    "attributes": {"revision_number": 1, "created_at": "2026-01-01T00:00:00Z"},
                    "meta": {},
                },
                {
                    "id": "RL1-rev2",
                    "attributes": {"revision_number": 2, "created_at": "2026-02-01T00:00:00Z"},
                    "meta": {},
                },
            ],
            "meta": {"pagination": {}},
        }),
    )
    result = list_rule_revisions(config, "RL1")
    assert len(result) == 2
    assert result[0]["revision_number"] == 1
    assert result[1]["revision_number"] == 2
    assert result[1]["id"] == "RL1-rev2"


# ---- revise_library_rules tests ----


def test_revise_library_rules_success(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        lambda url, headers, json, timeout: _Resp(200, {
            "data": [
                {"type": "rules", "id": "RL-new-1"},
                {"type": "rules", "id": "RL-new-2"},
            ],
        }),
    )
    result = revise_library_rules(config, "LB1", ["RL-origin-1", "RL-origin-2"])
    assert result["revised_count"] == 2
    assert result["new_rule_ids"] == ["RL-new-1", "RL-new-2"]


def test_revise_library_rules_empty(tags_env):
    config = _make_config()
    result = revise_library_rules(config, "LB1", [])
    assert result["revised_count"] == 0
    assert result["new_rule_ids"] == []


def test_revise_library_rules_409_retry(tags_env, monkeypatch):
    """When POST revise returns 409, old revisions are deleted and retried."""
    config = _make_config()
    call_count = {"post": 0, "delete": 0}

    def mock_post(url, headers, json, timeout):
        call_count["post"] += 1
        if call_count["post"] == 1:
            return _Resp(409, {"errors": [{"detail": "conflict"}]})
        return _Resp(200, {
            "data": [{"type": "rules", "id": "RL-new-1"}],
        })

    def mock_get(url, headers, timeout):
        return _Resp(200, {
            "data": [{
                "id": "RL-rev-old",
                "attributes": {"name": "Rule 1", "revision_number": 3, "dirty": False},
                "meta": {"latest_revision_number": 3},
                "relationships": {"origin": {"data": {"id": "RL-origin-1", "type": "rules"}}},
            }],
            "meta": {"pagination": {}},
        })

    def mock_delete(url, headers, json, timeout):
        call_count["delete"] += 1
        return _Resp(200, {})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        mock_post,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        mock_get,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.delete",
        mock_delete,
    )
    result = revise_library_rules(config, "LB1", ["RL-origin-1"])
    assert result["revised_count"] == 1
    assert result["new_rule_ids"] == ["RL-new-1"]
    assert call_count["post"] == 2  # first 409, then retry
    assert call_count["delete"] == 1


# ---- find_dirty_origin_rules tests ----


def test_find_dirty_origin_rules(tags_env, monkeypatch):
    config = _make_config()

    def mock_get(url, headers, timeout):
        if "/libraries/" in url and "/rules" in url:
            # Library rules list (revision copies)
            return _Resp(200, {
                "data": [
                    {
                        "id": "RL-rev-1",
                        "attributes": {"name": "Rule 1", "revision_number": 3, "dirty": False},
                        "meta": {"latest_revision_number": 3},
                        "relationships": {"origin": {"data": {"id": "RL-origin-1", "type": "rules"}}},
                    },
                    {
                        "id": "RL-rev-2",
                        "attributes": {"name": "Rule 2", "revision_number": 5, "dirty": False},
                        "meta": {"latest_revision_number": 5},
                        "relationships": {"origin": {"data": {"id": "RL-origin-2", "type": "rules"}}},
                    },
                ],
                "meta": {"pagination": {}},
            })
        if "RL-origin-1" in url:
            # Origin rule 1: dirty
            return _Resp(200, {
                "data": {
                    "id": "RL-origin-1",
                    "attributes": {"name": "Rule 1", "revision_number": 0, "dirty": True},
                },
            })
        if "RL-origin-2" in url:
            # Origin rule 2: clean
            return _Resp(200, {
                "data": {
                    "id": "RL-origin-2",
                    "attributes": {"name": "Rule 2", "revision_number": 0, "dirty": False},
                },
            })
        return _Resp(200, {"data": [], "meta": {"pagination": {}}})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        mock_get,
    )
    result = find_dirty_origin_rules(config, "LB1")
    assert result == ["RL-origin-1"]


# ---- deploy_library tests ----


def test_deploy_library(tags_env, monkeypatch):
    config = _make_config()
    call_log = []

    def mock_get(url, headers, timeout):
        if "/libraries/" in url and "/rules" in url:
            return _Resp(200, {
                "data": [{
                    "id": "RL-rev-1",
                    "attributes": {"name": "Rule 1", "revision_number": 3, "dirty": False},
                    "meta": {"latest_revision_number": 3},
                    "relationships": {"origin": {"data": {"id": "RL-origin-1", "type": "rules"}}},
                }],
                "meta": {"pagination": {}},
            })
        if "RL-origin-1" in url:
            return _Resp(200, {
                "data": {
                    "id": "RL-origin-1",
                    "attributes": {"name": "Rule 1", "revision_number": 0, "dirty": True},
                },
            })
        return _Resp(200, {"data": [], "meta": {"pagination": {}}})

    def mock_post(url, headers, json, timeout):
        call_log.append(url)
        if "/relationships/rules" in url:
            return _Resp(200, {"data": [{"type": "rules", "id": "RL-new-1"}]})
        if "/builds" in url:
            return _Resp(201, {
                "data": {
                    "id": "BL123",
                    "type": "builds",
                    "attributes": {"status": "pending", "created_at": "2026-03-07T15:00:00Z"},
                },
            })
        return _Resp(200, {})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.get",
        mock_get,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        mock_post,
    )

    result = deploy_library(config, "LB1")
    assert result["dirty_count"] == 1
    assert result["revised_count"] == 1
    assert result["build"]["id"] == "BL123"
    assert result["build"]["status"] == "pending"
    # Verify both revise POST and build POST were called
    assert any("/relationships/rules" in u for u in call_log)
    assert any("/builds" in u for u in call_log)
