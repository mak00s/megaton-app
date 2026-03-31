from __future__ import annotations

import json

import pytest

from megaton_lib.audit.providers.tag_config.adobe_tags import (
    _reactor_get,
    _reactor_post,
    _export_items,
    build_library,
    deploy_library,
    export_property,
    extract_mapping_from_settings,
    find_dirty_origin_rules,
    list_library_resources,
    list_rule_revisions,
    parse_settings_object,
    refresh_library_resources,
    revise_library_data_elements,
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
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(201, {"data": {"id": "BL1"}}),
    )
    result = _reactor_post(config, "/libraries/LB1/builds", {"data": {}})
    assert result["data"]["id"] == "BL1"


def test_reactor_post_error_raises(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(422, {"errors": [{"detail": "bad"}]}),
    )
    with pytest.raises(RuntimeError, match="POST failed"):
        _reactor_post(config, "/libraries/LB1/builds", {"data": {}})


# ---- build_library tests ----


def test_build_library_returns_summary(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(201, {
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

    def mock_request(method, url, **kw):
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
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        mock_request,
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
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(200, {
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
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(200, {
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

    def mock_request(method, url, **kw):
        if method == "DELETE":
            call_count["delete"] += 1
            return _Resp(200, {})
        # GET (for paginated list of library rules)
        return _Resp(200, {
            "data": [{
                "id": "RL-rev-old",
                "attributes": {"name": "Rule 1", "revision_number": 3, "dirty": False},
                "meta": {"latest_revision_number": 3},
                "relationships": {"origin": {"data": {"id": "RL-origin-1", "type": "rules"}}},
            }],
            "meta": {"pagination": {}},
        })

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        mock_post,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        mock_request,
    )
    result = revise_library_rules(config, "LB1", ["RL-origin-1"])
    assert result["revised_count"] == 1
    assert result["new_rule_ids"] == ["RL-new-1"]
    assert call_count["post"] == 2  # first 409, then retry
    assert call_count["delete"] == 1


def test_revise_library_data_elements_success(tags_env, monkeypatch):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.post",
        lambda url, headers, json, timeout: _Resp(200, {
            "data": [
                {"type": "data_elements", "id": "DE-new-1"},
                {"type": "data_elements", "id": "DE-new-2"},
            ],
        }),
    )
    result = revise_library_data_elements(config, "LB1", ["DE-origin-1", "DE-origin-2"])
    assert result["revised_count"] == 2
    assert result["new_de_ids"] == ["DE-new-1", "DE-new-2"]


def test_refresh_library_resources_rolls_back_failed_rule_refresh(tags_env, monkeypatch):
    config = _make_config()
    state = {
        "rules": [
            {
                "id": "RL-rev-1",
                "relationships": {"origin": {"data": {"id": "RL-origin-1", "type": "rules"}}},
            },
        ],
        "data_elements": [],
    }

    def mock_paginated_list(cfg, endpoint, *, sort="name", extra_query=""):
        if endpoint.endswith("/rules"):
            return [dict(item) for item in state["rules"]]
        if endpoint.endswith("/data_elements"):
            return [dict(item) for item in state["data_elements"]]
        raise AssertionError(endpoint)

    def mock_delete(cfg, endpoint, payload):
        remove_ids = {item["id"] for item in payload["data"]}
        if endpoint.endswith("/relationships/rules"):
            state["rules"] = [item for item in state["rules"] if item["id"] not in remove_ids]
            return
        if endpoint.endswith("/relationships/data_elements"):
            state["data_elements"] = [
                item for item in state["data_elements"] if item["id"] not in remove_ids
            ]
            return
        raise AssertionError(endpoint)

    def mock_post(cfg, endpoint, payload):
        restored = payload["data"][0]
        if endpoint.endswith("/relationships/rules"):
            state["rules"].append({
                "id": restored["id"],
                "relationships": {
                    "origin": {"data": {"id": "RL-origin-1", "type": "rules"}},
                },
            })
            return {"data": []}
        raise AssertionError(endpoint)

    def mock_revise(cfg, library_id, resource_type, origin_ids):
        raise RuntimeError("revise failed")

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._paginated_list",
        mock_paginated_list,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._reactor_delete",
        mock_delete,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._reactor_post",
        mock_post,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._revise_library_resources",
        mock_revise,
    )

    with pytest.raises(RuntimeError, match="revise failed"):
        refresh_library_resources(config, "LB1")

    assert [item["id"] for item in state["rules"]] == ["RL-rev-1"]


# ---- find_dirty_origin_rules tests ----


def test_find_dirty_origin_rules(tags_env, monkeypatch):
    config = _make_config()

    def mock_request(method, url, **kw):
        if "/libraries/" in url and "/rules" in url:
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
            return _Resp(200, {
                "data": {
                    "id": "RL-origin-1",
                    "attributes": {"name": "Rule 1", "revision_number": 0, "dirty": True},
                },
            })
        if "RL-origin-2" in url:
            return _Resp(200, {
                "data": {
                    "id": "RL-origin-2",
                    "attributes": {"name": "Rule 2", "revision_number": 0, "dirty": False},
                },
            })
        return _Resp(200, {"data": [], "meta": {"pagination": {}}})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        mock_request,
    )
    result = find_dirty_origin_rules(config, "LB1")
    assert result == ["RL-origin-1"]


# ---- deploy_library tests ----


def test_deploy_library(tags_env, monkeypatch):
    config = _make_config()
    call_log = []

    def mock_request(method, url, **kw):
        if method == "GET":
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
        # POST
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

    def mock_post(url, headers, json, timeout):
        """Handle direct requests.post calls from _revise_library_resources."""
        call_log.append(url)
        if "/relationships/rules" in url:
            return _Resp(200, {"data": [{"type": "rules", "id": "RL-new-1"}]})
        return _Resp(200, {})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        mock_request,
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


def test_export_property_no_longer_raises_name_error(tags_env, monkeypatch, tmp_path):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._reactor_get",
        lambda cfg, endpoint, params=None: {"data": {"id": "PR123"}},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._export_rules",
        lambda cfg, out_dir: 1,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._export_data_elements",
        lambda cfg, out_dir: 2,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.list_extensions",
        lambda cfg: [{"id": "EX1", "attributes": {"name": "Core"}}],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.list_environments",
        lambda cfg: [],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.list_libraries",
        lambda cfg: [],
    )

    result = export_property(config, tmp_path)

    assert result == {
        "rules": 1,
        "data-elements": 2,
        "extensions": 1,
        "environments": 0,
        "libraries": 0,
    }
    assert (tmp_path / "property.json").exists()
    assert (tmp_path / "extensions" / "ex1_core.json").exists()


def test_export_items_uses_id_prefixed_names_for_duplicate_titles(tmp_path):
    items = [
        {"id": "EX1", "attributes": {"name": "Same Name"}},
        {"id": "EX2", "attributes": {"name": "Same Name"}},
    ]

    count = _export_items(items, tmp_path)

    assert count == 2
    assert (tmp_path / "ex1_same-name.json").exists()
    assert (tmp_path / "ex2_same-name.json").exists()


# ---- 404 retry tests ----


def test_reactor_get_retries_on_404_with_oauth_cache(monkeypatch, tmp_path):
    """404 with an OAuth token cache should clear cache and retry once."""
    from megaton_lib.audit.config import AdobeOAuthConfig

    cache_file = tmp_path / ".token_cache.json"
    cache_file.write_text('{"access_token": "stale", "expires_at": 9999999999}')

    oauth = AdobeOAuthConfig(
        client_id="test-id",
        client_secret="test-secret",
        org_id="test-org",
        scopes="openid",
        token_cache_file=str(cache_file),
    )
    config = AdobeTagsConfig(property_id="PR123", oauth=oauth, page_size=25)

    call_count = {"n": 0}

    def fake_request(method, url, **kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _Resp(404, {"errors": [{"status": "404"}]})
        return _Resp(200, {"data": {"id": "PR123"}})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        fake_request,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags._get_auth_headers",
        lambda config: {"Authorization": "Bearer mock", "x-api-key": "mock"},
    )
    result = _reactor_get(config, "/properties/PR123")
    assert result["data"]["id"] == "PR123"
    assert call_count["n"] == 2
    assert not cache_file.exists()


def test_reactor_get_no_retry_without_oauth(tags_env, monkeypatch):
    """404 without OAuth config should raise immediately (no retry)."""
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.adobe_tags.requests.request",
        lambda method, url, **kw: _Resp(404, {"errors": [{"status": "404"}]}),
    )
    with pytest.raises(RuntimeError, match="404"):
        _reactor_get(config, "/properties/PR123")


# ---- _should_retry_404 unit tests ----


def test_should_retry_404_clears_cache(tmp_path):
    from megaton_lib.audit.providers.tag_config.adobe_tags import _should_retry_404
    from megaton_lib.audit.config import AdobeOAuthConfig

    cache = tmp_path / ".cache.json"
    cache.write_text("{}")
    oauth = AdobeOAuthConfig(token_cache_file=str(cache))
    config = AdobeTagsConfig(property_id="PR1", oauth=oauth)

    assert _should_retry_404(config) is True
    assert not cache.exists()


def test_should_retry_404_no_cache_file(tmp_path):
    from megaton_lib.audit.providers.tag_config.adobe_tags import _should_retry_404
    from megaton_lib.audit.config import AdobeOAuthConfig

    cache = tmp_path / "nonexistent.json"
    oauth = AdobeOAuthConfig(token_cache_file=str(cache))
    config = AdobeTagsConfig(property_id="PR1", oauth=oauth)

    assert _should_retry_404(config) is False


def test_should_retry_404_no_oauth():
    from megaton_lib.audit.providers.tag_config.adobe_tags import _should_retry_404

    config = AdobeTagsConfig(property_id="PR1")
    assert _should_retry_404(config) is False
