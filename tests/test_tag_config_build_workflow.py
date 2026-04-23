from __future__ import annotations

import pytest

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.build_workflow import run_build_workflow, wait_for_build_completion


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


def test_wait_for_build_completion_succeeds(monkeypatch, tags_env):
    config = _make_config()
    statuses = iter([
        {"id": "BL1", "status": "pending", "updated_at": "t1", "created_at": "t0"},
        {"id": "BL1", "status": "succeeded", "updated_at": "t2", "created_at": "t0"},
    ])

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.get_build_status",
        lambda cfg, build_id: next(statuses),
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.time.sleep",
        lambda seconds: None,
    )

    result = wait_for_build_completion(config, "BL1", timeout_seconds=30, poll_interval_seconds=1)

    assert result["status"] == "succeeded"


def test_wait_for_build_completion_times_out(monkeypatch, tags_env):
    config = _make_config()

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.get_build_status",
        lambda cfg, build_id: {"id": "BL1", "status": "pending", "updated_at": "t1", "created_at": "t0"},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.time.sleep",
        lambda seconds: None,
    )

    times = iter([0, 1, 2, 3, 4, 5, 6])
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.time.monotonic",
        lambda: next(times),
    )

    with pytest.raises(RuntimeError, match="did not complete within 3s"):
        wait_for_build_completion(config, "BL1", timeout_seconds=3, poll_interval_seconds=1)


def test_run_build_workflow_fails_fast_on_failed_build(monkeypatch, tags_env, tmp_path):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.collect_changed_resources",
        lambda cfg, root, dry_run, allow_stale_base=False: [{"id": "RL1", "type": "rules"}],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.refresh_library_resources",
        lambda cfg, library_id, new_resources: {"rules_count": 1, "data_elements_count": 2},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.build_library",
        lambda cfg, library_id: {"id": "BL1", "status": "pending"},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.wait_for_build_completion",
        lambda cfg, build_id, timeout_seconds, poll_interval_seconds: {
            "id": build_id,
            "status": "failed",
            "updated_at": "t1",
            "created_at": "t0",
        },
    )

    export_called = {"called": False}

    def _export_property(cfg, root, resources=None):
        export_called["called"] = True
        export_called["resources"] = resources
        return {}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.export_property",
        _export_property,
    )

    rc = run_build_workflow(
        config,
        root=tmp_path,
        library_id="LB1",
        apply=True,
        build_wait_timeout=30,
        build_poll_interval=1,
    )

    assert rc == 2
    assert export_called["called"] is False


def test_run_build_workflow_reexports_rules_and_data_elements_by_default(monkeypatch, tags_env, tmp_path):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.collect_changed_resources",
        lambda cfg, root, dry_run, allow_stale_base=False: [{"id": "RL1", "type": "rules"}],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.refresh_library_resources",
        lambda cfg, library_id, new_resources: {"rules_count": 1, "data_elements_count": 0},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.build_library",
        lambda cfg, library_id: {"id": "BL1", "status": "pending"},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.wait_for_build_completion",
        lambda cfg, build_id, timeout_seconds, poll_interval_seconds: {
            "id": build_id,
            "status": "succeeded",
            "updated_at": "t1",
            "created_at": "t0",
        },
    )
    export_calls = []
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.build_workflow.export_property",
        lambda cfg, root, resources=None: export_calls.append(resources) or {"rules": {}, "data-elements": {}},
    )

    rc = run_build_workflow(config, root=tmp_path, library_id="LB1", apply=True)

    assert rc == 0
    assert export_calls == [["rules", "data-elements"]]
