from __future__ import annotations

import json

import pytest

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.baseline import APPLY_BASELINE_FILENAME, hash_normalized_text
from megaton_lib.audit.providers.tag_config.workspace import (
    LIBRARY_SCOPE_FILENAME,
    ScopeItem,
    TAG_CONFLICTS_FILENAME,
    add_to_library_scope,
    build_library_scope_snapshot,
    checkout_library_scope,
    list_workspace_conflicts,
    pull_library_scope,
    push_library_scope,
    render_workspace_conflict,
    resolve_workspace_conflict,
    status_library_scope,
    workspace_result_exit_code,
)


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


def _snapshot(*, property_id: str = "PR123", library_id: str = "LB1", items=None, files=None):
    return {
        "property_id": property_id,
        "library_id": library_id,
        "generated_at": "2026-04-24T00:00:00Z",
        "items": items or [],
        "files": files or {
            "property.json": "{}\n",
            "rules/index.json": "[]\n",
            "data-elements/index.json": "[]\n",
            APPLY_BASELINE_FILENAME: json.dumps({"schema_version": 1, "property_id": property_id, "resources": {}}, ensure_ascii=False, indent=2) + "\n",
            LIBRARY_SCOPE_FILENAME: json.dumps({"schema_version": 1, "property_id": property_id, "library_id": library_id}, ensure_ascii=False, indent=2) + "\n",
        },
    }


def test_pull_library_scope_initial_sync(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    snapshot = _snapshot(
        files={
            "property.json": "{}\n",
            "rules/index.json": "[]\n",
            "data-elements/index.json": '[{"id":"DE1","name":"Demo"}]\n',
            "data-elements/de1_demo.json": '{"id":"DE1"}\n',
            APPLY_BASELINE_FILENAME: json.dumps(
                {
                    "schema_version": 1,
                    "property_id": "PR123",
                    "resources": {
                        "data-elements/de1_demo.json": {
                            "kind": "metadata",
                            "component_id": "DE1",
                            "resource_type": "data_elements",
                            "content_hash": hash_normalized_text('{"id":"DE1"}\n'),
                        }
                    },
                },
                ensure_ascii=False,
                indent=2,
            ) + "\n",
            LIBRARY_SCOPE_FILENAME: json.dumps({"schema_version": 1, "property_id": "PR123", "library_id": "LB1"}, ensure_ascii=False, indent=2) + "\n",
        },
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: snapshot,
    )

    result = pull_library_scope(_make_config(), root=root, library_id="LB1")

    assert (root / "data-elements/de1_demo.json").exists()
    assert result["summary"]["added"] == 3
    assert result["schema_version"] == 1
    assert result["command"] == "pull"
    assert result["ok"] is True
    assert result["exit_code"] == 0
    assert result["severity"] == "ok"


def test_pull_library_scope_accepts_snapshot_workers(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    seen = []

    def fake_snapshot(config, library_id, *, max_workers=None):
        seen.append(max_workers)
        return _snapshot()

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        fake_snapshot,
    )

    pull_library_scope(_make_config(), root=root, library_id="LB1", snapshot_workers=20)

    assert seen == [20]


def test_pull_library_scope_keeps_outside_scope_files_separate(monkeypatch, tags_env, tmp_path, capsys):
    root = tmp_path / "tags-root"
    rule_dir = root / "rules/outside"
    rule_dir.mkdir(parents=True)
    local_text = '{"id":"RL1","name":"Outside"}\n'
    (rule_dir / "rule.json").write_text(local_text, encoding="utf-8")
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "property_id": "PR123",
                "resources": {
                    "rules/outside/rule.json": {
                        "kind": "metadata",
                        "component_id": "RL1",
                        "resource_type": "rules",
                        "content_hash": hash_normalized_text(local_text),
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(items=[]),
    )

    result = pull_library_scope(_make_config(), root=root, library_id="LB1")

    assert (rule_dir / "rule.json").exists()
    assert result["summary"]["outside_library_scope_kept_local"] == 1
    assert result["exit_code"] == 4
    assert result["ok"] is False
    assert result["severity"] == "outside_scope"
    assert result["summary"].get("kept_local_removed", 0) == 0
    assert result["summary"].get("deleted", 0) == 0
    captured = capsys.readouterr()
    assert "outside_library_scope_files: 1" in captured.err
    assert "outside_library_scope_files_kept_local" not in captured.err


def test_pull_library_scope_summary_only_suppresses_warnings(monkeypatch, tags_env, tmp_path, capsys):
    root = tmp_path / "tags-root"
    rule_dir = root / "rules/outside"
    rule_dir.mkdir(parents=True)
    (rule_dir / "rule.json").write_text('{"id":"RL1","name":"Outside"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(items=[]),
    )

    pull_library_scope(_make_config(), root=root, library_id="LB1", summary_only=True)

    captured = capsys.readouterr()
    assert "Summary: pull completed" in captured.err
    assert "Warnings:" not in captured.err
    assert "Next:" not in captured.err


def test_pull_library_scope_writes_conflict_artifact(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    de_path = root / "data-elements/de1_demo.json"
    de_path.parent.mkdir(parents=True)
    baseline_text = '{"id":"DE1","name":"Base"}\n'
    local_text = '{"id":"DE1","name":"Local"}\n'
    remote_text = '{"id":"DE1","name":"Remote"}\n'
    de_path.write_text(local_text, encoding="utf-8")
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "property_id": "PR123",
                "resources": {
                    "data-elements/de1_demo.json": {
                        "kind": "metadata",
                        "component_id": "DE1",
                        "resource_type": "data_elements",
                        "baseline_text": baseline_text,
                        "content_hash": hash_normalized_text(baseline_text),
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(
            files={
                "property.json": "{}\n",
                "rules/index.json": "[]\n",
                "data-elements/index.json": '[{"id":"DE1","name":"Demo"}]\n',
                "data-elements/de1_demo.json": remote_text,
                APPLY_BASELINE_FILENAME: json.dumps(
                    {
                        "schema_version": 1,
                        "property_id": "PR123",
                        "resources": {
                            "data-elements/de1_demo.json": {
                                "kind": "metadata",
                                "component_id": "DE1",
                                "resource_type": "data_elements",
                                "content_hash": hash_normalized_text(remote_text),
                            }
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ) + "\n",
                LIBRARY_SCOPE_FILENAME: json.dumps(
                    {
                        "schema_version": 1,
                        "property_id": "PR123",
                        "library_id": "LB1",
                        "data_elements": [{"id": "DE1", "revision_id": "DErev1", "name": "Demo"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ) + "\n",
            },
        ),
    )

    result = pull_library_scope(_make_config(), root=root, library_id="LB1")

    assert result["summary"]["conflicts"] == 1
    assert result["exit_code"] == 2
    assert result["severity"] == "conflict"
    assert (root / TAG_CONFLICTS_FILENAME).exists()
    payload = list_workspace_conflicts(root)
    assert payload["property_id"] == "PR123"
    assert payload["library_id"] == "LB1"
    assert payload["conflicts"][0]["path"] == "data-elements/de1_demo.json"
    assert payload["conflicts"][0]["baseline_text"] == baseline_text
    assert payload["conflicts"][0]["local_text"] == local_text
    assert payload["conflicts"][0]["remote_text"] == remote_text
    rendered = render_workspace_conflict(root, "data-elements/de1_demo.json")
    assert "=== baseline -> local ===" in rendered
    assert "--- baseline/data-elements/de1_demo.json" in rendered
    assert "--- local/data-elements/de1_demo.json" in rendered
    assert "+++ remote/data-elements/de1_demo.json" in rendered
    assert '-{"id":"DE1","name":"Base"}' in rendered
    assert '-{"id":"DE1","name":"Local"}' in rendered
    assert '+{"id":"DE1","name":"Remote"}' in rendered


def test_resolve_workspace_conflict_dry_run_keeps_files(tmp_path):
    root = tmp_path / "tags-root"
    path = root / "data-elements/de1_demo.json"
    path.parent.mkdir(parents=True)
    path.write_text("local\n", encoding="utf-8")
    (root / TAG_CONFLICTS_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [
                    {
                        "path": "data-elements/de1_demo.json",
                        "local_text": "local\n",
                        "remote_text": "remote\n",
                    },
                ],
            },
        )
        + "\n",
        encoding="utf-8",
    )

    result = resolve_workspace_conflict(root, "data-elements/de1_demo.json", use="remote", apply=False)

    assert result["summary"]["ready_to_resolve"] == 1
    assert result["ok"] is True
    assert path.read_text(encoding="utf-8") == "local\n"
    assert (root / TAG_CONFLICTS_FILENAME).exists()


def test_resolve_workspace_conflict_apply_remote_updates_file_and_removes_artifact(tmp_path):
    root = tmp_path / "tags-root"
    path = root / "data-elements/de1_demo.json"
    path.parent.mkdir(parents=True)
    path.write_text("local\n", encoding="utf-8")
    (root / TAG_CONFLICTS_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [
                    {
                        "path": "data-elements/de1_demo.json",
                        "local_text": "local\n",
                        "remote_text": "remote\n",
                    },
                ],
            },
        )
        + "\n",
        encoding="utf-8",
    )

    result = resolve_workspace_conflict(root, "data-elements/de1_demo.json", use="remote", apply=True)

    assert result["summary"]["resolved"] == 1
    assert path.read_text(encoding="utf-8") == "remote\n"
    assert not (root / TAG_CONFLICTS_FILENAME).exists()


def test_resolve_workspace_conflict_apply_local_removes_only_target(tmp_path):
    root = tmp_path / "tags-root"
    path = root / "data-elements/de1_demo.json"
    path.parent.mkdir(parents=True)
    path.write_text("local\n", encoding="utf-8")
    (root / TAG_CONFLICTS_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [
                    {
                        "path": "data-elements/de1_demo.json",
                        "local_text": "local\n",
                        "remote_text": "remote\n",
                    },
                    {
                        "path": "data-elements/de2_demo.json",
                        "local_text": "local2\n",
                        "remote_text": "remote2\n",
                    },
                ],
            },
        )
        + "\n",
        encoding="utf-8",
    )

    result = resolve_workspace_conflict(root, "data-elements/de1_demo.json", use="local", apply=True)

    assert result["summary"]["resolved"] == 1
    assert path.read_text(encoding="utf-8") == "local\n"
    payload = list_workspace_conflicts(root)
    assert [item["path"] for item in payload["conflicts"]] == ["data-elements/de2_demo.json"]


def test_resolve_workspace_conflict_apply_baseline_updates_file(tmp_path):
    root = tmp_path / "tags-root"
    path = root / "data-elements/de1_demo.json"
    path.parent.mkdir(parents=True)
    path.write_text("local\n", encoding="utf-8")
    (root / TAG_CONFLICTS_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [
                    {
                        "path": "data-elements/de1_demo.json",
                        "baseline_text": "base\n",
                        "local_text": "local\n",
                        "remote_text": "remote\n",
                    },
                ],
            },
        )
        + "\n",
        encoding="utf-8",
    )

    result = resolve_workspace_conflict(root, "data-elements/de1_demo.json", use="baseline", apply=True)

    assert result["summary"]["resolved"] == 1
    assert path.read_text(encoding="utf-8") == "base\n"
    assert not (root / TAG_CONFLICTS_FILENAME).exists()


def test_checkout_library_scope_force_allows_existing_local_files(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    (root / "rules/demo").mkdir(parents=True)
    (root / "rules/demo/rule.json").write_text('{"id":"RL1","name":"Local"}\n', encoding="utf-8")
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "property_id": "PR123",
                "resources": {
                    "rules/demo/rule.json": {
                        "kind": "metadata",
                        "component_id": "RL1",
                        "resource_type": "rules",
                        "content_hash": hash_normalized_text('{"id":"RL1","name":"Remote"}\n'),
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(),
    )

    result = checkout_library_scope(_make_config(), root=root, library_id="LB1", force=True)

    assert result["summary"]["deleted"] == 1
    assert not (root / "rules/demo/rule.json").exists()


def test_checkout_library_scope_requires_force_when_managed_files_exist(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    (root / "rules/demo").mkdir(parents=True)
    (root / "rules/demo/rule.json").write_text('{"id":"RL1","name":"Remote"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(),
    )

    with pytest.raises(RuntimeError, match="without --force"):
        checkout_library_scope(_make_config(), root=root, library_id="LB1", force=False)


def test_status_library_scope_reports_out_of_library(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    (root / "rules/demo").mkdir(parents=True)
    (root / "data-elements").mkdir(parents=True)
    (root / "rules/demo/rule.json").write_text('{"id":"RL1","name":"Demo"}\n', encoding="utf-8")
    (root / "data-elements/de1_demo.json").write_text('{"id":"DE1","name":"Demo DE"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(items=[]),
    )

    result = status_library_scope(_make_config(), root=root, library_id="LB1")

    assert result["summary"]["outside_library_scope"] == 2
    assert result["exit_code"] == 4
    assert result["summary"]["outside_library_scope_resources"] == 2
    assert result["summary"]["outside_library_scope_rules"] == 1
    assert result["summary"]["outside_library_scope_data_elements"] == 1
    assert result["summary"].get("kept_local_removed", 0) == 0


def test_status_library_scope_groups_out_of_library_warnings(monkeypatch, tags_env, tmp_path, capsys):
    root = tmp_path / "tags-root"
    (root / "rules/demo").mkdir(parents=True)
    (root / "data-elements").mkdir(parents=True)
    (root / "rules/demo/rule.json").write_text('{"id":"RL1","name":"Demo"}\n', encoding="utf-8")
    (root / "data-elements/de1_demo.json").write_text('{"id":"DE1","name":"Demo DE"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(items=[]),
    )

    status_library_scope(_make_config(), root=root, library_id="LB1")

    captured = capsys.readouterr()
    assert "outside_library_scope_files: 2" in captured.err
    assert "[OUTSIDE_LIBRARY_SCOPE_RESOURCES] 2 resources skipped" in captured.err
    assert "rules: 1" in captured.err
    assert "data_elements: 1" in captured.err
    assert "--verbose for full list" in captured.err


def test_status_library_scope_since_pull_uses_local_baseline(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    (root / "data-elements").mkdir(parents=True)
    local_text = '{"id":"DE1","name":"Demo"}\n'
    (root / "data-elements/de1_demo.json").write_text(local_text, encoding="utf-8")
    (root / APPLY_BASELINE_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "property_id": "PR123",
                "resources": {
                    "data-elements/de1_demo.json": {
                        "kind": "metadata",
                        "component_id": "DE1",
                        "resource_type": "data_elements",
                        "content_hash": hash_normalized_text(local_text),
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    (root / LIBRARY_SCOPE_FILENAME).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "property_id": "PR123",
                "library_id": "LB1",
                "data_elements": [{"id": "DE1", "revision_id": "DErev1", "name": "Demo"}],
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: (_ for _ in ()).throw(AssertionError("remote fetch should not run")),
    )

    result = status_library_scope(_make_config(), root=root, library_id="LB1", since_pull=True)

    assert result["mode"] == "since_pull"
    assert result["summary"]["unchanged"] == 1
    assert result["command"] == "status"
    assert result["ok"] is True


def test_status_library_scope_summary_only_suppresses_warnings(monkeypatch, tags_env, tmp_path, capsys):
    root = tmp_path / "tags-root"
    (root / "rules/demo").mkdir(parents=True)
    (root / "rules/demo/rule.json").write_text('{"id":"RL1","name":"Demo"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.build_library_scope_snapshot",
        lambda config, library_id: _snapshot(items=[]),
    )

    status_library_scope(_make_config(), root=root, library_id="LB1", summary_only=True)

    captured = capsys.readouterr()
    assert "Summary: status" in captured.err
    assert "Warnings:" not in captured.err
    assert "Next:" not in captured.err


def test_workspace_result_exit_code_contract():
    assert workspace_result_exit_code({"summary": {}}) == 0
    assert workspace_result_exit_code({"summary": {"conflicts": 1}}) == 2
    assert workspace_result_exit_code({"summary": {"name_conflicts": 1}}) == 2
    assert workspace_result_exit_code({"summary": {"stale_remote": 1}}) == 3
    assert workspace_result_exit_code({"summary": {"kept_local_removed": 1}}) == 3
    assert workspace_result_exit_code({"summary": {"outside_library_scope": 1}}) == 4
    assert workspace_result_exit_code({"summary": {"outside_library_scope_resources": 1}}) == 4


def test_build_library_scope_snapshot_emits_elapsed_heartbeat(monkeypatch, tags_env, capsys):
    config = _make_config()
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._library_scope_items",
        lambda config, library_id: [
            ScopeItem("rules", "RL1", "RLrev1", "Rule 1"),
            ScopeItem("rules", "RL2", "RLrev2", "Rule 2"),
            ScopeItem("data_elements", "DE1", "DErev1", "DE 1"),
        ],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._reactor_get",
        lambda config, path: {"data": {"id": "PR123", "attributes": {"name": "Demo"}}}
        if path == "/properties/PR123"
        else {"data": {}},
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._rule_snapshot_entry",
        lambda config, rule_id: (
            {f"rules/{rule_id.lower()}/rule.json": "{}\n"},
            {
                f"rules/{rule_id.lower()}/rule.json": {
                    "kind": "metadata",
                    "component_id": rule_id,
                    "resource_type": "rules",
                    "content_hash": hash_normalized_text("{}\n"),
                }
            },
            {"id": rule_id, "name": rule_id, "component_count": 0},
        ),
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._data_element_snapshot_entry",
        lambda config, data_element_id: (
            {f"data-elements/{data_element_id.lower()}.json": "{}\n"},
            {
                f"data-elements/{data_element_id.lower()}.json": {
                    "kind": "metadata",
                    "component_id": data_element_id,
                    "resource_type": "data_elements",
                    "content_hash": hash_normalized_text("{}\n"),
                }
            },
            {"id": data_element_id, "name": data_element_id},
        ),
    )
    ticks = iter([0.0, 0.0, 6.2, 12.4, 12.4, 12.4])
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.time.monotonic",
        lambda: next(ticks),
    )

    build_library_scope_snapshot(config, "LB1", max_workers=1)

    captured = capsys.readouterr().err
    assert "snapshot_workers=1" in captured
    assert "exported 1/3 rules:RL1 elapsed=0.0s" in captured
    assert "exported 2/3 rules:RL2 elapsed=6.2s" in captured
    assert "exported 3/3 data_elements:DE1 elapsed=12.4s" in captured


def test_add_to_library_scope_reports_already_in_library(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    rule_dir = root / "rules/demo"
    rule_dir.mkdir(parents=True)
    (rule_dir / "rule.json").write_text('{"id":"RL1","attributes":{"name":"Demo"}}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._library_scope_items",
        lambda config, library_id: [
            type("Item", (), {"resource_type": "rules", "origin_id": "RL1", "revision_id": "RLrev1", "name": "Demo"})(),
        ],
    )

    result = add_to_library_scope(
        _make_config(),
        root=root,
        library_id="LB1",
        from_paths=[str(rule_dir)],
        apply=False,
    )

    assert result["summary"]["already_in_library"] == 1


def test_push_library_scope_dry_run_reports_out_of_library_resource(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    rule_dir = root / "rules/demo"
    rule_dir.mkdir(parents=True)
    (rule_dir / "rule.json").write_text('{"id":"RL1","name":"Demo"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._library_scope_items",
        lambda config, library_id: [],
    )

    result = push_library_scope(_make_config(), root=root, library_id="LB1", apply=False)

    assert result["summary"]["outside_library_scope"] == 1
    assert result["summary"]["outside_library_scope_resources"] == 1
    assert result["exit_code"] == 4
    assert result["severity"] == "outside_scope"


def test_push_library_scope_apply_rejects_out_of_library_resource(monkeypatch, tags_env, tmp_path):
    root = tmp_path / "tags-root"
    rule_dir = root / "rules/demo"
    rule_dir.mkdir(parents=True)
    (rule_dir / "rule.json").write_text('{"id":"RL1","name":"Demo"}\n', encoding="utf-8")
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace._library_scope_items",
        lambda config, library_id: [],
    )

    with pytest.raises(RuntimeError, match="not in the current library scope"):
        push_library_scope(_make_config(), root=root, library_id="LB1", apply=True)
