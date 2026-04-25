from __future__ import annotations

import json

import pytest

from megaton_lib.audit.config import AdobeTagsConfig
from megaton_lib.audit.providers.tag_config.cli import analysis_tags_workspace_main, tags_workspace_main


def _factory(*, property_id: str, page_size: int = 100) -> AdobeTagsConfig:
    return AdobeTagsConfig(
        property_id=property_id,
        api_key_env="TEST_API_KEY",
        bearer_token_env="TEST_BEARER_TOKEN",
        page_size=page_size,
    )


def _env_file(root, account: str = "csk", property_id: str = "PR123", library_id: str = "LB1") -> None:
    (root / f".env.{account}").write_text(
        "\n".join(
            [
                f"TAGS_PROPERTY_ID={property_id}",
                f"TAGS_DEV_LIBRARY_ID={library_id}",
                "TEST_API_KEY=test-key",
                "TEST_BEARER_TOKEN=test-token",
            ],
        )
        + "\n",
        encoding="utf-8",
    )


def test_tags_workspace_main_status_json_exits_with_result_code(monkeypatch, tmp_path, capsys):
    _env_file(tmp_path)
    seen = {}

    def fake_status(config, **kwargs):
        seen.update(kwargs)
        return {
            "schema_version": 1,
            "command": "status",
            "mode": "since_pull",
            "ok": False,
            "exit_code": 4,
            "severity": "outside_scope",
            "summary": {"outside_library_scope": 1},
            "details": {},
        }

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.status_library_scope",
        fake_status,
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(
            tags_config_factory=_factory,
            project_root=tmp_path,
            argv=["--account", "csk", "--format", "json", "status", "--since-pull"],
        )

    assert exc.value.code == 4
    payload = json.loads(capsys.readouterr().out)
    assert payload["exit_code"] == 4
    assert seen["library_id"] == "LB1"
    assert seen["since_pull"] is True


def test_tags_workspace_main_pull_passes_workers_and_summary_flags(monkeypatch, tmp_path):
    _env_file(tmp_path)
    seen = {}

    def fake_pull(config, **kwargs):
        seen.update(kwargs)
        return {"summary": {}, "details": {}, "exit_code": 0}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.pull_library_scope",
        fake_pull,
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(
            tags_config_factory=_factory,
            project_root=tmp_path,
            argv=["--account", "csk", "--workers", "20", "--summary-only", "pull"],
        )

    assert exc.value.code == 0
    assert seen["snapshot_workers"] == 20
    assert seen["summary_only"] is True
    assert seen["verbose"] is False


def test_tags_workspace_main_explicit_account_ignores_stale_env_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("ACCOUNT", "wws")
    monkeypatch.setenv("TAGS_PROPERTY_ID", "PR-WWS")
    monkeypatch.setenv("TAGS_DEV_LIBRARY_ID", "LB-WWS")
    _env_file(tmp_path, account="csk", property_id="PR-CSK", library_id="LB-CSK")
    seen = {}

    def fake_status(config, **kwargs):
        seen["property_id"] = config.property_id
        seen.update(kwargs)
        return {"summary": {}, "details": {}, "exit_code": 0}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.status_library_scope",
        fake_status,
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(
            tags_config_factory=_factory,
            project_root=tmp_path,
            argv=["--account", "csk", "status", "--since-pull"],
        )

    assert exc.value.code == 0
    assert seen["property_id"] == "PR-CSK"
    assert seen["library_id"] == "LB-CSK"


def test_tags_workspace_main_conflict_list_does_not_require_account(tmp_path, capsys):
    root = tmp_path / "tags-root"
    root.mkdir()
    (root / ".tag-conflicts.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [{"path": "data-elements/de1.json"}],
            },
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(project_root=tmp_path, argv=["--root", str(root), "--format", "json", "conflict", "--list"])

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "conflict"
    assert payload["mode"] == "list"
    assert payload["exit_code"] == 2
    assert payload["summary"]["conflicts"] == 1
    assert payload["details"]["conflicts"][0]["path"] == "data-elements/de1.json"


def test_tags_workspace_main_conflict_list_bootstraps_env_when_root_omitted(tmp_path, capsys):
    _env_file(tmp_path)
    root = tmp_path / "adobe-tags/PR123"
    root.mkdir(parents=True)
    (root / ".tag-conflicts.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [{"path": "data-elements/de1.json"}],
            },
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(project_root=tmp_path, argv=["--account", "csk", "--format", "json", "conflict", "--list"])

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "conflict"
    assert payload["summary"]["conflicts"] == 1
    assert payload["details"]["conflicts"][0]["path"] == "data-elements/de1.json"


def test_tags_workspace_main_conflict_resolve_json(tmp_path, capsys):
    root = tmp_path / "tags-root"
    path = root / "data-elements/de1.json"
    path.parent.mkdir(parents=True)
    path.write_text("local\n", encoding="utf-8")
    (root / ".tag-conflicts.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "conflicts": [
                    {
                        "path": "data-elements/de1.json",
                        "local_text": "local\n",
                        "remote_text": "remote\n",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(
            project_root=tmp_path,
            argv=[
                "--root",
                str(root),
                "--format",
                "json",
                "conflict",
                "--resolve",
                "data-elements/de1.json",
                "--use",
                "remote",
                "--apply",
            ],
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "conflict"
    assert payload["mode"] == "resolve"
    assert payload["summary"]["resolved"] == 1
    assert path.read_text(encoding="utf-8") == "remote\n"


def test_tags_workspace_main_push_apply_runs_local_status_hooks(monkeypatch, tmp_path):
    _env_file(tmp_path)
    calls = []

    def fake_status(config, **kwargs):
        calls.append(("status", kwargs))
        return {"summary": {}, "details": {}, "exit_code": 0}

    def fake_push(config, **kwargs):
        calls.append(("push", kwargs))
        return {"summary": {}, "details": {}, "exit_code": 0}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.status_library_scope",
        fake_status,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.push_library_scope",
        fake_push,
    )

    with pytest.raises(SystemExit) as exc:
        tags_workspace_main(
            tags_config_factory=_factory,
            project_root=tmp_path,
            argv=["--account", "csk", "push", "--apply"],
        )

    assert exc.value.code == 0
    assert [name for name, _ in calls] == ["status", "push", "status"]
    assert calls[0][1]["since_pull"] is True
    assert calls[0][1]["summary_only"] is True


def test_analysis_tags_workspace_main_uses_repo_factory_and_account_cache(monkeypatch, tmp_path):
    key_dir = tmp_path / "key"
    key_dir.mkdir()
    (key_dir / "adobe_credentials.json").write_text(
        json.dumps({"client_id": "file-id", "client_secret": "file-secret", "org_id": "file-org"}),
        encoding="utf-8",
    )
    _env_file(tmp_path, account="wws", property_id="PR-WWS", library_id="LB-WWS")
    seen = {}

    def fake_status(config, **kwargs):
        seen["property_id"] = config.property_id
        seen["token_cache_file"] = config.oauth.token_cache_file
        seen.update(kwargs)
        return {"summary": {}, "details": {}, "exit_code": 0}

    monkeypatch.setattr(
        "megaton_lib.audit.providers.tag_config.workspace.status_library_scope",
        fake_status,
    )

    with pytest.raises(SystemExit) as exc:
        analysis_tags_workspace_main(
            project_root=tmp_path,
            account_hints={"wws": {"property_ids": ["PR-WWS"]}},
            known_accounts=("wws",),
            credentials_candidates=["key/adobe_credentials.json"],
            token_cache_dir="key",
            argv=["--account", "wws", "status", "--since-pull"],
        )

    assert exc.value.code == 0
    assert seen["property_id"] == "PR-WWS"
    assert seen["library_id"] == "LB-WWS"
    assert seen["token_cache_file"] == str(
        (tmp_path / "key" / ".adobe_token_cache.wws.json").resolve(),
    )
