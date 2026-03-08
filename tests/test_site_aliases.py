from __future__ import annotations

import json
from pathlib import Path

from megaton_lib.batch_runner import run_batch
from megaton_lib.site_aliases import resolve_site_alias


def test_resolve_site_alias_gsc_from_config_dir(tmp_path):
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir()
    (configs_dir / "sites.local.json").write_text(
        json.dumps({"corp": {"gsc_site_url": "https://corp.example/"}}),
        encoding="utf-8",
    )

    result = resolve_site_alias(
        {"schema_version": "1.0", "source": "gsc", "site": "corp"},
        config_dir=configs_dir,
    )

    assert result["site_url"] == "https://corp.example/"
    assert "site" not in result


def test_run_batch_resolves_site_alias_before_validation(tmp_path, monkeypatch):
    config_path = tmp_path / "01.json"
    config_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "source": "gsc",
                "site": "corp",
                "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
                "dimensions": ["query"],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "megaton_lib.batch_runner.resolve_site_alias",
        lambda raw: {
            "schema_version": raw["schema_version"],
            "source": raw["source"],
            "date_range": raw["date_range"],
            "dimensions": raw["dimensions"],
            "site_url": "https://corp.example/",
        },
    )

    received: dict[str, object] = {}

    def _execute(params, config_file: Path):
        received.update(params)
        return {"status": "ok"}

    result = run_batch(str(tmp_path), execute_fn=_execute)

    assert result["succeeded"] == 1
    assert received["site_url"] == "https://corp.example/"
    assert "site" not in received
