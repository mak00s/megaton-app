from __future__ import annotations

import json

import pytest

from megaton_lib.audit.config import ConfigError, load_project_config, parse_project_config


def test_parse_project_config_minimal_gtm() -> None:
    payload = {
        "project_id": "corp",
        "tag_source": {
            "source": "gtm",
            "gtm": {
                "container_public_id": "GTM-TEST",
                "variable_name": "Site Name",
            },
        },
        "ga4": {
            "property_id": "123456",
        },
    }

    cfg = parse_project_config(payload)
    assert cfg.project_id == "corp"
    assert cfg.tag_source.source == "gtm"
    assert cfg.tag_source.gtm is not None
    assert cfg.tag_source.gtm.container_public_id == "GTM-TEST"
    assert cfg.ga4.property_id == "123456"
    assert cfg.aa is None


def test_parse_project_config_requires_tag_source() -> None:
    payload = {
        "project_id": "corp",
        "ga4": {"property_id": "123"},
    }

    with pytest.raises(ConfigError):
        parse_project_config(payload)


def test_load_project_config_json(tmp_path) -> None:
    config_dir = tmp_path / "projects"
    config_dir.mkdir()
    config_file = config_dir / "corp.json"
    config_file.write_text(
        json.dumps(
            {
                "project_id": "corp",
                "tag_source": {
                    "source": "adobe_tags",
                    "adobe_tags": {
                        "property_id": "PR123",
                    },
                },
                "ga4": {
                    "property_id": "123456",
                },
            },
        ),
        encoding="utf-8",
    )

    cfg = load_project_config("corp", config_root=config_dir)
    assert cfg.project_id == "corp"
    assert cfg.tag_source.source == "adobe_tags"
    assert cfg.tag_source.adobe_tags is not None
    assert cfg.tag_source.adobe_tags.property_id == "PR123"
