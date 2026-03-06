from __future__ import annotations

from megaton_lib.audit.providers.tag_config.adobe_tags import (
    extract_mapping_from_settings,
    parse_settings_object,
)


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
