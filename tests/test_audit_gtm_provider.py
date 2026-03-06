from __future__ import annotations

from megaton_lib.audit.providers.tag_config.gtm import parse_regex_table_variable


def test_parse_regex_table_variable() -> None:
    var = {
        "name": "Site Name",
        "type": "remm",
        "parameter": [
            {
                "key": "map",
                "type": "list",
                "list": [
                    {
                        "map": [
                            {"key": "key", "value": "^/jp/"},
                            {"key": "value", "value": "JP"},
                        ]
                    },
                    {
                        "map": [
                            {"key": "key", "value": "^/en/"},
                            {"key": "value", "value": "EN"},
                        ]
                    },
                ],
            },
            {
                "key": "defaultValue",
                "value": "UNKNOWN",
            },
        ],
    }

    mapping = parse_regex_table_variable(var)
    assert mapping["^/jp/"] == "JP"
    assert mapping["^/en/"] == "EN"
    assert mapping["__default__"] == "UNKNOWN"
