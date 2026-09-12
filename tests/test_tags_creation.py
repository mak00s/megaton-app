import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from megaton_lib.audit.providers.tag_config import adobe_tags as tags


def test_creation_helpers_are_exported_from_package_root():
    from megaton_lib.audit.providers import tag_config

    for name in ("create_rule", "create_data_element"):
        assert name in tag_config.__all__
        assert getattr(tag_config, name) is getattr(tags, name)


def test_create_rule_returns_resource_without_build(monkeypatch):
    post = Mock(return_value={"data": {"id": "RL-new"}})
    monkeypatch.setattr(tags, "_reactor_post", post)
    config = SimpleNamespace(property_id="PR-test")
    assert tags.create_rule(config, name="Example") == {"id": "RL-new"}
    post.assert_called_once_with(config, "/properties/PR-test/rules", {
        "data": {"type": "rules", "attributes": {"name": "Example"}},
    })


def test_create_data_element_preserves_settings_and_extension(monkeypatch):
    post = Mock(return_value={"data": {"id": "DE-new"}})
    monkeypatch.setattr(tags, "_reactor_post", post)
    config = SimpleNamespace(property_id="PR-test")
    assert tags.create_data_element(
        config, name="Example", extension_id="EX-core",
        delegate_descriptor_id="core::dataElements::custom-code", settings={"source": "return 1;"},
    ) == {"id": "DE-new"}
    assert post.call_count == 1
    args = post.call_args.args
    assert args[1] == "/properties/PR-test/data_elements"
    data = args[2]["data"]
    assert json.loads(data["attributes"]["settings"]) == {"source": "return 1;"}
    assert data["attributes"]["storage_duration"] is None
    assert data["relationships"]["extension"]["data"] == {"id": "EX-core", "type": "extensions"}


def test_empty_rule_name_fails_before_http(monkeypatch):
    post = Mock()
    monkeypatch.setattr(tags, "_reactor_post", post)
    with pytest.raises(ValueError):
        tags.create_rule(SimpleNamespace(property_id="PR-test"), name=" ")
    post.assert_not_called()


@pytest.mark.parametrize("missing", ["name", "extension_id", "delegate_descriptor_id"])
def test_missing_data_element_fields_fail_before_http(monkeypatch, missing):
    post = Mock()
    monkeypatch.setattr(tags, "_reactor_post", post)
    args = dict(name="Example", extension_id="EX-core", delegate_descriptor_id="core::test", settings={})
    args[missing] = ""
    with pytest.raises(ValueError):
        tags.create_data_element(SimpleNamespace(property_id="PR-test"), **args)
    post.assert_not_called()
