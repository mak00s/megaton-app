from __future__ import annotations

from megaton_lib.validation.metadata import build_validation_run_metadata
from megaton_lib.validation.playwright_pages import TagsLaunchOverride


def test_build_validation_run_metadata_live_defaults():
    metadata = build_validation_run_metadata()
    assert metadata == {"executionMode": "live"}


def test_build_validation_run_metadata_with_tags_override():
    override = TagsLaunchOverride(
        launch_url="https://assets.adobedtm.com/x/y/launch-dev.min.js",
        exact_match_urls=("https://assets.adobedtm.com/x/y/launch-prod.min.js",),
    )
    metadata = build_validation_run_metadata(
        execution_mode="tags_override",
        project="dms-analysis",
        scenario="samacsys-cn",
        config_path="validation/aa-samacsys-check.json",
        tags_override=override,
        extra={"region": "cn"},
    )
    assert metadata["executionMode"] == "tags_override"
    assert metadata["project"] == "dms-analysis"
    assert metadata["scenario"] == "samacsys-cn"
    assert metadata["configPath"] == "validation/aa-samacsys-check.json"
    assert metadata["region"] == "cn"
    assert metadata["tagsOverride"]["launchUrl"].endswith("launch-dev.min.js")
    assert metadata["tagsOverride"]["exactMatchUrls"] == [
        "https://assets.adobedtm.com/x/y/launch-prod.min.js",
    ]
