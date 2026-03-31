from __future__ import annotations

from megaton_lib.validation.metadata import build_validation_run_metadata
from megaton_lib.validation.playwright_pages import GtmPreviewOverride, TagsLaunchOverride


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


def test_build_validation_run_metadata_with_gtm_preview():
    metadata = build_validation_run_metadata(
        execution_mode="gtm_preview",
        project="shiseido-analysis",
        scenario="ga4-beacon-check",
        config_path="validation/formulas-and-ingredients-ga4-beacon-check.json",
        gtm_preview=GtmPreviewOverride(
            container_id="GTM-TJKK7S5",
            auth_token="secret-token",
            preview_id="env-361",
        ),
    )
    assert metadata["executionMode"] == "gtm_preview"
    assert metadata["gtmPreview"] == {
        "containerId": "GTM-TJKK7S5",
        "previewId": "env-361",
        "cookiesWin": "x",
        "authTokenPresent": True,
    }
