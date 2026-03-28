"""Thin validation entrypoint template.

Copy this into an analysis repo and keep only project-specific selectors,
steps, expectations, and output naming local.
"""

from __future__ import annotations

from pathlib import Path

from megaton_lib.validation import (
    build_tags_launch_override,
    build_validation_run_metadata,
    load_validation_config,
    run_page,
    write_validation_json,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "validation" / "example-check.local.json"
OUTPUT_DIR = PROJECT_ROOT / "output" / "validation"


def run_example(*, use_dev: bool = False) -> dict:
    config = load_validation_config(CONFIG_PATH)
    tags_override = build_tags_launch_override(
        config.get("tagsOverride"),
        require=use_dev,
        label="tagsOverride",
    )

    result: dict = {
        "issues": [],
    }

    def callback(page):
        page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(config.get("waitMs", 3000))
        # project-specific checks here
        return {"title": page.title(), "url": page.url}

    page_result = run_page(
        config["url"],
        headless=config.get("headless", True),
        basic_auth=config.get("basicAuth"),
        tags_override=tags_override,
        callback=callback,
    )
    result["page"] = page_result

    result.update(
        build_validation_run_metadata(
            execution_mode="tags_override" if tags_override else "live",
            project="example-analysis",
            scenario="example-check",
            config_path=CONFIG_PATH,
            tags_override=tags_override,
        )
    )
    return result


def save_result(result: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    suffix = "_dev" if result.get("executionMode") == "tags_override" else ""
    out_path = OUTPUT_DIR / f"example_check{suffix}.json"
    return write_validation_json(out_path, result)


if __name__ == "__main__":
    payload = run_example(use_dev=False)
    import json

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"saved: {save_result(payload)}")
