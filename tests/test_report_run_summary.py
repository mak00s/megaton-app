from __future__ import annotations

from megaton_lib.report_run_summary import SCHEMA_VERSION, github_run_url, normalize_report_summary


def test_github_run_url_from_env() -> None:
    assert github_run_url(
        {
            "GITHUB_SERVER_URL": "https://github.com",
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_RUN_ID": "123",
        }
    ) == "https://github.com/owner/repo/actions/runs/123"


def test_normalize_report_summary_adds_standard_keys_and_preserves_extra_fields() -> None:
    summary = normalize_report_summary(
        {
            "status": "success",
            "window": {"report_start": "2026-05-01"},
            "artifacts": {"box_uploads": [{"shared_url": "https://app.box.com/s/test"}]},
            "custom": {"kept": True},
        },
        env={
            "GITHUB_SERVER_URL": "https://github.com",
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_RUN_ID": "456",
        },
        default_report="test-report",
    )

    assert summary["schema_version"] == SCHEMA_VERSION
    assert summary["report"] == "test-report"
    assert summary["status"] == "success"
    assert summary["run_url"] == "https://github.com/owner/repo/actions/runs/456"
    assert summary["validation"] == {"status": "passed", "notes": [], "errors": []}
    assert summary["entries"] == []
    assert summary["delivery"]["box_uploads"] == [{"shared_url": "https://app.box.com/s/test"}]
    assert summary["custom"] == {"kept": True}


def test_normalize_report_summary_keeps_existing_validation() -> None:
    summary = normalize_report_summary(
        {
            "report": "x",
            "status": "failed",
            "validation": {"status": "failed", "errors": ["bad"], "notes": ["checked"]},
        }
    )

    assert summary["validation"] == {"status": "failed", "notes": ["checked"], "errors": ["bad"]}


def test_empty_env_does_not_fall_back_to_process(monkeypatch):
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "999")
    assert github_run_url({}) == ""


def test_normalization_does_not_mutate_input():
    original = {"artifacts": {"box_uploads": [{"id": "one"}]}}
    result = normalize_report_summary(original, env={})
    result["delivery"]["box_uploads"][0]["id"] = "two"
    assert original == {"artifacts": {"box_uploads": [{"id": "one"}]}}
