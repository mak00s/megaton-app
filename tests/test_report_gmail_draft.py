from __future__ import annotations

import json

import pytest

from megaton_lib.report_gmail_draft import (
    assert_report_success,
    box_upload_lines,
    build_report_draft_content,
    create_report_gmail_draft_from_env,
    first_sheet_url,
    period_label,
)


def _summary(**overrides):
    base = {
        "status": "success",
        "window": {"report_start": "2026-04-01", "report_end": "2026-04-30"},
        "run_url": "https://github.com/example/repo/actions/runs/1",
        "validation": {"status": "passed", "notes": [], "errors": []},
        "entries": [{"target_url": "https://docs.google.com/spreadsheets/d/sheet-id/edit"}],
        "artifacts": {
            "box_uploads": [
                {
                    "uploaded_file_name": "report.xlsx",
                    "target_subfolder_name": "202604",
                    "shared_url": "https://shiseido.app.box.com/s/shared",
                }
            ]
        },
    }
    base.update(overrides)
    return json.loads(json.dumps(base))


def test_assert_report_success_rejects_failed_validation():
    summary = _summary(validation={"status": "failed", "notes": [], "errors": ["bad"]})

    with pytest.raises(RuntimeError, match="did not finish successfully"):
        assert_report_success(summary)


def test_report_context_helpers_read_summary():
    summary = _summary()

    assert period_label(summary) == "2026年4月"
    assert first_sheet_url(summary) == "https://docs.google.com/spreadsheets/d/sheet-id/edit"
    assert box_upload_lines(summary) == [
        "- Box: 202604/report.xlsx\n  https://shiseido.app.box.com/s/shared"
    ]


def test_build_report_draft_content_uses_box_link_by_default():
    content = build_report_draft_content(_summary(), report_label="DEI Lab 月次レポート")

    assert content.subject == "DEI Lab 月次レポート（2026年4月）"
    assert "https://shiseido.app.box.com/s/shared" in content.body
    assert "report.xlsx" in content.body
    assert "GitHub Actions" not in content.body


def test_build_report_draft_content_supports_templates():
    content = build_report_draft_content(
        _summary(),
        report_label="WITH",
        subject_template="{report_label} {period_label}",
        body_template="{box_urls}\n{sheet_url}\n{run_url}",
    )

    assert content.subject == "WITH 2026年4月"
    assert "https://shiseido.app.box.com/s/shared" in content.body
    assert "https://docs.google.com/spreadsheets/d/sheet-id/edit" in content.body
    assert "https://github.com/example/repo/actions/runs/1" in content.body


def test_build_report_draft_content_reports_unknown_template_placeholder():
    with pytest.raises(ValueError, match="Unknown Gmail draft body template placeholder"):
        build_report_draft_content(
            _summary(),
            report_label="WITH",
            body_template="{missing_url}",
        )


def test_create_report_gmail_draft_can_require_box_url(tmp_path, monkeypatch):
    summary = _summary(artifacts={"box_uploads": [{"uploaded_file_name": "report.xlsx"}]})
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    monkeypatch.setenv("DEI_GMAIL_DRAFT_REQUIRE_BOX_URL", "true")
    monkeypatch.setenv("DEI_GMAIL_DRAFT_SENDER", "sender@example.com")
    monkeypatch.setenv("DEI_GMAIL_DRAFT_TO", "to@example.com")

    with pytest.raises(RuntimeError, match="requires a Box shared URL"):
        create_report_gmail_draft_from_env(
            summary_path=summary_path,
            report_label="DEI",
            env_prefix="DEI",
            client_factory=lambda creds: None,
        )
