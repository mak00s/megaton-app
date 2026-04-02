from __future__ import annotations

import json
from pathlib import Path

from megaton_lib.report_validation import finish_report_tracker, init_report_tracker


def test_report_tracker_persists_summary(monkeypatch, tmp_path, capsys):
    summary_path = tmp_path / "run-summary.json"
    monkeypatch.setenv("MEGATON_RUN_SUMMARY_PATH", str(summary_path))

    tracker = init_report_tracker(
        "demo-report",
        report_start="2026-04-01",
        report_end="2026-04-30",
    )
    finish_report_tracker(tracker, status="passed", notes=["ok"], errors=[])

    payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    assert payload["report"] == "demo-report"
    assert payload["window"] == {
        "report_start": "2026-04-01",
        "report_end": "2026-04-30",
    }
    assert payload["validation"]["status"] == "passed"
    assert payload["validation"]["notes"] == ["ok"]
    assert payload["status"] == "success"

    stdout = capsys.readouterr().out
    assert "Execution summary:" in stdout
    assert "Validation status: passed" in stdout
