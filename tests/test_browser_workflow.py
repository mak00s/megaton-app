from contextlib import contextmanager
import importlib
import json
import pathlib
from unittest.mock import Mock

import pytest

from megaton_lib import browser_workflow as workflow


def test_guide_is_copy_safe_and_entrypoints_exist():
    result = workflow.workflow_guide()
    assert result["effects"] == "none"
    assert result["workflows"]["explore"]["route"] == "agent_browser_tools"
    for item in result["workflows"].values():
        for entry in item["entrypoints"]:
            module, _, name = entry.rpartition(".")
            assert hasattr(importlib.import_module(module), name)
    result["workflows"].clear()
    assert len(workflow.workflow_guide()["workflows"]) == 4


def test_invalid_task_rejected():
    with pytest.raises(ValueError):
        workflow.workflow_guide("arbitrary_automation")


def test_doctor_default_does_not_launch(monkeypatch):
    monkeypatch.setattr(workflow, "_package_version", lambda _: "1.0")
    probe = Mock()
    monkeypatch.setattr(workflow, "_probe_browser", probe)
    result = workflow.browser_doctor()
    assert result["ok"] is True
    assert result["scope"] == "python_dependencies_only"
    assert result["checks"]["browser_launch"]["status"] == "not_checked"
    assert result["checks"]["workflow_result"]["status"] == "not_checked"
    probe.assert_not_called()


def test_doctor_reports_loaded_library_path(monkeypatch):
    import megaton_lib

    monkeypatch.setattr(workflow, "_package_version", lambda _: "1.0")
    result = workflow.browser_doctor()
    assert result["megaton_lib_path"] == str(pathlib.Path(megaton_lib.__file__).resolve().parent)


def test_missing_package_fails_without_launch(monkeypatch):
    monkeypatch.setattr(workflow, "_package_version", lambda _: None)
    probe = Mock()
    monkeypatch.setattr(workflow, "_probe_browser", probe)
    result = workflow.browser_doctor(check_browser=True)
    assert result["exit_code"] == 1
    assert result["checks"]["browser_launch"]["status"] == "not_checked"
    probe.assert_not_called()


def test_launch_failure_has_actionable_output(monkeypatch):
    monkeypatch.setattr(workflow, "_package_version", lambda _: "1.0")
    monkeypatch.setattr(workflow, "_probe_browser", Mock(side_effect=RuntimeError("Executable missing")))
    result = workflow.browser_doctor(check_browser=True)
    assert result["ok"] is False
    assert "Executable missing" in result["checks"]["browser_launch"]["error"]
    assert "playwright install chromium" in result["next_actions"][0]


def test_probe_uses_fresh_context_and_closes(monkeypatch):
    from megaton_lib import playwright_browser
    page = Mock()
    page.evaluate.return_value = 2
    closed = []

    @contextmanager
    def browser(**kwargs):
        assert kwargs == {"headless": True, "stealth": False}
        try:
            yield page
        finally:
            closed.append(True)

    monkeypatch.setattr(playwright_browser, "browser_page", browser)
    assert workflow._probe_browser()["status"] == "passed"
    page.goto.assert_called_once_with("about:blank", timeout=5000)
    route = Mock()
    page.route.call_args.args[1](route)
    route.abort.assert_called_once()
    assert closed == [True]


def test_json_output_and_failure_exit(monkeypatch, capsys):
    monkeypatch.setattr(workflow, "_package_version", lambda _: None)
    assert workflow.main(["doctor", "--format", "json"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["command"] == "doctor"
    assert result["schema_version"] == "browser-workflow/v1"


def test_guide_cli_is_self_describing(capsys):
    assert workflow.main(["guide", "--task", "validate"]) == 0
    text = capsys.readouterr().out
    assert "run_page_session" in text
    assert "live versus override" in text


def test_help_explains_launch_opt_in(capsys):
    with pytest.raises(SystemExit) as exc:
        workflow.main(["doctor", "--help"])
    assert exc.value.code == 0
    text = capsys.readouterr().out
    assert "--check-browser" in text
    assert "then close" in " ".join(text.split())
