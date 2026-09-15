import json
import subprocess
import sys
from unittest.mock import Mock

import pytest

from megaton_lib import docs_edit
from megaton_lib.docs_client import DocsEditError, DocsEditPlan


@pytest.fixture
def saved(tmp_path):
    plan = DocsEditPlan("doc1", "t.0", "r1", "replace", "old", "new", 1, 1, 4, "old\n", "new\n")
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan.preview()))
    return path


def test_help_without_google_imports(capsys):
    with pytest.raises(SystemExit) as exc:
        docs_edit.main(["--help"])
    assert exc.value.code == 0
    assert "--expected-email" in capsys.readouterr().out


def test_module_import_does_not_load_google_dependencies():
    subprocess.run([sys.executable, "-c", (
        "import sys; before = set(sys.modules); import megaton_lib.docs_edit; "
        "assert not any(n.startswith(('google.', 'googleapiclient')) for n in set(sys.modules) - before)"
    )], check=True)


def test_missing_plan_is_structured_error(capsys):
    assert docs_edit.main(["--token", "token", "--expected-email", "a@b.test",
                           "apply", "--plan", "/nonexistent/docs-plan.json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "invalid_input_file"


def test_no_apply_does_not_authenticate(saved, monkeypatch, capsys):
    auth = Mock(side_effect=AssertionError("must not authenticate"))
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", auth)
    assert docs_edit.main(["--token", "missing", "--expected-email", "a@b.test",
                           "apply", "--plan", str(saved)]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "dry_run"
    auth.assert_not_called()


def test_cli_apply_failure_exit_and_json(saved, monkeypatch, capsys):
    client = Mock()
    client.apply.return_value = {"ok": False, "write_status": "unknown"}
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", Mock(return_value=client))
    assert docs_edit.main(["--token", "token", "--expected-email", "a@b.test",
                           "apply", "--plan", str(saved), "--apply"]) == 1
    assert json.loads(capsys.readouterr().out)["write_status"] == "unknown"
    assert client.apply.call_args.kwargs == {"apply": True}


@pytest.mark.parametrize("error,expected", [(DocsEditError("Specify tab_id"), "validation_failed"),
                                           (RuntimeError("SECRET"), "preflight_or_read_failed")])
def test_cli_error_is_actionable_but_sanitized(error, expected, monkeypatch, capsys):
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", Mock(side_effect=error))
    assert docs_edit.main(["--token", "token", "--expected-email", "a@b.test", "get", "doc1"]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["error"] == expected
    assert "SECRET" not in output.out + output.err


@pytest.mark.parametrize("command", ["get", "plan", "verify"])
def test_cli_read_routes(command, saved, monkeypatch, capsys):
    client = Mock()
    client.get.return_value = {"documentId": "doc1", "tabs": []}
    client.plan.return_value.preview.return_value = {"schema_version": 1, "mode": "dry_run"}
    client.verify.return_value = {"verified": False}
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", Mock(return_value=client))
    args = ["--token", "token", "--expected-email", "a@b.test", command]
    if command == "verify":
        args.extend(["--plan", str(saved)])
    else:
        args.append("doc1")
    if command == "plan":
        args.extend(["--match", "old", "--text", "new", "--tab-id", "t.0"])
    assert docs_edit.main(args) == (1 if command == "verify" else 0)
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == 1
    client.apply.assert_not_called()
    if command == "plan":
        client.plan.assert_called_once_with("doc1", match="old", text="new", operation="replace", tab_id="t.0")


def test_reject_future_schema_before_auth(saved, monkeypatch, capsys):
    payload = json.loads(saved.read_text())
    payload["schema_version"] = 2
    saved.write_text(json.dumps(payload))
    auth = Mock()
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", auth)
    assert docs_edit.main(["--token", "token", "--expected-email", "a@b.test", "apply",
                           "--plan", str(saved), "--apply"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "validation_failed"
    auth.assert_not_called()


@pytest.mark.parametrize("command", ["apply", "verify"])
@pytest.mark.parametrize("corruption", ["missing_plan", "missing_field", "extra_field", "list", "null"])
def test_malformed_plan_is_validation_error_before_auth(command, corruption, saved, monkeypatch, capsys):
    payload = json.loads(saved.read_text())
    if corruption == "missing_plan":
        del payload["plan"]
    elif corruption == "missing_field":
        del payload["plan"]["document_id"]
    elif corruption == "extra_field":
        payload["plan"]["unexpected"] = "SECRET"
    else:
        payload["plan"] = [] if corruption == "list" else None
    saved.write_text(json.dumps(payload))
    auth = Mock()
    monkeypatch.setattr(docs_edit.DocsClient, "from_oauth_file", auth)
    args = ["--token", "token", "--expected-email", "a@b.test", command, "--plan", str(saved)]
    if command == "apply":
        args.append("--apply")
    assert docs_edit.main(args) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["error"] == "validation_failed"
    assert "SECRET" not in output.out + output.err
    auth.assert_not_called()
