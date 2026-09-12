from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import check_consumer_contracts as cli


def test_selected_tests_and_library_are_forwarded(tmp_path, monkeypatch):
    (tmp_path / "megaton_lib").mkdir()
    (tmp_path / "megaton_lib" / "__init__.py").touch()
    (tmp_path / "test_example.py").touch()
    run = Mock(return_value=SimpleNamespace(returncode=5))
    monkeypatch.setattr(cli.subprocess, "run", run)
    assert cli.main(["--repo", str(tmp_path), "--library-path", str(tmp_path), "--test", "test_example.py"]) == 5
    assert run.call_args.kwargs["cwd"] == tmp_path
    assert run.call_args.kwargs["env"]["PYTHONPATH"] == str(tmp_path)
    assert run.call_args.args[0][-1] == str(tmp_path / "test_example.py")


def test_rejects_test_outside_repo(tmp_path):
    (tmp_path / "megaton_lib").mkdir()
    (tmp_path / "megaton_lib" / "__init__.py").touch()
    with pytest.raises(SystemExit) as exc:
        cli.main(["--repo", str(tmp_path), "--library-path", str(tmp_path), "--test", "../outside.py"])
    assert exc.value.code == 2


def test_wrong_library_in_consumer_fails_closed(tmp_path):
    repo = tmp_path / "repo"
    library = tmp_path / "installed"
    for root in (repo, library):
        (root / "megaton_lib").mkdir(parents=True)
        (root / "megaton_lib" / "__init__.py").touch()
    (repo / "test_example.py").write_text("def test_ok(): assert True\n", encoding="utf-8")
    assert cli.main([
        "--repo", str(repo), "--library-path", str(library), "--test", "test_example.py",
    ]) != 0
