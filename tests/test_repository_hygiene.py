"""The denylist is keyed; tests install their own key and harmless tokens."""

from __future__ import annotations

import subprocess

import pytest

from tools import check_repository_hygiene as hygiene

KEY = "test-only-key"
WORD = "zzprivate"
# Assembled at runtime so this file itself never contains a key-shaped literal.
BEGIN = "-----BEGIN " + "PRIVATE KEY-----"
END = "-----END " + "PRIVATE KEY-----"
DOMAIN = "zz-private.example"


@pytest.fixture(autouse=True)
def denylist(monkeypatch):
    key = KEY.encode()
    monkeypatch.setenv(hygiene.KEY_ENV, KEY)
    monkeypatch.delenv(hygiene.KEY_FILE_ENV, raising=False)
    monkeypatch.setattr(hygiene, "_KEY", key)
    monkeypatch.setattr(hygiene, "FORBIDDEN_TOKEN_MACS",
                        {hygiene._token_mac(WORD, key), hygiene._token_mac(DOMAIN, key)})
    monkeypatch.setattr(hygiene, "KEY_CHECK_MAC", hygiene._token_mac(hygiene.KEY_CHECK_TEXT, key))
    monkeypatch.setattr(hygiene, "_TOKEN_CACHE", {})


@pytest.mark.parametrize("token", [
    WORD, WORD.upper(), f"{WORD}-analysis", f"PR-{WORD.upper()}", f".env.{WORD}",
    f"analysis-{WORD}.git", f"{WORD}_id", f"user@{DOMAIN}", f"corp.{DOMAIN}", DOMAIN,
])
def test_identifier_is_found_inside_compounds(token):
    assert hygiene._is_forbidden(token)


@pytest.mark.parametrize("token", [
    f"{WORD}s", f"x{WORD}", f"{WORD}2", "elapsedMs", "zz-private", "private.example", "example.com", "",
])
def test_adjacent_letters_and_partial_domains_do_not_match(token):
    assert not hygiene._is_forbidden(token)


def _git(repo, *args, **env):
    base = {"GIT_AUTHOR_NAME": "a", "GIT_AUTHOR_EMAIL": "a@example.com",
            "GIT_COMMITTER_NAME": "a", "GIT_COMMITTER_EMAIL": "a@example.com",
            "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null",
            "HOME": str(repo), "PATH": __import__("os").environ["PATH"]}
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, env={**base, **env})


@pytest.fixture
def repo(tmp_path, monkeypatch):
    _git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "a.txt").write_text("clean\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "initial")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_clean_history_passes(repo, capsys):
    assert hygiene.main(["--history"]) == 0
    assert "full history" in capsys.readouterr().out


def test_removed_file_version_is_still_reported_without_echoing_it(repo, capsys):
    (repo / "a.txt").write_text(f"account {WORD}-analysis\n")
    _git(repo, "commit", "-qam", "add")
    (repo / "a.txt").write_text("clean again\n")
    _git(repo, "commit", "-qam", "remove")
    assert hygiene.main([]) == 0
    assert hygiene.main(["--history"]) == 1
    output = capsys.readouterr().out
    assert "a.txt: forbidden private identifier in file history" in output
    assert WORD not in output


@pytest.mark.parametrize("where", ["message", "author", "committer", "tag", "path"])
def test_history_metadata_is_checked(repo, capsys, where):
    if where == "message":
        _git(repo, "commit", "-q", "--allow-empty", "-m", f"tested with {WORD}-analysis")
    elif where == "author":
        _git(repo, "commit", "-q", "--allow-empty", "-m", "x", GIT_AUTHOR_EMAIL=f"me@{DOMAIN}")
    elif where == "committer":
        _git(repo, "commit", "-q", "--allow-empty", "-m", "x", GIT_COMMITTER_EMAIL=f"me@{DOMAIN}")
    elif where == "tag":
        _git(repo, "tag", "-a", "v1", "-m", f"release for {WORD}")
    else:
        (repo / f"{WORD}.txt").write_text("x\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "add")
        _git(repo, "rm", "-q", f"{WORD}.txt")
        _git(repo, "commit", "-q", "-m", "remove")
    assert hygiene.main(["--history"]) == 1
    assert WORD not in capsys.readouterr().out


def test_multiline_secret_is_reported_per_file_version_only(repo, capsys):
    body = "\n".join(["A" * 40] * 4)
    (repo / "k.txt").write_text(f"{BEGIN}\n{body}\n{END}\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "key")
    _git(repo, "rm", "-q", "k.txt")
    _git(repo, "commit", "-q", "-m", "drop")
    assert hygiene.main(["--history"]) == 1
    assert "possible private key in a past file version" in capsys.readouterr().out


def test_short_fixture_markers_in_separate_commits_do_not_combine(repo):
    for name in ("one.txt", "two.txt"):
        (repo / name).write_text(f'"{BEGIN}\\nabc\\n{END}"\n' + "x" * 200 + "\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", name)
    assert hygiene.main(["--history"]) == 0


def test_missing_key_fails_closed_unless_explicitly_allowed(repo, tmp_path, monkeypatch, capsys):
    (repo / "a.txt").write_text(f"{WORD}\n")
    monkeypatch.delenv(hygiene.KEY_ENV)
    monkeypatch.setenv(hygiene.KEY_FILE_ENV, str(tmp_path / "absent.key"))
    assert hygiene.main([]) == 1
    assert "key not found" in capsys.readouterr().out
    assert hygiene.main(["--allow-missing-key"]) == 0
    assert "SKIPPED" in capsys.readouterr().out


def test_key_file_is_used_and_wrong_key_is_rejected(repo, tmp_path, monkeypatch, capsys):
    (repo / "a.txt").write_text(f"{WORD}\n")
    key_file = tmp_path / "hygiene.key"
    key_file.write_text(KEY + "\n")
    monkeypatch.delenv(hygiene.KEY_ENV)
    monkeypatch.setenv(hygiene.KEY_FILE_ENV, str(key_file))
    assert hygiene.main([]) == 1
    assert "forbidden private identifier" in capsys.readouterr().out
    key_file.write_text("another-key\n")
    assert hygiene.main([]) == 1
    assert "does not match" in capsys.readouterr().out


def test_secret_patterns_still_run_without_key(repo, tmp_path, monkeypatch, capsys):
    (repo / "k.txt").write_text(f"{BEGIN}\n" + "\n".join(["A" * 40] * 4) + f"\n{END}\n")
    _git(repo, "add", ".")
    monkeypatch.delenv(hygiene.KEY_ENV)
    monkeypatch.setenv(hygiene.KEY_FILE_ENV, str(tmp_path / "absent.key"))
    assert hygiene.main(["--allow-missing-key"]) == 1
    assert "possible private key" in capsys.readouterr().out


def test_denylist_is_not_recoverable_as_plain_hashes():
    import hashlib

    plain = hashlib.sha256(WORD.encode()).hexdigest()
    assert plain not in hygiene.FORBIDDEN_TOKEN_MACS
    assert hygiene._token_mac(WORD, b"other-key") not in hygiene.FORBIDDEN_TOKEN_MACS


def test_history_requires_full_clone(repo, tmp_path_factory, monkeypatch, capsys):
    _git(repo, "commit", "-q", "--allow-empty", "-m", "second")
    shallow = tmp_path_factory.mktemp("shallow")
    subprocess.run(["git", "clone", "-q", "--depth", "1", f"file://{repo}", str(shallow)], check=True, capture_output=True)
    monkeypatch.chdir(shallow)
    assert hygiene.main(["--history"]) == 1
    assert "full clone" in capsys.readouterr().out


def test_workflow_gives_the_key_only_to_trusted_runs_and_never_skips():
    yaml = pytest.importorskip("yaml")
    from pathlib import Path

    workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/tests.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert "pull_request_target" not in triggers
    job = workflow["jobs"]["repository-hygiene"]
    assert "if" not in job
    trusted = job["env"]["HYGIENE_TRUSTED"]
    assert "github.repository == " in trusted
    assert "github.event.pull_request.head.repo.full_name == github.repository" in trusted
    checks = [step for step in job["steps"] if "check_repository_hygiene.py" in step.get("run", "")]
    keyed = [step for step in checks if "secrets." in str(step.get("env", ""))]
    keyless = [step for step in checks if step not in keyed]
    assert len(keyed) == 1 and len(keyless) == 1
    assert keyed[0]["if"] == "env.HYGIENE_TRUSTED == 'true'"
    assert "--history" in keyed[0]["run"] and "--allow-missing-key" not in keyed[0]["run"]
    assert keyless[0]["if"] == "env.HYGIENE_TRUSTED != 'true'"
    assert "--allow-missing-key" in keyless[0]["run"]
    assert "secrets." not in str(keyless[0])
    assert not any("secrets." in str(step) for step in job["steps"] if step is not keyed[0])
