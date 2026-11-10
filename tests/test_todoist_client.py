from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest
import requests

from megaton_lib import todoist_tasks
from megaton_lib.todoist_client import (
    API_BASE, TodoistClient, TodoistError, find_unique_task, task_fingerprint,
)


def task(**kwargs):
    return {"id": "6Xtask", "content": "Review report", "description": "", "priority": 1,
            "project_id": "6Xproject", "parent_id": None, "labels": [], "due": None,
            "checked": False, "is_deleted": False, **kwargs}


class Session:
    def __init__(self):
        self.tasks = {"6Xtask": task()}
        self.calls = []
        self.pages = None
        self.post_error = None
        self.read_error = False
        self.user_id = "123"
        self.completed_pages = None
        self.completed_get_status = 404

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, deepcopy(kwargs)))
        path = url.removeprefix(API_BASE)
        status, data = 200, None
        if path == "/user":
            data = {"id": self.user_id, "token": "DO_NOT_LEAK"}
        elif method == "POST":
            if self.post_error:
                raise self.post_error
            if path == '/sync':
                command = kwargs['json']['commands'][0]
                self.tasks[command['args']['id']]['due'] = deepcopy(command['args']['due'])
                data = {'sync_status': {command['uuid']: 'ok'}}
            elif path == "/tasks":
                payload = deepcopy(kwargs["json"])
                if "due_string" in payload:
                    payload["due"] = {"date": "2026-10-17", "string": payload.pop("due_string"),
                                      "is_recurring": False, "lang": "en"}
                data = task(id="new-task", **payload)
                self.tasks[data["id"]] = deepcopy(data)
            elif not path.endswith('/close'):
                selected = self.tasks[path.split('/')[2]]
                selected.update(kwargs['json'])
                data = deepcopy(selected)
            else:
                selected = self.tasks[path.split("/")[2]]
                if (selected.get('due') or {}).get('is_recurring'):
                    selected['due']['date'] = '2026-11-24'
                    selected['completed_count'] += 1
                else:
                    selected["checked"] = True
                    selected["completed_at"] = datetime.now(timezone.utc).isoformat()
                status = 200
        elif path == "/tasks":
            data = self.pages.pop(0) if self.pages is not None else {
                "results": [deepcopy(t) for t in self.tasks.values() if not t["checked"]],
                "next_cursor": None,
            }
        elif path == "/tasks/completed/by_completion_date":
            data = self.completed_pages.pop(0) if self.completed_pages is not None else {
                "items": [deepcopy(t) for t in self.tasks.values() if t["checked"]],
                "next_cursor": None,
            }
        elif path.startswith("/tasks/"):
            if self.read_error or (self.tasks[path.split("/")[2]]["checked"]
                                   and self.completed_get_status == 404):
                status = 404
            else:
                data = deepcopy(self.tasks[path.split("/")[2]])
        return SimpleNamespace(status_code=status, json=lambda: data, text="DO_NOT_LEAK")

    @property
    def posts(self):
        return [call for call in self.calls if call[0] == "POST"]


@pytest.fixture
def client():
    session = Session()
    return TodoistClient("secret-token", session=session), session


def test_pagination_preserves_filters_and_checks_all_pages(client):
    c, s = client
    s.pages = [{"results": [task(id="one")], "next_cursor": "next"},
               {"results": [task(id="two")], "next_cursor": None}]
    assert [t["id"] for t in c.list_tasks(label="work", project_id="p")] == ["one", "two"]
    assert s.calls[-1][2]["params"] == {"label": "work", "project_id": "p", "cursor": "next"}
    assert all(call[2]["allow_redirects"] is False for call in s.calls)


def test_repeated_cursor_is_not_partial_success(client):
    c, s = client
    s.pages = [{"results": [], "next_cursor": "same"}] * 2
    with pytest.raises(TodoistError, match="cursor"):
        c.list_tasks()


def test_unique_match_rejects_ambiguous_and_partial_number():
    assert find_unique_task([task(content="[r] #10: x")], content_prefix="[r] #1") is None
    assert find_unique_task([task(content="[r] #1: x")], content_prefix="[r] #1")["id"] == "6Xtask"
    with pytest.raises(TodoistError, match="Multiple"):
        find_unique_task([task(content="[r] #1"), task(content="[r] #1: duplicate")], content_prefix="[r] #1")


def test_account_guard_and_secret_redaction(client):
    _, s = client
    c = TodoistClient("secret-token", session=s, expected_user_id="other")
    with pytest.raises(TodoistError, match="account") as exc:
        c.create_task(content="x", apply=True, receipt_path="unused")
    assert "DO_NOT_LEAK" not in str(exc.value)
    assert not s.posts


def test_create_preview_has_no_write_or_receipt(client, tmp_path):
    c, s = client
    receipt = tmp_path / "receipt.json"
    result = c.create_task(content="next", receipt_path=receipt)
    assert result["ok"] and not result["verified"]
    assert result["write_status"] == "not_attempted"
    assert "priority" not in result["preview"]
    assert "due_string" not in result["preview"]
    assert not s.posts and not receipt.exists()


def test_complete_preview_returns_review_fingerprint(client):
    c, s = client
    result = c.complete_task("6Xtask")
    assert result["expected_fingerprint"] == task_fingerprint(s.tasks["6Xtask"])
    assert not s.posts


def test_create_readback_receipt_replay_and_no_input_mutation(client, tmp_path):
    c, s = client
    path = tmp_path / "receipt.json"
    labels = ["review", "review"]
    kwargs = dict(content="next", description="report link", priority=3, labels=labels,
                  due_string="tomorrow", project_id="p", parent_id="parent", apply=True, receipt_path=path)
    result = c.create_task(**kwargs)
    assert result["ok"] and result["verified"]
    assert result["task_id"] == "new-task"
    assert result["task_url"] == "https://app.todoist.com/app/task/new-task"
    assert path.stat().st_mode & 0o777 == 0o600
    assert labels == ["review", "review"]
    assert c.create_task(**kwargs)["verified"]
    assert len(s.posts) == 1
    assert "secret-token" not in path.read_text()
    assert "DO_NOT_LEAK" not in path.read_text()


def test_complete_verifies_checked_and_never_reposts(client, tmp_path):
    c, s = client
    path = tmp_path / "receipt.json"
    fingerprint = task_fingerprint(s.tasks["6Xtask"])
    kwargs = dict(expected_fingerprint=fingerprint, apply=True, receipt_path=path)
    result = c.complete_task("6Xtask", **kwargs)
    assert result["verified"] and s.tasks["6Xtask"]["checked"]
    assert c.complete_task("6Xtask", **kwargs)["verified"]
    assert len(s.posts) == 1
    with pytest.raises(TodoistError, match="404"):
        c.get_task("6Xtask")
    receipt = json.loads(path.read_text())
    history_calls = [call for call in s.calls if call[1].endswith("/completed/by_completion_date")]
    assert history_calls and history_calls[0][2]["params"] == receipt["completion_window"]


def test_receipt_is_claimed_before_post(client, tmp_path):
    c, s = client
    path = tmp_path / "receipt.json"
    original = s.request

    def request(method, url, **kwargs):
        if method == "POST":
            assert json.loads(path.read_text())["write_status"] == "unknown"
            assert path.stat().st_mode & 0o777 == 0o600
        return original(method, url, **kwargs)

    s.request = request
    assert c.create_task(content="next", apply=True, receipt_path=path)["verified"]


def test_unknown_write_never_retries_and_preserves_receipt(client, tmp_path):
    c, s = client
    path = tmp_path / "receipt.json"
    s.post_error = requests.Timeout("DO_NOT_LEAK secret-token")
    kwargs = dict(content="next", apply=True, receipt_path=path)
    result = c.create_task(**kwargs)
    assert not result["ok"] and result["write_status"] == "unknown"
    assert "DO_NOT_LEAK" not in json.dumps(result)
    assert not c.create_task(**kwargs)["ok"]
    assert len(s.posts) == 1
    assert json.loads(path.read_text())["write_status"] == "unknown"


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500])
def test_write_http_errors_are_redacted_and_never_retried(client, tmp_path, status):
    c, s = client
    original = s.request

    def request(method, url, **kwargs):
        if method == "POST":
            s.calls.append((method, url, kwargs))
            return SimpleNamespace(status_code=status, text="SECRET_TOKEN_AND_TASK_BODY")
        return original(method, url, **kwargs)

    s.request = request
    path = tmp_path / "r.json"
    kwargs = dict(content="next", apply=True, receipt_path=path)
    result = c.create_task(**kwargs)
    expected_status = "unknown" if status == 500 else "rejected"
    assert not result["ok"] and result["write_status"] == expected_status
    receipt = json.loads(path.read_text())
    assert receipt["write_status"] == expected_status
    if expected_status == "rejected":
        assert receipt["rejection_http_status"] == status
        assert "new receipt" in result["next_step"]
    assert "SECRET" not in json.dumps(result)
    c.create_task(**kwargs)
    assert len(s.posts) == 1


def test_write_requires_receipt_and_fails_on_foreign_intent(client, tmp_path):
    c, s = client
    with pytest.raises(TodoistError, match="receipt_path"):
        c.create_task(content="next", apply=True)
    path = tmp_path / "receipt.json"
    c.create_task(content="next", apply=True, receipt_path=path)
    with pytest.raises(TodoistError, match="different operation"):
        c.create_task(content="other", apply=True, receipt_path=path)
    assert len(s.posts) == 1


def test_receipt_claim_race_does_not_post(client, tmp_path, monkeypatch):
    from megaton_lib import todoist_client
    c, s = client
    monkeypatch.setattr(todoist_client.os, "link", lambda *a: (_ for _ in ()).throw(FileExistsError()))
    with pytest.raises(FileExistsError):
        c.create_task(content="next", apply=True, receipt_path=tmp_path / "r.json")
    assert not s.posts
    assert not list(tmp_path.glob(".todoist-*"))


def test_recurring_and_stale_task_stop_before_post(client, tmp_path):
    c, s = client
    fingerprint = task_fingerprint(s.tasks["6Xtask"])
    s.tasks["6Xtask"]["content"] = "changed"
    with pytest.raises(TodoistError, match="changed"):
        c.complete_task("6Xtask", expected_fingerprint=fingerprint, apply=True, receipt_path=tmp_path / "r.json")
    s.tasks["6Xtask"]["due"] = {"is_recurring": True}
    with pytest.raises(TodoistError, match="Recurring"):
        c.complete_task("6Xtask", apply=True, receipt_path=tmp_path / "r.json")
    assert not s.posts


def recurring_task():
    return task(due={'date': '2026-10-24', 'is_recurring': True,
                     'string': 'every 24', 'lang': 'en', 'timezone': None}, completed_count=4)


def test_rename_preserves_recurrence_and_only_verifies_on_replay(client, tmp_path):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    original = deepcopy(s.tasks['6Xtask'])
    args = dict(content='Invoice: Example', expected_fingerprint=task_fingerprint(original),
                receipt_path=tmp_path / 'rename.json')
    assert c.rename_task('6Xtask', **args)['ok']
    assert not s.posts
    assert c.rename_task('6Xtask', **args, apply=True)['verified']
    assert s.tasks['6Xtask'] == {**original, 'content': 'Invoice: Example'}
    assert c.rename_task('6Xtask', **args, apply=True)['verified']
    assert len(s.posts) == 1


def test_verified_recurring_occurrence_survives_later_title_change(client, tmp_path):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    path = tmp_path / 'complete.json'
    assert c.complete_task('6Xtask', expected_fingerprint=task_fingerprint(s.tasks['6Xtask']),
             expected_occurrence='2026-10-24', apply=True, receipt_path=path)['verified']
    s.tasks['6Xtask']['content'] = 'Invoice: renamed'
    assert c.verify_receipt(path)['verified']
    assert len(s.posts) == 1


def test_stale_rename_does_not_post(client, tmp_path):
    c, s = client
    with pytest.raises(TodoistError, match='changed'):
        c.rename_task('6Xtask', content='Invoice: Example', expected_fingerprint='old',
                       apply=True, receipt_path=tmp_path / 'rename.json')
    assert not s.posts


@pytest.mark.parametrize('action', ['rename', 'restore'])
def test_metadata_readback_rejects_changed_completed_count(client, tmp_path, action):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    path = tmp_path / 'metadata.json'
    fingerprint = task_fingerprint(s.tasks['6Xtask'])
    if action == 'rename':
        result = c.rename_task('6Xtask', content='Invoice: Example', expected_fingerprint=fingerprint,
                               apply=True, receipt_path=path)
    else:
        result = c.restore_recurring_occurrence('6Xtask', expected_due='2026-10-24',
                   occurrence_date='2026-09-24', expected_fingerprint=fingerprint,
                   apply=True, receipt_path=path)
    assert result['verified']
    assert json.loads(path.read_text())['verified_at']
    s.tasks['6Xtask']['completed_count'] += 1
    result = c.verify_receipt(path)
    assert not result['verified']
    assert 'completed_count' in result['mismatches']
    assert len(s.posts) == 1


def test_restore_occurrence_preserves_rule_and_count_then_only_verifies(client, tmp_path):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    s.tasks['6Xtask']['due'].update(date='2026-11-25', string='Every 25')
    options = dict(expected_due='2026-11-25', occurrence_date='2026-10-25',
                   expected_fingerprint=task_fingerprint(s.tasks['6Xtask']),
                   receipt_path=tmp_path / 'restore.json')
    assert c.restore_recurring_occurrence('6Xtask', **options)['ok']
    assert not s.posts
    assert c.restore_recurring_occurrence('6Xtask', **options, apply=True)['verified']
    assert s.tasks['6Xtask']['due']['date'] == '2026-10-25'
    assert s.tasks['6Xtask']['due']['string'] == 'Every 25'
    assert s.tasks['6Xtask']['completed_count'] == 4
    assert c.restore_recurring_occurrence('6Xtask', **options, apply=True)['verified']
    assert len(s.posts) == 1


@pytest.mark.parametrize('case', ['stale', 'wrong_date', 'forward', 'nonrecurring'])
def test_restore_occurrence_guards_prevent_post(client, tmp_path, case):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    fingerprint = task_fingerprint(s.tasks['6Xtask'])
    expected = '2026-10-24'
    occurrence = '2026-09-24'
    if case == 'stale':
        fingerprint = 'wrong'
    if case == 'wrong_date':
        expected = '2026-11-24'
    if case == 'forward':
        occurrence = '2026-11-24'
    if case == 'nonrecurring':
        s.tasks['6Xtask']['due']['is_recurring'] = False
    with pytest.raises(TodoistError):
        c.restore_recurring_occurrence('6Xtask', expected_due=expected, occurrence_date=occurrence,
            expected_fingerprint=fingerprint, apply=True, receipt_path=tmp_path / 'restore.json')
    assert not s.posts


def test_recurring_preview_complete_and_reverify_never_posts_twice(client, tmp_path):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    fingerprint = task_fingerprint(s.tasks['6Xtask'])
    path = tmp_path / 'recurring.json'
    kwargs = dict(expected_fingerprint=fingerprint, expected_occurrence='2026-10-24', receipt_path=path)
    assert c.complete_task('6Xtask', **kwargs)['ok']
    assert not path.exists() and not s.posts
    result = c.complete_task('6Xtask', **kwargs, apply=True)
    assert result['verified']
    assert s.tasks['6Xtask']['due']['date'] == '2026-11-24'
    assert s.tasks['6Xtask']['completed_count'] == 5
    assert c.verify_receipt(path)['verified']
    assert c.complete_task('6Xtask', **kwargs, apply=True)['verified']
    assert len(s.posts) == 1


@pytest.mark.parametrize('case', ['missing_occurrence', 'wrong_occurrence', 'missing_fingerprint', 'missing_count'])
def test_recurring_guards_reject_before_post(client, tmp_path, case):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    occurrence = '2026-10-24'
    if case == 'missing_count':
        del s.tasks['6Xtask']['completed_count']
    fingerprint = task_fingerprint(s.tasks['6Xtask'])
    if case == 'missing_occurrence':
        occurrence = None
    if case == 'wrong_occurrence':
        occurrence = '2026-11-24'
    if case == 'missing_fingerprint':
        fingerprint = None
    with pytest.raises(TodoistError):
        c.complete_task('6Xtask', expected_fingerprint=fingerprint, expected_occurrence=occurrence,
                        apply=True, receipt_path=tmp_path / 'r.json')
    assert not s.posts


@pytest.mark.parametrize('case', ['count_unchanged', 'due_unchanged', 'rule_changed', 'checked', 'missing_count'])
def test_recurring_readback_requires_positive_completion_and_advanced_due(client, tmp_path, case):
    c, s = client
    s.tasks['6Xtask'] = recurring_task()
    path = tmp_path / 'r.json'
    c.complete_task('6Xtask', expected_fingerprint=task_fingerprint(s.tasks['6Xtask']),
                    expected_occurrence='2026-10-24', apply=True, receipt_path=path)
    current = s.tasks['6Xtask']
    if case == 'count_unchanged':
        current['completed_count'] = 4
    if case == 'due_unchanged':
        current['due']['date'] = '2026-10-24'
    if case == 'rule_changed':
        current['due']['string'] = 'every 25'
    if case == 'checked':
        current['checked'] = True
    if case == 'missing_count':
        del current['completed_count']
    assert not c.verify_receipt(path)['verified']
    assert len(s.posts) == 1


def test_untracked_completed_task_is_not_inferred_from_404(client, tmp_path):
    c, s = client
    s.tasks["6Xtask"]["checked"] = True
    with pytest.raises(TodoistError, match="404"):
        c.complete_task("6Xtask", apply=True, receipt_path=tmp_path / "r.json")
    assert not (tmp_path / "r.json").exists()
    assert not s.posts


def test_parent_with_active_subtask_does_not_cascade_completion(client, tmp_path):
    c, s = client
    s.tasks["child"] = task(id="child", parent_id="6Xtask")
    with pytest.raises(TodoistError, match="subtasks"):
        c.complete_task("6Xtask", apply=True, receipt_path=tmp_path / "r.json")
    assert not s.posts


def test_acknowledged_write_with_readback_failure_can_be_verified_later(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    s.read_error = True
    result = c.create_task(content="next", apply=True, receipt_path=path)
    assert not result["verified"] and result["write_status"] == "acknowledged"
    assert result["task_id"] == "new-task"
    s.read_error = False
    assert c.verify_receipt(path)["verified"]
    assert len(s.posts) == 1


@pytest.mark.parametrize("change", [{"content": "other"}, {"checked": True}, {"is_deleted": True}])
def test_create_readback_mismatch(client, tmp_path, change):
    c, s = client
    path = tmp_path / "r.json"
    c.create_task(content="next", apply=True, receipt_path=path)
    s.tasks["new-task"].update(change)
    assert not c.verify_receipt(path)["verified"]
    assert len(s.posts) == 1


def test_404_is_not_proof_of_completed_task(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    s.read_error = True
    s.completed_pages = [{"items": [], "next_cursor": None}]
    result = c.verify_receipt(path)
    assert not result["ok"] and result["mismatches"] == ["completion_record_missing_or_ambiguous"]


def test_completion_history_paginates_and_does_not_require_active_get(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    assert c.complete_task("6Xtask", apply=True, receipt_path=path)["verified"]
    completed = deepcopy(s.tasks["6Xtask"])
    # History membership + completion timestamp are authoritative, not this flag.
    completed["checked"] = False
    s.completed_pages = [{"items": [task(id="other")], "next_cursor": "page2"},
                         {"items": [completed], "next_cursor": None}]
    assert c.verify_receipt(path)["verified"]
    assert s.calls[-1][2]["params"]["cursor"] == "page2"
    assert len(s.posts) == 1


def test_live_api_completed_get_200_still_requires_history(client, tmp_path):
    c, s = client
    s.completed_get_status = 200
    path = tmp_path / "r.json"
    result = c.complete_task("6Xtask", apply=True, receipt_path=path)
    assert result["verified"]
    assert c.get_task("6Xtask")["checked"] is True
    assert c.verify_receipt(path)["verified"]
    s.completed_pages = [{"items": [], "next_cursor": None}]
    result = c.verify_receipt(path)
    assert not result["verified"]
    assert result["mismatches"] == ["completion_record_missing_or_ambiguous"]
    assert len(s.posts) == 1


def test_untracked_completed_get_200_does_not_post(client, tmp_path):
    c, s = client
    s.completed_get_status = 200
    s.tasks["6Xtask"]["checked"] = True
    path = tmp_path / "r.json"
    with pytest.raises(TodoistError, match="already completed"):
        c.complete_task("6Xtask", apply=True, receipt_path=path)
    assert not path.exists() and not s.posts


@pytest.mark.parametrize("change", [
    {"id": "different-id"}, {"project_id": "different-project"},
    {"completed_at": None}, {"completed_at": "2020-01-01T00:00:00Z"},
    {"content": "changed"}, {"is_deleted": True},
])
def test_completion_history_must_match_id_project_time_and_content(client, tmp_path, change):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    record = {**s.tasks["6Xtask"], **change}
    s.completed_pages = [{"items": [record], "next_cursor": None}]
    assert not c.verify_receipt(path)["verified"]
    assert len(s.posts) == 1


def test_completion_history_duplicate_or_repeated_cursor_is_not_success(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    s.completed_pages = [{"items": [s.tasks["6Xtask"]] * 2, "next_cursor": None}]
    assert not c.verify_receipt(path)["verified"]
    s.completed_pages = [{"items": [], "next_cursor": "same"}] * 2
    assert c.verify_receipt(path)["mismatches"] == ["readback_failed"]
    assert len(s.posts) == 1


def test_reopened_task_is_not_verified_from_old_history(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    s.tasks["6Xtask"]["checked"] = False
    result = c.verify_receipt(path)
    assert not result["verified"] and result["mismatches"] == ["task_still_active"]
    assert len(s.posts) == 1


def test_history_visibility_delay_only_retries_reads(client, tmp_path):
    c, s = client
    original = s.request

    def request(method, url, **kwargs):
        if url.endswith("/completed/by_completion_date"):
            s.completed_pages = [{"items": [], "next_cursor": None}]
        return original(method, url, **kwargs)

    s.request = request
    path = tmp_path / "r.json"
    result = c.complete_task("6Xtask", apply=True, receipt_path=path)
    assert not result["verified"] and result["write_status"] == "acknowledged"
    s.request = original
    s.completed_pages = None
    assert c.verify_receipt(path)["verified"]
    assert len(s.posts) == 1


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_active_get_failure_other_than_404_is_not_completion(client, tmp_path, status):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    original = s.request

    def request(method, url, **kwargs):
        if url.endswith("/tasks/6Xtask"):
            return SimpleNamespace(status_code=status, text="PRIVATE")
        return original(method, url, **kwargs)

    s.request = request
    result = c.verify_receipt(path)
    assert not result["verified"] and result["mismatches"] == ["readback_failed"]
    assert len(s.posts) == 1


def test_completion_window_must_contain_saved_write_time(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    receipt = json.loads(path.read_text())
    receipt["completion_window"].update(since="2020-01-01T00:00:00Z", until="2020-01-01T01:00:00Z")
    path.write_text(json.dumps(receipt))
    with pytest.raises(TodoistError, match="receipt"):
        c.verify_receipt(path)
    assert len(s.posts) == 1


def test_completion_uses_saved_time_window_even_when_verified_later(client, tmp_path, monkeypatch):
    from megaton_lib import todoist_client
    c, s = client
    path = tmp_path / "r.json"
    c.complete_task("6Xtask", apply=True, receipt_path=path)
    original = json.loads(path.read_text())["completion_window"]

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(timezone.utc) + timedelta(days=200)

    monkeypatch.setattr(todoist_client, "datetime", Later)
    assert c.verify_receipt(path)["verified"]
    assert s.calls[-1][2]["params"] == original


def test_rejected_write_can_be_corrected_with_a_new_receipt(client, tmp_path):
    c, s = client
    original = s.request

    def rejected(method, url, **kwargs):
        if method == "POST":
            s.calls.append((method, url, kwargs))
            return SimpleNamespace(status_code=400, text="PRIVATE")
        return original(method, url, **kwargs)

    s.request = rejected
    rejected_path = tmp_path / "rejected.json"
    result = c.create_task(content="next", due_string="bad date", apply=True, receipt_path=rejected_path)
    assert result["write_status"] == "rejected"
    rejected_bytes = rejected_path.read_bytes()
    s.request = original
    assert c.create_task(content="next", due_string="tomorrow", apply=True,
                         receipt_path=tmp_path / "corrected.json")["verified"]
    assert rejected_path.read_bytes() == rejected_bytes
    assert len(s.posts) == 2


@pytest.mark.parametrize("payload", [{}, {"schema_version": "wrong"}, []])
def test_invalid_receipt_does_not_write(client, tmp_path, payload):
    c, s = client
    path = tmp_path / "r.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(TodoistError, match="receipt"):
        c.verify_receipt(path)
    assert not s.posts


def test_modified_receipt_payload_is_rejected(client, tmp_path):
    c, s = client
    path = tmp_path / "r.json"
    c.create_task(content="next", apply=True, receipt_path=path)
    receipt = json.loads(path.read_text())
    receipt["payload"]["content"] = "changed"
    path.write_text(json.dumps(receipt))
    with pytest.raises(TodoistError, match="receipt"):
        c.verify_receipt(path)
    assert len(s.posts) == 1


@pytest.mark.parametrize("kwargs", [{"priority": True}, {"priority": 0}, {"content": " "},
                                  {"labels": "work"}, {"due_string": ""}, {"project_id": "https://x"}])
def test_invalid_inputs_do_not_write(client, kwargs):
    c, s = client
    with pytest.raises(TodoistError):
        c.create_task(**{"content": "next", **kwargs})
    assert not s.calls


def test_cli_help_is_offline_and_describes_safety(monkeypatch, capsys):
    monkeypatch.delenv("TODOIST_TOKEN", raising=False)
    with pytest.raises(SystemExit) as exc:
        todoist_tasks.main(["--help"])
    assert exc.value.code == 0
    assert "Writes preview by default" in capsys.readouterr().out


def test_cli_preview_apply_verify_and_safe_errors(client, tmp_path, monkeypatch, capsys):
    c, s = client
    monkeypatch.setattr(TodoistClient, "from_env", lambda **kwargs: c)
    assert todoist_tasks.main(["create", "--content", "next"]) == 0
    assert json.loads(capsys.readouterr().out)["write_status"] == "not_attempted"
    path = tmp_path / "r.json"
    assert todoist_tasks.main(["create", "--content", "next", "--apply", "--receipt", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["verified"]
    assert todoist_tasks.main(["verify", "--receipt", str(path)]) == 0
    capsys.readouterr()
    assert todoist_tasks.main(["create", "--content", "next", "--apply"]) == 1
    assert "--receipt" in capsys.readouterr().out
    assert len(s.posts) == 1


def _window(days=1):
    until = datetime.now(timezone.utc) + timedelta(minutes=1)
    return until - timedelta(days=days), until


def test_list_completed_tasks_reads_history_with_window_and_project(client):
    c, s = client
    since, until = _window()
    done = task(id="done", checked=True, completed_at=datetime.now(timezone.utc).isoformat())
    s.completed_pages = [{"items": [done], "next_cursor": "next"}, {"items": [], "next_cursor": None}]
    assert [t["id"] for t in c.list_completed_tasks(since=since, until=until, project_id="p")] == ["done"]
    params = [call[2]["params"] for call in s.calls if call[1].endswith("/completed/by_completion_date")]
    assert params[0] == {"since": since.isoformat(), "until": until.isoformat(), "project_id": "p"}
    assert params[1]["cursor"] == "next"
    assert not s.posts


@pytest.mark.parametrize("bounds", ["naive", "reversed", "too_long"])
def test_list_completed_tasks_rejects_invalid_windows_before_request(client, bounds):
    c, s = client
    since, until = _window()
    if bounds == "naive":
        since = since.replace(tzinfo=None)
    elif bounds == "reversed":
        since, until = until, since
    else:
        since = until - timedelta(days=90)
    with pytest.raises(TodoistError, match="window|timezone"):
        c.list_completed_tasks(since=since, until=until)
    assert not s.calls


@pytest.mark.parametrize("completed_at", [None, "not-a-date", "2020-01-01T00:00:00+00:00"])
def test_list_completed_tasks_requires_completion_time_inside_window(client, completed_at):
    c, s = client
    since, until = _window()
    s.completed_pages = [{"items": [task(id="done", checked=True, completed_at=completed_at)],
                          "next_cursor": None}]
    with pytest.raises(TodoistError, match="completed_at|outside"):
        c.list_completed_tasks(since=since, until=until)


def test_list_completed_tasks_checks_account(client):
    _, s = client
    since, until = _window()
    c = TodoistClient("secret-token", session=s, expected_user_id="other")
    with pytest.raises(TodoistError, match="account"):
        c.list_completed_tasks(since=since, until=until)
    assert not any(call[1].endswith("/completed/by_completion_date") for call in s.calls)


def test_rename_parent_with_active_subtask_is_allowed(client, tmp_path):
    c, s = client
    s.tasks["6Xchild"] = task(id="6Xchild", content="Child", parent_id="6Xtask")
    original = deepcopy(s.tasks["6Xtask"])
    result = c.rename_task("6Xtask", content="Renamed parent", expected_fingerprint=task_fingerprint(original),
                           apply=True, receipt_path=tmp_path / "rename.json")
    assert result["verified"] and s.tasks["6Xtask"]["content"] == "Renamed parent"


def test_complete_parent_with_active_subtask_still_stops(client, tmp_path):
    c, s = client
    s.tasks["6Xchild"] = task(id="6Xchild", content="Child", parent_id="6Xtask")
    with pytest.raises(TodoistError, match="subtasks"):
        c.complete_task("6Xtask", apply=True, receipt_path=tmp_path / "complete.json")
    assert not s.posts
