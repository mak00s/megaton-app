import json
from datetime import UTC, datetime

import pytest
import requests
from megaton_lib import todoist_client as shared
from megaton_lib import todoist_completion as adapter


class FakeTodoist:
    def __init__(self):
        self.task = {
            "id": "task1", "content": "Monthly handoff", "checked": False,
            "is_deleted": False, "project_id": "project1", "parent_id": None,
            "description": "", "labels": [], "priority": 1,
            "due": {"date": "2026-10-16", "is_recurring": False},
        }
        self.posts = 0
        self.closed = False
        self.failure = None
        self.history = True
        self.child = False
        self.account = "account1"
        self.next_due = "2026-11-16"

    def request(self, method, url, **kwargs):
        status, payload = 200, None
        if method == "POST":
            self.posts += 1
            if self.failure == "unknown":
                raise requests.ConnectionError("PRIVATE TOKEN BODY")
            if self.failure == "rejected":
                status = 400
            else:
                if self.task["due"].get("is_recurring"):
                    self.task = {**self.task, "due": {**self.task["due"], "date": self.next_due},
                                 "completed_count": self.task["completed_count"] + 1}
                else:
                    self.closed = True
                status = 204
        elif url.endswith("/user"):
            payload = {"id": self.account, "token": "PRIVATE TOKEN"}
        elif "/completed/" in url:
            items = [{**self.task, "checked": True,
                      "completed_at": datetime.now(UTC).isoformat()}] if self.history else []
            payload = {"items": items, "next_cursor": None}
        elif url.endswith("/tasks/" + self.task["id"]):
            if self.closed:
                status = 404
            else:
                payload = self.task
        elif url.endswith("/tasks"):
            items = [self.task]
            if self.child:
                items += [{**self.task, "id": "child", "parent_id": "task1"}]
            payload = {"results": items, "next_cursor": None}
        else:
            raise AssertionError("Unexpected request")
        class Response:
            status_code = status
            def json(self):
                return payload
        return Response()


@pytest.fixture
def api(monkeypatch):
    monkeypatch.delenv("TODOIST_EXPECTED_USER_ID", raising=False)
    fake = FakeTodoist()
    monkeypatch.setattr(shared.requests, "Session", lambda: fake)
    return fake


def test_list_maps_record_and_fingerprint(api):
    task = adapter.list_active_tasks(token="token")[0]
    assert task.id == "task1"
    assert task.fingerprint == shared.task_fingerprint(api.task)


def test_scope_receipt_is_account_and_period_bound(api, tmp_path):
    a = adapter.completion_receipt_path(token="token", scope={"month": "2026-10"}, receipt_dir=tmp_path)
    assert a.parent == tmp_path / "account1"
    b = adapter.completion_receipt_path(token="token", scope={"month": "2026-11"}, receipt_dir=tmp_path)
    assert a != b
    api.account = "account2"
    assert a != adapter.completion_receipt_path(token="token", scope={"month": "2026-10"}, receipt_dir=tmp_path)


def test_preview_never_posts_or_saves(api, tmp_path):
    receipt = tmp_path / "attempt.json"
    result = adapter.close_task(token="token", task_id="task1", receipt_path=receipt)
    assert result["write_status"] == "not_attempted"
    assert api.posts == 0
    assert not receipt.exists()


def test_complete_and_resume_verifies_history_without_second_post(api, tmp_path):
    receipt = tmp_path / "attempt.json"
    result = adapter.close_task(token="token", task_id="task1", receipt_path=receipt,
                                expected_fingerprint=shared.task_fingerprint(api.task), apply=True)
    assert result["verified"]
    assert api.posts == 1
    assert adapter.verify_completion(token="token", receipt_path=receipt).id == "task1"
    assert api.posts == 1
    assert receipt.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("failure", ["unknown", "rejected"])
def test_failed_write_receipt_blocks_replay(api, tmp_path, failure):
    api.failure = failure
    receipt = tmp_path / "attempt.json"
    with pytest.raises(adapter.TodoistAPIError) as error:
        adapter.close_task(token="token", task_id="task1", receipt_path=receipt, apply=True)
    assert "PRIVATE" not in str(error.value)
    assert json.loads(receipt.read_text())["write_status"] == failure
    with pytest.raises(adapter.TodoistAPIError):
        adapter.verify_completion(token="token", receipt_path=receipt)
    assert api.posts == 1


def test_missing_history_is_not_success(api, tmp_path):
    api.history = False
    receipt = tmp_path / "attempt.json"
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=receipt, apply=True)
    assert api.posts == 1
    assert json.loads(receipt.read_text())["write_status"] == "acknowledged"


@pytest.mark.parametrize("guard", ["recurring", "child", "fingerprint"])
def test_shared_completion_guards_run_before_post(api, tmp_path, guard):
    if guard == "recurring":
        api.task["due"]["is_recurring"] = True
    if guard == "child":
        api.child = True
    fingerprint = "wrong" if guard == "fingerprint" else None
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "attempt.json",
                           expected_fingerprint=fingerprint, apply=True)
    assert api.posts == 0


def test_foreign_receipt_account_and_custom_endpoint_rejected(api, tmp_path):
    receipt = tmp_path / "attempt.json"
    adapter.close_task(token="token", task_id="task1", receipt_path=receipt, apply=True)
    api.account = "other"
    with pytest.raises(adapter.TodoistAPIError):
        adapter.verify_completion(token="token", receipt_path=receipt)
    with pytest.raises(adapter.TodoistAPIError):
        adapter.list_active_tasks(token="token", api_base="https://other.example")
    assert api.posts == 1


def test_expected_account_guard_prevents_write(api, tmp_path, monkeypatch):
    monkeypatch.setenv("TODOIST_EXPECTED_USER_ID", "other")
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "attempt.json",
                           apply=True)
    assert api.posts == 0


def test_non_completion_receipt_is_not_resumed(api, tmp_path):
    receipt = tmp_path / "attempt.json"
    receipt.write_text('{"action": "create"}')
    with pytest.raises(adapter.TodoistAPIError):
        adapter.verify_completion(token="token", receipt_path=receipt)
    assert api.posts == 0


@pytest.mark.parametrize("change", [
    {"match": "exact"}, {"content": "refined title"}, {"project_id": "project1"},
    {"allow_missing_due": True},
])


def test_filter_refinement_keeps_operation_receipt(api, tmp_path, change):
    scope = {"workflow": "monthly", "month": "2026-10", "content": "title",
             "match": "contains", "project_id": "", "allow_missing_due": False}
    original = adapter.completion_receipt_path(token="token", scope=scope, receipt_dir=tmp_path)
    refined = adapter.completion_receipt_path(token="token", scope={**scope, **change}, receipt_dir=tmp_path)
    assert original == refined


def test_old_unknown_receipt_blocks_new_alias(api, tmp_path):
    api.failure = "unknown"
    old = tmp_path / "legacy-filter-name.json"
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=old, apply=True)
    api.failure = None
    with pytest.raises(adapter.TodoistAPIError, match="prior attempt"):
        adapter.close_task(token="token", task_id="task1",
                           receipt_path=tmp_path / "new-operation-name.json", apply=True)
    assert api.posts == 1
    assert not (tmp_path / "new-operation-name.json").exists()


def test_confirmed_rejection_allows_reviewed_new_attempt(api, tmp_path):
    old = tmp_path / "rejected.json"
    api.failure = "rejected"
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=old, apply=True)
    api.failure = None
    result = adapter.close_task(token="token", task_id="task1",
                                receipt_path=tmp_path / "reviewed-new.json", apply=True)
    assert result["verified"]
    assert api.posts == 2
    assert json.loads(old.read_text())["write_status"] == "rejected"


def test_source_context_preview_and_readback(api, tmp_path):
    path = tmp_path / "attempt.json"
    context = {"workflow": "with-csv", "month": "2026-10", "message_id": "source1"}
    adapter.close_task(token="token", task_id="task1", receipt_path=path, context=context)
    assert list(tmp_path.iterdir()) == []
    adapter.close_task(token="token", task_id="task1", receipt_path=path, context=context, apply=True)
    assert adapter.verify_completion(token="token", receipt_path=path, context=context).id == "task1"
    assert (tmp_path / "attempt.json.context").stat().st_mode & 0o777 == 0o600
    for apply in (False, True):
        with pytest.raises(adapter.TodoistAPIError, match="context mismatch"):
            adapter.close_task(token="token", task_id="task1", receipt_path=path,
                               context={**context, "message_id": "source2"}, apply=apply)
    with pytest.raises(adapter.TodoistAPIError, match="context mismatch"):
        adapter.verify_completion(token="token", receipt_path=path,
                                  context={**context, "message_id": "source2"})
    assert api.posts == 1


def test_receipt_without_source_binding_cannot_be_adopted(api, tmp_path):
    path = tmp_path / "legacy.json"
    adapter.close_task(token="token", task_id="task1", receipt_path=path, apply=True)
    with pytest.raises(adapter.TodoistAPIError, match="no source binding"):
        adapter.verify_completion(token="token", receipt_path=path, context={"message_id": "source1"})
    assert api.posts == 1


def test_same_task_parallel_alias_is_rejected(api, tmp_path):
    path = tmp_path / "first.json"
    with adapter._task_lock(path, "task1"), pytest.raises(adapter.TodoistAPIError, match="running completion"):
        adapter.close_task(token="token", task_id="task1",
                           receipt_path=tmp_path / "second.json", apply=True)
    assert api.posts == 0


def test_malformed_legacy_journal_blocks_new_attempt(api, tmp_path):
    (tmp_path / "legacy.json").write_text('{"payload": {}}')
    with pytest.raises(adapter.TodoistAPIError, match="Unreadable prior Receipt"):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "new.json", apply=True)
    assert api.posts == 0


@pytest.mark.parametrize("failure", ["unknown", "unverified"])
def test_recurring_prior_unverified_attempt_blocks_next_month(api, tmp_path, failure):
    api.task["due"]["is_recurring"] = True
    api.task["completed_count"] = 5
    api.failure = "unknown" if failure == "unknown" else None
    if failure == "unverified":
        api.next_due = "2026-10-16"
    with pytest.raises(adapter.TodoistAPIError):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "oct.json",
                           expected_fingerprint=shared.task_fingerprint(api.task),
                           expected_occurrence="2026-10-16", apply=True)
    api.failure = None
    api.task["due"]["date"] = "2026-11-16"
    # The due date moved on without the count progressing, so a live readback
    # still cannot prove the October completion.
    api.task["completed_count"] = 5
    with pytest.raises(adapter.TodoistAPIError, match="prior attempt"):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "nov.json",
                           expected_fingerprint=shared.task_fingerprint(api.task),
                           expected_occurrence="2026-11-16", apply=True)
    assert api.posts == 1


def test_recurring_same_occurrence_cannot_use_another_receipt(api, tmp_path):
    api.task["due"]["is_recurring"] = True
    api.task["completed_count"] = 5
    adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "first.json",
                       expected_fingerprint=shared.task_fingerprint(api.task),
                       expected_occurrence="2026-10-16", apply=True)
    with pytest.raises(adapter.TodoistAPIError, match="prior attempt"):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "alias.json",
                           expected_fingerprint=shared.task_fingerprint(api.task),
                           expected_occurrence="2026-10-16", apply=True)
    assert api.posts == 1


def test_recurring_wrong_occurrence_is_rejected_before_write(api, tmp_path):
    api.task["due"]["is_recurring"] = True
    api.task["completed_count"] = 5
    with pytest.raises(adapter.TodoistAPIError, match="exact expected_occurrence"):
        adapter.close_task(token="token", task_id="task1", receipt_path=tmp_path / "wrong.json",
                           expected_fingerprint=shared.task_fingerprint(api.task),
                           expected_occurrence="2026-10-01", apply=True)
    assert api.posts == 0
    assert list(tmp_path.rglob("*.json")) == []


def test_completed_history_maps_records_and_recurring_flag(api):
    from datetime import timedelta
    until = datetime.now(UTC) + timedelta(minutes=1)
    api.history = True
    tasks = adapter.list_completed_tasks(token="token", since=until - timedelta(days=2), until=until,
                                         project_id="project1")
    assert [t.id for t in tasks] == ["task1"]
    assert tasks[0].is_recurring is False
    assert api.posts == 0


def test_recurring_next_month_allowed_only_after_verified_prior_occurrence(api, tmp_path):
    from copy import deepcopy
    from types import SimpleNamespace
    api.task['due'] = {'date': '2026-10-24', 'is_recurring': True, 'string': 'every 24'}
    api.task['completed_count'] = 4
    original = api.request
    def request(method, url, **kwargs):
        if method == 'POST':
            api.posts += 1
            api.task['completed_count'] += 1
            month = int(api.task['due']['date'][5:7]) + 1
            api.task['due']['date'] = f'2026-{month:02d}-24'
            return SimpleNamespace(status_code=204)
        response = original(method, url, **kwargs)
        return SimpleNamespace(status_code=response.status_code, json=lambda: deepcopy(response.json()))
    api.request = request
    for month in ('10', '11'):
        result = adapter.close_task(token='token', task_id='task1',
                expected_fingerprint=shared.task_fingerprint(api.task),
                expected_occurrence=f'2026-{month}-24',
                receipt_path=tmp_path / f'{month}.json', apply=True)
        assert result['verified']
    assert api.posts == 2


def test_same_recurring_occurrence_alias_never_reposts(api, tmp_path):
    api.task['due'] = {'date': '2026-10-24', 'is_recurring': True}
    api.task['completed_count'] = 4
    api.failure = 'unknown'
    for path in ('first.json', 'alias.json'):
        with pytest.raises(adapter.TodoistAPIError):
            adapter.close_task(token='token', task_id='task1',
                    expected_fingerprint=shared.task_fingerprint(api.task),
                    expected_occurrence='2026-10-24', receipt_path=tmp_path / path, apply=True)
    assert api.posts == 1


@pytest.mark.parametrize('action', ['rename', 'restore'])
def test_unknown_metadata_write_blocks_later_completion(api, tmp_path, action):
    api.task['due'] = {'date': '2026-11-25', 'is_recurring': True, 'string': 'every 25'}
    api.task['completed_count'] = 4
    client = shared.TodoistClient('token')
    fingerprint = shared.task_fingerprint(api.task)
    api.failure = 'unknown'
    if action == 'rename':
        result = client.rename_task('task1', content='Invoice: Example',
            expected_fingerprint=fingerprint, apply=True, receipt_path=tmp_path / 'metadata.json')
    else:
        result = client.restore_recurring_occurrence('task1', expected_due='2026-11-25',
            occurrence_date='2026-10-25', expected_fingerprint=fingerprint,
            apply=True, receipt_path=tmp_path / 'metadata.json')
    assert result['write_status'] == 'unknown'
    api.failure = None
    with pytest.raises(adapter.TodoistAPIError, match='Unresolved metadata'):
        adapter.close_task(token='token', task_id='task1', expected_fingerprint=fingerprint,
            expected_occurrence='2026-11-25', apply=True, receipt_path=tmp_path / 'complete.json')
    assert api.posts == 1
    assert not (tmp_path / 'complete.json').exists()


@pytest.mark.parametrize('verified', [False, True])
def test_acknowledged_metadata_requires_verification_before_completion(tmp_path, verified):
    from unittest.mock import Mock
    prior = {'schema_version': shared.SCHEMA_VERSION, 'action': 'rename',
             'account_id': 'account1', 'task_id': 'task1', 'write_status': 'acknowledged',
             'payload': {'task_id': 'task1'}}
    (tmp_path / 'metadata.json').write_text(json.dumps(prior))
    client = Mock()
    client.verify_receipt.return_value = {'verified': verified}
    if verified:
        adapter._guard_prior_attempts(tmp_path / 'complete.json', 'task1', 'account1', client=client)
    else:
        with pytest.raises(adapter.TodoistAPIError, match='Unverified metadata'):
            adapter._guard_prior_attempts(tmp_path / 'complete.json', 'task1', 'account1', client=client)


def test_previously_verified_metadata_allows_later_legitimate_state_changes(tmp_path):
    from unittest.mock import Mock
    prior = {'schema_version': shared.SCHEMA_VERSION, 'action': 'rename',
             'account_id': 'account1', 'task_id': 'task1', 'write_status': 'acknowledged',
             'verified_at': '2026-10-26T01:00:00+00:00', 'payload': {'task_id': 'task1'}}
    (tmp_path / 'metadata.json').write_text(json.dumps(prior))
    client = Mock()
    adapter._guard_prior_attempts(tmp_path / 'complete.json', 'task1', 'account1', client=client)
    client.verify_receipt.assert_not_called()


def test_legacy_restore_proved_by_later_verified_completion(tmp_path):
    from unittest.mock import Mock
    expected = {'id': 'task1', 'content': 'Example', 'due': {'date': '2026-10-25'},
                'completed_count': 4}
    prior = {'schema_version': shared.SCHEMA_VERSION, 'action': 'restore_occurrence',
             'account_id': 'account1', 'task_id': 'task1', 'write_status': 'acknowledged',
             'payload': {'task_id': 'task1'}, 'expected': expected,
             'write_finished_at': '2026-10-26T01:00:00+00:00'}
    later = {'schema_version': shared.SCHEMA_VERSION, 'action': 'complete',
             'account_id': 'account1', 'task_id': 'task1', 'write_status': 'acknowledged',
             'payload': {'task_id': 'task1', 'expected_occurrence': '2026-10-25'},
             'before': expected, 'write_started_at': '2026-10-26T01:01:00+00:00'}
    (tmp_path / 'restore.json').write_text(json.dumps(prior))
    (tmp_path / 'complete.json').write_text(json.dumps(later))
    client = Mock()
    client.verify_receipt.return_value = {'verified': True}
    adapter._guard_prior_attempts(tmp_path / 'next.json', 'task1', 'account1', client=client,
                                  expected_occurrence='2026-11-25')


@pytest.mark.parametrize('old,new,expected', [
    ('2026-10-25', '2026-11-25', True),
    ('2026-10-25', '2026-10-25', False),
    ('2026-10-25T01:00:00Z', '2026-10-25T09:00:00+09:00', False),
    ('2026-10-25T09:00:00+09:00', '2026-10-25T01:00:00Z', True),
    ('2026-10-25T09:00:00+09:00', '2026-10-25T00:00:00Z', False),
    ('2026-10-25T09:00:00', '2026-10-25T10:00:00', True),
])


def test_occurrence_comparison_uses_dates_and_normalized_timestamps(old, new, expected):
    assert adapter._earlier_occurrence(old, new) is expected


@pytest.mark.parametrize('old,new', [
    ('2026-10-25', '2026-10-25T09:00:00'),
    ('2026-10-25T09:00:00', '2026-10-25T09:00:00Z'),
    ('bad', '2026-11-25'),
])


def test_mixed_or_invalid_occurrence_representations_stop(old, new):
    with pytest.raises(adapter.TodoistAPIError):
        adapter._earlier_occurrence(old, new)
