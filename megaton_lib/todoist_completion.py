"""Guarded task completion for consumer workflows, over the shared Todoist client.

One business operation completes at most one task occurrence: the receipt path
is bound to the account and operation, an optional source ``context`` is bound
to the receipt, earlier attempts for the same task block a new write, and a
resume is only ever a readback. Which task, which period and where receipts
live are consumer decisions; pass ``receipt_dir`` explicitly.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
from typing import Any

from .todoist_client import (
    API_BASE,
    SCHEMA_VERSION,
    TodoistClient,
    TodoistError,
    task_fingerprint,
)

TodoistAPIError = TodoistError  # historical consumer alias


@dataclass(frozen=True)
class TodoistTask:
    id: str
    content: str
    url: str
    project_id: str | None
    due_date: str
    due_string: str
    fingerprint: str = ""
    is_recurring: bool = False


def _task_from_payload(payload: dict[str, Any]) -> TodoistTask:
    due = payload.get("due") or {}
    return TodoistTask(
        id=payload["id"], content=payload["content"],
        url=f"https://app.todoist.com/app/task/{payload['id']}",
        project_id=payload.get("project_id"),
        due_date=str(due.get("date") or ""), due_string=str(due.get("string") or ""),
        fingerprint=task_fingerprint(payload),
        is_recurring=bool(due.get("is_recurring")),
    )


def _client(token: str, api_base: str, timeout: float) -> TodoistClient:
    if api_base.rstrip("/") != API_BASE:
        raise TodoistError("Custom Todoist API endpoints are not supported.")
    return TodoistClient(token, timeout=timeout,
                         expected_user_id=os.environ.get("TODOIST_EXPECTED_USER_ID") or None)


def list_active_tasks(*, token: str, api_base: str = API_BASE,
                      project_id: str | None = None, timeout: float = 15.0) -> list[TodoistTask]:
    client = _client(token, api_base, timeout)
    return [_task_from_payload(task) for task in client.list_tasks(project_id=project_id)]


def list_completed_tasks(*, token: str, since: datetime, until: datetime,
                         api_base: str = API_BASE, project_id: str | None = None,
                         timeout: float = 15.0) -> list[TodoistTask]:
    """Completed-task history (read-only), used as positive completion evidence."""
    client = _client(token, api_base, timeout)
    return [_task_from_payload(task) for task in
            client.list_completed_tasks(since=since, until=until, project_id=project_id)]


def completion_receipt_path(*, token: str, scope: dict,
                            receipt_dir: str | Path,
                            api_base: str = API_BASE, timeout: float = 15.0) -> Path:
    """Bind a business operation/period to one account, before looking up tasks."""
    account = _client(token, api_base, timeout).account()["id"]
    # Search refinements must not create a new write attempt for the same operation.
    identity = {key: value for key, value in scope.items()
                if key not in {"content", "match", "project_id", "allow_missing_due"}}
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return Path(receipt_dir) / account / f"{key}.json"


def _check_context(path: Path, context: dict | None, *, claim: bool = False) -> None:
    if context is None:
        return
    digest = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()
    binding = path.with_suffix(path.suffix + ".context")
    expected = {"schema_version": "todoist-context/v1", "digest": digest}
    if binding.exists():
        try:
            if json.loads(binding.read_text(encoding="utf-8")) != expected:
                raise ValueError
        except (OSError, ValueError):
            raise TodoistError("Receipt context mismatch; no write or follow-up is allowed.") from None
    elif path.exists():
        raise TodoistError("Legacy Receipt has no source binding; inspect manually, do not adopt it.")
    elif claim:
        try:
            fd = os.open(binding, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            _check_context(path, context)
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(expected, handle)
                handle.flush()
                os.fsync(handle.fileno())


def _occurrence_value(value: str) -> tuple[str, date | datetime]:
    if not isinstance(value, str):
        raise TodoistError('Invalid occurrence date in prior Receipt.')
    try:
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            return 'date', date.fromisoformat(value)
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            return 'floating', parsed
        return 'aware', parsed.astimezone(timezone.utc)
    except ValueError:
        raise TodoistError('Invalid occurrence date in prior Receipt.') from None


def _earlier_occurrence(previous: str, current: str) -> bool:
    old_kind, old = _occurrence_value(previous)
    new_kind, new = _occurrence_value(current)
    if old_kind != new_kind:
        raise TodoistError('Occurrence date representations differ; inspect the original Receipt.')
    return old < new


def _metadata_verified(prior: dict, siblings: list[Path], client: TodoistClient) -> bool:
    if prior.get('verified_at'):
        try:
            stamp = datetime.fromisoformat(prior['verified_at'].replace('Z', '+00:00'))
            if stamp.tzinfo is not None:
                return True
        except (TypeError, ValueError, AttributeError):
            pass
        raise TodoistError('Invalid verification record; inspect the original Receipt.')
    # Old Receipts predate verified_at. A later verified completion whose before
    # snapshot exactly matches the metadata write proves the old transition.
    for sibling in siblings:
        try:
            later = json.loads(sibling.read_text(encoding='utf-8'))
            if (later.get('action') != 'complete'
                    or later.get('account_id') != prior['account_id']
                    or later.get('task_id') != prior.get('task_id')
                    or later.get('write_status') != 'acknowledged'):
                continue
            expected, before = prior.get('expected'), later.get('before')
            if not isinstance(expected, dict) or not isinstance(before, dict):
                continue
            keys = ('id', 'content', 'description', 'project_id', 'parent_id',
                    'labels', 'priority', 'due', 'checked', 'completed_count')
            if any(expected.get(k) != before.get(k) for k in keys):
                continue
            finished = datetime.fromisoformat(prior['write_finished_at'].replace('Z', '+00:00'))
            started = datetime.fromisoformat(later['write_started_at'].replace('Z', '+00:00'))
            if (finished.tzinfo is not None and started.tzinfo is not None
                    and finished <= started and client.verify_receipt(sibling).get('verified')):
                return True
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            continue
    return False


def _guard_prior_attempts(path: Path, task_id: str, account_id: str,
                          *, client: TodoistClient, expected_occurrence: str | None = None) -> None:
    # Include pre-migration filter-keyed Receipts, not only the new canonical path.
    siblings = list(path.parent.glob('*.json'))
    for sibling in siblings:
        if sibling.resolve() == path.resolve():
            continue
        try:
            prior = json.loads(sibling.read_text(encoding="utf-8"))
            if not isinstance(prior, dict) or not isinstance(prior.get("payload", {}), dict):
                raise TypeError
            if (prior.get("schema_version") != SCHEMA_VERSION
                    or not isinstance(prior.get("account_id"), str)
                    or prior.get("action") not in {"create", "complete", "restore_occurrence", "rename"}
                    or prior.get("write_status") not in {"unknown", "acknowledged", "rejected"}
                    or (prior["action"] != "create" and not isinstance(prior["payload"].get("task_id"), str))):
                raise ValueError
        except (OSError, ValueError, TypeError):
            raise TodoistError("Unreadable prior Receipt; inspect before another write.") from None
        if (prior.get('account_id') == account_id
                and prior.get('action') in {'rename', 'restore_occurrence'}
                and prior['payload']['task_id'] == task_id
                and prior['write_status'] != 'rejected'):
            if prior['write_status'] == 'unknown':
                raise TodoistError('Unresolved metadata write; verify its original Receipt before completion.')
            if (_metadata_verified(prior, siblings, client)
                    or client.verify_receipt(sibling).get('verified')):
                continue
            raise TodoistError('Unverified metadata write; verify its original Receipt before completion.')
        if (prior.get("account_id") == account_id and prior.get("action") == "complete"
                and (prior.get("payload") or {}).get("task_id") == task_id
                and prior.get("write_status") != "rejected"):
            previous = prior['payload'].get('expected_occurrence')
            if (expected_occurrence and previous and _earlier_occurrence(previous, expected_occurrence)
                    and client.verify_receipt(sibling).get('verified')):
                continue
            raise TodoistError("A prior attempt exists for this task; verify its original Receipt, do not write again.")


@contextmanager
def _task_lock(path: Path, task_id: str):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = path.parent / (".task-" + hashlib.sha256(task_id.encode()).hexdigest() + ".lock")
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise TodoistError("This task already has a running completion; inspect its Receipt.") from None
        yield
    finally:
        os.close(fd)


def verify_completion(*, token: str, receipt_path: str | Path,
                      context: dict | None = None,
                      api_base: str = API_BASE, timeout: float = 15.0) -> TodoistTask | None:
    """Resume only by GET verification; absence of a task is never success."""
    path = Path(receipt_path)
    _check_context(path, context)
    if not path.exists():
        return None
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if receipt.get("action") != "complete":
            raise ValueError
    except (OSError, ValueError, AttributeError):
        raise TodoistError("Invalid completion Receipt; inspect it before proceeding.") from None
    result = _client(token, api_base, timeout).verify_receipt(path)
    if not result["ok"] or not result["verified"]:
        raise TodoistError(
            f"Todoist {result['write_status']}; verify/inspect the existing Receipt. No repeated write."
        )
    return _task_from_payload(receipt["before"])


def close_task(*, token: str, task_id: str, receipt_path: str | Path,
               expected_fingerprint: str | None = None, apply: bool = False,
               expected_occurrence: str | None = None,
               context: dict | None = None,
               api_base: str = API_BASE, timeout: float = 15.0) -> dict:
    """Preview by default; applied completion must pass shared history readback."""
    client = _client(token, api_base, timeout)
    path = Path(receipt_path)
    _check_context(path, context)
    account_id = client.account()["id"]
    occurrence = {"expected_occurrence": expected_occurrence} if expected_occurrence is not None else {}
    if apply:
        with _task_lock(path, task_id):
            _guard_prior_attempts(path, task_id, account_id, client=client,
                                  expected_occurrence=expected_occurrence)
            # Validate the shared guards before persisting a source binding.
            client.complete_task(task_id, expected_fingerprint=expected_fingerprint,
                                 apply=False, receipt_path=path, **occurrence)
            _check_context(path, context, claim=True)
            result = client.complete_task(
                task_id, expected_fingerprint=expected_fingerprint, apply=True, receipt_path=path,
                **occurrence,
            )
    else:
        _guard_prior_attempts(path, task_id, account_id, client=client,
                              expected_occurrence=expected_occurrence)
        result = client.complete_task(task_id, expected_fingerprint=expected_fingerprint,
                                      apply=False, receipt_path=path, **occurrence)
    if not result["ok"] or (apply and not result["verified"]):
        raise TodoistError(
            f"Todoist {result['write_status']}; inspect Receipt, do not automatically repeat. "
            + result["next_step"]
        )
    return result
