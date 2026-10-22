"""Narrow task handoff adapter; no scheduling policy or automatic completion hooks."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import tempfile
from uuid import uuid4

import requests

API_BASE = "https://api.todoist.com/api/v1"
SCHEMA_VERSION = "todoist-task/v1"
# The completed-history endpoint accepts ranges of up to three months.
MAX_COMPLETED_WINDOW = timedelta(days=89)


class TodoistError(ValueError):
    """Safe-to-display input, API or receipt error (never an API response body)."""


class TodoistHTTPError(TodoistError):
    """HTTP status only; never retain response bodies or credential headers."""

    def __init__(self, status: int):
        self.status = status
        super().__init__(f"Todoist HTTP {status}; no automatic retry or fallback.")


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone is required.")
    return parsed.astimezone(timezone.utc)


def _id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise TodoistError("Use an explicit Todoist ID, not a URL.")
    return value


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def task_fingerprint(task: dict) -> str:
    """Detect content/state edits; not an atomic lock against other Todoist clients."""
    return _digest({key: task.get(key) for key in (
        "id", "content", "description", "project_id", "parent_id", "labels", "priority", "due", "checked",
    )})


def find_unique_task(tasks: list[dict], *, content_prefix: str) -> dict | None:
    """Match a consumer prefix at a word/colon boundary; reject ambiguity."""
    if not content_prefix:
        raise TodoistError("A nonempty task prefix is required.")
    matches = []
    for task in tasks:
        content = task.get("content", "")
        if content.startswith(content_prefix):
            rest = content[len(content_prefix):]
            if not rest or rest[0] == ":" or rest[0].isspace():
                matches.append(task)
    if len(matches) > 1:
        raise TodoistError("Multiple matching tasks; select an explicit task ID.")
    return matches[0] if matches else None


def _save(path: Path, value: dict, *, exclusive: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".todoist-", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        if exclusive:
            os.link(temporary, path)
        else:
            os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _load_receipt(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if (value["schema_version"] != SCHEMA_VERSION
                or value["action"] not in {"create", "complete"}
                or not isinstance(value["intent_digest"], str)
                or not isinstance(value["account_id"], str)
                or not isinstance(value["request_id"], str)
                or value["write_status"] not in {"unknown", "acknowledged", "rejected"}
                or not isinstance(value["payload"], dict)):
            raise ValueError
        if value["intent_digest"] != _digest({
            "account_id": value["account_id"], "action": value["action"], "payload": value["payload"],
        }):
            raise ValueError
        if value["task_id"] is not None:
            _id(value["task_id"])
        if value["write_status"] == "acknowledged":
            TodoistClient._validate_task(value["expected"], value["task_id"])
        if value["action"] == "complete" and value["task_id"] != value["payload"].get("task_id"):
            raise ValueError
        if value["action"] == "complete":
            window = value["completion_window"]
            since, until = _timestamp(window["since"]), _timestamp(window["until"])
            if not timedelta(0) < until - since <= timedelta(days=1):
                raise ValueError
            _id(window["project_id"])
            if (not since <= _timestamp(value["write_started_at"]) < until
                    or window["project_id"] != value["before"]["project_id"]):
                raise ValueError
        if value["write_status"] == "rejected" and value["rejection_http_status"] not in {400, 401, 403, 404, 429}:
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError, OSError):
        raise TodoistError("Invalid or unreadable Todoist receipt; inspect it before proceeding.") from None
    return value


class TodoistClient:
    """Caller-selected tasks only. Writes default to preview and never retry.

    Pass a personal API token explicitly (or use from_env). No token discovery,
    browser/connector fallback, or workflow-success-to-task-completion inference.
    """

    def __init__(self, token: str, *, timeout: float = 15.0, session=None,
                 expected_user_id: str | None = None):
        if not isinstance(token, str) or not token.strip():
            raise TodoistError("Set TODOIST_TOKEN explicitly.")
        if timeout <= 0:
            raise TodoistError("timeout must be positive.")
        self._token = token.strip()
        self._session = session if session is not None else requests.Session()
        self._timeout = timeout
        self._expected_user_id = _id(expected_user_id) if expected_user_id is not None else None

    @classmethod
    def from_env(cls, **kwargs):
        return cls(os.environ.get("TODOIST_TOKEN", ""), **kwargs)

    def _request(self, method: str, path: str, *, params=None, payload=None, request_id=None):
        headers = {"Authorization": f"Bearer {self._token}"}
        if request_id:
            headers["X-Request-Id"] = request_id
        try:
            response = self._session.request(
                method, API_BASE + path, headers=headers, params=params, json=payload,
                timeout=self._timeout, allow_redirects=False,
            )
        except requests.RequestException:
            raise TodoistError("Todoist request failed; no automatic retry or fallback.") from None
        if response.status_code not in (200, 201, 204):
            raise TodoistHTTPError(response.status_code)
        if response.status_code == 204:
            return None
        try:
            return response.json()
        except ValueError:
            raise TodoistError("Invalid Todoist JSON response.") from None

    def account(self) -> dict:
        user = self._request("GET", "/user")
        if not isinstance(user, dict) or not isinstance(user.get("id"), (str, int)):
            raise TodoistError("Todoist account identity is missing.")
        account_id = _id(str(user["id"]))
        if self._expected_user_id is not None and account_id != self._expected_user_id:
            raise TodoistError("Todoist account does not match expected_user_id.")
        return {"id": account_id}

    def list_tasks(self, *, label: str | None = None, project_id: str | None = None) -> list[dict]:
        self.account()
        params = {}
        if label is not None:
            params["label"] = label
        if project_id is not None:
            params["project_id"] = _id(project_id)
        return self._list_tasks("/tasks", "results", params)

    def list_completed_tasks(self, *, since: datetime, until: datetime,
                             project_id: str | None = None) -> list[dict]:
        """Read completed-task history by completion time; never writes.

        Membership in this history with ``completed_at`` inside ``[since, until)``
        is the completion evidence. The active-task endpoints cannot return
        completed tasks, so a missing active task alone proves nothing.
        """
        for value in (since, until):
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TodoistError("since/until must be timezone-aware datetimes.")
        since_utc, until_utc = since.astimezone(timezone.utc), until.astimezone(timezone.utc)
        if not timedelta(0) < until_utc - since_utc <= MAX_COMPLETED_WINDOW:
            raise TodoistError("Completed-task window must be positive and at most 89 days.")
        self.account()
        params = {"since": since_utc.isoformat(), "until": until_utc.isoformat()}
        if project_id is not None:
            params["project_id"] = _id(project_id)
        tasks = self._list_tasks("/tasks/completed/by_completion_date", "items", params)
        for task in tasks:
            try:
                completed_at = _timestamp(task["completed_at"])
            except (KeyError, TypeError, ValueError, AttributeError):
                raise TodoistError("Completed task has no valid completed_at.") from None
            if not since_utc <= completed_at < until_utc:
                raise TodoistError("Completed task lies outside the requested window.")
        return tasks

    def _list_tasks(self, path: str, key: str, params: dict) -> list[dict]:
        tasks, seen = [], set()
        while True:
            response = self._request("GET", path, params=params.copy())
            if not isinstance(response, dict) or not isinstance(response.get(key), list):
                raise TodoistError("Invalid Todoist task listing.")
            for task in response[key]:
                self._validate_task(task)
                tasks.append(task)
            cursor = response.get("next_cursor")
            if not cursor:
                return tasks
            if not isinstance(cursor, str) or cursor in seen:
                raise TodoistError("Invalid/repeated Todoist cursor; incomplete listing.")
            seen.add(cursor)
            params["cursor"] = cursor

    @staticmethod
    def _validate_task(task, task_id=None):
        if (not isinstance(task, dict) or not isinstance(task.get("id"), str)
                or not isinstance(task.get("content"), str)
                or not isinstance(task.get("checked"), bool) or task.get("is_deleted") is not False):
            raise TodoistError("Todoist task ID/content/completion state is missing.")
        _id(task["id"])
        if task_id is not None and task["id"] != task_id:
            raise TodoistError("Todoist returned a different task ID.")

    def get_task(self, task_id: str) -> dict:
        """Fetch task state; completed tasks may return either 404 or checked=true.

        Neither response alone is sufficient for receipt completion verification.
        """
        _id(task_id)
        self.account()
        task = self._request("GET", f"/tasks/{task_id}")
        self._validate_task(task, task_id)
        return task

    def create_task(self, *, content: str, description: str = "", priority: int | None = None,
                    due_string: str | None = None, labels: list[str] | None = None,
                    project_id: str | None = None, parent_id: str | None = None,
                    apply: bool = False, receipt_path: str | Path | None = None) -> dict:
        if not isinstance(content, str) or not content.strip() or not isinstance(description, str):
            raise TodoistError("A nonempty content string and a description string are required.")
        if priority is not None and (type(priority) is not int or priority not in (1, 2, 3, 4)):
            raise TodoistError("priority must be a native API integer from 1 to 4.")
        if labels is not None and (not isinstance(labels, list) or any(
            not isinstance(label, str) or not label.strip() for label in labels
        )):
            raise TodoistError("labels must be a list of nonempty strings.")
        payload = {"content": content, "description": description,
                   "labels": list(dict.fromkeys(labels or []))}
        if priority is not None:
            payload["priority"] = priority
        if due_string is not None:
            if not isinstance(due_string, str) or not due_string.strip():
                raise TodoistError("due_string must be nonempty when specified.")
            payload["due_string"] = due_string
        for key, value in (("project_id", project_id), ("parent_id", parent_id)):
            if value is not None:
                payload[key] = _id(value)
        return self._write("create", payload, apply=apply, receipt_path=receipt_path)

    def complete_task(self, task_id: str, *, expected_fingerprint: str | None = None,
                      apply: bool = False, receipt_path: str | Path | None = None) -> dict:
        """Complete an explicit nonrecurring task; recurring completion is unsupported."""
        payload = {"task_id": _id(task_id), "expected_fingerprint": expected_fingerprint}
        return self._write("complete", payload, apply=apply, receipt_path=receipt_path)

    def _write(self, action, payload, *, apply, receipt_path):
        account_id = self.account()["id"]
        intent = _digest({"account_id": account_id, "action": action, "payload": payload})
        path = Path(receipt_path).expanduser() if receipt_path is not None else None
        if path is not None and path.exists():
            receipt = _load_receipt(path)
            if receipt["intent_digest"] != intent or receipt["account_id"] != account_id:
                raise TodoistError("Receipt belongs to a different operation; inspect it, do not overwrite.")
            return self.verify_receipt(path)
        before = None
        if action == "complete":
            before = self.get_task(payload["task_id"])
            if before["checked"]:
                raise TodoistError("Task is already completed; verify its existing receipt instead. No write attempted.")
            _id(before.get("project_id"))
            if (before.get("due") or {}).get("is_recurring"):
                raise TodoistError("Recurring tasks are not supported for completion; inspect the occurrence manually.")
            if payload["expected_fingerprint"] is not None and task_fingerprint(before) != payload["expected_fingerprint"]:
                raise TodoistError("Task changed since review; get and review it again.")
            if any(
                task.get("parent_id") == before["id"]
                for task in self.list_tasks(project_id=before.get("project_id"))
            ):
                raise TodoistError("Task has active subtasks; complete them explicitly before the parent.")
        receipt = {"schema_version": SCHEMA_VERSION, "action": action, "account_id": account_id,
                   "intent_digest": intent, "request_id": str(uuid4()), "payload": payload,
                   "before": before, "write_status": "not_attempted", "task_id": payload.get("task_id"),
                   "expected": None, "write_started_at": None, "write_finished_at": None}
        if action == "complete":
            started = datetime.now(timezone.utc)
            receipt["completion_window"] = {
                "since": (started - timedelta(minutes=5)).isoformat(),
                "until": (started + timedelta(minutes=5)).isoformat(),
                "project_id": before["project_id"],
            }
        if not apply:
            return {**self._result(receipt), "preview": payload,
                    "expected_fingerprint": task_fingerprint(before) if before else None,
                    "next_step": "Review preview; use apply=True and a durable private receipt_path to write."}
        if path is None:
            raise TodoistError("apply requires a durable receipt_path; never delete an unknown receipt to retry.")
        # Claim before POST. A crash or ambiguous response leaves a durable stop marker.
        receipt["write_status"] = "unknown"
        receipt["write_started_at"] = datetime.now(timezone.utc).isoformat()
        _save(path, receipt, exclusive=True)
        try:
            response = self._request(
                "POST", "/tasks" if action == "create" else f"/tasks/{payload['task_id']}/close",
                payload=payload if action == "create" else None, request_id=receipt["request_id"],
            )
            if action == "create":
                self._validate_task(response)
                receipt.update(task_id=response["id"], expected=response)
            else:
                receipt["expected"] = {**before, "checked": True}
                receipt["completion_window"]["until"] = (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat()
            receipt["write_status"] = "acknowledged"
            receipt["write_finished_at"] = datetime.now(timezone.utc).isoformat()
        except TodoistHTTPError as exc:
            if exc.status in {400, 401, 403, 404, 429}:
                receipt.update(write_status="rejected", rejection_http_status=exc.status,
                               write_finished_at=datetime.now(timezone.utc).isoformat())
                _save(path, receipt)
            return self._result(receipt)
        except Exception:
            # No exception text: HTTP bodies and transport errors can contain secrets.
            return self._result(receipt)
        _save(path, receipt)
        try:
            return self.verify_receipt(path)
        except TodoistError:
            return self._result(receipt, mismatches=["readback_failed"])

    @staticmethod
    def _result(receipt, *, verified=False, mismatches=None):
        status = receipt["write_status"]
        return {"schema_version": SCHEMA_VERSION, "action": receipt["action"],
                "account_id": receipt["account_id"], "request_id": receipt["request_id"],
                "task_id": receipt["task_id"], "task_url": (
                    f"https://app.todoist.com/app/task/{receipt['task_id']}" if receipt["task_id"] else ""),
                "ok": verified or status == "not_attempted", "verified": verified,
                "write_status": status, "mismatches": mismatches or [],
                "rejection_http_status": receipt.get("rejection_http_status"),
                "next_step": "No further write needed." if verified else (
                    "Write rejected. Correct input/authorization or wait for rate limits; review and use a new receipt to retry. Keep this receipt."
                    if status == "rejected" else
                    "Inspect Todoist and this receipt; do not automatically repeat the write.")}

    def _verify_completion(self, receipt: dict) -> dict:
        # History records survive reopening; absence from active tasks is also required.
        try:
            current = self.get_task(receipt["task_id"])
        except TodoistHTTPError as exc:
            if exc.status != 404:
                raise
        else:
            if not current["checked"]:
                return self._result(receipt, mismatches=["task_still_active"])
        window = receipt["completion_window"]
        tasks = self._list_tasks("/tasks/completed/by_completion_date", "items", window.copy())
        matches = [task for task in tasks if task["id"] == receipt["task_id"]]
        if len(matches) != 1:
            return self._result(receipt, mismatches=["completion_record_missing_or_ambiguous"])
        observed, expected = matches[0], receipt["expected"]
        keys = ("content", "description", "project_id", "parent_id", "labels", "priority", "due")
        mismatches = [key for key in keys if observed.get(key) != expected.get(key)]
        try:
            completed_at = _timestamp(observed["completed_at"])
            if not _timestamp(window["since"]) <= completed_at < _timestamp(window["until"]):
                mismatches.append("completed_at_outside_window")
        except (KeyError, ValueError, TypeError, AttributeError):
            mismatches.append("completed_at_invalid")
        return self._result(receipt, verified=not mismatches, mismatches=mismatches)

    def verify_receipt(self, receipt_path: str | Path) -> dict:
        """Read back known IDs only; a 404 is never proof of completion. No POSTs."""
        receipt = _load_receipt(Path(receipt_path).expanduser())
        if self.account()["id"] != receipt["account_id"]:
            raise TodoistError("Receipt belongs to a different Todoist account.")
        if receipt.get("write_status") != "acknowledged" or not receipt.get("expected"):
            return self._result(receipt)
        try:
            if receipt["action"] == "complete":
                return self._verify_completion(receipt)
            observed = self.get_task(receipt["task_id"])
        except TodoistError:
            return self._result(receipt, mismatches=["readback_failed"])
        expected = receipt["expected"]
        keys = ("content", "description", "project_id", "parent_id", "labels", "priority", "due", "checked")
        mismatches = [key for key in keys if observed.get(key) != expected.get(key)]
        if receipt["action"] == "create":
            for key, value in receipt["payload"].items():
                if key == "due_string":
                    if not observed.get("due"):
                        mismatches.append("due")
                elif key == "labels":
                    if set(value) != set(observed.get(key, [])):
                        mismatches.append(key)
                elif observed.get(key) != value:
                    mismatches.append(key)
            if observed["checked"]:
                mismatches.append("checked")
        return self._result(receipt, verified=not mismatches, mismatches=sorted(set(mismatches)))
