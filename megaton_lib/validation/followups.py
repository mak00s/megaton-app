"""Shared helpers for AA follow-up task registration and status tracking."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


JST = timezone(timedelta(hours=9))
DEFAULT_PENDING_COMMENT = "AA verification follow-up tasks. due_at is the expected next AA batch reflection time."


def next_aa_reflection_time(now: datetime) -> datetime:
    """Return the next expected AA batch reflection time in JST."""
    base = now.replace(second=0, microsecond=0)
    if base.minute < 30:
        bucket_close = base.replace(minute=30)
    else:
        bucket_close = base.replace(minute=0) + timedelta(hours=1)
    return bucket_close + timedelta(minutes=30)


def load_pending_verification_store(
    pending_file: str | Path,
    *,
    comment: str = DEFAULT_PENDING_COMMENT,
) -> dict[str, Any]:
    """Load a pending follow-up store, creating a default structure when absent."""
    path = Path(pending_file)
    if not path.exists():
        return {"_comment": comment, "verifications": []}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        data = {"verifications": []}
    data.setdefault("_comment", comment)
    data.setdefault("verifications", [])
    return data


def save_pending_verification_store(
    pending_file: str | Path,
    data: dict[str, Any],
) -> Path:
    """Persist a pending follow-up store with stable formatting."""
    path = Path(pending_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def _normalize_path_value(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    return str(Path(text).expanduser().resolve())


def build_pending_verification_task(
    *,
    task_id: str,
    description: str,
    verification_file: str = "",
    expected: dict[str, Any] | None = None,
    verification_type: str = "",
    aa_verifier: str = "",
    delay_minutes: int | None = None,
    due_at: datetime | None = None,
    now: datetime | None = None,
    status: str = "pending",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized pending follow-up task."""
    created_at = now or datetime.now(JST)
    resolved_due_at = due_at or (
        created_at + timedelta(minutes=delay_minutes)
        if delay_minutes is not None
        else next_aa_reflection_time(created_at)
    )
    task: dict[str, Any] = {
        "id": task_id,
        "description": description,
        "test_time": created_at.isoformat(),
        "due_at": resolved_due_at.isoformat(),
        "status": status,
    }
    if verification_file:
        task["verification_file"] = verification_file
    if expected:
        task["expected"] = expected
    if verification_type:
        task["verification_type"] = verification_type
    if aa_verifier:
        task["aa_verifier"] = aa_verifier
    if extra:
        task.update(extra)
    return task


def append_pending_verification_task(
    pending_file: str | Path,
    task: dict[str, Any],
    *,
    duplicate_keys: tuple[str, ...] = (),
) -> tuple[bool, dict[str, Any] | None]:
    """Append a pending task unless a duplicate already exists."""
    data = load_pending_verification_store(pending_file)
    verifications = data.setdefault("verifications", [])
    if duplicate_keys:
        for existing in verifications:
            if all(existing.get(key) == task.get(key) for key in duplicate_keys):
                return False, existing
    verifications.append(dict(task))
    save_pending_verification_store(pending_file, data)
    return True, None


def register_pending_verification_task(
    pending_file: str | Path,
    *,
    task_id: str,
    description: str,
    verification_file: str = "",
    expected: dict[str, Any] | None = None,
    verification_type: str = "",
    aa_verifier: str = "",
    delay_minutes: int | None = None,
    due_at: datetime | None = None,
    now: datetime | None = None,
    duplicate_keys: tuple[str, ...] = (),
    extra: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any], dict[str, Any] | None]:
    """Build and append a pending task."""
    task = build_pending_verification_task(
        task_id=task_id,
        description=description,
        verification_file=verification_file,
        expected=expected,
        verification_type=verification_type,
        aa_verifier=aa_verifier,
        delay_minutes=delay_minutes,
        due_at=due_at,
        now=now,
        extra=extra,
    )
    added, existing = append_pending_verification_task(
        pending_file,
        task,
        duplicate_keys=duplicate_keys,
    )
    return added, task, existing


def get_pending_verification_tasks(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Return all pending tasks."""
    return [task for task in data.get("verifications", []) if task.get("status") == "pending"]


def get_overdue_verification_tasks(
    data: dict[str, Any],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return pending tasks whose due_at is already in the past."""
    current = now or datetime.now(JST)
    overdue: list[dict[str, Any]] = []
    for task in get_pending_verification_tasks(data):
        due_at = datetime.fromisoformat(str(task["due_at"]))
        if current > due_at:
            task_copy = dict(task)
            task_copy["_overdue_minutes"] = int((current - due_at).total_seconds() / 60)
            overdue.append(task_copy)
    return overdue


def mark_verification_task_completed(
    pending_file: str | Path,
    task_id: str,
    *,
    result: str = "verified",
    notes: str = "",
    now: datetime | None = None,
    extra_updates: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Mark a task as completed and persist the update."""
    data = load_pending_verification_store(pending_file)
    for task in data.get("verifications", []):
        if task.get("id") != task_id:
            continue
        task["status"] = "completed"
        task["completed_at"] = (now or datetime.now(JST)).isoformat()
        task["result"] = result
        if notes:
            task["notes"] = notes
        if extra_updates:
            task.update(extra_updates)
        save_pending_verification_store(pending_file, data)
        return task
    return None


def mark_verification_task_completed_by_file(
    pending_file: str | Path,
    verification_file: str,
    *,
    verification_type: str = "",
    result: str = "verified",
    notes: str = "",
    now: datetime | None = None,
    extra_updates: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Mark the pending task for a verification file as completed."""
    data = load_pending_verification_store(pending_file)
    for task in data.get("verifications", []):
        if task.get("status") != "pending":
            continue
        if _normalize_path_value(task.get("verification_file", "")) != _normalize_path_value(verification_file):
            continue
        if verification_type and str(task.get("verification_type", "")).strip() != verification_type:
            continue
        task["status"] = "completed"
        task["completed_at"] = (now or datetime.now(JST)).isoformat()
        task["result"] = result
        if notes:
            task["notes"] = notes
        if extra_updates:
            task.update(extra_updates)
        save_pending_verification_store(pending_file, data)
        return task
    return None


def format_pending_verification_task(task: dict[str, Any]) -> str:
    """Format a task for CLI display."""
    overdue_min = int(task.get("_overdue_minutes", 0) or 0)
    overdue = f" (⚠️ {overdue_min}分超過)" if overdue_min > 0 else ""
    lines = [
        f"📋 {task['id']}{overdue}",
        f"   {task['description']}",
        f"   期限: {task['due_at']}",
    ]
    for key in ("verification_type", "renkeiid", "transaction_id", "region", "page"):
        value = task.get(key)
        if value:
            lines.append(f"   {key}: {value}")
    expected = task.get("expected") or {}
    if expected:
        expected_text = ", ".join(f"{key}={value}" for key, value in expected.items())
        lines.append(f"   期待値: {expected_text}")
    if task.get("verification_file"):
        lines.append(f"   検証ファイル: {task['verification_file']}")
    if task.get("aa_verifier"):
        lines.append(f"   verifier: {task['aa_verifier']}")
    return "\n".join(lines)


def _resolve_verifier_command(task: dict[str, Any]) -> str:
    verifier = str(task.get("aa_verifier", "")).strip() or "verify_aa_data.py"
    verification_file = str(task.get("verification_file", "")).strip()
    if "/" in verifier:
        base = f"python {verifier}"
    else:
        base = f"python validation/{verifier}"
    return f"{base} {verification_file}".strip()


def _parse_expected_pairs(items: list[str]) -> dict[str, Any]:
    expected: dict[str, Any] = {}
    for item in items:
        key, _, value = item.partition("=")
        key = key.strip()
        if key:
            expected[key] = value.strip()
    return expected


def run_pending_verification_cli(
    *,
    default_pending_file: str | Path = "validation/pending_aa_verifications.json",
) -> int:
    """Run the shared pending follow-up CLI."""
    parser = argparse.ArgumentParser(description="AA follow-up task management")
    parser.add_argument("--file", default=str(default_pending_file), help="Pending task JSON path")
    parser.add_argument("--all", action="store_true", help="Show all pending tasks")
    parser.add_argument("--complete", metavar="TASK_ID", help="Mark a task as completed")
    parser.add_argument("--result", default="verified", help="Completion result label")
    parser.add_argument("--notes", default="", help="Completion notes")
    parser.add_argument("--add", action="store_true", help="Add a pending task")
    parser.add_argument("--id", default="", help="Task id for --add")
    parser.add_argument("--description", default="", help="Task description for --add")
    parser.add_argument("--verification-file", default="", help="Verification JSON path for --add")
    parser.add_argument("--verification-type", default="", help="Verification type for --add")
    parser.add_argument("--aa-verifier", default="", help="Verifier script name for --add")
    parser.add_argument("--renkeiid", default="", help="renkeiid for --add")
    parser.add_argument("--transaction-id", default="", help="transaction id for --add")
    parser.add_argument("--region", default="", help="region for --add")
    parser.add_argument("--page", default="", help="page for --add")
    parser.add_argument("--expected", action="append", default=[], help="Expected key=value pair")
    parser.add_argument("--delay-minutes", type=int, default=None, help="Fixed delay instead of next batch time")
    parser.add_argument("--quiet", "-q", action="store_true", help="Only print when overdue tasks exist")
    args = parser.parse_args()

    pending_file = Path(args.file).expanduser().resolve()

    if args.complete:
        task = mark_verification_task_completed(
            pending_file,
            args.complete,
            result=args.result,
            notes=args.notes,
        )
        if task is None:
            print(f"❌ タスク {args.complete} が見つかりません")
            return 1
        print(f"✅ {args.complete} を完了としてマーク")
        return 0

    if args.add:
        task_id = args.id.strip() or input("タスクID: ").strip()
        description = args.description.strip() or input("説明: ").strip()
        verification_file = args.verification_file.strip() or input("検証ファイルパス: ").strip()
        expected = _parse_expected_pairs(args.expected)
        added, task, existing = register_pending_verification_task(
            pending_file,
            task_id=task_id,
            description=description,
            verification_file=verification_file,
            expected=expected or None,
            verification_type=args.verification_type.strip(),
            aa_verifier=args.aa_verifier.strip(),
            delay_minutes=args.delay_minutes,
            extra={
                key: value
                for key, value in {
                    "renkeiid": args.renkeiid.strip(),
                    "transaction_id": args.transaction_id.strip(),
                    "region": args.region.strip(),
                    "page": args.page.strip(),
                }.items()
                if value
            },
        )
        if not added:
            print(f"ℹ️ 既存タスクあり: {(existing or {}).get('id', task_id)}")
            return 0
        print(f"✅ 検証タスク追加: {task['id']}")
        print(f"   期限: {task['due_at']}")
        return 0

    data = load_pending_verification_store(pending_file)
    tasks = get_pending_verification_tasks(data) if args.all else get_overdue_verification_tasks(data)
    if not tasks:
        if not args.quiet:
            if args.all:
                print("✅ 保留中の検証タスクはありません")
            else:
                print("✅ 期限超過の検証タスクはありません")
        return 0

    heading = "保留中" if args.all else "期限超過"
    print("=" * 60)
    print(f"⚠️  {heading}の AA 検証タスク ({len(tasks)}件)")
    print("=" * 60)
    for task in tasks:
        print()
        print(format_pending_verification_task(task))
    print()
    print("検証コマンド例:")
    for task in tasks:
        if task.get("verification_file"):
            print(f"  {_resolve_verifier_command(task)}")
    return 0


__all__ = [
    "DEFAULT_PENDING_COMMENT",
    "JST",
    "append_pending_verification_task",
    "build_pending_verification_task",
    "format_pending_verification_task",
    "get_overdue_verification_tasks",
    "get_pending_verification_tasks",
    "load_pending_verification_store",
    "mark_verification_task_completed",
    "mark_verification_task_completed_by_file",
    "next_aa_reflection_time",
    "register_pending_verification_task",
    "run_pending_verification_cli",
    "save_pending_verification_store",
]
