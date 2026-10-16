"""JSON CLI for caller-selected task handoffs, not general project management."""

from __future__ import annotations

import json
from pathlib import Path

from .cli_help import build_parser
from .todoist_client import SCHEMA_VERSION, TodoistClient, TodoistError, task_fingerprint


def main(argv=None) -> int:
    parser = build_parser(
        description="Todoist task handoff: list/get/create/complete/verify. Writes preview by default.",
        examples=[
            "TODOIST_TOKEN=... python -m megaton_lib.todoist_tasks list --label my-work",
            "python -m megaton_lib.todoist_tasks create --content 'Review report'",
            "python -m megaton_lib.todoist_tasks create --content 'Review report' --apply --receipt output/task.json",
            "python -m megaton_lib.todoist_tasks get --task-id TASK_ID",
            "python -m megaton_lib.todoist_tasks complete --task-id TASK_ID --expected-fingerprint HASH --apply --receipt output/complete.json",
            "python -m megaton_lib.todoist_tasks verify --receipt output/task.json",
        ],
        notes=["Set TODOIST_TOKEN explicitly. No credential discovery or browser/connector fallback.",
               "Keep receipts private and durable. Never delete an unknown receipt to retry.",
               "Recurring task completion, deletion, updates and automatic workflow hooks are unsupported."],
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("list", "get", "create", "complete", "verify"):
        sub = commands.add_parser(name)
        sub.add_argument("--expected-user-id", help="Optional Todoist account identity guard.")
        if name in {"get", "complete"}:
            sub.add_argument("--task-id", required=True, help="Explicit Todoist task ID, not a title or URL.")
        if name in {"create", "complete", "verify"}:
            sub.add_argument("--receipt", type=Path, required=name == "verify",
                             help="Private durable receipt; required when writing with --apply.")
        if name in {"create", "complete"}:
            sub.add_argument("--apply", action="store_true", help="Perform one write after review; default is preview.")
        if name in {"create", "list"}:
            sub.add_argument("--label", action="append" if name == "create" else "store")
            sub.add_argument("--project-id")
        if name == "create":
            sub.add_argument("--content", required=True)
            sub.add_argument("--description", default="")
            sub.add_argument("--priority", type=int, choices=(1, 2, 3, 4),
                             help="Native API value, passed unchanged; omitted uses Todoist's default.")
            sub.add_argument("--due", help="Natural-language due date, interpreted by Todoist in account context.")
            sub.add_argument("--parent-id")
        if name == "complete":
            sub.add_argument("--expected-fingerprint", help="From get/preview; rejects intervening task edits.")
    args = parser.parse_args(argv)
    try:
        if args.command in {"create", "complete"} and args.apply and args.receipt is None:
            raise TodoistError("--apply requires --receipt.")
        client = TodoistClient.from_env(expected_user_id=args.expected_user_id)
        if args.command == "list":
            result = {"ok": True, "tasks": client.list_tasks(label=args.label, project_id=args.project_id)}
        elif args.command == "get":
            task = client.get_task(args.task_id)
            result = {"ok": True, "task": task, "fingerprint": task_fingerprint(task)}
        elif args.command == "create":
            result = client.create_task(content=args.content, description=args.description,
                                        priority=args.priority, due_string=args.due, labels=args.label,
                                        project_id=args.project_id, parent_id=args.parent_id,
                                        apply=args.apply, receipt_path=args.receipt)
        elif args.command == "complete":
            result = client.complete_task(args.task_id, expected_fingerprint=args.expected_fingerprint,
                                          apply=args.apply, receipt_path=args.receipt)
        else:
            result = client.verify_receipt(args.receipt)
        print(json.dumps({"schema_version": SCHEMA_VERSION, "command": args.command, **result}, ensure_ascii=False))
        return 0 if result["ok"] else 1
    except Exception as exc:
        message = str(exc) if isinstance(exc, TodoistError) else "Todoist operation failed; inspect local input/receipt. No automatic retry."
        print(json.dumps({"schema_version": SCHEMA_VERSION, "command": args.command, "ok": False,
                          "verified": False, "error": message, "next_step": "Inspect before retrying."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
