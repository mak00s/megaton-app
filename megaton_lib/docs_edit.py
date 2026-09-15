"""CLI for previewed, revision-guarded edits to caller-selected Google Docs."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from .cli_help import build_parser
from .docs_client import DocsClient, DocsEditError, DocsEditPlan


def main(argv=None) -> int:
    parser = build_parser(
        description="Read Google Docs or plan one literal text edit. No writes without apply --apply.",
        examples=[
            "python -m megaton_lib.docs_edit --token TOKEN --expected-email EMAIL get DOCUMENT_ID",
            "python -m megaton_lib.docs_edit --token TOKEN --expected-email EMAIL plan DOCUMENT_ID --tab-id TAB --match OLD --text NEW",
            "python -m megaton_lib.docs_edit --token TOKEN --expected-email EMAIL apply --plan reviewed.json --apply",
        ],
        notes=["JSON output may contain confidential document text; store privately outside Git.",
               "No implicit OAuth flow, service accounts, connector fallback, or write retries."],
    )
    parser.add_argument("--token", required=True, help="Existing Docs user OAuth token file")
    parser.add_argument("--expected-email", required=True, help="Verified OAuth identity to require")
    commands = parser.add_subparsers(dest="command", required=True)
    get = commands.add_parser("get", help="Read structured document including all tabs")
    get.add_argument("document_id")
    plan = commands.add_parser("plan", help="Read and preview one change; never writes")
    plan.add_argument("document_id")
    plan.add_argument("--tab-id", help="Required for multi-tab documents")
    plan.add_argument("--operation", choices=["replace", "insert_after"], default="replace")
    plan.add_argument("--match", required=True, help="Unique single-line literal in a plain body paragraph")
    plan.add_argument("--text", required=True, help="Replacement or text to insert after the match")
    for name in ("apply", "verify"):
        sub = commands.add_parser(name, help="Apply a reviewed plan" if name == "apply" else "Read back and verify a plan")
        sub.add_argument("--plan", required=True, help="JSON output saved from plan")
        if name == "apply":
            sub.add_argument("--apply", action="store_true", help="Explicitly authorize the saved change")
    args = parser.parse_args(argv)
    try:
        saved = None
        if args.command in {"apply", "verify"}:
            payload = json.loads(Path(args.plan).read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("schema_version") != 1 or payload.get("mode") != "dry_run":
                raise DocsEditError("Expected schema_version=1 dry_run JSON from the plan command.")
            try:
                saved = DocsEditPlan(**payload["plan"])
            except (KeyError, TypeError) as exc:
                raise DocsEditError("Invalid plan fields; use JSON from the plan command.") from exc
        if args.command == "apply" and not args.apply:
            result = saved.preview()
        else:
            print(f"Docs {args.command}: checking OAuth identity and document access...", file=sys.stderr)
            client = DocsClient.from_oauth_file(args.token, expected_email=args.expected_email)
            if args.command == "get":
                result = {"schema_version": 1, "document": client.get(args.document_id)}
            elif args.command == "plan":
                result = client.plan(args.document_id, match=args.match, text=args.text,
                                     operation=args.operation, tab_id=args.tab_id).preview()
            elif args.command == "verify":
                result = {"schema_version": 1, "document_id": saved.document_id, **client.verify(saved)}
            else:
                result = client.apply(saved, apply=True)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get("ok", result.get("verified", True)) else 1
    except DocsEditError as exc:
        print(json.dumps({"schema_version": 1, "ok": False, "error": "validation_failed",
                          "message": str(exc)}, ensure_ascii=False))
        return 1
    except (FileNotFoundError, json.JSONDecodeError):
        print(json.dumps({"schema_version": 1, "ok": False, "error": "invalid_input_file",
                          "next_step": "Check the token/plan file exists and contains valid JSON."}))
        return 1
    except Exception as exc:
        # API exception strings can contain document text or credential details.
        status = getattr(getattr(exc, "resp", None), "status", None)
        print(json.dumps({"schema_version": 1, "ok": False, "error": "preflight_or_read_failed",
                          "http_status": status if isinstance(status, int) else None,
                          "next_step": "Check OAuth scopes, Docs API enablement, account and document access; no write was confirmed."}))
        print("Docs preflight/read failed; no automatic retry or fallback performed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
