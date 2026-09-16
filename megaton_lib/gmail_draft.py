"""Draft-only Gmail CLI. No connector/browser fallback and no send command."""

from __future__ import annotations

import json
import mimetypes
import os
import sys
from pathlib import Path

from .cli_help import build_parser, MegatonHelpFormatter


def create_parser():
    parser = build_parser(
        description="Prepare, read, update and verify Gmail drafts. Never sends mail.",
        examples=[
            "python -m megaton_lib.gmail_draft reply --message-id ID --reply-all --body-file reply.txt",
            "python -m megaton_lib.gmail_draft get --draft-id ID --body-output draft.txt",
            "python -m megaton_lib.gmail_draft update --draft-id ID --expected-fingerprint HASH --attach report.pdf --apply",
        ],
        notes=["Set GMAIL_DRAFT_TOKEN_PATH (user OAuth) and GMAIL_DRAFT_EXPECTED_EMAIL.",
               "Writes default to preview. --apply writes once and verifies by reading back.",
               "No automatic retry, alternative connector, browser fallback, or sending."],
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("create", "reply", "get", "update", "verify"):
        sub = commands.add_parser(name, formatter_class=MegatonHelpFormatter)
        sub.add_argument("--expected-email", default=os.getenv("GMAIL_DRAFT_EXPECTED_EMAIL", ""),
                         help="Required OAuth mailbox identity; mismatches stop before writing")
        sub.add_argument("--format", choices=("json", "text"), default="json", help="Result output")
        if name in ("get", "update", "verify"):
            sub.add_argument("--draft-id", required=True, help="Gmail draft ID, not its message ID")
        if name == "get":
            sub.add_argument("--body-output", type=Path, help="Explicit sensitive body export; new file only, mode 0600")
        if name == "verify":
            sub.add_argument("--expected-json", type=Path, required=True,
                             help="Saved preview/get/write JSON containing the expected content summary")
        if name in ("create", "reply", "update"):
            sub.add_argument("--apply", action="store_true", help="Create/update on Gmail; never send")
            sub.add_argument("--body-file", type=Path, required=name != "update", help="UTF-8 plain-text body")
            files = sub.add_mutually_exclusive_group()
            files.add_argument("--attach", action="append", type=Path,
                               help="Repeat for each file; update replaces ordinary attachments, preserves inline parts")
            if name == "update":
                files.add_argument("--clear-attachments", action="store_true", help="Remove ordinary attachments; preserve inline parts")
                sub.add_argument("--expected-fingerprint", required=True, help="Fingerprint from get; refuses stale updates")
        if name in ("create", "update"):
            sub.add_argument("--to", action="append", required=name == "create", help="To addresses; repeatable")
            sub.add_argument("--cc", action="append", help="CC addresses; repeatable")
            sub.add_argument("--bcc", action="append", help="Explicit BCC addresses; never inherited from a reply source")
            sub.add_argument("--subject", required=name == "create", help="Subject; threaded drafts cannot change it")
            if name == "update":
                sub.add_argument("--clear-cc", action="store_true", help="Remove CC")
                sub.add_argument("--clear-bcc", action="store_true", help="Remove BCC")
        if name == "reply":
            sub.add_argument("--message-id", required=True, help="Source Gmail message ID, not RFC Message-ID or thread ID")
            recipients = sub.add_mutually_exclusive_group()
            recipients.add_argument("--reply-all", dest="reply_all", action="store_true", default=True,
                                    help="Include original To/CC, excluding self and duplicates (default)")
            recipients.add_argument("--sender-only", dest="reply_all", action="store_false",
                                    help="Reply only to Reply-To (or From); do not inherit original To/CC")
            sub.add_argument("--self-alias", action="append", default=[], help="Additional own address to exclude; repeatable")
    return parser


def _attachments(args):
    if getattr(args, "clear_attachments", False):
        return []
    paths = getattr(args, "attach", None)
    if paths is None:
        return None
    return [(p.name, p.read_bytes(), mimetypes.guess_type(p.name)[0] or "application/octet-stream") for p in paths]


def _run(args):
    # Help works without the optional Google dependencies installed.
    from .gmail_client import GmailClient, GmailDraftContent, SCOPES_DRAFT, SCOPES_REPLY, load_draft_credentials_from_env

    if not args.expected_email:
        raise ValueError("Set GMAIL_DRAFT_EXPECTED_EMAIL or --expected-email before accessing Gmail")
    for field in ("cc", "bcc"):
        if getattr(args, f"clear_{field}", False) and getattr(args, field, None):
            raise ValueError(f"--{field} and --clear-{field} cannot be combined")
    scopes = SCOPES_REPLY if args.command == "reply" else SCOPES_DRAFT
    client = GmailClient(load_draft_credentials_from_env(scopes=scopes))
    account = client.assert_account(args.expected_email)
    base = {"schema_version": "gmail-draft/v1", "ok": True, "exit_code": 0,
            "action": args.command, "applied": False, "verified": False,
            "account": account, "draft_id": None, "message_id": None, "errors": []}
    if args.command in ("get", "verify"):
        draft = client.get_draft(args.draft_id)
        result = {**base, **draft.summary()}
        if args.command == "get" and args.body_output:
            body = draft.content.message.get_body(preferencelist=("plain", "html"))
            if body is None:
                raise ValueError("No exportable text body")
            fd = os.open(args.body_output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as output:
                output.write(body.get_content())
            result["body_output"] = str(args.body_output)
        if args.command == "verify":
            expected = json.loads(args.expected_json.read_text(encoding="utf-8"))
            result.update(client.verify_draft_summary(draft, expected=expected))
        return result
    body = args.body_file.read_text(encoding="utf-8") if args.body_file else None
    attachments = _attachments(args)
    if args.command == "reply":
        content = client.prepare_reply(args.message_id, expected_email=account, body_text=body,
                                       reply_all=args.reply_all, self_aliases=args.self_alias, attachments=attachments)
    elif args.command == "create":
        content = GmailDraftContent.new(sender=account, to=args.to, cc=args.cc, bcc=args.bcc,
                                       subject=args.subject, body_text=body, attachments=attachments)
    else:
        draft = client.get_draft(args.draft_id)
        if draft.fingerprint != args.expected_fingerprint:
            raise ValueError("Draft changed since read; get and review it again")
        content = draft.content.updated(body_text=body, to=args.to,
                                        cc=[] if args.clear_cc else args.cc,
                                        bcc=[] if args.clear_bcc else args.bcc,
                                        subject=args.subject, attachments=attachments)
        base.update(draft_id=draft.id, message_id=draft.message_id, fingerprint=draft.fingerprint)
    if not args.apply:
        return {**base, **content.summary()}
    print("gmail-draft: writing once, then reading back for verification", file=sys.stderr)
    result = client.save_draft(content, expected_email=account,
                               draft_id=args.draft_id if args.command == "update" else None,
                               expected_fingerprint=args.expected_fingerprint if args.command == "update" else None)
    return {**base, **result, "action": args.command}


def main(argv=None) -> int:
    args = create_parser().parse_args(argv)
    try:
        result = _run(args)
    except Exception as exc:
        result = {"schema_version": "gmail-draft/v1", "ok": False, "exit_code": 1,
                  "action": args.command, "applied": False, "verified": False,
                  "draft_id": getattr(args, "draft_id", None),
                  "errors": [type(exc).__name__],
                  "next_action": str(exc) if type(exc) is ValueError else
                  "Check user OAuth token/scopes, input files and network. No automatic fallback or retry."}
        if isinstance(exc, ValueError):
            from .gmail_client import GmailDraftStateError

            if isinstance(exc, GmailDraftStateError):
                result.update(exc.result())
    result = {"account": None, "draft_id": None, "message_id": None, "thread_id": "",
              "fingerprint": None, "sender": [], "to": [], "cc": [], "bcc": [],
              "subject": "", "in_reply_to": "", "references": "",
              "attachments": [], "body_parts": [], **result}
    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"{result['action']}: ok={result['ok']} applied={result['applied']} verified={result['verified']}")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
