"""Structured report editing CLI. No body/plan data on stdout; explicit files only."""
from __future__ import annotations

import json
from pathlib import Path

from .cli_help import build_parser
from .docs_client import DocsClient, DocsEditError
from .docs_mutations import DocsMutationPlan, get_normalized_outline
from .docs_mutation_io import create_receipt


def main(argv=None):
    parser = build_parser(description="Plan, apply and verify scoped report edits. No write retries.", notes=[
        "Plans, snapshots and receipts are confidential. Output files are new-only, mode 0600.",
        "Apply changes the original. Keep the approved digest separately; verify never replays writes.",
        "Images need explicit temporary public-access approval; no automatic browser/connector fallback."])
    parser.add_argument("--token", required=True)
    parser.add_argument("--expected-email", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("get", "plan"):
        sub = commands.add_parser(name)
        sub.add_argument("document_id")
        sub.add_argument("--tab-id", help="Required for multi-tab documents")
        sub.add_argument("--output", required=True, type=Path, help="New private snapshot/plan file")
        if name == "plan":
            sub.add_argument("--operations", required=True, type=Path, help="Reviewed operation list JSON")
    for name in ("apply", "verify"):
        sub = commands.add_parser(name)
        sub.add_argument("--plan", required=True, type=Path)
        sub.add_argument("--receipt", type=Path, help="Apply: new journal. Verify: existing journal with object IDs")
        if name == "apply":
            sub.add_argument("--apply", action="store_true")
            sub.add_argument("--approved-digest")
            sub.add_argument("--image-token", type=Path, help="Explicit user OAuth with drive.file + userinfo.email")
            sub.add_argument("--image-folder-id", help="Approved temporary Drive folder")
            sub.add_argument("--allow-public-images", action="store_true")
    args = parser.parse_args(argv)
    try:
        plan = None
        if args.command in {"apply", "verify"}:
            payload = json.loads(args.plan.read_text())
            plan = DocsMutationPlan.from_dict(payload.get("plan", payload))
        if args.command == "apply" and not args.apply:
            result = {k: v for k, v in plan.preview().items() if k != "plan"}
        else:
            client = DocsClient.from_oauth_file(args.token, expected_email=args.expected_email)
            if args.command == "get":
                document = client.get(args.document_id)
                snapshot = {"document": document}
                if args.tab_id:
                    snapshot["outline"] = get_normalized_outline(document, tab_id=args.tab_id)
                create_receipt(args.output, snapshot)
                result = {"ok": True, "write_status": "not_started"}
            elif args.command == "plan":
                plan = client.plan_mutations(args.document_id, tab_id=args.tab_id,
                                             operations=json.loads(args.operations.read_text()))
                create_receipt(args.output, plan.preview())
                result = {"ok": True, "write_status": "not_started", "digest": plan.digest,
                          "summary": plan.preview()["summary"]}
            elif args.command == "verify":
                receipt = json.loads(args.receipt.read_text()) if args.receipt else None
                result = client.verify_mutation_plan(plan, receipt=receipt)
            else:
                stager = None
                if plan.to_dict()["assets"]:
                    if not args.image_token or not args.image_folder_id or not args.allow_public_images:
                        raise DocsEditError("Image apply requires explicit token, folder and public-access approval.")
                    from .docs_assets import DriveImageStager
                    stager = DriveImageStager.from_oauth_file(args.image_token, expected_email=args.expected_email,
                        folder_id=args.image_folder_id, allow_public=True)
                result = client.apply_mutation_plan(plan, apply=True, approved_digest=args.approved_digest,
                                                    receipt_path=args.receipt, image_stager=stager)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": "validation_failed" if isinstance(exc, DocsEditError) else "operation_failed",
                          **({"message": str(exc)} if isinstance(exc, DocsEditError) else {}),
                          "next_step": "Inspect inputs and any receipt before retrying; no automatic replay or fallback."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
