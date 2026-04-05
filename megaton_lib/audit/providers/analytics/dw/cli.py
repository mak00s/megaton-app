"""CLI for Adobe Analytics Data Warehouse scheduling helpers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .runtime import build_dw_client
from .scheduler import (
    bulk_create_requests_from_template,
    collect_scheduled_requests,
    find_template_requests,
    resolve_template_request,
    summarize_template_detail,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Adobe Analytics Data Warehouse scheduled request helper.",
    )
    parser.add_argument("--company-id", default="", help="Adobe Analytics company ID")
    parser.add_argument("--creds-file", default="", help="Path to Adobe OAuth credentials JSON")
    parser.add_argument("--client-id", default="", help="Adobe client ID override")
    parser.add_argument("--client-secret", default="", help="Adobe client secret override")
    parser.add_argument("--org-id", default="", help="Adobe org ID override")
    parser.add_argument("--scopes", default="", help="Adobe OAuth scopes override")
    parser.add_argument("--token-cache", default="", help="Path to token cache file")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--manifest", type=Path, help="Manifest JSON path")
    mode.add_argument("--list", action="store_true", help="List scheduled requests")
    mode.add_argument("--status", action="store_true", help="Get one scheduled request by UUID")
    mode.add_argument("--find-template", action="store_true", help="Find candidate templates")
    mode.add_argument(
        "--describe-template",
        action="store_true",
        help="Resolve one template and print a compact summary",
    )

    parser.add_argument("--create", action="store_true", help="Create requests from manifest")
    parser.add_argument("--dry-run", action="store_true", help="Preview manifest without creating")

    parser.add_argument("--scheduled-request-uuid", default="", help="Scheduled request UUID")
    parser.add_argument("--rsid", default="", help="Report suite ID")
    parser.add_argument("--name-contains", default="", help="Template name substring")
    parser.add_argument("--output-file-basename", default="", help="Output file basename filter")
    parser.add_argument("--segment-id", default="", help="Segment ID filter")
    parser.add_argument("--owner-login", default="", help="Owner login filter")
    parser.add_argument("--updated-after", default="", help="Updated-after ISO datetime filter")
    parser.add_argument("--updated-before", default="", help="Updated-before ISO datetime filter")
    parser.add_argument("--created-after", default="", help="Created-after ISO datetime filter")
    parser.add_argument("--created-before", default="", help="Created-before ISO datetime filter")
    parser.add_argument("--status-filter", action="append", default=[], help="Status filter. Repeatable.")
    parser.add_argument("--limit", type=int, default=100, help="Result limit")
    return parser.parse_args()


def _load_manifest(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Manifest must be a JSON object: {path}")
    return raw


def _build_client_from_namespace(args: argparse.Namespace):
    if not str(args.company_id).strip():
        raise ValueError("--company-id is required")
    return build_dw_client(
        company_id=args.company_id,
        creds_file=args.creds_file,
        client_id=args.client_id,
        client_secret=args.client_secret,
        org_id=args.org_id,
        scopes=args.scopes,
        token_cache_file=args.token_cache,
    )


def _manifest_template_config(payload: dict[str, Any]) -> dict[str, Any]:
    template = payload.get("template")
    if isinstance(template, dict):
        return dict(template)
    return {
        "scheduled_request_uuid": payload.get("template_scheduled_request_uuid", ""),
        "rsid": payload.get("rsid", ""),
    }


def _normalize_status_list(raw: Any) -> list[str]:
    if isinstance(raw, str):
        value = raw.strip()
        return [value] if value else []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _run_manifest(args: argparse.Namespace) -> int:
    manifest = _load_manifest(args.manifest)
    company_id = str(manifest.get("company_id") or args.company_id).strip()
    if not company_id:
        raise ValueError("Manifest must contain company_id or pass --company-id")

    client = build_dw_client(
        company_id=company_id,
        creds_file=str(manifest.get("creds_file") or args.creds_file),
        client_id=str(manifest.get("client_id") or args.client_id),
        client_secret=str(manifest.get("client_secret") or args.client_secret),
        org_id=str(manifest.get("org_id") or args.org_id),
        scopes=str(manifest.get("scopes") or args.scopes),
        token_cache_file=str(manifest.get("token_cache_file") or args.token_cache),
    )

    template_cfg = _manifest_template_config(manifest)
    rsid = str(template_cfg.get("rsid") or manifest.get("rsid") or "").strip()
    if not rsid:
        raise ValueError("Manifest template config must include rsid")

    template_detail = resolve_template_request(
        client,
        rsid=rsid,
        scheduled_request_uuid=str(template_cfg.get("scheduled_request_uuid", "")),
        name_contains=str(template_cfg.get("name_contains", "")),
        updated_after=str(template_cfg.get("updated_after", "")),
        updated_before=str(template_cfg.get("updated_before", "")),
        created_after=str(template_cfg.get("created_after", "")),
        created_before=str(template_cfg.get("created_before", "")),
        output_file_basename=str(template_cfg.get("output_file_basename", "")),
        segment_id=str(template_cfg.get("segment_id", "")),
        owner_login=str(template_cfg.get("owner_login", "")),
        status=_normalize_status_list(template_cfg.get("status")),
        require_unique=bool(template_cfg.get("require_unique", True)),
        limit=int(template_cfg.get("limit", args.limit)),
    )

    requests = manifest.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("Manifest must include a non-empty requests array")

    dry_run = args.dry_run or not args.create
    result = bulk_create_requests_from_template(
        client,
        template_detail=template_detail,
        requests=requests,
        dry_run=dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_list(args: argparse.Namespace) -> int:
    if not args.rsid.strip():
        raise ValueError("--rsid is required with --list")
    client = _build_client_from_namespace(args)
    status_filter = args.status_filter[0] if len(args.status_filter) == 1 else None
    result = collect_scheduled_requests(
        client,
        rsid=args.rsid,
        created_after=args.created_after or None,
        created_before=args.created_before or None,
        updated_after=args.updated_after or None,
        updated_before=args.updated_before or None,
        status=status_filter,
        limit=args.limit,
    )
    result["totalReturned"] = min(len(result.get("scheduledRequests", [])), args.limit)
    result["scheduledRequests"] = (result.get("scheduledRequests", []) or [])[: args.limit]
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_status(args: argparse.Namespace) -> int:
    if not args.scheduled_request_uuid.strip():
        raise ValueError("--scheduled-request-uuid is required with --status")
    client = _build_client_from_namespace(args)
    result = client.get_scheduled_request(args.scheduled_request_uuid)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_find_template(args: argparse.Namespace) -> int:
    if not args.rsid.strip():
        raise ValueError("--rsid is required with --find-template")
    client = _build_client_from_namespace(args)
    result = find_template_requests(
        client,
        rsid=args.rsid,
        name_contains=args.name_contains or None,
        updated_after=args.updated_after or None,
        updated_before=args.updated_before or None,
        created_after=args.created_after or None,
        created_before=args.created_before or None,
        status=args.status_filter or None,
        owner_login=args.owner_login or None,
        limit=args.limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_describe_template(args: argparse.Namespace) -> int:
    if not args.scheduled_request_uuid.strip() and not args.rsid.strip():
        raise ValueError(
            "--describe-template requires --scheduled-request-uuid or --rsid"
        )
    client = _build_client_from_namespace(args)
    detail = resolve_template_request(
        client,
        rsid=args.rsid or "",
        scheduled_request_uuid=args.scheduled_request_uuid or None,
        name_contains=args.name_contains or None,
        updated_after=args.updated_after or None,
        updated_before=args.updated_before or None,
        created_after=args.created_after or None,
        created_before=args.created_before or None,
        output_file_basename=args.output_file_basename or None,
        segment_id=args.segment_id or None,
        owner_login=args.owner_login or None,
        status=args.status_filter or None,
        require_unique=True,
        limit=args.limit,
    )
    print(json.dumps(summarize_template_detail(detail), ensure_ascii=False, indent=2))
    return 0


def main() -> None:
    args = parse_args()
    if args.manifest:
        raise SystemExit(_run_manifest(args))
    if args.list:
        raise SystemExit(_run_list(args))
    if args.status:
        raise SystemExit(_run_status(args))
    if args.find_template:
        raise SystemExit(_run_find_template(args))
    if args.describe_template:
        raise SystemExit(_run_describe_template(args))
    raise SystemExit("Unsupported mode")


if __name__ == "__main__":
    main()
