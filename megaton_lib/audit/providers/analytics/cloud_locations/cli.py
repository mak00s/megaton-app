"""CLI for Adobe Analytics Cloud Accounts and Locations helpers."""

from __future__ import annotations

import argparse
import json

from megaton_lib.cli_help import build_parser

from .manager import ensure_gcp_dw_location
from .runtime import build_cloud_locations_client


def parse_args() -> argparse.Namespace:
    parser = build_parser(
        description="Inspect and ensure Adobe Analytics Cloud Accounts and Locations.",
        examples=[
            (
                "python -m megaton_lib.audit.providers.analytics.cloud_locations.cli "
                "--company-id omronc0 --list-accounts --type gcp"
            ),
            (
                "python -m megaton_lib.audit.providers.analytics.cloud_locations.cli "
                "--company-id omronc0 --list-locations --application DATA_WAREHOUSE"
            ),
            (
                "python -m megaton_lib.audit.providers.analytics.cloud_locations.cli "
                "--company-id omronc0 --ensure-gcp-dw-location "
                "--account-name 'Shimizu GCS' --gcp-project-id ajuma-8 "
                "--location-name 'DMS AA DW' --bucket dms-aa --prefix cx-v2/dw/ --apply"
            ),
        ],
        notes=[
            "The ensure command is dry-run by default; pass --apply to actually create Adobe resources.",
            "Even with --apply it only prints (never runs) the GCS IAM command.",
            "Run the printed IAM command separately when you want to grant bucket access.",
        ],
    )
    parser.add_argument("--company-id", default="", metavar="COMPANY_ID", help="Adobe Analytics company ID")
    parser.add_argument("--creds-file", default="", metavar="CREDS.json", help="Path to Adobe OAuth credentials JSON")
    parser.add_argument("--client-id", default="", metavar="CLIENT_ID", help="Adobe client ID override")
    parser.add_argument("--client-secret", default="", metavar="CLIENT_SECRET", help="Adobe client secret override")
    parser.add_argument("--org-id", default="", metavar="ORG_ID", help="Adobe org ID override")
    parser.add_argument("--scopes", default="", metavar="SCOPES", help="Adobe OAuth scopes override")
    parser.add_argument("--token-cache", default="", metavar="TOKEN.json", help="Path to token cache file")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--list-accounts", action="store_true", help="List Cloud Accounts")
    mode.add_argument("--list-locations", action="store_true", help="List Cloud Locations")
    mode.add_argument("--ensure-gcp-dw-location", action="store_true", help="Ensure a GCP account and DW location")

    parser.add_argument("--type", default="", metavar="TYPE", help="Cloud Account type filter, e.g. gcp")
    parser.add_argument("--account-uuid", default="", metavar="UUID", help="Cloud Account UUID filter")
    parser.add_argument("--application", default="DATA_WAREHOUSE", metavar="APP", help="Location application")
    parser.add_argument("--limit", type=int, default=100, metavar="N", help="List result limit")
    parser.add_argument("--page", type=int, default=0, metavar="N", help="List page number")

    parser.add_argument("--account-name", default="", metavar="NAME", help="Cloud Account name")
    parser.add_argument("--account-description", default="", metavar="TEXT", help="Cloud Account description")
    parser.add_argument("--gcp-project-id", default="", metavar="PROJECT_ID", help="GCP project ID for account")
    parser.add_argument("--location-name", default="", metavar="NAME", help="Cloud Location name")
    parser.add_argument("--location-description", default="", metavar="TEXT", help="Cloud Location description")
    parser.add_argument("--bucket", default="", metavar="BUCKET", help="GCS bucket name")
    parser.add_argument("--prefix", default="", metavar="PREFIX", help="GCS prefix")
    parser.add_argument("--application-tag", default="", metavar="TAG", help="Location application tag")
    parser.add_argument("--shared-to", default="", metavar="EMAIL", help="Adobe user to share resource with")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the create for --ensure-gcp-dw-location (default is dry-run)",
    )
    return parser.parse_args()


def _build_client_from_namespace(args: argparse.Namespace):
    if not str(args.company_id).strip():
        raise ValueError("--company-id is required")
    return build_cloud_locations_client(
        company_id=args.company_id,
        creds_file=args.creds_file,
        client_id=args.client_id,
        client_secret=args.client_secret,
        org_id=args.org_id,
        scopes=args.scopes,
        token_cache_file=args.token_cache,
    )


def _run_list_accounts(args: argparse.Namespace) -> int:
    client = _build_client_from_namespace(args)
    result = client.list_accounts(
        account_type=args.type or None,
        limit=args.limit,
        page=args.page,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_list_locations(args: argparse.Namespace) -> int:
    client = _build_client_from_namespace(args)
    result = client.list_locations(
        account_uuid=args.account_uuid or None,
        application=args.application or None,
        limit=args.limit,
        page=args.page,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _run_ensure_gcp_dw_location(args: argparse.Namespace) -> int:
    missing = [
        name
        for name, value in [
            ("--account-name", args.account_name),
            ("--gcp-project-id", args.gcp_project_id),
            ("--location-name", args.location_name),
            ("--bucket", args.bucket),
            ("--prefix", args.prefix),
        ]
        if not str(value).strip()
    ]
    if missing:
        raise ValueError(f"Missing required arguments: {', '.join(missing)}")

    client = _build_client_from_namespace(args)
    result = ensure_gcp_dw_location(
        client,
        account_name=args.account_name,
        project_id=args.gcp_project_id,
        location_name=args.location_name,
        bucket=args.bucket,
        prefix=args.prefix,
        account_description=args.account_description,
        location_description=args.location_description,
        application=args.application or "DATA_WAREHOUSE",
        application_tag=args.application_tag,
        shared_to=args.shared_to,
        dry_run=not args.apply,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main() -> None:
    args = parse_args()
    if args.list_accounts:
        raise SystemExit(_run_list_accounts(args))
    if args.list_locations:
        raise SystemExit(_run_list_locations(args))
    if args.ensure_gcp_dw_location:
        raise SystemExit(_run_ensure_gcp_dw_location(args))
    raise SystemExit("Unsupported mode")


if __name__ == "__main__":
    main()
