#!/usr/bin/env python
"""Reusable audit CLI for GTM/Adobe Tags + GA4/AA."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

# Add project root to import path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from megaton_lib.audit import AuditRunner, load_project_config


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run reusable audit tasks")
    sub = parser.add_subparsers(dest="command")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", required=True, help="project id or config file path")
    common.add_argument(
        "--config-root",
        default="configs/audit/projects",
        help="project config directory (default: configs/audit/projects)",
    )
    common.add_argument("--output", default=None, help="output directory")
    common.add_argument("--json", action="store_true", help="emit JSON output")

    p_sm = sub.add_parser("site-mapping", parents=[common], help="run site mapping audit")
    p_sm.add_argument("--days", type=int, default=30, help="period length if start/end omitted")
    p_sm.add_argument("--start-date", default=None, help="start date (YYYY-MM-DD)")
    p_sm.add_argument("--end-date", default=None, help="end date (YYYY-MM-DD)")
    p_sm.add_argument("--with-aa", action="store_true", help="include Adobe Analytics in cross-check")

    sub.add_parser("export-tag-config", parents=[common], help="export tag mapping snapshot")

    return parser


def _print_site_mapping_summary(report: dict) -> None:
    print(f"project: {report.get('project_id')}")
    print(f"period:  {report.get('period_start')} -> {report.get('period_end')}")
    print(f"source:  {report.get('tag_source')}")
    print(f"sessions: total={report.get('total_sessions', 0):,} unclassified={report.get('unclassified_sessions', 0):,} ({report.get('unclassified_pct', 0)}%)")
    print(f"tag/ga sites: {report.get('tag_site_count', 0)} / {report.get('ga4_site_count', 0)}")

    if report.get("aa_enabled"):
        print(f"aa sites: {report.get('aa_site_count', 0)}")

    missing = report.get("in_ga4_no_tag") or []
    if missing:
        print("ga4 only:")
        for item in missing[:10]:
            print(f"  - {item}")

    missing_tag = report.get("in_tag_no_ga4") or []
    if missing_tag:
        print("tag only:")
        for item in missing_tag[:10]:
            print(f"  - {item}")

    artifacts = report.get("artifacts")
    if isinstance(artifacts, dict) and artifacts:
        print("artifacts:")
        for key, value in artifacts.items():
            print(f"  - {key}: {value}")


def _format_sync_stats(stats: dict) -> str:
    """Format {added, updated, deleted, unchanged} as compact string."""
    a, u, d, eq = stats.get("added", 0), stats.get("updated", 0), stats.get("deleted", 0), stats.get("unchanged", 0)
    parts = []
    if a:
        parts.append(f"+{a}")
    if u:
        parts.append(f"~{u}")
    if d:
        parts.append(f"-{d}")
    parts.append(f"={eq}")
    return " ".join(parts)


def _print_export_summary(payload: dict) -> None:
    print(f"project: {payload.get('project_id')}")
    print(f"source:  {payload.get('tag_source')}")
    print(f"mapping_count: {len(payload.get('mapping', {}))}")
    container_export = payload.get("container_export")
    if isinstance(container_export, dict):
        resource_parts = []
        for k, v in container_export.items():
            if isinstance(v, dict):
                resource_parts.append(f"{k}: {_format_sync_stats(v)}")
            elif isinstance(v, str) and v != "unchanged":
                resource_parts.append(f"{k}: {v}")
        if resource_parts:
            print(f"container: {', '.join(resource_parts)}")
    if "has_changes" in payload:
        print(f"has_changes: {payload['has_changes']}")
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, dict):
        for key, value in artifacts.items():
            print(f"{key}: {value}")


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    try:
        config = load_project_config(args.project, config_root=args.config_root)
        runner = AuditRunner(config)

        output_dir = Path(args.output) if args.output else None

        if args.command == "site-mapping":
            report = runner.run_site_mapping(
                days=args.days,
                start_date=args.start_date,
                end_date=args.end_date,
                with_aa=bool(args.with_aa),
                output_dir=output_dir,
            )
            if args.json:
                print(json.dumps(report, ensure_ascii=False))
            else:
                _print_site_mapping_summary(report)
            return 0

        if args.command == "export-tag-config":
            if not output_dir:
                raise ValueError("--output is required for export-tag-config")
            payload = runner.export_tag_mapping(output_dir=output_dir)
            if args.json:
                print(json.dumps(payload, ensure_ascii=False))
            else:
                _print_export_summary(payload)
            return 0

        raise ValueError(f"Unknown command: {args.command}")
    except Exception as exc:
        if getattr(args, "json", False):
            print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False))
        else:
            print(f"[error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
