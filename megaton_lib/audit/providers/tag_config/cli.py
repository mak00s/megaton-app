"""Reusable CLI entrypoints for Adobe Tags and GTM export/apply workflows.

Analysis repos should call these from thin wrapper scripts, passing their
own ``tags_config_factory`` for repo-specific credential resolution.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Callable

from ...config import AdobeTagsConfig
from .bootstrap import adobe_tags_output_root, build_tags_config


def _default_config_factory(*, property_id: str, page_size: int = 100) -> AdobeTagsConfig:
    """Fallback factory that reads everything from env vars."""
    return build_tags_config(property_id=property_id, page_size=page_size)


def _resolve_property_ids(
    cli_property_id: str,
    default_ids: list[str] | None = None,
) -> list[str]:
    """Resolve property IDs from CLI arg, env var, or defaults."""
    if cli_property_id.strip():
        return [cli_property_id.strip()]
    if default_ids:
        return [pid for pid in default_ids if pid.strip()]
    env_id = os.environ.get("TAGS_PROPERTY_ID", "").strip()
    if env_id:
        return [env_id]
    return []


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def tags_export_main(
    *,
    tags_config_factory: Callable[..., AdobeTagsConfig] | None = None,
    property_ids: list[str] | None = None,
    project_root: str | Path | None = None,
) -> None:
    """Reusable ``export_adobe_tags.py`` entrypoint.

    Parameters
    ----------
    tags_config_factory:
        Callable that accepts ``property_id`` and ``page_size`` keyword args
        and returns an ``AdobeTagsConfig``.  Falls back to env-var based
        resolution when omitted.
    property_ids:
        Pre-resolved list of property IDs.  When omitted, reads from
        ``--property-id`` CLI arg or ``TAGS_PROPERTY_ID`` env var.
    project_root:
        Project root for resolving relative output paths.
    """
    parser = argparse.ArgumentParser(
        description="Export Adobe Tags property config",
        epilog=(
            "Defaults:\n"
            "  - property is resolved from --property-id or TAGS_PROPERTY_ID\n"
            "  - resources default to TAGS_EXPORT_RESOURCES or\n"
            "    rules,data-elements,extensions,environments,libraries\n"
            "  - filters come from TAGS_RULE_NAME_CONTAINS,\n"
            "    TAGS_RULE_ENABLED_ONLY, and TAGS_DE_ENABLED_ONLY\n"
            "  - export also refreshes .apply-baseline.json for later stale-base checks\n"
            "\n"
            "Important:\n"
            "  Filtered exports write a subset snapshot into the canonical output root.\n"
            "  Use full-property export for canonical sync; use filtered export only when\n"
            "  you intentionally want a focused local subset."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--property-id", default="", help="Explicit property id")
    args, _ = parser.parse_known_args()

    factory = tags_config_factory or _default_config_factory
    root = Path(project_root or Path.cwd())
    pids = property_ids or _resolve_property_ids(args.property_id)
    if not pids:
        print("ERROR: no Adobe Tags property IDs resolved. Set TAGS_PROPERTY_ID.", file=sys.stderr)
        sys.exit(1)

    page_size = int(os.environ.get("TAGS_PAGE_SIZE", "100"))

    # Pre-build config for the first property to trigger load_env_file()
    # so that TAGS_EXPORT_RESOURCES, filter env vars, etc. are available.
    first_config = factory(property_id=pids[0], page_size=page_size)

    resources_str = os.environ.get(
        "TAGS_EXPORT_RESOURCES",
        "rules,data-elements,extensions,environments,libraries",
    )
    resources = [r.strip() for r in resources_str.split(",") if r.strip()]

    # Build filters from env vars
    filters: dict[str, Any] = {}
    rule_name_contains = os.environ.get("TAGS_RULE_NAME_CONTAINS", "").strip()
    if rule_name_contains:
        filters["rules"] = {"name_contains": rule_name_contains}
    if os.environ.get("TAGS_RULE_ENABLED_ONLY", "").strip().lower() in ("1", "true", "yes"):
        filters.setdefault("rules", {})["enabled_only"] = True
    if os.environ.get("TAGS_DE_ENABLED_ONLY", "").strip().lower() in ("1", "true", "yes"):
        filters.setdefault("data-elements", {})["enabled_only"] = True

    from .adobe_tags import export_property  # noqa: WPS433

    for i, pid in enumerate(pids):
        tags_config = first_config if i == 0 else factory(property_id=pid, page_size=page_size)
        output = root / adobe_tags_output_root(pid)

        print(f"Syncing Adobe Tags property: {pid}")
        print(f"  resources: {resources}")
        if filters:
            print(f"  filters: {filters}")
        print(f"  output: {output}")
        if filters:
            print("  note: filtered export updates the canonical local snapshot for the selected subset")

        summary = export_property(tags_config, output, resources, filters=filters or None)
        has_changes = False
        for resource, stats in summary.items():
            if isinstance(stats, dict):
                a, u, d, eq = stats.get("added", 0), stats.get("updated", 0), stats.get("deleted", 0), stats.get("unchanged", 0)
                parts = []
                if a:
                    parts.append(f"+{a}")
                if u:
                    parts.append(f"~{u}")
                if d:
                    parts.append(f"-{d}")
                parts.append(f"={eq}")
                print(f"  {resource}: {' '.join(parts)}")
                if a or u or d:
                    has_changes = True
            elif isinstance(stats, str) and stats != "unchanged":
                print(f"  {resource}: {stats}")
                has_changes = True
        baseline_status = summary.get(".apply-baseline.json")
        if baseline_status:
            print(f"  .apply-baseline.json: {baseline_status}")
        print(f"  has_changes: {has_changes}")

    print("Done.")


# ---------------------------------------------------------------------------
# GTM Export
# ---------------------------------------------------------------------------


def gtm_export_main(
    *,
    container_public_id: str | None = None,
    project_root: str | Path | None = None,
) -> None:
    """Reusable GTM container export entrypoint.

    Parameters
    ----------
    container_public_id:
        GTM container public ID (e.g. ``GTM-XXXXX``).  Falls back to
        ``--container-id`` CLI arg or ``GTM_CONTAINER_PUBLIC_ID`` env var.
    project_root:
        Project root for resolving relative output paths.
    """
    parser = argparse.ArgumentParser(description="Export GTM container config")
    parser.add_argument("--container-id", default="", help="GTM container public ID")
    parser.add_argument(
        "--resources", default="",
        help="Comma-separated resource types to export (default: all)",
    )
    parser.add_argument("--output", default="", help="Output directory")
    args, _ = parser.parse_known_args()

    cid = (
        container_public_id
        or args.container_id.strip()
        or os.environ.get("GTM_CONTAINER_PUBLIC_ID", "").strip()
    )
    if not cid:
        print("ERROR: no GTM container ID. Set GTM_CONTAINER_PUBLIC_ID or use --container-id.", file=sys.stderr)
        sys.exit(1)

    from ...audit.config import GtmConfig
    from .gtm import export_container, ALL_RESOURCES

    resources_str = args.resources.strip() or os.environ.get("GTM_EXPORT_RESOURCES", "")
    resources: list[str] | None = None
    if resources_str:
        resources = [r.strip() for r in resources_str.split(",") if r.strip()]

    config = GtmConfig(container_public_id=cid)
    root = Path(project_root or Path.cwd())
    output = Path(args.output) if args.output.strip() else root / "gtm" / cid

    print(f"Exporting GTM container: {cid}")
    print(f"  resources: {resources or list(ALL_RESOURCES)}")
    print(f"  output: {output}")

    summary = export_container(config, output, resources=resources)
    has_changes = False
    for resource, stats in summary.items():
        if isinstance(stats, dict):
            a, u, d, eq = stats.get("added", 0), stats.get("updated", 0), stats.get("deleted", 0), stats.get("unchanged", 0)
            parts = []
            if a:
                parts.append(f"+{a}")
            if u:
                parts.append(f"~{u}")
            if d:
                parts.append(f"-{d}")
            parts.append(f"={eq}")
            print(f"  {resource}: {' '.join(parts)}")
            if a or u or d:
                has_changes = True
        elif isinstance(stats, str) and stats != "unchanged":
            print(f"  {resource}: {stats}")
            has_changes = True
    print(f"  has_changes: {has_changes}")

    print("Done.")


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def tags_apply_main(
    *,
    tags_config_factory: Callable[..., AdobeTagsConfig] | None = None,
    property_ids: list[str] | None = None,
    library_id: str | None = None,
    verify_url: str | None = None,
    project_root: str | Path | None = None,
) -> None:
    """Reusable ``apply_adobe_tags.py`` entrypoint.

    Parameters
    ----------
    tags_config_factory:
        Callable that accepts ``property_id`` and ``page_size`` keyword args
        and returns an ``AdobeTagsConfig``.
    property_ids:
        Pre-resolved list of property IDs.
    library_id:
        Dev library ID.  Falls back to ``TAGS_DEV_LIBRARY_ID`` env var.
    verify_url:
        Dev launch JS URL for CDN verification.  Falls back to
        ``TAGS_DEV_LAUNCH_URL`` env var.
    project_root:
        Project root for resolving relative output paths.
    """
    parser = argparse.ArgumentParser(
        description="Apply Adobe Tags exported changes",
        epilog=(
            "Defaults:\n"
            "  - no --apply means dry-run only\n"
            "  - property is resolved from --property-id or TAGS_PROPERTY_ID\n"
            "  - if TAGS_DEV_LIBRARY_ID is set and --skip-build is not used, the command runs:\n"
            "      apply -> revise library -> build -> optional verify -> re-export\n"
            "  - Step 5 re-export defaults to TAGS_REEXPORT_RESOURCES or rules,data-elements\n"
            "  - TAGS_DEV_LAUNCH_URL enables marker verification when markers are supplied\n"
            "  - .apply-baseline.json is used to detect stale-base conflicts when present\n"
            "\n"
            "Operational guidance:\n"
            "  - Use dry-run first to see which resources would be patched\n"
            "  - Use auto-build when you want the dev library to reflect the PATCHed origins\n"
            "  - Use --skip-build only when you explicitly want apply-only behavior\n"
            "  - Re-export before apply when the same resource may have changed in the UI"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip revise + build + verify even when TAGS_DEV_LIBRARY_ID is set",
    )
    parser.add_argument(
        "--allow-stale-base",
        action="store_true",
        help="Allow apply even when local and remote both changed since the last export baseline",
    )
    parser.add_argument("--property-id", default="", help="Explicit property id")
    # Legacy flags (hidden)
    parser.add_argument("--build", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--refresh", action="store_true", help=argparse.SUPPRESS)
    args, _ = parser.parse_known_args()

    factory = tags_config_factory or _default_config_factory
    root = Path(project_root or Path.cwd())
    pids = property_ids or _resolve_property_ids(args.property_id)
    if not pids:
        print("ERROR: no Adobe Tags property IDs resolved. Set TAGS_PROPERTY_ID.", file=sys.stderr)
        sys.exit(1)

    page_size = int(os.environ.get("TAGS_PAGE_SIZE", "100"))

    # Pre-build config for the first property to trigger load_env_file()
    # so that TAGS_DEV_LIBRARY_ID etc. are available from .env files.
    first_config = factory(property_id=pids[0], page_size=page_size)

    lib_id = (library_id or os.environ.get("TAGS_DEV_LIBRARY_ID", "")).strip()
    v_url = (verify_url or os.environ.get("TAGS_DEV_LAUNCH_URL", "")).strip() or None
    re_export_resources_str = os.environ.get("TAGS_REEXPORT_RESOURCES", "rules,data-elements")
    re_export_resources = [r.strip() for r in re_export_resources_str.split(",") if r.strip()]
    if not re_export_resources:
        re_export_resources = ["rules", "data-elements"]
    should_build = bool(lib_id) and not args.skip_build

    for i, pid in enumerate(pids):
        tags_config = first_config if i == 0 else factory(property_id=pid, page_size=page_size)
        output = root / adobe_tags_output_root(pid)

        # Default: build workflow (apply → revise → build → verify → re-export)
        if should_build:
            from .build_workflow import run_build_workflow  # noqa: WPS433
            from .sync import StaleBaseConflictError  # noqa: WPS433

            mode = "APPLY + BUILD" if args.apply else "DRY-RUN"
            print(f"[{mode}] Adobe Tags exported changes")
            print(f"  property: {pid}")
            print(f"  output_root: {output}")
            print("  workflow: apply -> revise library -> build -> verify -> re-export")
            print(f"  library: {lib_id}")
            print(f"  verify_url: {v_url or '(disabled)'}")
            print(f"  re_export_resources: {re_export_resources}")
            baseline_path = output / ".apply-baseline.json"
            print(
                "  stale_base_guard: "
                + (f"enabled ({baseline_path})" if baseline_path.exists() else "disabled (no baseline manifest)")
            )

            try:
                rc = run_build_workflow(
                    tags_config,
                    root=output,
                    library_id=lib_id,
                    apply=args.apply,
                    verify_asset_url=v_url,
                    re_export_resources=re_export_resources,
                    allow_stale_base=args.allow_stale_base,
                )
            except StaleBaseConflictError as exc:
                print(f"\nERROR: {exc}", file=sys.stderr)
                sys.exit(4)
            if rc != 0:
                sys.exit(rc)
            continue

        # Fallback: apply-only
        from .adobe_tags import export_property  # noqa: WPS433
        from .sync import (  # noqa: WPS433
            StaleBaseConflictError,
            apply_exported_changes_tree,
            raise_for_stale_base_conflicts,
        )

        mode = "APPLY" if args.apply else "DRY-RUN"
        print(f"[{mode}] Applying Adobe Tags exported changes")
        print(f"  property: {pid}")
        print(f"  output_root: {output}")
        print("  workflow: apply-only (no library revise/build)")
        if not lib_id:
            print("  reason: TAGS_DEV_LIBRARY_ID is not set")
        elif args.skip_build:
            print("  reason: --skip-build was specified")
        print(f"  re_export_resources_on_refresh: {re_export_resources}")
        baseline_path = output / ".apply-baseline.json"
        print(
            "  stale_base_guard: "
            + (f"enabled ({baseline_path})" if baseline_path.exists() else "disabled (no baseline manifest)")
        )

        changed_count = 0
        applied_count = 0
        results = apply_exported_changes_tree(tags_config, output, dry_run=not args.apply)
        stale_remote_count = 0
        conflict_count = 0
        for result in results:
            stale_status = result.get("stale_status")
            if stale_status == "conflict":
                conflict_count += 1
                status = "CONFLICT"
            elif stale_status == "remote_only":
                stale_remote_count += 1
                status = "STALE-REMOTE"
            elif result.get("changed"):
                changed_count += 1
                status = "APPLIED" if result.get("applied") else "CHANGED"
                if result.get("applied"):
                    applied_count += 1
            else:
                status = "OK"
            print(f"  [{status}] {result['path']} → {result['component_id']}")
            if stale_status:
                print(f"    {result.get('stale_detail', '')}")

        print(
            f"\n{changed_count} changes detected, {applied_count} applied, "
            f"{stale_remote_count} stale-remote skipped, {conflict_count} conflict(s).",
        )
        try:
            raise_for_stale_base_conflicts(results, allow_stale_base=args.allow_stale_base)
        except StaleBaseConflictError as exc:
            print(f"\nERROR: {exc}", file=sys.stderr)
            sys.exit(4)

        if args.apply and applied_count > 0 and not lib_id:
            print(
                "\nWARNING: Changes applied but NOT built.\n"
                "  Set TAGS_DEV_LIBRARY_ID to enable auto-build.",
                file=sys.stderr,
            )

        # Legacy --refresh
        if args.apply and args.refresh and not should_build:
            print("\nRefreshing export...")
            print(f"  resources: {re_export_resources}")
            export_property(tags_config, output, resources=re_export_resources)
            print("Refresh complete.")
