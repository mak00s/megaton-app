"""Reusable Adobe Tags build-and-verify workflow helpers."""

from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from ...config import AdobeTagsConfig
from .adobe_tags import _reactor_get, build_library, export_property, refresh_library_resources
from .sync import (
    StaleBaseConflictError,
    apply_exported_changes_tree,
    raise_for_stale_base_conflicts,
)


BUILD_TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "errored"}
BUILD_SUCCESS_STATUSES = {"succeeded"}


def _log(message: str = "") -> None:
    """Print workflow logs immediately even when stdout is piped."""
    print(message, flush=True)


def collect_changed_resources(
    config: AdobeTagsConfig,
    root: str | Path,
    *,
    dry_run: bool,
    allow_stale_base: bool = False,
) -> list[dict]:
    """Apply exported sidecars and return changed rule/data-element origins."""
    changed: list[dict] = []
    changed_count = 0
    applied_count = 0
    root_path = Path(root)

    all_results = apply_exported_changes_tree(config, root_path, dry_run=dry_run)
    stale_remote_count = 0
    conflict_count = 0

    for result in all_results:
        path = result["path"]
        component_id = result["component_id"]

        stale_status = result.get("stale_status")
        if stale_status == "conflict":
            status = "CONFLICT"
            conflict_count += 1
        elif stale_status == "remote_only":
            status = "STALE-REMOTE"
            stale_remote_count += 1
        elif result.get("changed"):
            changed_count += 1
            if result.get("applied"):
                applied_count += 1
                status = "APPLIED"
            else:
                status = "CHANGED"

            if "rules/" in path:
                for part in Path(path).parts:
                    if part.startswith("rl"):
                        # Restore Reactor ID casing: "RL" prefix + lowercase hex
                        raw_id = part.split("_")[0]
                        origin_id = "RL" + raw_id[2:]
                        changed.append({"id": origin_id, "type": "rules"})
                        break
            elif "data-elements/" in path:
                changed.append({"id": component_id, "type": "data_elements"})
        else:
            status = "OK"

        _log(f"  [{status}] {path} → {component_id}")
        if stale_status:
            _log(f"    {result.get('stale_detail', '')}")

    _log(
        f"\n{changed_count} changes detected, {applied_count} applied, "
        f"{stale_remote_count} stale-remote skipped, {conflict_count} conflict(s).",
    )
    raise_for_stale_base_conflicts(all_results, allow_stale_base=allow_stale_base)

    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for item in changed:
        key = (item["id"], item["type"])
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def fetch_text_url(url: str) -> str | None:
    """Fetch URL and return decoded text, or ``None`` on failure."""
    try:
        req = Request(url, headers={"Cache-Control": "no-cache"})
        with urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except URLError as exc:
        print(f"  Fetch failed ({url}): {exc}")
        return None


def verify_build_markers(
    *,
    asset_url: str,
    markers: list[str],
    max_retries: int = 5,
) -> bool:
    """Verify that marker strings appear in the built JS asset corpus."""
    _log(f"\nVerifying build: {asset_url}")
    _log(f"  Markers to find: {markers}")

    missing: list[str] = list(markers)
    for attempt in range(max_retries):
        if attempt > 0:
            wait = 5 * attempt
            _log(f"  Retry {attempt}/{max_retries - 1} (waiting {wait}s for CDN propagation)...")
            time.sleep(wait)

        js_content = fetch_text_url(asset_url)
        if js_content is None:
            continue

        _log(f"  Fetched main JS: {len(js_content)} bytes")

        ext_urls = re.findall(
            r"https://assets\.adobedtm\.com/[^'\"]+?-source\.js",
            js_content,
        )
        if ext_urls:
            ext_urls = list(set(ext_urls))
            _log(f"  Found {len(ext_urls)} external source files")

        corpus = js_content
        for ext_url in ext_urls:
            ext_content = fetch_text_url(ext_url)
            if ext_content:
                corpus += "\n" + ext_content

        _log(f"  Total corpus: {len(corpus)} bytes")

        found: list[str] = []
        missing = []
        for marker in markers:
            if marker in corpus:
                found.append(marker)
            else:
                missing.append(marker)

        if not missing:
            _log(f"  ALL {len(found)} markers found")
            for marker in found:
                _log(f"    [OK] {marker}")
            return True

        _log(f"  Found {len(found)}/{len(markers)} markers")
        for marker in found:
            _log(f"    [OK] {marker}")
        for marker in missing:
            _log(f"    [MISSING] {marker}")

    _log(f"\n  VERIFICATION FAILED: {len(missing)} markers not found after {max_retries} attempts")
    return False


def get_build_status(config: AdobeTagsConfig, build_id: str) -> dict[str, str]:
    """Fetch the latest status summary for a Reactor build."""
    payload = _reactor_get(config, f"/builds/{build_id}")
    data = payload.get("data", {})
    attrs = data.get("attributes", {})
    return {
        "id": data.get("id", build_id),
        "status": attrs.get("status", ""),
        "created_at": attrs.get("created_at", ""),
        "updated_at": attrs.get("updated_at", ""),
    }


def wait_for_build_completion(
    config: AdobeTagsConfig,
    build_id: str,
    *,
    timeout_seconds: int = 300,
    poll_interval_seconds: int = 5,
) -> dict[str, str]:
    """Wait for a Reactor build to reach a terminal status."""
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    last_status = ""

    while True:
        attempt += 1
        status = get_build_status(config, build_id)
        current = status["status"] or "unknown"
        updated_at = status["updated_at"] or status["created_at"] or "n/a"

        if current != last_status or attempt == 1:
            _log(f"  Poll {attempt}: status={current} updated_at={updated_at}")
            last_status = current

        if current in BUILD_TERMINAL_STATUSES:
            return status

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                f"Adobe Tags build {build_id} did not complete within {timeout_seconds}s "
                f"(last status: {current})",
            )

        sleep_for = min(poll_interval_seconds, max(1, int(remaining)))
        _log(f"  Waiting {sleep_for}s before next poll...")
        time.sleep(sleep_for)


def run_build_workflow(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    apply: bool,
    verify_asset_url: str | None = None,
    markers: list[str] | None = None,
    re_export_resources: list[str] | None = None,
    allow_stale_base: bool = False,
    verify_retries: int = 5,
    build_wait_timeout: int = 300,
    build_poll_interval: int = 5,
) -> int:
    """Run apply→revise→build→verify→re-export workflow. Returns process code."""
    markers = markers or []
    re_export_resources = re_export_resources or ["rules", "data-elements"]
    root_path = Path(root)
    mode = "APPLY + BUILD" if apply else "DRY-RUN"
    _log(f"[{mode}] Adobe Tags Dev library build")
    _log(f"  Library: {library_id}")
    _log()

    _log("Step 1: Apply custom code changes")
    changed = collect_changed_resources(
        config,
        root_path,
        dry_run=not apply,
        allow_stale_base=allow_stale_base,
    )

    if not apply:
        if changed:
            _log(f"\nDry-run complete. {len(changed)} resources would be updated.")
        else:
            _log("\nNo changes detected.")
        return 0

    if not changed:
        _log("\nNo changes to apply. Skipping build.")
        return 0

    _log("\nStep 2: Refresh library resources from origins")
    _log("  Existing library rules/data elements are re-revised from their origin heads.")
    _log("  Changed origins not yet in the library are added during this step.")
    counts = refresh_library_resources(config, library_id, new_resources=changed)
    for resource_type, count in counts.items():
        _log(f"  {resource_type}: {count}")

    _log("\nStep 3: Build library")
    result = build_library(config, library_id)
    _log(f"  Build: {result['id']}  status={result['status']}")
    _log(
        f"  Waiting for build completion "
        f"(timeout={build_wait_timeout}s, poll={build_poll_interval}s)...",
    )
    final_build = wait_for_build_completion(
        config,
        result["id"],
        timeout_seconds=build_wait_timeout,
        poll_interval_seconds=build_poll_interval,
    )
    _log(f"  Final build status: {final_build['status']}")
    if final_build["status"] not in BUILD_SUCCESS_STATUSES:
        _log("\nERROR: Adobe Tags build did not succeed.")
        return 2

    if markers:
        if not verify_asset_url:
            raise ValueError("verify_asset_url is required when markers are provided")
        _log("\nStep 4: Verify build")
        ok = verify_build_markers(
            asset_url=verify_asset_url,
            markers=markers,
            max_retries=verify_retries,
        )
        if not ok:
            _log("\nWARNING: Build verification FAILED. Changes may not be reflected.")
            return 3
    else:
        _log("\nStep 4: Skipped (no --markers specified)")

    _log("\nStep 5: Re-export to sync local files")
    _log(f"  resources: {re_export_resources}")
    export_summary = export_property(config, root_path, resources=re_export_resources)
    _log(
        "  Export summary: "
        + " ".join(
            f"{resource}={export_summary.get(resource)}"
            for resource in re_export_resources
        ),
    )
    _log("Done.")
    return 0
