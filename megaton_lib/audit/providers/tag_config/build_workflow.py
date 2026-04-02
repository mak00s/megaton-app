"""Reusable Adobe Tags build-and-verify workflow helpers."""

from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from ...config import AdobeTagsConfig
from .adobe_tags import build_library, export_property, refresh_library_resources
from .sync import apply_custom_code_tree


def collect_changed_resources(
    config: AdobeTagsConfig,
    root: str | Path,
    *,
    dry_run: bool,
) -> list[dict]:
    """Apply custom code tree and return changed rule/data-element origins."""
    changed: list[dict] = []
    changed_count = 0
    applied_count = 0
    root_path = Path(root)

    for result in apply_custom_code_tree(config, root_path, dry_run=dry_run):
        path = result["path"]
        component_id = result["component_id"]

        if result.get("changed"):
            changed_count += 1
            if result.get("applied"):
                applied_count += 1
                status = "APPLIED"
            else:
                status = "CHANGED"

            if "rules/" in path:
                for part in Path(path).parts:
                    if part.startswith("rl"):
                        changed.append({"id": part.split("_")[0].upper(), "type": "rules"})
                        break
            elif "data-elements/" in path:
                changed.append({"id": component_id, "type": "data_elements"})
        else:
            status = "OK"

        print(f"  [{status}] {path} → {component_id}")

    print(f"\n{changed_count} changes detected, {applied_count} applied.")

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
    print(f"\nVerifying build: {asset_url}")
    print(f"  Markers to find: {markers}")

    missing: list[str] = list(markers)
    for attempt in range(max_retries):
        if attempt > 0:
            wait = 5 * attempt
            print(f"  Retry {attempt}/{max_retries - 1} (waiting {wait}s for CDN propagation)...")
            time.sleep(wait)

        js_content = fetch_text_url(asset_url)
        if js_content is None:
            continue

        print(f"  Fetched main JS: {len(js_content)} bytes")

        ext_urls = re.findall(
            r"https://assets\.adobedtm\.com/[^'\"]+?-source\.js",
            js_content,
        )
        if ext_urls:
            ext_urls = list(set(ext_urls))
            print(f"  Found {len(ext_urls)} external source files")

        corpus = js_content
        for ext_url in ext_urls:
            ext_content = fetch_text_url(ext_url)
            if ext_content:
                corpus += "\n" + ext_content

        print(f"  Total corpus: {len(corpus)} bytes")

        found: list[str] = []
        missing = []
        for marker in markers:
            if marker in corpus:
                found.append(marker)
            else:
                missing.append(marker)

        if not missing:
            print(f"  ALL {len(found)} markers found")
            for marker in found:
                print(f"    [OK] {marker}")
            return True

        print(f"  Found {len(found)}/{len(markers)} markers")
        for marker in found:
            print(f"    [OK] {marker}")
        for marker in missing:
            print(f"    [MISSING] {marker}")

    print(f"\n  VERIFICATION FAILED: {len(missing)} markers not found after {max_retries} attempts")
    return False


def run_build_workflow(
    config: AdobeTagsConfig,
    *,
    root: str | Path,
    library_id: str,
    apply: bool,
    verify_asset_url: str | None = None,
    markers: list[str] | None = None,
    verify_retries: int = 5,
) -> int:
    """Run apply→revise→build→verify→re-export workflow. Returns process code."""
    markers = markers or []
    root_path = Path(root)
    mode = "APPLY + BUILD" if apply else "DRY-RUN"
    print(f"[{mode}] Adobe Tags Dev library build")
    print(f"  Library: {library_id}")
    print()

    print("Step 1: Apply custom code changes")
    changed = collect_changed_resources(config, root_path, dry_run=not apply)

    if not apply:
        if changed:
            print(f"\nDry-run complete. {len(changed)} resources would be updated.")
        else:
            print("\nNo changes detected.")
        return 0

    if not changed:
        print("\nNo changes to apply. Skipping build.")
        return 0

    print("\nStep 2: Refresh all library resources (re-revise from origins)")
    counts = refresh_library_resources(config, library_id, new_resources=changed)
    for resource_type, count in counts.items():
        print(f"  {resource_type}: {count}")

    print("\nStep 3: Build library")
    result = build_library(config, library_id)
    print(f"  Build: {result['id']}  status={result['status']}")

    if markers:
        if not verify_asset_url:
            raise ValueError("verify_asset_url is required when markers are provided")
        print("\nStep 4: Verify build")
        ok = verify_build_markers(
            asset_url=verify_asset_url,
            markers=markers,
            max_retries=verify_retries,
        )
        if not ok:
            print("\nWARNING: Build verification FAILED. Changes may not be reflected.")
            return 2
    else:
        print("\nStep 4: Skipped (no --markers specified)")

    print("\nStep 5: Re-export to sync local files")
    export_property(config, root_path)
    print("Done.")
    return 0
