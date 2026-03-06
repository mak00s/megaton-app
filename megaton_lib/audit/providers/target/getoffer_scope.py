"""getOffer scope detection and scoped export.

Mirrors ``export_target_recs_getoffer_scope.sh`` from at-recs.
Detects which Target criteria, collections, and designs are currently
active on the page by inspecting Playwright-captured delivery calls.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from megaton_lib.audit.providers.target.client import AdobeTargetClient
from megaton_lib.audit.providers.target.recs import export_recs


def detect_getoffer_scope(
    captures_dir: str | Path,
    custom_code_path: str | Path | None = None,
) -> dict[str, Any]:
    """Detect current getOffer scope from captured delivery calls.

    Parameters
    ----------
    captures_dir : directory containing ``delivery-calls.json``
    custom_code_path : path to ``getoffer.custom-code.js`` (optional)

    Returns
    -------
    dict with ``mboxes``, ``criteria_names``, ``collection_names``,
    ``design_names``, and computed regex filters.
    """
    captures = Path(captures_dir)
    delivery_file = captures / "delivery-calls.json"

    if not delivery_file.exists():
        raise FileNotFoundError(f"delivery-calls.json not found in {captures}")

    delivery_data = json.loads(delivery_file.read_text(encoding="utf-8"))

    # Extract mbox names and collection hints from custom code
    mbox_names: list[str] = []
    code_collection_names: list[str] = []
    if custom_code_path:
        code_path = Path(custom_code_path)
        if code_path.exists():
            code = code_path.read_text(encoding="utf-8")
            # Pattern 1: mboxes: { A: "CSK-A", B: "CSK-B" } (object values)
            mboxes_block = re.search(r'mboxes\s*:\s*\{([^}]+)\}', code)
            if mboxes_block:
                mbox_names = re.findall(r'["\']([^"\']+)["\']', mboxes_block.group(1))
            # Pattern 2: mbox: "name" (single mbox per getOffer call)
            if not mbox_names:
                mbox_names = re.findall(r'mbox\s*:\s*["\']([^"\']+)["\']', code)
            # collectionByMbox: { "CSK-A": "collection-name", ... }
            coll_block = re.search(r'collectionByMbox\s*:\s*\{([^}]+)\}', code)
            if coll_block:
                # Extract key: value pairs — keys are mbox names, values are collection names
                pairs = re.findall(r'["\']([^"\']+)["\']\s*:\s*["\']([^"\']+)["\']', coll_block.group(1))
                code_collection_names = [v for _, v in pairs]

    # Parse delivery calls for active resources
    criteria_names: set[str] = set()
    collection_names: set[str] = set(code_collection_names)
    design_names: set[str] = set()
    activity_ids: set[int] = set()

    if isinstance(delivery_data, list):
        calls = delivery_data
    elif isinstance(delivery_data, dict):
        calls = delivery_data.get("calls", delivery_data.get("deliveries", [delivery_data]))
    else:
        calls = []

    for call in calls:
        if not isinstance(call, dict):
            continue

        # Actual capture structure: call["response"]["body"]["prefetch|execute"]
        # Also support flat structure where prefetch/execute are at top level.
        response_obj = call.get("response")
        if isinstance(response_obj, dict):
            body = response_obj.get("body")
            response_body = body if isinstance(body, dict) else call
        else:
            response_body = call  # flat structure: prefetch/execute at top level

        for section in ("prefetch", "execute"):
            section_data = response_body.get(section, {})
            if not isinstance(section_data, dict):
                continue

            mboxes = section_data.get("mboxes", [])
            if not isinstance(mboxes, list):
                continue

            for mbox in mboxes:
                if not isinstance(mbox, dict):
                    continue

                options = mbox.get("options", [])
                if not isinstance(options, list):
                    continue

                for option in options:
                    if not isinstance(option, dict):
                        continue

                    # --- responseTokens (highest priority) ---
                    meta = option.get("responseTokens", {})
                    if not isinstance(meta, dict):
                        meta = {}

                    # --- option.content → recs.activity (fallback) ---
                    # Delivery call content may be a JSON string with nested
                    # recs.activity containing criteria/algorithm details.
                    recs_activity: dict[str, Any] = {}
                    content_raw = option.get("content")
                    if isinstance(content_raw, str):
                        try:
                            parsed = json.loads(content_raw)
                        except (json.JSONDecodeError, ValueError):
                            parsed = None
                    elif isinstance(content_raw, dict):
                        parsed = content_raw
                    else:
                        parsed = None
                    if isinstance(parsed, dict):
                        recs = parsed.get("recs", {})
                        if isinstance(recs, dict):
                            act = recs.get("activity", {})
                            if isinstance(act, dict):
                                recs_activity = act

                    # Extract criteria/algorithm info (responseTokens → content fallback)
                    criteria = (
                        meta.get("recommendation.criteria.title")
                        or recs_activity.get("criteria.title")
                        or ""
                    )
                    if criteria:
                        criteria_names.add(criteria)

                    algo = (
                        meta.get("recommendation.algorithm.name")
                        or recs_activity.get("algorithm.name")
                        or ""
                    )
                    if algo:
                        criteria_names.add(algo)

                    # activity.id from responseTokens or content
                    act_id = meta.get("activity.id") or recs_activity.get("campaign.id")
                    if act_id is not None:
                        try:
                            activity_ids.add(int(act_id))
                        except (TypeError, ValueError):
                            pass

    scope = {
        "mboxes": mbox_names,
        "criteria_names": sorted(criteria_names),
        "collection_names": sorted(collection_names),
        "design_names": sorted(design_names),
        "activity_ids": sorted(activity_ids),
    }

    # Build regex filters for export
    if criteria_names:
        scope["criteria_name_regex"] = "^(" + "|".join(re.escape(n) for n in sorted(criteria_names)) + ")$"
    if collection_names:
        scope["collections_name_regex"] = "^(" + "|".join(re.escape(n) for n in sorted(collection_names)) + ")$"
    if design_names:
        scope["designs_name_regex"] = "^(" + "|".join(re.escape(n) for n in sorted(design_names)) + ")$"

    return scope


def export_getoffer_scope(
    client: AdobeTargetClient,
    output_root: str | Path,
    captures_dir: str | Path,
    custom_code_path: str | Path | None = None,
    *,
    include_designs: bool = False,
    designs_name_regex: str | None = None,
) -> dict[str, Any]:
    """Detect current scope and export matching resources.

    Parameters
    ----------
    client : AdobeTargetClient
    output_root : root output directory
    captures_dir : directory with delivery-calls.json
    custom_code_path : path to getoffer.custom-code.js
    include_designs : if True, always include designs in the export
    designs_name_regex : optional regex to filter designs by name

    Returns
    -------
    dict with ``scope`` and ``export_summary``.
    """
    scope = detect_getoffer_scope(captures_dir, custom_code_path)

    root = Path(output_root)

    # Save scope manifest
    scope_dir = root / "scope"
    scope_dir.mkdir(parents=True, exist_ok=True)
    (scope_dir / "getoffer-current-scope.json").write_text(
        json.dumps(scope, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Build per-resource regex filters.
    # Only export resource types for which we have a filter; skip the rest
    # so that we don't accidentally export everything unscoped.
    name_regex: dict[str, str] = {}
    scoped_resources: list[str] = []
    if "criteria_name_regex" in scope:
        name_regex["criteria"] = scope["criteria_name_regex"]
        scoped_resources.append("criteria")
    if "collections_name_regex" in scope:
        name_regex["collections"] = scope["collections_name_regex"]
        scoped_resources.append("collections")
    if "designs_name_regex" in scope:
        name_regex["designs"] = scope["designs_name_regex"]
        scoped_resources.append("designs")

    # Include designs when explicitly requested (delivery calls rarely expose
    # design names, so the old shell script included them by default with a
    # name regex like ``^(JSON99)$``).
    if include_designs:
        if "designs" not in scoped_resources:
            scoped_resources.append("designs")
        # Explicit designs_name_regex overrides auto-detected value
        if designs_name_regex:
            name_regex["designs"] = designs_name_regex

    # No filters detected and designs not explicitly requested:
    # skip export rather than exporting unscoped resources.
    if not scoped_resources:
        return {
            "scope": scope,
            "export_summary": {},
        }

    export_summary = export_recs(
        client,
        root,
        resources=scoped_resources,
        name_regex=name_regex if name_regex else None,
    )

    return {
        "scope": scope,
        "export_summary": export_summary,
    }
