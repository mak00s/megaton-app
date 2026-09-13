"""Choose an analytics browser workflow and check local execution prerequisites.

This is not a general browser agent or an arbitrary navigation/click API.
Guide does not open a browser. Doctor launches only with --check-browser and
uses a fresh, temporary context without credentials or a real site.
"""

from __future__ import annotations

from copy import deepcopy
from importlib import metadata
import json
from pathlib import Path
import sys
from typing import Any

import megaton_lib

from .cli_help import build_parser

__all__ = ["workflow_guide", "browser_doctor", "main"]

_WORKFLOWS = {
    "explore": {
        "route": "agent_browser_tools",
        "purpose": "Inspect an unfamiliar page and discover the required steps.",
        "entrypoints": [],
        "next": "Use the agent's authorized MCP/plugin browser tools; save stable steps in the consumer repo.",
        "boundary": "Do not recreate a general browser agent in megaton-app. Exploration is not an apply approval.",
    },
    "validate": {
        "route": "consumer_cli_with_shared_validation",
        "purpose": "Repeat analytics runtime checks and collect comparable evidence.",
        "entrypoints": [
            "megaton_lib.validation.run_page_session",
            "megaton_lib.validation.build_validation_run_metadata",
            "megaton_lib.validation.get_tags_launch_override_report",
        ],
        "next": "Run the consumer's existing validation CLI; keep selectors and expected results there.",
        "boundary": "Specify live versus override. Page load alone is not proof of analytics delivery.",
    },
    "extract": {
        "route": "api_first_then_consumer_cli",
        "purpose": "Acquire analytics input data repeatedly.",
        "entrypoints": [
            "megaton_lib.playwright_browser.browser_page",
            "megaton_lib.playwright_browser.async_browser_page",
        ],
        "next": "Prefer a supported data API; otherwise use a consumer CLI over the shared browser session.",
        "boundary": "Keep site selectors and transformations local. Browser navigation can still emit analytics events.",
    },
    "deliver": {
        "route": "api_first_then_analytics_delivery_adapter",
        "purpose": "Deliver analytics reports or evidence with an explicit write decision.",
        "entrypoints": [
            "megaton_lib.report_gmail_draft.create_report_gmail_draft_from_env",
            "megaton_lib.box_ui.upload_files_to_box_folder_via_ui_sync",
        ],
        "next": "Use an existing report delivery CLI; plan first, then explicitly approve its write mode.",
        "boundary": "No generic mailbox/file automation. Draft creation does not authorize email sending.",
    },
}


def workflow_guide(task: str | None = None) -> dict[str, Any]:
    """Return the task-routing catalog without importing browser dependencies."""
    if task is not None and task not in _WORKFLOWS:
        raise ValueError(f"Unknown task: {task}")
    selected = _WORKFLOWS if task is None else {task: _WORKFLOWS[task]}
    return {
        "schema_version": "browser-workflow/v1",
        "command": "guide",
        "ok": True,
        "exit_code": 0,
        "effects": "none",
        "workflows": deepcopy(selected),
        "session_policy": {
            "default": "Fresh browser context for repeatable jobs; explicitly provide an approved storage state if needed.",
            "existing_chrome": "Use connected_browser_page only when needed; verify profile ownership and target first.",
            "ownership_check": "megaton_lib.playwright_browser.assert_cdp_profile_owner",
            "stealth": "Choose explicitly: generic sessions default False; validation sessions default True.",
            "agent_portability": "Reuse the same consumer CLI, dependencies and result assertions; agent permissions still apply.",
        },
    }


def _package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _probe_browser() -> dict[str, Any]:
    from .playwright_browser import browser_page

    with browser_page(headless=True, stealth=False) as page:
        # No existing profile, storage state, real URL, or download is used.
        page.route("**/*", lambda route: route.abort())
        page.goto("about:blank", timeout=5000)
        if page.evaluate("() => 1 + 1") != 2:
            raise RuntimeError("Browser JavaScript execution failed")
        return {"status": "passed", "scope": "fresh bundled Chromium, about:blank and JavaScript"}


def browser_doctor(*, check_browser: bool = False) -> dict[str, Any]:
    """Check Python dependencies, optionally launching an isolated Chromium.

    Success is limited to the checks requested; it says nothing about login,
    CDP ownership, target access, stealth behavior, or workflow correctness.
    """
    version = _package_version("playwright")
    checks: dict[str, Any] = {
        "playwright": {"status": "passed" if version else "failed", "version": version},
        "browser_launch": {"status": "not_checked"},
        "site_login": {"status": "not_checked"},
        "cdp_profile": {"status": "not_checked"},
        "workflow_result": {"status": "not_checked"},
    }
    actions = []
    if not version:
        actions.append("Install Playwright in this Python environment: python -m pip install playwright")
    elif check_browser:
        try:
            checks["browser_launch"] = _probe_browser()
        except Exception as exc:
            checks["browser_launch"] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
            actions.append("Check the launch error; if Chromium is missing, run: python -m playwright install chromium")
    else:
        actions.append("Verify Chromium launch: python -m megaton_lib.browser_workflow doctor --check-browser")
    ok = all(check["status"] != "failed" for check in checks.values())
    return {
        "schema_version": "browser-workflow/v1",
        "command": "doctor",
        "ok": ok,
        "exit_code": 0 if ok else 1,
        "scope": "local_runtime" if check_browser else "python_dependencies_only",
        "effects": "temporary_browser_if_available" if check_browser else "none",
        "python": sys.executable,
        # Distribution metadata can lag an editable checkout until reinstall;
        # the loaded path is the authoritative "which code runs" signal.
        "megaton_app_version": _package_version("megaton-app"),
        "megaton_lib_path": str(Path(megaton_lib.__file__).resolve().parent),
        "checks": checks,
        "next_actions": actions,
    }


def main(argv=None) -> int:
    parser = build_parser(
        description="Choose analytics browser workflows or check prerequisites. No arbitrary website operations.",
        examples=[
            "python -m megaton_lib.browser_workflow guide --task validate --format json",
            "python -m megaton_lib.browser_workflow doctor",
            "python -m megaton_lib.browser_workflow doctor --check-browser --format json",
        ],
        notes=["Exit 0: requested checks passed; 1: doctor failure; 2: invalid CLI arguments.",
               "Doctor does not verify login or analytics delivery. No profile or storage-state files are read."],
    )
    sub = parser.add_subparsers(dest="command", required=True)
    guide = sub.add_parser(
        "guide", help="Choose exploration tools versus a repeatable analytics CLI",
        description="Show purpose, existing APIs, next steps and safety boundaries. No browser is opened.",
        formatter_class=parser.formatter_class,
    )
    guide.add_argument("--task", choices=tuple(_WORKFLOWS), help="Omit to list all workflow routes")
    doctor = sub.add_parser(
        "doctor", help="Check Python dependencies; browser launch is opt-in",
        description="Check this Python environment. Optional fresh-browser test never loads a real site or existing profile. Login and workflow validity stay unchecked.",
        formatter_class=parser.formatter_class,
    )
    doctor.add_argument("--check-browser", action="store_true", help="Launch fresh bundled Chromium, test about:blank, then close")
    for command in (guide, doctor):
        command.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    if args.command == "doctor" and args.check_browser:
        print("doctor: checking temporary Chromium launch; no real site or existing profile", file=sys.stderr, flush=True)
    result = workflow_guide(args.task) if args.command == "guide" else browser_doctor(check_browser=args.check_browser)
    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "guide":
        for task, item in result["workflows"].items():
            print(f"{task}: {item['route']}\n  {item['purpose']}\n  Next: {item['next']}\n  Boundary: {item['boundary']}")
            for entry in item["entrypoints"]:
                print(f"  API: {entry}")
        for key, value in result["session_policy"].items():
            print(f"{key}: {value}")
    else:
        print(f"Scope: {result['scope']} | effects: {result['effects']}\nPython: {result['python']}")
        print(f"megaton_lib: {result['megaton_lib_path']} (dist metadata {result['megaton_app_version']})")
        for name, check in result["checks"].items():
            print(f"{name}: {check['status']}" + (f" ({check['error']})" if "error" in check else ""))
        for action in result["next_actions"]:
            print(f"Next: {action}")
        print(f"Result: {'passed' if result['ok'] else 'failed'} (requested checks only)")
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
