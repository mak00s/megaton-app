#!/usr/bin/env python3
"""Detect validation scripts that bypass shared Playwright helpers."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


DIRECT_PLAYWRIGHT_PATTERNS = (
    re.compile(r"\bsync_playwright\s*\("),
    re.compile(r"\bfrom\s+playwright\b"),
    re.compile(r"\bimport\s+playwright\b"),
)
RAW_ROUTE_PATTERNS = (
    re.compile(r"\bpage\.route\s*\("),
    re.compile(r"\bcontext\.route\s*\("),
)
PLAYWRIGHTISH_PATTERNS = (
    re.compile(r"\bpage\.goto\s*\("),
    re.compile(r"\bpage\.click\s*\("),
    re.compile(r"\bpage\.query_selector"),
    re.compile(r"\bPage\b"),
)
SHARED_VALIDATION_PATTERNS = (
    re.compile(r"megaton_lib\.validation\.playwright_pages"),
    re.compile(r"megaton_lib\.validation\.playwright_capture"),
    re.compile(r"megaton_lib\.validation\.adobe_analytics"),
    re.compile(r"megaton_lib\.validation\.contracts"),
    re.compile(r"from\s+megaton_lib\.validation\s+import\b"),
    re.compile(r"import\s+megaton_lib\.validation\b"),
)
RESULT_PRODUCER_NAME_PATTERNS = (
    re.compile(r"^(check_|run_|verify_).+\.py$"),
)
RESULT_PRODUCER_EXCLUDES = {
    "check_pending_verifications.py",
}
METADATA_HELPER_PATTERNS = (
    re.compile(r"build_validation_run_metadata"),
)
SAVE_HELPER_PATTERNS = (
    re.compile(r"write_validation_json"),
)
JSON_OUTPUT_PATTERNS = (
    re.compile(r"\bjson\.dump\s*\("),
    re.compile(r"\bjson\.dumps\s*\("),
    re.compile(r"\bwrite_text\s*\("),
)


def _iter_validation_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
        and "validation" in path.parts
        and path.name != "__init__.py"
    )


def _check_file(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    findings: list[str] = []
    seems_like_entrypoint = (
        path.name.startswith(("check_", "run_", "verify_", "capture_", "update_"))
        or "argparse" in text
        or "__main__" in text
    )
    expects_validation_result_schema = (
        any(pattern.match(path.name) for pattern in RESULT_PRODUCER_NAME_PATTERNS)
        and path.name not in RESULT_PRODUCER_EXCLUDES
    )

    if any(pattern.search(text) for pattern in DIRECT_PLAYWRIGHT_PATTERNS):
        findings.append("direct playwright import/use")
    if any(pattern.search(text) for pattern in RAW_ROUTE_PATTERNS):
        findings.append("raw route intercept")
    uses_playwright_flow = any(pattern.search(text) for pattern in PLAYWRIGHTISH_PATTERNS)
    if (uses_playwright_flow or findings) and expects_validation_result_schema and not any(
        pattern.search(text) for pattern in SHARED_VALIDATION_PATTERNS
    ):
        findings.append("no megaton_lib.validation helper import")
    if expects_validation_result_schema and any(pattern.search(text) for pattern in JSON_OUTPUT_PATTERNS):
        if not any(pattern.search(text) for pattern in METADATA_HELPER_PATTERNS):
            findings.append("no build_validation_run_metadata")
        if re.search(r"\bjson\.dump\s*\(|\bwrite_text\s*\(", text) and not any(
            pattern.search(text) for pattern in SAVE_HELPER_PATTERNS
        ):
            findings.append("no write_validation_json")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect validation scripts that bypass megaton_lib.validation",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Repo root or validation directory to scan",
    )
    args = parser.parse_args()

    findings_found = False
    for raw_path in args.paths:
        root = Path(raw_path).expanduser().resolve()
        if not root.exists():
            print(f"[missing] {root}")
            findings_found = True
            continue

        files = _iter_validation_files(root if root.is_dir() else root.parent)
        for file_path in files:
            findings = _check_file(file_path)
            if findings:
                findings_found = True
                joined = ", ".join(findings)
                print(f"{file_path}: {joined}")

    if not findings_found:
        print("No validation policy violations found.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
