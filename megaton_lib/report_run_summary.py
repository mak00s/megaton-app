"""Pure report delivery-summary normalization; separate from ReportRun lifecycle.

Preserves the notebook report-run-summary/v1 contract, including legacy status
inference. Callers should supply validation explicitly when execution success
alone does not establish data validity.
"""

from __future__ import annotations

import os
from copy import deepcopy
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "report-run-summary/v1"
DEFAULT_TIMEZONE = "Asia/Tokyo"


def now_jst_iso() -> str:
    return datetime.now(ZoneInfo(DEFAULT_TIMEZONE)).isoformat(timespec="seconds")


def github_run_url(env: dict[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    server = values.get("GITHUB_SERVER_URL", "").strip()
    repo = values.get("GITHUB_REPOSITORY", "").strip()
    run_id = values.get("GITHUB_RUN_ID", "").strip()
    if server and repo and run_id:
        return f"{server.rstrip('/')}/{repo.strip('/')}/actions/runs/{run_id}"
    return ""


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _default_validation(status: str) -> dict[str, Any]:
    if status == "success":
        validation_status = "passed"
    elif status in {"failed", "failure", "cancelled"}:
        validation_status = "failed"
    else:
        validation_status = ""
    return {"status": validation_status, "notes": [], "errors": []}


def normalize_report_summary(
    summary: dict[str, Any],
    *,
    env: dict[str, str] | None = None,
    default_report: str = "",
    default_status: str = "",
) -> dict[str, Any]:
    """Return a backward-compatible report run summary with standard top-level keys."""
    data = deepcopy(summary)
    status = str(data.get("status") or default_status or "").strip()
    validation = {**_default_validation(status), **_as_dict(data.get("validation"))}
    artifacts = _as_dict(data.get("artifacts"))
    delivery = _as_dict(data.get("delivery"))
    if "box_uploads" in artifacts and "box_uploads" not in delivery:
        # Transitional shim; remove after all notebooks migrate to writing delivery directly.
        # TODO(2026-09-30): drop artifacts.box_uploads mirroring once report summaries use delivery.box_uploads.
        delivery["box_uploads"] = artifacts.get("box_uploads")

    normalized = {
        "schema_version": str(data.get("schema_version") or SCHEMA_VERSION),
        "report": str(data.get("report") or default_report or "").strip(),
        "status": status,
        "started_at_jst": str(data.get("started_at_jst") or ""),
        "finished_at_jst": str(data.get("finished_at_jst") or ""),
        "timezone": str(data.get("timezone") or DEFAULT_TIMEZONE),
        "window": _as_dict(data.get("window")),
        "run_url": str(data.get("run_url") or github_run_url(env)),
        "entries": _as_list(data.get("entries")),
        "validation": {
            "status": str(validation.get("status") or ""),
            "notes": _as_list(validation.get("notes")),
            "errors": _as_list(validation.get("errors")),
        },
        "artifacts": artifacts,
        "delivery": delivery,
        "next_actions": _as_list(data.get("next_actions")),
    }

    for key, value in data.items():
        normalized.setdefault(key, value)
    return normalized
