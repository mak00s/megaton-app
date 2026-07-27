"""Portable audit trail for LLM advisor calls.

Persists the exact prompts, provider responses, request IDs, and validation
output of scheduled LLM runs to local artifacts, and mirrors them to a
compressed, chunked Google Sheets tab so another machine can restore the
latest audit without ad hoc authentication. Pairs with
``megaton_lib.llm_advisor`` (whose ``call_records`` this module serializes),
but only duck-typed shapes are required:

- ``advisor``: has ``call_records`` (list[dict]), ``name``, ``model``.
- ``report``: attributes read via ``getattr`` with defaults — ``summary``,
  ``actions``, ``raw``, ``validation_status``, ``validation_generated``,
  ``validation_valid``, ``validation_filtered``, ``validation_dropped``,
  ``validation_valid_actions``. Missing attributes degrade to empty values.
- ``workbook``: has ``exists(name)``, ``read_df(name) -> DataFrame``,
  ``replace_df(name, df)`` — retry/backoff is the workbook's responsibility.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from megaton_lib.json_cache import load_cache, save_cache
from megaton_lib.tz_utils import JST

AUDIT_SCHEMA_VERSION = 1
AUDIT_SHEET_HEADER = [
    "run_id",
    "started_at",
    "source",
    "chunk_index",
    "chunk_count",
    "payload_b64",
]
AUDIT_CHUNK_CHARS = 40_000
AUDIT_MAX_RUNS = 20


def _text_meta(value: object) -> dict[str, Any]:
    text = str(value or "")
    return {
        "chars": len(text),
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def _safe_timestamp(value: str) -> str:
    try:
        observed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        observed = datetime.now(JST)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=JST)
    return observed.astimezone(JST).strftime("%Y%m%dT%H%M%S%f%z")


def write_advice_audit(
    artifacts_dir: str | Path,
    *,
    started_at: str,
    source: str,
    advisor: object,
    report: object,
    dry_run: bool,
    execution_mode: str,
    call_start_index: int = 0,
) -> Path | None:
    """Atomically persist one advice audit and update ``latest.json``."""
    if not hasattr(advisor, "call_records"):
        return None
    calls = list((getattr(advisor, "call_records", []) or [])[call_start_index:])
    if not calls and not str(getattr(report, "raw", "") or "").strip():
        return None

    for call in calls:
        call["system_prompt_meta"] = _text_meta(call.get("system_prompt"))
        call["user_prompt_meta"] = _text_meta(call.get("user_prompt"))
        call["raw_response_meta"] = _text_meta(call.get("raw_response"))

    finished_at = datetime.now(JST)
    payload = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "started_at": started_at,
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "source": source,
        "dry_run": dry_run,
        "execution_mode": execution_mode,
        "provider": str(getattr(advisor, "name", "") or ""),
        "requested_model": str(getattr(advisor, "model", "") or ""),
        "calls": calls,
        "result": {
            "summary": str(getattr(report, "summary", "") or ""),
            "actions": list(getattr(report, "actions", []) or []),
            "raw_response": str(getattr(report, "raw", "") or ""),
            "validation_status": str(getattr(report, "validation_status", "") or ""),
            "validation_generated": int(getattr(report, "validation_generated", 0) or 0),
            "validation_valid": int(getattr(report, "validation_valid", 0) or 0),
            "validation_filtered": int(getattr(report, "validation_filtered", 0) or 0),
            "validation_dropped": list(getattr(report, "validation_dropped", []) or []),
            "validation_valid_actions": list(
                getattr(report, "validation_valid_actions", []) or []
            ),
        },
    }
    payload["result"]["raw_response_meta"] = _text_meta(payload["result"]["raw_response"])

    base = Path(artifacts_dir) / "advice"
    run_id = f"{_safe_timestamp(started_at)}-{finished_at.strftime('%f')}"
    run_path = base / "runs" / f"{run_id}.json"
    save_cache(run_path, payload)
    save_cache(base / "latest.json", payload)
    return run_path


def load_advice_audit(
    artifacts_dir: str | Path,
    *,
    path: str | Path | None = None,
    missing_hint: str = "run the advice job first",
) -> tuple[Path, dict[str, Any]]:
    audit_path = Path(path) if path else Path(artifacts_dir) / "advice" / "latest.json"
    payload = load_cache(audit_path)
    if not payload:
        raise FileNotFoundError(
            f"advice audit not found or invalid: {audit_path}; {missing_hint}"
        )
    return audit_path, payload


def _audit_run_id(payload: dict[str, Any]) -> str:
    started_at = str(payload.get("started_at") or "")
    source = str(payload.get("source") or "advice")
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"{_safe_timestamp(started_at)}-{source}-{digest}"


def advice_audit_sheet_rows(payload: dict[str, Any]) -> pd.DataFrame:
    """Encode one payload into Sheets-cell-safe, deterministic chunks."""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode("ascii")
    chunks = [
        encoded[offset : offset + AUDIT_CHUNK_CHARS]
        for offset in range(0, len(encoded), AUDIT_CHUNK_CHARS)
    ] or [""]
    run_id = _audit_run_id(payload)
    rows = [
        {
            "run_id": run_id,
            "started_at": str(payload.get("started_at") or ""),
            "source": str(payload.get("source") or ""),
            "chunk_index": index,
            "chunk_count": len(chunks),
            "payload_b64": chunk,
        }
        for index, chunk in enumerate(chunks)
    ]
    return pd.DataFrame(rows, columns=AUDIT_SHEET_HEADER)


def _decode_audit_rows(rows: pd.DataFrame) -> dict[str, Any]:
    missing = set(AUDIT_SHEET_HEADER) - set(rows.columns)
    if missing:
        raise ValueError(f"advice audit sheet is missing columns: {sorted(missing)}")
    ordered = rows.copy()
    ordered["chunk_index"] = pd.to_numeric(ordered["chunk_index"], errors="raise").astype(int)
    expected = int(pd.to_numeric(ordered["chunk_count"], errors="raise").iloc[0])
    ordered = ordered.sort_values("chunk_index")
    indexes = ordered["chunk_index"].tolist()
    if len(ordered) != expected or indexes != list(range(expected)):
        raise ValueError(
            f"advice audit chunks incomplete: expected={expected}, found={indexes}"
        )
    encoded = "".join(ordered["payload_b64"].fillna("").astype(str))
    return json.loads(gzip.decompress(base64.b64decode(encoded)).decode("utf-8"))


def latest_advice_audit_from_sheet(frame: pd.DataFrame) -> dict[str, Any]:
    """Decode the latest complete run from an audit-sheet DataFrame."""
    if frame is None or frame.empty:
        raise FileNotFoundError("Google Sheets advice audit is empty")
    missing = set(AUDIT_SHEET_HEADER) - set(frame.columns)
    if missing:
        raise ValueError(f"advice audit sheet is missing columns: {sorted(missing)}")
    candidates = (
        frame[["run_id", "started_at"]]
        .drop_duplicates()
        .sort_values(["started_at", "run_id"], ascending=False)
    )
    errors: list[str] = []
    for run_id in candidates["run_id"].astype(str):
        try:
            return _decode_audit_rows(frame[frame["run_id"].astype(str) == run_id])
        except (ValueError, TypeError, json.JSONDecodeError, gzip.BadGzipFile) as exc:
            errors.append(f"{run_id}: {exc}")
    raise ValueError("no complete advice audit run found; " + "; ".join(errors[:3]))


def sync_advice_audit_to_sheet(
    workbook: object,
    sheet_name: str,
    payload: dict[str, Any],
    *,
    max_runs: int = AUDIT_MAX_RUNS,
) -> int:
    """Idempotently mirror an audit payload through the workbook's retry layer."""
    new_rows = advice_audit_sheet_rows(payload)
    existing = (
        workbook.read_df(sheet_name)
        if workbook.exists(sheet_name)
        else pd.DataFrame(columns=AUDIT_SHEET_HEADER)
    )
    if not set(AUDIT_SHEET_HEADER).issubset(existing.columns):
        existing = pd.DataFrame(columns=AUDIT_SHEET_HEADER)
    run_id = str(new_rows.iloc[0]["run_id"])
    existing = existing[existing["run_id"].astype(str) != run_id]
    combined = pd.concat([existing[AUDIT_SHEET_HEADER], new_rows], ignore_index=True)
    keep_ids = (
        combined[["run_id", "started_at"]]
        .drop_duplicates()
        .sort_values(["started_at", "run_id"], ascending=False)
        .head(max_runs)["run_id"]
        .astype(str)
        .tolist()
    )
    combined = combined[combined["run_id"].astype(str).isin(keep_ids)]
    combined["chunk_index"] = pd.to_numeric(combined["chunk_index"], errors="coerce").fillna(0).astype(int)
    combined["chunk_count"] = pd.to_numeric(combined["chunk_count"], errors="coerce").fillna(0).astype(int)
    combined = combined.sort_values(
        ["started_at", "run_id", "chunk_index"], ascending=[False, False, True]
    ).reset_index(drop=True)
    workbook.replace_df(sheet_name, combined[AUDIT_SHEET_HEADER])
    return len(new_rows)


def restore_advice_audit_from_sheet(
    workbook: object,
    sheet_name: str,
    artifacts_dir: str | Path,
) -> tuple[Path, dict[str, Any]]:
    """Read the latest GS audit and populate the normal local cache."""
    if not workbook.exists(sheet_name):
        raise FileNotFoundError(f"Google Sheets tab not found: {sheet_name}")
    payload = latest_advice_audit_from_sheet(workbook.read_df(sheet_name))
    base = Path(artifacts_dir) / "advice"
    run_path = base / "runs" / f"{_audit_run_id(payload)}.json"
    save_cache(run_path, payload)
    latest_path = base / "latest.json"
    save_cache(latest_path, payload)
    return latest_path, payload


def _call_label(call: dict[str, Any], index: int) -> str:
    usage = call.get("usage") if isinstance(call.get("usage"), dict) else {}
    system_meta = (
        call.get("system_prompt_meta") if isinstance(call.get("system_prompt_meta"), dict) else {}
    )
    user_meta = call.get("user_prompt_meta") if isinstance(call.get("user_prompt_meta"), dict) else {}
    response_meta = (
        call.get("raw_response_meta") if isinstance(call.get("raw_response_meta"), dict) else {}
    )
    input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens") or "?"
    output_tokens = usage.get("completion_tokens") or usage.get("output_tokens") or "?"
    model = call.get("response_model") or call.get("requested_model") or "unknown"
    request_id = call.get("response_id") or "-"
    return (
        f"call {index}: model={model} id={request_id} "
        f"tokens_in={input_tokens} tokens_out={output_tokens} "
        f"chars_system={system_meta.get('chars', '?')} "
        f"chars_user={user_meta.get('chars', '?')} "
        f"chars_response={response_meta.get('chars', '?')}"
    )


def render_advice_audit_summary(
    path: Path,
    payload: dict[str, Any],
    *,
    label: str = "advice-inspect",
    detail_command: str = "advice-inspect",
) -> str:
    calls = payload.get("calls") if isinstance(payload.get("calls"), list) else []
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    lines = [
        f"[{label}] LOCAL-FIRST audit; no LLM call",
        f"audit_file: {path}",
        (
            f"run: {payload.get('started_at', '')} source={payload.get('source', '')} "
            f"dry_run={payload.get('dry_run', False)} mode={payload.get('execution_mode', '')}"
        ),
        (
            f"advisor: provider={payload.get('provider', '')} "
            f"requested_model={payload.get('requested_model', '')} calls={len(calls)}"
        ),
    ]
    lines.extend(_call_label(call, index) for index, call in enumerate(calls, start=1))
    lines.append(
        "validation: "
        f"status={result.get('validation_status', '')} "
        f"generated={result.get('validation_generated', 0)} "
        f"valid={result.get('validation_valid', 0)} "
        f"dropped={len(result.get('validation_dropped') or [])} "
        f"filtered={result.get('validation_filtered', 0)}"
    )
    lines.append("final_summary:")
    lines.append(str(result.get("summary") or "(empty)"))
    lines.append("final_actions:")
    actions = result.get("actions") or []
    lines.extend(f"{index}. {action}" for index, action in enumerate(actions, start=1))
    if not actions:
        lines.append("(none)")
    lines.append(
        f"detail: use `{detail_command} --show "
        "{system-prompt,user-prompt,raw-response,all}`"
    )
    return "\n".join(lines)


def render_advice_audit_detail(payload: dict[str, Any], show: str) -> str:
    calls = payload.get("calls") if isinstance(payload.get("calls"), list) else []
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    sections: list[str] = []
    for index, call in enumerate(calls, start=1):
        if show in {"system-prompt", "all"}:
            sections.append(f"===== call {index} system prompt =====\n{call.get('system_prompt', '')}")
        if show in {"user-prompt", "all"}:
            sections.append(f"===== call {index} user prompt =====\n{call.get('user_prompt', '')}")
        if show in {"raw-response", "all"}:
            sections.append(f"===== call {index} raw response =====\n{call.get('raw_response', '')}")
    if show in {"raw-response", "all"} and not calls:
        sections.append(f"===== combined raw response =====\n{result.get('raw_response', '')}")
    return "\n\n".join(sections)


def audit_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
