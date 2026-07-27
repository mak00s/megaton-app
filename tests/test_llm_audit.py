"""llm_audit: local write/load, sheet chunk round-trip, renderers."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pandas as pd

from megaton_lib.llm_audit import (
    advice_audit_sheet_rows,
    latest_advice_audit_from_sheet,
    load_advice_audit,
    render_advice_audit_detail,
    render_advice_audit_summary,
    restore_advice_audit_from_sheet,
    sync_advice_audit_to_sheet,
    write_advice_audit,
)


class _Advisor:
    name = "openrouter"
    model = "anthropic/claude-opus-5"
    call_records = [
        {
            "started_at": "2026-07-27T07:17:00+09:00",
            "finished_at": "2026-07-27T07:17:10+09:00",
            "provider": "openrouter",
            "requested_model": "anthropic/claude-opus-5",
            "response_id": "gen-test",
            "response_model": "anthropic/claude-opus-5",
            "max_tokens": 16000,
            "temperature": 0.1,
            "system_prompt": "system exact",
            "user_prompt": "user exact",
            "raw_response": '{"summary":"model raw","actions":[]}',
            "usage": {"prompt_tokens": 100, "completion_tokens": 20},
            "finish_reason": "stop",
            "error": "",
        }
    ]


def _report(**overrides):
    base = {
        "summary": "final summary",
        "actions": ["A(0000): 様子見 → 様子見"],
        "raw": '{"summary":"model raw","actions":[]}',
        "validation_status": "ok",
        "validation_generated": 2,
        "validation_valid": 1,
        "validation_filtered": 1,
        "validation_dropped": [],
        "validation_valid_actions": [],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_write_and_load_advice_audit(tmp_path):
    run_path = write_advice_audit(
        tmp_path,
        started_at="2026-07-27T07:17:00+09:00",
        source="advice",
        advisor=_Advisor(),
        report=_report(),
        dry_run=False,
        execution_mode="morning",
    )

    assert run_path is not None and run_path.exists()
    latest_path, payload = load_advice_audit(tmp_path)
    assert latest_path == tmp_path / "advice" / "latest.json"
    assert payload["calls"][0]["response_id"] == "gen-test"
    assert payload["calls"][0]["system_prompt_meta"]["chars"] == len("system exact")
    assert payload["result"]["validation_filtered"] == 1
    assert json.loads(run_path.read_text(encoding="utf-8")) == payload
    assert not run_path.with_suffix(".json.tmp").exists()


def test_write_skips_advisor_without_call_records(tmp_path):
    plain = SimpleNamespace(name="x", model="m")  # no call_records attribute
    assert write_advice_audit(
        tmp_path, started_at="", source="advice", advisor=plain,
        report=_report(raw=""), dry_run=False, execution_mode="",
    ) is None


def test_missing_audit_uses_custom_hint(tmp_path):
    try:
        load_advice_audit(tmp_path, missing_hint="run `poimak advice` first")
    except FileNotFoundError as exc:
        assert "run `poimak advice` first" in str(exc)
    else:
        raise AssertionError("missing audit must fail")


def test_renderers_use_label_and_detail_command(tmp_path):
    write_advice_audit(
        tmp_path,
        started_at="2026-07-27T07:17:00+09:00",
        source="refresh-trigger",
        advisor=_Advisor(),
        report=_report(),
        dry_run=True,
        execution_mode="morning",
    )
    path, payload = load_advice_audit(tmp_path)

    summary = render_advice_audit_summary(
        path, payload, label="minkabu:advice-inspect", detail_command="minkabu advice-inspect",
    )
    assert summary.startswith("[minkabu:advice-inspect] LOCAL-FIRST audit; no LLM call")
    assert "model=anthropic/claude-opus-5 id=gen-test" in summary
    assert "minkabu advice-inspect --show" in summary
    assert "system exact" in render_advice_audit_detail(payload, "system-prompt")
    assert "user exact" in render_advice_audit_detail(payload, "user-prompt")
    assert "model raw" in render_advice_audit_detail(payload, "raw-response")


def test_audit_sheet_round_trip_chunks_large_payload():
    payload = {
        "started_at": "2026-07-27T07:17:00+09:00",
        "source": "advice",
        "calls": [{
            "user_prompt": "".join(
                hashlib.sha256(str(index).encode()).hexdigest()
                for index in range(3_000)
            )
        }],
    }

    rows = advice_audit_sheet_rows(payload)

    assert len(rows) > 1
    assert rows["payload_b64"].str.len().max() <= 40_000
    assert latest_advice_audit_from_sheet(rows) == payload


class _FakeWorkbook:
    def __init__(self):
        self.frame = pd.DataFrame()

    def exists(self, _name):
        return not self.frame.empty

    def read_df(self, _name):
        return self.frame.copy()

    def replace_df(self, _name, frame):
        self.frame = frame.copy()


def test_sync_is_idempotent_and_restore_populates_local_cache(tmp_path):
    workbook = _FakeWorkbook()
    _, payload = load_advice_audit(
        tmp_path,
        path=write_advice_audit(
            tmp_path,
            started_at="2026-07-27T07:17:00+09:00",
            source="advice",
            advisor=_Advisor(),
            report=_report(summary="portable"),
            dry_run=False,
            execution_mode="morning",
        ),
    )

    sync_advice_audit_to_sheet(workbook, "_advice_audit", payload)
    first_rows = len(workbook.frame)
    sync_advice_audit_to_sheet(workbook, "_advice_audit", payload)
    assert len(workbook.frame) == first_rows

    restored_dir = tmp_path / "other-machine"
    path, restored = restore_advice_audit_from_sheet(
        workbook, "_advice_audit", restored_dir
    )
    assert path.exists()
    assert restored["result"]["summary"] == "portable"


def test_sync_prunes_to_max_runs():
    workbook = _FakeWorkbook()
    for i in range(4):
        payload = {"started_at": f"2026-07-2{i}T07:00:00+09:00", "source": "advice",
                   "calls": [{"user_prompt": f"p{i}"}]}
        sync_advice_audit_to_sheet(workbook, "_advice_audit", payload, max_runs=3)
    assert workbook.frame["run_id"].nunique() == 3
    # 最新 run が残る
    latest = latest_advice_audit_from_sheet(workbook.frame)
    assert latest["calls"][0]["user_prompt"] == "p3"
