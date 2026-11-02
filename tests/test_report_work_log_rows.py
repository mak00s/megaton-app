"""Request-scoped native row plans; no network, credentials or customer data."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from megaton_lib import report_work_log as log


def cell(raw, **extra):
    return {"userEnteredValue": {"numberValue" if isinstance(raw, (int, float)) else "stringValue": raw}, **extra}


@pytest.fixture
def row_env(monkeypatch, tmp_path):
    state = {"sheet_id": 7, "row_count": 2,
             "rows": [[cell(f"column{i}") for i in range(9)], [cell("history")]]}
    after = [cell("period"), cell("customer"), cell(46328), cell("task"), cell(0.5),
             {"userEnteredValue": {"formulaValue": "=2*E3"}}, {}, {}, cell("request:example")]
    plan = {"action": "append", "row": 3, "sheet_id": 7, "before": [{}] * 9, "after": after}
    path = tmp_path / "private" / "receipt.json"
    calls = []
    reads = []
    def read(book, tab, **kwargs):
        assert tab == "Log" and kwargs["min_columns"] == 9
        reads.append(1)
        return deepcopy(state)
    monkeypatch.setattr(log, "snapshot", read)
    def write(book, requests, *, retry):
        assert retry is False
        assert json.loads(path.read_text())["status"] == "pending"
        calls.append(deepcopy(requests))
        for request in requests:
            if "appendDimension" in request:
                state["row_count"] += request["appendDimension"]["length"]
                continue
            update = request["updateCells"]
            bounds = update["range"]
            row, column = bounds["startRowIndex"], bounds["startColumnIndex"]
            while len(state["rows"]) <= row:
                state["rows"].append([])
            state["rows"][row] = (state["rows"][row] + [{}] * 9)[:9]
            for i, new in enumerate(update["rows"][0]["values"]):
                if update["fields"] == "userEnteredValue":
                    state["rows"][row][column + i]["userEnteredValue"] = deepcopy(new["userEnteredValue"])
                else:
                    state["rows"][row][column + i] = deepcopy(new)
    monkeypatch.setattr(log, "batch_update_spreadsheet", write)
    book = SimpleNamespace(id="book")
    def record(apply=False, operation=None):
        return log.record_row_plan(book, operation or plan, tab="Log", apply=apply, receipt_path=path)
    return state, plan, path, calls, reads, record, write


def test_row_preview_append_once_native_formula_and_receipt(row_env):
    state, plan, path, calls, _, record, _ = row_env
    assert record()["action"] == "append" and not calls and not path.exists()
    assert record(True)["verified"]
    assert record(True)["action"] == "skip"
    assert record()["verified"] and len(calls) == 1
    assert calls[0][0] == {"appendDimension": {"sheetId": 7, "dimension": "ROWS", "length": 1}}
    assert state["rows"][2] == plan["after"]
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text())["status"] == "verified"


def test_complete_row_preserves_existing_native_metadata(row_env):
    state, plan, _, calls, _, record, _ = row_env
    before = deepcopy(plan["after"])
    before[2] = {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "m/d"}}}
    before[8] = cell("unclaimed")
    state["rows"][1] = deepcopy(before)
    after = deepcopy(before)
    after[2]["userEnteredValue"] = {"numberValue": 46328}
    after[8] = cell("request:example")
    op = {"action": "complete", "row": 2, "sheet_id": 7, "before": before, "after": after}
    assert record(True, op)["verified"]
    assert len(calls[0]) == 2
    assert all(r["updateCells"]["fields"] == "userEnteredValue" for r in calls[0])
    assert state["rows"][1] == after


@pytest.mark.parametrize("accepted", [False, True])
def test_uncertain_row_write_never_repeats(row_env, monkeypatch, accepted):
    _, _, path, calls, _, record, write = row_env
    attempts = []
    def fail(*args, **kwargs):
        attempts.append(1)
        if accepted:
            write(*args, **kwargs)
        raise TimeoutError("unknown")
    monkeypatch.setattr(log, "batch_update_spreadsheet", fail)
    with pytest.raises(TimeoutError):
        record(True)
    assert json.loads(path.read_text())["status"] == "pending"
    if accepted:
        assert record(True)["verified"]
    else:
        with pytest.raises(RuntimeError, match="unresolved"):
            record(True)
    assert len(attempts) == 1


def test_changed_row_and_wrong_receipt_binding_stop(row_env):
    state, plan, _, calls, _, record, _ = row_env
    record(True)
    other = deepcopy(plan)
    other["after"][8] = cell("other request")
    with pytest.raises(ValueError, match="another operation"):
        record(True, other)
    state["rows"][2][5]["userEnteredValue"] = {"formulaValue": "=99"}
    with pytest.raises(RuntimeError, match="unresolved"):
        record(True)
    assert len(calls) == 1


def test_changed_snapshot_stops_before_receipt_or_write(row_env, monkeypatch):
    state, _, path, calls, _, record, _ = row_env
    counter = []
    def read(*args, **kwargs):
        observed = deepcopy(state)
        if counter:
            observed["row_count"] += 1
        counter.append(1)
        return observed
    monkeypatch.setattr(log, "snapshot", read)
    with pytest.raises(ValueError, match="changed"):
        record(True)
    assert not calls and not path.exists()


def test_non_tail_and_format_mutation_are_not_written(row_env):
    state, plan, path, calls, _, record, _ = row_env
    state["rows"].append([cell("concurrent task")])
    with pytest.raises(ValueError, match="changed"):
        record(True)
    before = deepcopy(plan["after"])
    state["rows"][1] = deepcopy(before)
    after = deepcopy(before)
    after[2]["userEnteredFormat"] = {"bold": True}
    with pytest.raises(ValueError, match="preserve"):
        record(True, {"action": "complete", "row": 2, "sheet_id": 7, "before": before, "after": after})
    assert not calls and not path.exists()


def test_readback_failure_leaves_pending_and_stops_retry(row_env, monkeypatch):
    _, _, path, calls, _, record, _ = row_env
    monkeypatch.setattr(log, "batch_update_spreadsheet", lambda *a, **kw: calls.append(1))
    with pytest.raises(RuntimeError, match="readback differs"):
        record(True)
    assert json.loads(path.read_text())["status"] == "pending"
    with pytest.raises(RuntimeError, match="unresolved"):
        record(True)
    assert len(calls) == 1


@pytest.mark.parametrize("invalid", [{}, None, []])
def test_invalid_existing_receipt_never_allows_another_append(row_env, invalid):
    _, _, path, calls, _, record, _ = row_env
    path.parent.mkdir()
    path.write_text(json.dumps(invalid))
    with pytest.raises(ValueError, match="invalid"):
        record(True)
    assert not calls
