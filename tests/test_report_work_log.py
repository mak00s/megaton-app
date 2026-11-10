from copy import deepcopy
from datetime import date
import json
from types import SimpleNamespace

import pytest

from megaton_lib import report_work_log as log


def spec(blank=False, pair=False):
    items = (("report", 1, None if blank else 60000),)
    if pair:
        items += (("management", None, 10000),)
    return log.MonthlyWorkLog("Log", ("month", "customer", "date", "item", "hours", "amount"),
                              "example", items, bill_month=not blank)


def cell(raw, fmt=None):
    out = {"userEnteredFormat": fmt or {}}
    if raw is not None:
        out["userEnteredValue"] = {"stringValue" if isinstance(raw, str) else "numberValue": raw}
    return out


def rows(policy, day):
    return [[cell((day.replace(day=1)-log.EPOCH).days if policy.bill_month else None, {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm"}}),
             cell(policy.customer), cell((day-log.EPOCH).days, {"numberFormat": {"type": "DATE", "pattern": "m/d"}}),
             cell(label), cell(hours), cell(amount)] for label, hours, amount in policy.items]


def source(policy):
    return {"sheet_id": 0, "rows": [[cell(x) for x in policy.headers], *rows(policy, date(2026,10,5)), [], [cell("other")]]}


@pytest.mark.parametrize("blank,pair", [(False,False),(True,False),(False,True)])
def test_native_dates_blank_policy_and_same_month_retry(blank,pair):
    policy=spec(blank,pair)
    data=source(policy)
    old=deepcopy(data)
    plan=log.plan_monthly(data,date(2026,11,2),policy)
    assert data==old and plan["action"]=="append"
    for i,r in enumerate(plan["cells"]):
        assert log.values(r["values"]) == [None if blank else 46327,"example",46328,*policy.items[i]]
        assert r["values"][2]["userEnteredFormat"]["numberFormat"]["pattern"]=="m/d"
    data["rows"].extend(r["values"] for r in plan["cells"])
    assert log.plan_monthly(data,date(2026,11,29),policy)["action"]=="skip"


@pytest.mark.parametrize("bad", ["duplicate","hours","amount","blank_month","partial","date"])
def test_conflicts_fail_closed(bad):
    policy=spec(blank=bad=="blank_month",pair=bad in ("partial","date"))
    data=source(policy)
    added=rows(policy,date(2026,11,2))
    if bad=="duplicate":
        added*=2
    elif bad=="partial":
        added=added[:1]
    elif bad=="date":
        added[1][2]=cell(46329)
    else:
        added[0][{"hours":4,"amount":5,"blank_month":0}[bad]]=cell(99)
    data["rows"].extend(added)
    with pytest.raises(ValueError):
        log.plan_monthly(data,date(2026,11,2),policy)


def test_missing_schema_or_exemplar_and_structured_exemplar_stop():
    policy=spec()
    for change in (lambda x:x["rows"][0].clear(),lambda x:x["rows"].pop(1),
                   lambda x:x["rows"][1][1].update(chipRuns=[{}])):
        data=source(policy)
        change(data)
        with pytest.raises(ValueError):
            log.plan_monthly(data,date(2026,11,2),policy)


def test_explicit_format_alias_preserves_history_and_only_renames_new_row():
    old_policy = spec()
    policy = log.MonthlyWorkLog(old_policy.tab, old_policy.headers, old_policy.customer,
                               (("monthly report", 1, 60000),),
                               format_item_aliases=(("monthly report", "report"),))
    data = source(old_policy)
    before = deepcopy(data)
    plan = log.plan_monthly(data, date(2026,11,2), policy)
    assert data == before
    assert log.values(plan["cells"][0]["values"])[3:] == ["monthly report", 1, 60000]
    assert plan["cells"][0]["values"][2]["userEnteredFormat"] == data["rows"][1][2]["userEnteredFormat"]
    data["rows"].extend(rows(policy, date(2026,11,2)))
    data["rows"][-1][4] = cell(2)
    with pytest.raises(ValueError, match="conflict"):
        log.plan_monthly(data, date(2026,11,2), policy)


def test_format_alias_requires_matching_historical_customer_amount_and_label():
    old_policy = spec()
    policy = log.MonthlyWorkLog(old_policy.tab, old_policy.headers, old_policy.customer,
                               (("monthly report", 1, 60000),),
                               format_item_aliases=(("monthly report", "report"),))
    for column, wrong in [(1, "other"), (3, "unapproved"), (4, 2), (5, 0)]:
        data = source(old_policy)
        data["rows"][1][column] = cell(wrong)
        with pytest.raises(ValueError, match="exemplars"):
            log.plan_monthly(data, date(2026,11,2), policy)


def test_append_once_with_formats_receipt_and_readback(monkeypatch,tmp_path):
    policy=spec(True)
    data=source(policy)
    monkeypatch.setattr(log,"snapshot",lambda *_:deepcopy(data))
    calls=[]
    receipt=tmp_path/"receipt.json"
    def write(book,requests,*,retry):
        assert retry is False
        assert json.loads(receipt.read_text())["status"]=="pending"
        calls.append(requests)
        assert list(requests[0])==["appendCells"]
        data["rows"].append([cell("concurrent other customer")])
        data["rows"].extend(r["values"] for r in requests[0]["appendCells"]["rows"])
    monkeypatch.setattr(log,"batch_update_spreadsheet",write)
    book=SimpleNamespace(id="book")
    assert log.record_monthly(book,date(2026,11,2),policy)["action"]=="append"
    assert not calls and not receipt.exists()
    assert log.record_monthly(book,date(2026,11,2),policy,apply=True,receipt_path=receipt)["verified"]
    assert log.record_monthly(book,date(2026,11,3),policy,apply=True,receipt_path=receipt)["action"]=="skip"
    assert len(calls)==1 and json.loads(receipt.read_text())["status"]=="verified"


def test_unknown_append_is_not_retried(monkeypatch,tmp_path):
    policy=spec()
    monkeypatch.setattr(log,"snapshot",lambda *_:source(policy))
    calls=[]
    def write(*a,**k):
        calls.append(1)
        raise TimeoutError("unknown write")
    monkeypatch.setattr(log,"batch_update_spreadsheet",write)
    path=tmp_path/"receipt.json"
    book=SimpleNamespace(id="book")
    with pytest.raises(TimeoutError):
        log.record_monthly(book,date(2026,11,2),policy,apply=True,receipt_path=path)
    with pytest.raises(RuntimeError,match="unresolved"):
        log.record_monthly(book,date(2026,11,2),policy,apply=True,receipt_path=path)
    assert len(calls)==1


def test_changed_snapshot_blocks_write(monkeypatch,tmp_path):
    policy=spec()
    a=source(policy)
    b=deepcopy(a)
    b["rows"].append([cell("new data")])
    sequence=iter([a,b])
    monkeypatch.setattr(log,"snapshot",lambda *_:next(sequence))
    monkeypatch.setattr(log,"batch_update_spreadsheet",lambda *a,**k:pytest.fail("no write"))
    with pytest.raises(ValueError,match="changed"):
        log.record_monthly(SimpleNamespace(id="book"),date(2026,11,2),policy,apply=True,receipt_path=tmp_path/"receipt.json")


def test_failed_readback_keeps_pending_receipt(monkeypatch,tmp_path):
    policy=spec()
    data=source(policy)
    monkeypatch.setattr(log,"snapshot",lambda *_:deepcopy(data))
    def write(book,requests,*,retry):
        added=deepcopy(requests[0]["appendCells"]["rows"][0]["values"])
        added[2]["userEnteredFormat"]={}
        data["rows"].append(added)
    monkeypatch.setattr(log,"batch_update_spreadsheet",write)
    path=tmp_path/"receipt.json"
    with pytest.raises(RuntimeError,match="format"):
        log.record_monthly(SimpleNamespace(id="book"),date(2026,11,2),policy,apply=True,receipt_path=path)
    assert json.loads(path.read_text())["status"]=="pending"
    with pytest.raises(RuntimeError,match="format"):
        log.record_monthly(SimpleNamespace(id="book"),date(2026,11,2),policy,apply=True,receipt_path=path)


def test_unknown_success_can_only_reconcile_exact_original_cells(monkeypatch,tmp_path):
    policy=spec()
    data=source(policy)
    monkeypatch.setattr(log,"snapshot",lambda *_:deepcopy(data))
    calls=[]
    def write(book,requests,*,retry):
        calls.append(1)
        data["rows"].extend(r["values"] for r in requests[0]["appendCells"]["rows"])
        raise TimeoutError("server accepted but response lost")
    monkeypatch.setattr(log,"batch_update_spreadsheet",write)
    path=tmp_path/"receipt.json"
    book=SimpleNamespace(id="book")
    with pytest.raises(TimeoutError):
        log.record_monthly(book,date(2026,11,2),policy,apply=True,receipt_path=path)
    assert log.record_monthly(book,date(2026,11,3),policy,apply=True,receipt_path=path)["verified"]
    assert len(calls)==1 and path.stat().st_mode & 0o777 == 0o600
    data["rows"].pop()
    with pytest.raises(RuntimeError,match="disappeared"):
        log.record_monthly(book,date(2026,11,3),policy,apply=True,receipt_path=path)
    with pytest.raises(ValueError,match="another operation"):
        log.record_monthly(SimpleNamespace(id="other-book"),date(2026,11,3),policy,apply=True,receipt_path=path)


@pytest.mark.parametrize("unsupported", ["tables","merges","sheetType","columnCount"])
def test_snapshot_refuses_unsupported_native_structure(unsupported):
    meta={"properties":{"title":"Log","sheetId":0,"gridProperties":{"rowCount":20,"columnCount":10}}}
    if unsupported in ("tables","merges"):
        meta[unsupported]=[{}]
    elif unsupported=="sheetType":
        meta["properties"]["sheetType"]="OBJECT"
    else:
        meta["properties"]["gridProperties"]["columnCount"]=5
    book=SimpleNamespace(fetch_sheet_metadata=lambda:{"sheets":[meta]})
    with pytest.raises(ValueError):
        log.snapshot(book,spec())


def test_snapshot_chunks_all_columns_and_keeps_blank_row_offsets():
    policy=spec()
    original=source(policy)
    calls=[]
    blocks=iter([original["rows"][:2], [], [[{}]*9+[cell("note after F")]]])
    def fetch(params=None):
        if params is None:
            return {"sheets":[{"properties":{"title":"Log","sheetId":0,
                    "gridProperties":{"rowCount":6,"columnCount":10000}}}]}
        calls.append(params["ranges"])
        return {"sheets":[{"data":[{"rowData":[{"values":r} for r in next(blocks)]}]}]}
    data=log.snapshot(SimpleNamespace(fetch_sheet_metadata=fetch),policy)
    assert calls==["'Log'!1:2","'Log'!3:4","'Log'!5:6"]
    assert len(data["rows"])==6 and data["rows"][2:4]==[[],[]]
    assert log.plan_monthly(data,date(2026,11,2),policy)["rows"]==[6]


def _appended(monkeypatch, tmp_path):
    policy = spec()
    data = source(policy)
    monkeypatch.setattr(log, "snapshot", lambda *_: deepcopy(data))

    def write(book, requests, *, retry):
        data["rows"].extend(deepcopy(requests[0]["appendCells"]["rows"]))
        data["rows"][-1] = [*data["rows"][-1]["values"]]

    monkeypatch.setattr(log, "batch_update_spreadsheet", write)
    path = tmp_path / "receipt.json"
    book = SimpleNamespace(id="book")
    result = log.record_monthly(book, date(2026, 11, 2), policy, apply=True, receipt_path=path)
    assert result["action"] == "appended" and "cells" not in result
    return policy, data, book, path


@pytest.mark.parametrize("change", ["date", "format"])
@pytest.mark.parametrize("apply", [False, True])
def test_verified_receipt_detects_later_cell_changes(monkeypatch, tmp_path, change, apply):
    policy, data, book, path = _appended(monkeypatch, tmp_path)
    saved = path.read_text()
    if change == "date":
        data["rows"][-1][2]["userEnteredValue"]["numberValue"] += 1
    else:
        data["rows"][-1][2]["userEnteredFormat"] = {}
    with pytest.raises(RuntimeError, match="readback differs"):
        log.record_monthly(book, date(2026, 11, 2), policy, apply=apply, receipt_path=path)
    assert path.read_text() == saved


def test_verified_receipt_reuse_keeps_expected_cells(monkeypatch, tmp_path):
    policy, data, book, path = _appended(monkeypatch, tmp_path)
    cells = json.loads(path.read_text())["result"]["cells"]
    assert log.record_monthly(book, date(2026, 11, 2), policy, apply=True, receipt_path=path)["verified"]
    assert json.loads(path.read_text())["result"]["cells"] == cells


def test_first_skip_receipt_pins_existing_cells(monkeypatch, tmp_path):
    policy = spec()
    data = source(policy)
    monkeypatch.setattr(log, "snapshot", lambda *_: deepcopy(data))
    path = tmp_path / "receipt.json"
    book = SimpleNamespace(id="book")
    assert log.record_monthly(book, date(2026, 10, 5), policy, apply=True, receipt_path=path)["action"] == "skip"
    data["rows"][1][2]["userEnteredFormat"] = {}
    with pytest.raises(RuntimeError, match="readback differs"):
        log.record_monthly(book, date(2026, 10, 5), policy, receipt_path=path)


def test_receipt_without_expected_cells_stops(monkeypatch, tmp_path):
    policy, data, book, path = _appended(monkeypatch, tmp_path)
    saved = json.loads(path.read_text())
    del saved["result"]["cells"]
    path.write_text(json.dumps(saved))
    with pytest.raises(RuntimeError, match="expected cells"):
        log.record_monthly(book, date(2026, 11, 2), policy, receipt_path=path)


def test_receipt_compares_all_six_cells_when_trailing_cell_is_omitted(monkeypatch, tmp_path):
    policy = spec(blank=True)
    data = source(policy)
    del data["rows"][1][5:]
    monkeypatch.setattr(log, "snapshot", lambda *_: deepcopy(data))
    path = tmp_path / "receipt.json"
    book = SimpleNamespace(id="book")
    assert log.record_monthly(book, date(2026, 10, 5), policy, apply=True, receipt_path=path)["action"] == "skip"
    assert len(json.loads(path.read_text())["result"]["cells"][0]["values"]) == 6
    data["rows"][1].append({"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}})
    with pytest.raises(RuntimeError, match="readback differs"):
        log.record_monthly(book, date(2026, 10, 5), policy, receipt_path=path)


def test_verify_cells_pads_short_expected_rows():
    observed = {"rows": [[{}] * 5 + [{"userEnteredFormat": {"bold": True}}]]}
    with pytest.raises(RuntimeError, match="readback differs"):
        log.verify_cells(observed, [1], [{"values": [{}] * 5}])


@pytest.mark.parametrize("invalid", [{}, None, []])
def test_invalid_receipt_never_allows_another_monthly_append(monkeypatch, tmp_path, invalid):
    policy = spec()
    monkeypatch.setattr(log, "snapshot", lambda *_: source(policy))
    monkeypatch.setattr(log, "batch_update_spreadsheet", lambda *a, **kw: pytest.fail("No write"))
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(invalid))
    with pytest.raises(ValueError, match="invalid"):
        log.record_monthly(SimpleNamespace(id="book"), date(2026, 11, 2), policy,
                           apply=True, receipt_path=path)


def test_volatile_computed_values_do_not_block_apply(monkeypatch, tmp_path):
    policy = spec()
    data = source(policy)
    data["rows"].append([{"userEnteredValue": {"formulaValue": "=TODAY()"}, "effectiveValue": {"numberValue": 1}}])
    reads = []

    def read(*_):
        # Every read recomputes =TODAY(): only its effective value changes.
        state = deepcopy(data)
        reads.append(1)
        for row in state["rows"]:
            if row and "formulaValue" in row[0].get("userEnteredValue", {}):
                row[0]["effectiveValue"] = {"numberValue": len(reads)}
        return state

    monkeypatch.setattr(log, "snapshot", read)

    def write(book, requests, *, retry):
        data["rows"].extend(deepcopy(requests[0]["appendCells"]["rows"][i]["values"])
                            for i in range(len(requests[0]["appendCells"]["rows"])))

    monkeypatch.setattr(log, "batch_update_spreadsheet", write)
    result = log.record_monthly(SimpleNamespace(id="book"), date(2026, 11, 2), policy,
                                apply=True, receipt_path=tmp_path / "receipt.json")
    assert result["verified"]


def test_definite_rejection_is_recorded_and_retryable(monkeypatch, tmp_path):
    policy = spec()
    data = source(policy)
    monkeypatch.setattr(log, "snapshot", lambda *_: deepcopy(data))
    attempts = []

    class Rejected(Exception):
        response = SimpleNamespace(status_code=403)

    def write(book, requests, *, retry):
        attempts.append(1)
        if len(attempts) == 1:
            raise Rejected("permission denied")
        data["rows"].extend(deepcopy(r["values"]) for r in requests[0]["appendCells"]["rows"])

    monkeypatch.setattr(log, "batch_update_spreadsheet", write)
    path = tmp_path / "receipt.json"
    book = SimpleNamespace(id="book")
    with pytest.raises(Rejected):
        log.record_monthly(book, date(2026, 11, 2), policy, apply=True, receipt_path=path)
    saved = json.loads(path.read_text())
    assert saved["status"] == "rejected" and saved["result"]["rejection_http_status"] == 403
    assert log.record_monthly(book, date(2026, 11, 2), policy, apply=True, receipt_path=path)["verified"]
    assert len(attempts) == 2


def test_month_rows_in_another_order_are_accepted():
    policy = spec(pair=True)
    data = source(policy)
    data["rows"].extend(reversed(rows(policy, date(2026, 11, 2))))
    assert log.plan_monthly(data, date(2026, 11, 2), policy)["action"] == "skip"
