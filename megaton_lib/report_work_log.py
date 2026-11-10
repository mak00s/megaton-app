"""Guarded monthly work-log recording for completed analytics workflows.

Callers own the destination, headers, customer, amounts and completion gates.
This module never discovers credentials or declares a report delivered.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile

from .sheets import batch_update_spreadsheet

EPOCH = date(1899, 12, 30)
CELL_FIELDS = "userEnteredValue,effectiveValue,userEnteredFormat,dataValidation,chipRuns"


@dataclass(frozen=True)
class MonthlyWorkLog:
    tab: str
    headers: tuple[str, ...]
    customer: str
    items: tuple[tuple[str, float | None, float | None], ...]
    bill_month: bool = True
    format_item_aliases: tuple[tuple[str, str], ...] = ()

    def __post_init__(self):
        if len(self.headers) != 6 or not self.items or len({i[0] for i in self.items}) != len(self.items):
            raise ValueError("Work log requires six headers and uniquely named items")
        if (len(dict(self.format_item_aliases)) != len(self.format_item_aliases)
                or any(target not in {i[0] for i in self.items} or not source
                       for target, source in self.format_item_aliases)):
            raise ValueError("Format aliases must map unique configured items to historical labels")


def value(cell):
    raw = cell.get("effectiveValue") or cell.get("userEnteredValue") or {}
    return next((raw[key] for key in ("numberValue", "stringValue", "boolValue") if key in raw), None)


def cell_date(cell):
    raw = value(cell)
    if type(raw) in (int, float) and int(raw) == raw:
        return EPOCH + timedelta(days=int(raw))
    if isinstance(raw, str):
        return date.fromisoformat(raw)
    raise ValueError("Work-log date must be a native date or ISO date")


def values(row):
    return [value(c) for c in (row + [{}] * 6)[:6]]


def snapshot(book, spec, *, cell_fields=CELL_FIELDS, min_columns=6):
    """Read a monthly spec or an explicit worksheet title, preserving row offsets."""
    tab = spec if isinstance(spec, str) else spec.tab
    matches = [s for s in book.fetch_sheet_metadata()["sheets"] if s["properties"]["title"] == tab]
    if len(matches) != 1 or matches[0].get("tables") or matches[0].get("merges"):
        raise ValueError("Work log must be one ordinary unmerged worksheet")
    props = matches[0]["properties"]
    if props.get("sheetType", "GRID") != "GRID" or props["gridProperties"]["columnCount"] < min_columns:
        raise ValueError("Work log requires a grid with enough columns")
    quoted = "'" + tab.replace("'", "''") + "'"
    height = props["gridProperties"]["rowCount"]
    # Whole rows: AppendCells places new rows after data in any column, so the
    # planned tail must see notes beyond F as well.
    width = props["gridProperties"]["columnCount"]
    chunk_size = max(1, 25000 // width)
    rows = []
    for start in range(0, height, chunk_size):
        stop = min(start + chunk_size, height)
        result = book.fetch_sheet_metadata(params={
            "includeGridData": True, "ranges": f"{quoted}!{start + 1}:{stop}",
            "fields": f"sheets(data(rowData(values({cell_fields}))))",
        })
        data = result["sheets"][0].get("data") or [{}]
        block = data[0].get("rowData", [])
        rows.extend(r.get("values", []) for r in block)
        rows.extend([] for _ in range(stop - start - len(block)))
    return {"sheet_id": props["sheetId"], "row_count": height, "rows": rows}


def plan_monthly(snapshot, work_date, spec):
    rows = snapshot["rows"]
    if not rows or values(rows[0]) != list(spec.headers):
        raise ValueError("Unexpected work-log A:F headers")
    month = work_date.replace(day=1)
    expected = {label: (hours, amount) for label, hours, amount in spec.items}
    current, exemplars = [], {}
    for index, row in enumerate(rows[1:], start=1):
        v = values(row)
        if v[1] != spec.customer or v[3] not in expected:
            continue
        # Blank billing-month policies use the work date, never a missing A cell.
        row_month = cell_date(row[0] if spec.bill_month else row[2]).replace(day=1)
        if row_month == month:
            current.append((index, row))
        elif (v[4], v[5]) == expected[v[3]]:
            exemplars[v[3]] = row
    # An explicitly approved label rename can reuse historical native formats.
    # It never changes existing rows or treats the old label as a current item.
    for target, source in spec.format_item_aliases:
        if target in exemplars:
            continue
        for row in rows[1:]:
            v = values(row)
            if (v[1] == spec.customer and v[3] == source
                    and (v[4], v[5]) == expected[target]
                    and cell_date(row[0] if spec.bill_month else row[2]).replace(day=1) < month):
                exemplars[target] = row
    if current:
        if len(current) != len(spec.items) or sorted(values(r)[3] for _, r in current) != sorted(expected):
            raise ValueError("Partial or duplicate monthly work-log rows; inspect without appending")
        days = [cell_date(row[2]) for _, row in current]
        if len(set(days)) != 1 or days[0].replace(day=1) != month:
            raise ValueError("Conflicting work-log dates")
        for _, row in current:
            v = values(row)
            if (v[4], v[5]) != expected[v[3]] or (not spec.bill_month and v[0] not in (None, "")):
                raise ValueError("Existing monthly work-log values conflict; never overwrite")
        return {"action": "skip", "month": month.isoformat(), "work_date": days[0].isoformat(),
                "rows": [i + 1 for i, _ in current], "verified": True}
    if set(exemplars) != set(expected):
        raise ValueError("Matching historical work-log format exemplars are required")
    cells = []
    for label, hours, amount in spec.items:
        exemplar = (exemplars[label] + [{}] * 6)[:6]
        if any(c.get("dataValidation") or c.get("chipRuns") for c in exemplar):
            raise ValueError("Work-log exemplar has unsupported validation/chips")
        row = []
        raw_values = [(month - EPOCH).days if spec.bill_month else None, spec.customer,
                      (work_date - EPOCH).days, label, hours, amount]
        for raw, template in zip(raw_values, exemplar):
            cell = {"userEnteredFormat": deepcopy(template.get("userEnteredFormat", {}))}
            if raw is not None:
                cell["userEnteredValue"] = {"stringValue" if isinstance(raw, str) else "numberValue": raw}
            row.append(cell)
        cells.append({"values": row})
    start = max((i for i, row in enumerate(rows) if any(
        c.get("userEnteredValue") or c.get("effectiveValue") for c in row)), default=0) + 1
    return {"action": "append", "month": month.isoformat(), "work_date": work_date.isoformat(),
            "rows": list(range(start + 1, start + len(cells) + 1)), "cells": cells}


def _persist_receipt(receipt, binding, status, result):
    receipt.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(mode="w", dir=receipt.parent, encoding="utf-8", delete=False) as tmp:
        json.dump({"binding": binding, "status": status, "result": result}, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
    os.replace(tmp.name, receipt)


# HTTP statuses for which Sheets rejected the whole batch before applying it.
_REJECTED_STATUSES = {400, 401, 403, 404, 429}


def _rejected_status(exc):
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status if status in _REJECTED_STATUSES else None


def _read_receipt(receipt, binding):
    """Return the prior pending/verified receipt; a recorded rejection allows a retry."""
    if receipt is None or not receipt.exists():
        return None
    previous = json.loads(receipt.read_text())
    if (not isinstance(previous, dict) or previous.get("binding") != binding
            or previous.get("status") not in {"pending", "verified", "rejected"}
            or not isinstance(previous.get("result"), dict)):
        raise ValueError("Work-log receipt belongs to another operation or is invalid; inspect without retry")
    return None if previous["status"] == "rejected" else previous


def _write_once(book, requests, receipt, binding, plan):
    """Claim, then submit one batch without retry; record definite rejections."""
    _persist_receipt(receipt, binding, "pending", plan)
    try:
        batch_update_spreadsheet(book, requests, retry=False)
    except Exception as exc:
        status = _rejected_status(exc)
        if status is not None:
            _persist_receipt(receipt, binding, "rejected", {**plan, "rejection_http_status": status})
        raise


def _stable(state):
    """Snapshot without computed values, so volatile formulas do not look like edits."""
    return {**state, "rows": [[{k: v for k, v in c.items() if k != "effectiveValue"} for c in row]
                              for row in state["rows"]]}


def record_row_plan(book, operation, *, tab, apply=False, receipt_path=None):
    """Apply a caller-reviewed, fixed-row work-log plan with exact native readback.

    The caller owns request-level duplicate detection, amounts/formulas, approval
    and serialization. This preserves planned formula row references; unlike the
    monthly append-only API it uses a fixed row, not an atomic tail append.
    Never modify a plan or switch receipt paths to recover an unknown write.
    """
    fields = "userEnteredValue,userEnteredFormat,dataValidation,chipRuns"
    if operation.get("action") not in {"append", "complete", "skip"}:
        raise ValueError("Unsupported work-log row operation")
    row = operation.get("row")
    after, before = operation.get("after"), operation.get("before")
    if (type(row) is not int or row < 2 or not isinstance(after, list) or not after
            or not isinstance(before, list) or len(before) != len(after)):
        raise ValueError("Work-log plan requires one non-header row and equal native cell widths")
    width = len(after)

    def normalized(item):
        if isinstance(item, dict):
            return {k: normalized(v) for k, v in item.items() if v not in ({}, [], None)}
        if isinstance(item, list):
            return [normalized(v) for v in item]
        return item

    def cells(state):
        raw = state["rows"][row - 1] if row <= len(state["rows"]) else []
        return normalized((raw + [{}] * width)[:width])

    def read():
        state = snapshot(book, tab, cell_fields=fields, min_columns=width)
        if state["sheet_id"] != operation.get("sheet_id"):
            raise ValueError("Work-log worksheet identity changed")
        return state

    binding = {"spreadsheet_id": book.id, "tab": tab, "schema": "work-log-row/v1",
               "plan_hash": hashlib.sha256(json.dumps(operation, sort_keys=True, ensure_ascii=False).encode()).hexdigest()}
    receipt = Path(receipt_path).expanduser() if receipt_path else None
    previous = _read_receipt(receipt, binding)
    observed = read()
    done = cells(observed) == normalized(after)
    if previous and not done:
        raise RuntimeError("Prior work-log write is unresolved; inspect without retry")
    if not done:
        if operation["action"] == "skip" or cells(observed) != normalized(before):
            raise ValueError("Work-log row changed after preview")
        if operation["action"] == "append":
            last = max((i for i, r in enumerate(observed["rows"], 1) if any(
                c.get("userEnteredValue") for c in r)), default=1)
            if row != last + 1 or any(c.get("userEnteredValue") for c in before):
                raise ValueError("Work-log append target is no longer the empty tail")
    if not apply:
        return {"action": "skip" if done else operation["action"], "row": row, "verified": done}
    if receipt is None:
        raise ValueError("Applying a work-log row requires a private receipt path")
    if not done:
        if read() != observed:
            raise ValueError("Work log changed after preview; no write attempted")
        requests = []
        if row > observed["row_count"]:
            requests.append({"appendDimension": {"sheetId": observed["sheet_id"], "dimension": "ROWS",
                                                  "length": row - observed["row_count"]}})
        if operation["action"] == "append":
            requests.append({"updateCells": {"range": {"sheetId": observed["sheet_id"],
                "startRowIndex": row - 1, "endRowIndex": row, "startColumnIndex": 0, "endColumnIndex": width},
                "rows": [{"values": after}], "fields": fields}})
        else:
            for index, (old, new) in enumerate(zip(before, after)):
                if normalized(old) == normalized(new):
                    continue
                if normalized({k: v for k, v in old.items() if k != "userEnteredValue"}) != normalized(
                        {k: v for k, v in new.items() if k != "userEnteredValue"}):
                    raise ValueError("Completing a work-log row must preserve formats/validation/chips")
                requests.append({"updateCells": {"range": {"sheetId": observed["sheet_id"],
                    "startRowIndex": row - 1, "endRowIndex": row, "startColumnIndex": index, "endColumnIndex": index + 1},
                    "rows": [{"values": [new]}], "fields": "userEnteredValue"}})
        _write_once(book, requests, receipt, binding, operation)
        if cells(read()) != normalized(after):
            raise RuntimeError("Work-log row readback differs; inspect without retry")
    result = {"action": "skip" if done else operation["action"], "row": row, "verified": True}
    _persist_receipt(receipt, binding, "verified", result)
    return result


def record_monthly(book, work_date, spec, *, apply=False, receipt_path=None):
    before = snapshot(book, spec)
    plan = plan_monthly(before, work_date, spec)
    binding = {"spreadsheet_id": book.id, "tab": spec.tab, "customer": spec.customer,
               "month": work_date.replace(day=1).isoformat(), "bill_month": spec.bill_month,
               "items": [list(item) for item in spec.items]}
    receipt = Path(receipt_path).expanduser() if receipt_path else None
    previous = _read_receipt(receipt, binding)
    if previous and plan["action"] != "skip":
        raise RuntimeError("Prior work-log write is unresolved or its row disappeared; inspect without another append")
    expected = None
    if previous:
        expected = previous["result"].get("cells")
        if not isinstance(expected, list) or not expected:
            raise RuntimeError("Work-log receipt has no expected cells; inspect without another append")
        verify_cells(before, plan["rows"], expected)
    if not apply:
        return plan
    if receipt is None:
        raise ValueError("Applying a work-log append requires a private receipt path")
    def persist(status, result):
        _persist_receipt(receipt, binding, status, result)
    if plan["action"] == "skip":
        persist("verified", {**plan, "cells": expected or observed_cells(before, plan["rows"])})
        return {**plan, "verified": True}
    if _stable(snapshot(book, spec)) != _stable(before):
        raise ValueError("Work log changed after preview; no write attempted")
    # AppendCells lets Sheets choose the tail atomically; other callers cannot be
    # overwritten by an outdated row index. Never retry an unknown append.
    _write_once(book, [{"appendCells": {
        "sheetId": before["sheet_id"], "rows": plan["cells"],
        "fields": "userEnteredValue,userEnteredFormat",
    }}], receipt, binding, plan)
    observed = snapshot(book, spec)
    verified = plan_monthly(observed, work_date, spec)
    if verified["action"] != "skip":
        raise RuntimeError("Work-log readback failed; inspect without retry")
    verify_cells(observed, verified["rows"], plan["cells"])
    result = {**verified, "action": "appended", "verified": True}
    persist("verified", {**result, "cells": plan["cells"]})
    return result


def six_cells(cells):
    return (list(cells) + [{}] * 6)[:6]


def observed_cells(observed, row_numbers):
    keys = ("userEnteredValue", "userEnteredFormat")
    return [
        {"values": [{k: cell[k] for k in keys if k in cell} for cell in six_cells(observed["rows"][number - 1])]}
        for number in row_numbers
    ]


def verify_cells(observed, row_numbers, wanted_rows):
    if len(row_numbers) != len(wanted_rows):
        raise RuntimeError("Work-log Receipt row count differs; inspect without retry")
    for number, wanted in zip(row_numbers, wanted_rows):
        for cell, expected in zip(six_cells(observed["rows"][number - 1]), six_cells(wanted["values"])):
            if any(cell.get(k, {}) != expected.get(k, {}) for k in ("userEnteredValue", "userEnteredFormat")):
                raise RuntimeError("Work-log values/format readback differs; inspect without retry")
