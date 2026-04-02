"""Generic execution tracking and sheet validation helpers for reports."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from .sheets import (
    duplicate_sheet_into,
    read_sheet_table,
    replace_sheet_by_group_keys,
    save_sheet_from_template,
    save_sheet_table,
    update_cells,
    upsert_or_skip,
)


def _github_run_url() -> str:
    server = os.getenv("GITHUB_SERVER_URL", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    run_id = os.getenv("GITHUB_RUN_ID", "").strip()
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return ""


def _now_jst_iso() -> str:
    return datetime.now(pytz.timezone("Asia/Tokyo")).isoformat()


def _normalize_summary_scalar(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def _pick_date_column(df: pd.DataFrame) -> str:
    for col in ["date", "month", "ym", "yearMonth", "published"]:
        if col in df.columns:
            return col
    return ""


def _summarize_df(df: pd.DataFrame | None) -> dict:
    if not isinstance(df, pd.DataFrame):
        return {"rows": None, "date_column": "", "min_value": "", "max_value": ""}
    if len(df) == 0:
        return {"rows": 0, "date_column": "", "min_value": "", "max_value": ""}

    date_column = _pick_date_column(df)
    min_value = ""
    max_value = ""
    if date_column:
        series = df[date_column].dropna()
        if len(series) > 0:
            try:
                min_value = _normalize_summary_scalar(series.min())
                max_value = _normalize_summary_scalar(series.max())
            except Exception:
                min_value = ""
                max_value = ""

    return {
        "rows": int(len(df)),
        "date_column": date_column,
        "min_value": min_value,
        "max_value": max_value,
    }


def _sheet_id_from_url(gs_url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", gs_url or "")
    return match.group(1) if match else ""


def _is_quota_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return "quota" in text or "[429]" in text or "too many requests" in text


def init_report_tracker(
    report_name: str,
    *,
    logger: logging.Logger | None = None,
    **window_values,
):
    tracker = ExecutionTracker(
        run_summary_path=os.getenv("MEGATON_RUN_SUMMARY_PATH", "").strip(),
        report_name=report_name,
        logger=logger,
    )
    if window_values:
        tracker.set_run_window(**window_values)
    else:
        tracker.persist()
    return tracker


def finish_report_tracker(
    tracker: "ExecutionTracker",
    *,
    status: str = "skipped",
    notes=None,
    errors=None,
):
    tracker.set_validation_summary(status, list(notes or []), list(errors or []))
    tracker.print_execution_summary()


@dataclass
class ExecutionTracker:
    """Track report writes and expose cached read helpers for validation."""

    run_summary_path: str = ""
    report_name: str = "report"
    logger: logging.Logger | None = None
    run_summary: dict = field(init=False)
    frame_cache: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    cell_update_cache: dict[tuple[str, str], dict] = field(default_factory=dict)
    remote_df_cache: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    remote_cell_cache: dict[tuple[str, str, str], str] = field(default_factory=dict)
    remote_state: dict[str, str | None] = field(default_factory=lambda: {"url": None, "sheet": None})

    def __post_init__(self):
        self.logger = self.logger or logging.getLogger(__name__)
        self.run_summary = {
            "report": self.report_name,
            "status": "running",
            "started_at_jst": _now_jst_iso(),
            "finished_at_jst": "",
            "window": {},
            "run_url": _github_run_url(),
            "entries": [],
            "validation": {
                "status": "pending",
                "notes": [],
                "errors": [],
            },
        }

    def persist(self):
        if not self.run_summary_path:
            return
        path = Path(self.run_summary_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.run_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def set_run_window(self, **window_values):
        self.run_summary["window"] = dict(window_values)
        self.persist()

    def record_sheet_event(
        self,
        gs_url: str,
        sheet_name: str,
        mode: str,
        *,
        df: pd.DataFrame | None = None,
        note: str = "",
        cell_updates: dict | None = None,
    ):
        key = (gs_url, sheet_name)
        if isinstance(df, pd.DataFrame):
            self.frame_cache[key] = df.copy()
        if cell_updates:
            self.cell_update_cache.setdefault(key, {}).update(dict(cell_updates))

        df_summary = _summarize_df(df)
        entry = {
            "target_url": gs_url,
            "spreadsheet_id": _sheet_id_from_url(gs_url),
            "sheet_name": sheet_name,
            "mode": mode,
            "rows": df_summary["rows"],
            "date_column": df_summary["date_column"],
            "min_value": df_summary["min_value"],
            "max_value": df_summary["max_value"],
            "cell_updates": cell_updates or {},
            "note": note,
        }
        self.run_summary["entries"].append(entry)
        self.persist()

    def set_validation_summary(self, status: str, notes=None, errors=None):
        self.run_summary["validation"] = {
            "status": status,
            "notes": list(notes or []),
            "errors": list(errors or []),
        }
        self.run_summary["status"] = {
            "passed": "success",
            "failed": "failed",
            "skipped": "skipped",
        }.get(status, "running")
        self.run_summary["finished_at_jst"] = _now_jst_iso()
        self.persist()

    def print_execution_summary(self):
        print("Execution summary:")
        for entry in self.run_summary["entries"]:
            date_part = ""
            if entry["date_column"]:
                date_part = (
                    f" {entry['date_column']}={entry['min_value']}..{entry['max_value']}"
                )
            rows_part = "" if entry["rows"] is None else f" rows={entry['rows']}"
            note_part = f" note={entry['note']}" if entry["note"] else ""
            cell_part = ""
            if entry["cell_updates"]:
                cell_part = f" cells={','.join(entry['cell_updates'].keys())}"
            print(
                f"  - {entry['spreadsheet_id']}:{entry['sheet_name']} "
                f"[{entry['mode']}]{rows_part}{date_part}{cell_part}{note_part}"
            )
        print(f"Validation status: {self.run_summary['validation']['status']}")
        if self.run_summary["run_url"]:
            print(f"GitHub run: {self.run_summary['run_url']}")

    @staticmethod
    def normalize_sheet_df(df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return pd.DataFrame()
        out = df.copy()
        out.columns = [str(col).strip() for col in out.columns]
        mask = out.fillna("").astype(str).apply(
            lambda row: any(str(cell).strip() for cell in row),
            axis=1,
        )
        return out.loc[mask].reset_index(drop=True)

    def _with_retry(self, label: str, func, max_attempts: int = 4):
        wait = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                return func()
            except Exception as exc:
                if attempt >= max_attempts or not _is_quota_error(exc):
                    raise
                self.logger.warning(
                    "%s hit Sheets quota; retrying in %.1fs (%s/%s)",
                    label,
                    wait,
                    attempt,
                    max_attempts,
                )
                time.sleep(wait)
                wait *= 2

    def _select_sheet(self, mg, gs_url: str, sheet_name: str):
        if self.remote_state["url"] != gs_url:
            opened = self._with_retry(
                f"open workbook {gs_url}",
                lambda: mg.open.sheet(gs_url),
            )
            if not opened:
                raise RuntimeError(f"Failed to open workbook: {gs_url}")
            self.remote_state["url"] = gs_url
            self.remote_state["sheet"] = None

        if self.remote_state["sheet"] != sheet_name:
            selected = self._with_retry(
                f"select sheet {sheet_name}",
                lambda: mg.sheets.select(sheet_name),
            )
            if not selected:
                raise RuntimeError(f"Sheet not found: {sheet_name}")
            self.remote_state["sheet"] = sheet_name

    def _read_sheet_df_remote(self, mg, gs_url: str, sheet_name: str) -> pd.DataFrame:
        key = (gs_url, sheet_name)
        if key not in self.remote_df_cache:
            def _read():
                self._select_sheet(mg, gs_url, sheet_name)
                return self.normalize_sheet_df(pd.DataFrame(mg.gs.sheet.data))

            self.remote_df_cache[key] = self._with_retry(
                f"read sheet {sheet_name}",
                _read,
            )
        return self.remote_df_cache[key].copy()

    def open_sheet_df(self, mg, gs_url: str, sheet_name: str, *, prefer_local: bool = True) -> pd.DataFrame:
        key = (gs_url, sheet_name)
        if prefer_local and key in self.frame_cache:
            return self.normalize_sheet_df(self.frame_cache[key])
        return self._read_sheet_df_remote(mg, gs_url, sheet_name)

    def read_sheet_df(self, mg, gs_url: str, sheet_name: str) -> pd.DataFrame:
        try:
            return self.normalize_sheet_df(
                read_sheet_table(mg, sheet_url=gs_url, sheet_name=sheet_name)
            )
        except Exception:
            return pd.DataFrame()

    def save_sheet(
        self,
        mg,
        *,
        gs_url: str,
        sheet_name: str,
        df: pd.DataFrame,
        sort_by=None,
        note: str = "",
        start_row: int | None = None,
    ) -> bool:
        saved = save_sheet_table(
            mg,
            sheet_url=gs_url,
            sheet_name=sheet_name,
            df=df,
            sort_by=sort_by,
            start_row=start_row,
        )
        if saved:
            self.record_sheet_event(
                gs_url,
                sheet_name,
                "overwrite",
                df=df,
                note=note or (f"sort_by={sort_by}" if sort_by else ""),
            )
        return saved

    def duplicate_sheet(
        self,
        mg,
        *,
        gs_url: str,
        source_sheet_name: str,
        new_sheet_name: str,
        cell_update: dict[str, str] | None = None,
    ) -> bool:
        duplicated = duplicate_sheet_into(
            mg,
            sheet_url=gs_url,
            source_sheet_name=source_sheet_name,
            new_sheet_name=new_sheet_name,
            cell_update=cell_update,
        )
        if duplicated:
            self.record_sheet_event(
                gs_url,
                new_sheet_name,
                "duplicate",
                note=f"source={source_sheet_name}",
                cell_updates={cell_update["cell"]: cell_update["value"]} if cell_update else None,
            )
        return duplicated

    def update_sheet_cells(self, mg, *, gs_url: str, cells_to_update: dict[str, dict[str, str]]):
        for sheet_name, updates in cells_to_update.items():
            update_cells(mg, sheet_url=gs_url, sheet_name=sheet_name, values=updates)
            self.record_sheet_event(
                gs_url,
                sheet_name,
                "cell_update",
                cell_updates=updates,
            )

    def upsert_sheet(
        self,
        mg,
        *,
        gs_url: str,
        sheet_name: str,
        df: pd.DataFrame,
        keys: list[str],
        sort_by: list[str] | None = None,
        note: str = "",
        **kwargs,
    ) -> bool:
        if not mg.open.sheet(gs_url):
            raise RuntimeError(f"Could not open sheet URL: {gs_url}")
        updated = upsert_or_skip(
            mg,
            sheet_name,
            df,
            keys=keys,
            sort_by=sort_by,
            **kwargs,
        )
        if updated:
            self.record_sheet_event(
                gs_url,
                sheet_name,
                "upsert",
                df=df,
                note=note or f"keys={','.join(keys)}",
            )
        return updated

    def append_sheet(
        self,
        mg,
        *,
        gs_url: str,
        sheet_name: str,
        df: pd.DataFrame,
        note: str = "",
    ) -> bool:
        if not mg.open.sheet(gs_url):
            raise RuntimeError(f"Could not open sheet URL: {gs_url}")
        mg.append.to.sheet(sheet_name=sheet_name)
        self.record_sheet_event(
            gs_url,
            sheet_name,
            "append",
            df=df,
            note=note,
        )
        return True

    def save_sheet_from_template(
        self,
        mg,
        *,
        gs_url: str,
        sheet_name: str,
        df: pd.DataFrame,
        start_row: int = 3,
        template_sheet: str | None = None,
        template_regex: str = r"^\d{6}$",
        note: str = "",
        **kwargs,
    ) -> bool:
        if not mg.open.sheet(gs_url):
            raise RuntimeError(f"Could not open sheet URL: {gs_url}")
        saved = save_sheet_from_template(
            mg,
            sheet_name,
            df,
            start_row=start_row,
            template_sheet=template_sheet,
            template_regex=template_regex,
            **kwargs,
        )
        if saved:
            self.record_sheet_event(
                gs_url,
                sheet_name,
                "template_save",
                df=df,
                note=note or f"start_row={start_row}",
            )
        return saved

    def replace_sheet_groups(
        self,
        mg,
        *,
        gs_url: str,
        sheet_name: str,
        df_new: pd.DataFrame,
        remove_group_keys: list[str],
        sort_by: list[str],
        columns: list[str],
        note: str = "",
    ) -> pd.DataFrame:
        out = replace_sheet_by_group_keys(
            mg,
            sheet_url=gs_url,
            sheet_name=sheet_name,
            df_new=df_new,
            remove_group_keys=remove_group_keys,
            sort_by=sort_by,
            columns=columns,
        )
        self.record_sheet_event(
            gs_url,
            sheet_name,
            "replace_groups",
            df=out,
            note=note or f"group_keys={','.join(remove_group_keys)}",
        )
        return out

    def get_sheet_cells(
        self,
        mg,
        gs_url: str,
        sheet_name: str,
        cells: list[str],
        *,
        prefer_local: bool = False,
    ) -> dict[str, str]:
        key = (gs_url, sheet_name)
        out: dict[str, str] = {}
        pending: list[str] = []
        local_updates = self.cell_update_cache.get(key, {}) if prefer_local else {}

        for cell in cells:
            cache_key = (gs_url, sheet_name, cell)
            if cell in local_updates:
                out[cell] = _normalize_summary_scalar(local_updates[cell]).strip()
            elif cache_key in self.remote_cell_cache:
                out[cell] = self.remote_cell_cache[cache_key]
            else:
                pending.append(cell)

        if pending:
            def _read_pending():
                self._select_sheet(mg, gs_url, sheet_name)
                if hasattr(mg.gs.sheet._driver, "batch_get"):
                    ranges = mg.gs.sheet._driver.batch_get(pending)
                    values = {}
                    for cell, value_range in zip(pending, ranges):
                        cell_value = ""
                        if value_range and value_range[0]:
                            cell_value = str(value_range[0][0]).strip()
                        values[cell] = cell_value
                    return values

                values = {}
                for cell in pending:
                    values[cell] = str(mg.gs.sheet.cell.select(cell) or "").strip()
                return values

            fetched = self._with_retry(
                f"read cells {sheet_name}",
                _read_pending,
            )
            for cell, value in fetched.items():
                self.remote_cell_cache[(gs_url, sheet_name, cell)] = value
                out[cell] = value

        return out
