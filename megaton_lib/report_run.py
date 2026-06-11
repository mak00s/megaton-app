"""Report-run scaffold: bundles per-report boilerplate into one object.

Replaces the hand-written sequence every report repeats: resolve dates →
init GA4 client → init tracker → ... → finish tracker.

Notebook (jupytext percent) usage — begin/end pair, cell-friendly::

    from megaton_lib.report_run import start_report_run

    run = start_report_run(
        "slqm",
        property_id=PROPERTY_ID,
        start_date=START_DATE,      # template ("prev-month-start") or absolute
        end_date=END_DATE,
    )
    mg = run.mg
    # ... report cells ...
    run.save_sheet(gs_url=SHEET_URL, sheet_name="_page", df=df)
    run.finish()

Script usage — context manager (failure auto-recorded)::

    with start_report_run("slqm", property_id=...) as run:
        ...

Delivery hooks (Box backup, Gmail draft, ...) stay in the calling repo::

    run.on_finish(lambda r: backup_to_box(...))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from megaton_lib.dates import resolve_date
from megaton_lib.report_validation import (
    ExecutionTracker,
    finish_report_tracker,
    init_report_tracker,
)

__all__ = ["ReportRun", "start_report_run"]


@dataclass
class ReportRun:
    """A started report run: client + tracker + collected notes/errors."""

    name: str
    tracker: ExecutionTracker
    mg: Any = None
    start_date: str | None = None
    end_date: str | None = None
    write_enabled: bool = True
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    _on_finish: list[Callable[["ReportRun"], None]] = field(default_factory=list)
    _finished: bool = False

    # --- collectors / hooks -------------------------------------------------

    def note(self, message: str) -> None:
        self.notes.append(str(message))

    def error(self, message: str) -> None:
        self.errors.append(str(message))

    def on_finish(self, callback: Callable[["ReportRun"], None]) -> None:
        """Register a delivery hook called after the tracker summary is final."""
        self._on_finish.append(callback)

    # --- tracker conveniences (mg is passed automatically) ------------------

    def save_sheet(self, **kwargs):
        return self.tracker.save_sheet(self.mg, **kwargs)

    def upsert_sheet(self, **kwargs):
        return self.tracker.upsert_sheet(self.mg, **kwargs)

    def read_sheet_df(self, **kwargs):
        return self.tracker.read_sheet_df(self.mg, **kwargs)

    # --- lifecycle -----------------------------------------------------------

    def finish(self, *, status: str | None = None, notes=None, errors=None) -> None:
        """Finalize the tracker summary. Idempotent.

        Status defaults to "failed" when any error was collected, else "passed".
        """
        if self._finished:
            return
        self._finished = True
        all_notes = self.notes + list(notes or [])
        all_errors = self.errors + list(errors or [])
        resolved_status = status or ("failed" if all_errors else "passed")
        finish_report_tracker(
            self.tracker,
            status=resolved_status,
            notes=all_notes,
            errors=all_errors,
        )
        for callback in self._on_finish:
            callback(self)

    def __enter__(self) -> "ReportRun":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None and not self._finished:
            self.error(f"{exc_type.__name__}: {exc}")
            self.finish(status="failed")
        else:
            self.finish()
        return False  # never swallow exceptions


def start_report_run(
    report_name: str,
    *,
    property_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    set_report_dates: bool = True,
    write_enabled: bool = True,
    logger: logging.Logger | None = None,
    **window_values,
) -> ReportRun:
    """Start a tracked report run (dates resolved, client ready, tracker open).

    Args:
        report_name: Tracker name (e.g. "slqm").
        property_id: When given, ``run.mg`` is an initialized GA4 client
            (``megaton_client.get_ga4``).
        start_date / end_date: Template ("prev-month-start") or absolute
            dates; resolved values are stored on the run and in the tracker
            window as report_start / report_end.
        set_report_dates: When True (default) and both property_id and dates
            are given, ``mg.report.set.dates(start, end)`` is called.
        write_enabled: Forwarded to the tracker (False = log-only mode).
        **window_values: Extra tracker window entries
            (e.g. ``rolling_13m_from=...``).
    """
    resolved_start = resolve_date(start_date) if start_date else None
    resolved_end = resolve_date(end_date) if end_date else None

    window = dict(window_values)
    if resolved_start:
        window.setdefault("report_start", resolved_start)
    if resolved_end:
        window.setdefault("report_end", resolved_end)

    mg = None
    if property_id:
        from megaton_lib.megaton_client import get_ga4

        mg = get_ga4(str(property_id))
        if set_report_dates and resolved_start and resolved_end:
            mg.report.set.dates(resolved_start, resolved_end)

    tracker = init_report_tracker(
        report_name,
        logger=logger,
        write_enabled=write_enabled,
        **window,
    )
    return ReportRun(
        name=report_name,
        tracker=tracker,
        mg=mg,
        start_date=resolved_start,
        end_date=resolved_end,
        write_enabled=write_enabled,
    )
