"""Notebook facade: the ~15 names to learn first, in one import.

Curated lazy re-exports for notebook/script work::

    from megaton_lib.notebook import (
        get_ga4, query_gsc, wrap, resolve_date, start_report_run, show,
    )

Everything is imported lazily on first access, so optional dependencies
(gspread, BigQuery, ...) are only required when actually used.

| name              | what it is                                            |
|-------------------|-------------------------------------------------------|
| get_ga4/get_gsc   | initialized Megaton client (credential auto-routed)   |
| get_bq_client     | BigQuery client                                       |
| query_ga4/query_gsc/query_bq | one-call query -> DataFrame                |
| wrap              | put any DataFrame on the megaton chain API            |
| resolve_date/resolve_month | "prev-month-start" -> "2026-05-01" etc.      |
| read_sheet_table/save_sheet_table/upsert_or_skip | Sheets I/O      |
| start_report_run  | dates+client+tracker scaffold for reports             |
| fetch_for_sites   | multi-site GSC fetch                                  |
| fillna_int        | NaN->0 int conversion for metric columns              |
| show              | rich DataFrame display                                |

Also keeps the legacy ``init()`` helper (sys.path + creds env + reload).
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def init() -> Path:
    """Detect project root and initialize paths/env/modules.

    Returns:
        Project root path.
    """
    root = _find_project_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    os.environ["MEGATON_CREDS_PATH"] = str(root / "credentials")

    _reload_lib()

    return root


def _find_project_root() -> Path:
    """Find project root by searching upward for a ``credentials/`` directory."""
    d = Path.cwd()
    while d != d.parent:
        if (d / "credentials").exists():
            return d
        d = d.parent
    raise RuntimeError("Project root not found (no 'credentials/' directory in parents)")


def _reload_lib() -> None:
    """Reload project modules in dependency order."""
    import megaton_lib.date_template
    import megaton_lib.credentials
    import megaton_lib.megaton_client
    import megaton_lib.analysis
    import megaton_lib.sheets
    import megaton_lib.periods

    importlib.reload(megaton_lib.date_template)
    importlib.reload(megaton_lib.credentials)
    importlib.reload(megaton_lib.megaton_client)
    importlib.reload(megaton_lib.analysis)
    importlib.reload(megaton_lib.sheets)
    importlib.reload(megaton_lib.periods)

    from megaton_lib.megaton_client import reset_registry
    reset_registry()


# --- Curated lazy facade -----------------------------------------------------

_FACADE = {
    "get_ga4": ("megaton_lib.megaton_client", "get_ga4"),
    "get_gsc": ("megaton_lib.megaton_client", "get_gsc"),
    "get_bq_client": ("megaton_lib.megaton_client", "get_bq_client"),
    "query_ga4": ("megaton_lib.megaton_client", "query_ga4"),
    "query_gsc": ("megaton_lib.megaton_client", "query_gsc"),
    "query_bq": ("megaton_lib.megaton_client", "query_bq"),
    "wrap": ("megaton.start", "wrap"),
    "resolve_date": ("megaton_lib.dates", "resolve_date"),
    "resolve_month": ("megaton_lib.dates", "resolve_month"),
    "read_sheet_table": ("megaton_lib.sheets", "read_sheet_table"),
    "save_sheet_table": ("megaton_lib.sheets", "save_sheet_table"),
    "upsert_or_skip": ("megaton_lib.sheets", "upsert_or_skip"),
    "start_report_run": ("megaton_lib.report_run", "start_report_run"),
    "fetch_for_sites": ("megaton_lib.gsc_utils", "fetch_for_sites"),
    "fillna_int": ("megaton.transform", "fillna_int"),
    "show": ("megaton_lib.analysis", "show"),
}


def __getattr__(name: str):
    spec = _FACADE.get(name)
    if spec is None:
        raise AttributeError(f"module 'megaton_lib.notebook' has no attribute {name!r}")
    module = importlib.import_module(spec[0])
    return getattr(module, spec[1])


def __dir__():
    return sorted(set(globals()) | set(_FACADE))
