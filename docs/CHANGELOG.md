# Changelog

Only user-impacting changes are listed here (feature additions, bug fixes, and behavior/spec changes). Minor wording edits are omitted.

### 2026-03-03 (v0.4.1) — Consolidated updates since last GitHub release

- Fixed JSON mode output stability in `scripts/query.py`: non-JSON log lines emitted by underlying calls are now captured instead of contaminating stdout
- Added `warnings` payload support for JSON responses (`list_*` and sync query paths)
- Preserved captured warning lines even when query/list execution raises exceptions (available in error `details.warnings`)
- Added regression tests for captured warnings on exception paths
- Expanded docs with GA4 alias usage (`"site": "corp"`), Spike Investigation template, and explicit note that delta comparison is computed outside one-shot CLI
- Expanded `megaton_lib.date_utils` with timezone-aware helpers: `now_in_tz()`, `previous_month_range()`, `month_start_months_ago()`, `previous_year_start()`, `month_suffix_months_ago()`
- Added month parsing/filter helpers for tabular workflows: `parse_year_month_series()`, `drop_current_month_rows()`, `select_recent_months()`
- Added `megaton_lib.gsc_utils`:
  - `aggregate_search_console_data()` for page normalization + metric aggregation
  - `deduplicate_queries()` for whitespace-variant query merge with weighted position
  - `filter_by_clinic_thresholds()` for clinic-specific row filtering
  - `force_text_on_numeric_column()` for Sheets-safe text handling
- Added `megaton_lib.table_utils`:
  - `apply_pattern_map()` and `classify_by_pattern_map()` for regex-based mapping/classification
- Expanded helper modules:
  - `megaton_lib.ga4_helpers.report_data_or_empty()` for stable GA4 DataFrame shape
  - `megaton_lib.sheets.replace_sheet_by_group_keys()` for monthly group refresh workflows
  - `megaton_lib.sheets.update_cells()` for batched A1 cell updates
- Added unit tests for the new helper modules and APIs

### 2026-02-25 (v0.4.0) — AI Agent DX improvements

- Added `--inline` CLI option: pass JSON params directly without a temp file
- Added site alias system (`configs/sites.json`): use `"site": "corp"` instead of full URLs/property IDs
- GA4 date columns now auto-convert to `YYYY-MM-DD` strings (instead of epoch ms in JSON output)
- Improved GA4 403 error messages: now shows property ID and suggests checking service account permissions
- CLI GSC queries now keep full URLs by default (`page_to_path=False`); use pipeline `transform: "page:path_only"` when needed
- Added CLAUDE.md and expanded AGENTS.md with available resources, filter syntax, recipes, and site aliases
- Added Site URL column to GA4 properties table in AGENTS.md

### 2026-02-21

- Added multilingual support to the Streamlit UI (Japanese and English)
- Unified internal representations in the filter/aggregation UI to be language-independent
- Added translation consistency tests

### 2026-02-16

- Added support for parameterized BigQuery queries (`query_bq()` / `get_bq_client()`)
- Standardized GA4 result handling to use `result.df`
- Added analysis helpers and extracted shared GA4 helpers
- Structured public API input validation and batch error reporting
- Improved the test foundation (pytest markers: `unit` / `contract` / `integration`)

### 2026-02-07

- Consolidated the CLI and added `scripts/query.py` (automatic source routing via `--params`)
- Removed `query_ga4.py`, `query_gsc.py`, and `query_bq.py`
- Unified Streamlit and CLI operation under a shared schema (`schema_version: "1.0"`)
- Added job management features (`--submit`, `--status`, `--result`, `--cancel`, `--list-jobs`)
- Added result pipeline options (`--where`, `--sort`, `--columns`, `--group-by`, `--aggregate`)

### 2026-02-06

- Migrated from Gradio to Streamlit
- Added BigQuery support in the Streamlit UI
- Increased the maximum row limit to 100,000

### 2026-02-04

- Built the Gradio UI and added CLI scripts

### 2026-02-03

- Added authentication checks
