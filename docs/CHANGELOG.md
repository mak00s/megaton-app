# Changelog

Only user-impacting changes are listed here (feature additions, bug fixes, and behavior/spec changes). Minor wording edits are omitted.

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
