# Changelog

Only user-impacting changes are listed here (feature additions, bug fixes, and behavior/spec changes). Minor wording edits are omitted.

### 2026-03-28 (v0.7.0)

- Added shared validation helpers in `megaton_lib.validation`:
  - `adobe_analytics.py` for reusable Playwright-based AA beacon validation flows
  - `metadata.py` for stable result metadata and JSON save helpers
  - `storefront_runtime.py` for shared storefront auth, embed override, beacon capture, and ECID capture
  - expanded `playwright_pages.py` with config-driven Tags override builders and exact-match production launch replacement
- Added shared validation guidance and tooling:
  - `docs/VALIDATION.md` for shared-first validation policy, result schema, and override rules
  - `docs/templates/validation_thin_entrypoint.py` as the recommended starting point for analysis-repo validators
  - `scripts/check_validation_usage.py` to detect direct Playwright / raw route usage and missing validation metadata patterns
- Expanded credential auto-discovery and Adobe routing:
  - `megaton_lib.credentials` now distinguishes Google service-account JSON and Adobe OAuth JSON during `credentials/` scanning
  - Adobe Analytics can auto-discover multiple Adobe OAuth files from `ADOBE_CREDS_PATH` or local `credentials/` and resolve the right credential by `company_id`
  - Streamlit, CLI, and shared Adobe clients can use credential files as fallback to `ADOBE_CLIENT_ID` / `ADOBE_CLIENT_SECRET` / `ADOBE_ORG_ID`
- Added shared Adobe Tags workflow helpers in `megaton_lib.audit.providers.tag_config`:
  - `bootstrap.py` for OAuth/env/bootstrap config assembly
  - `build_workflow.py` for apply → refresh → build → verify → re-export flows
- Expanded site-mapping markdown parsing:
  - `parse_mapping_markdown()` now supports section scoping via `allowed_sections`
- Added tests for validation metadata handling
- Made Playwright imports optional in shared validation modules so CI/test environments without Playwright can still import `megaton_lib.validation`
- Updated GitHub Actions workflow dependencies to Node 24-compatible action versions
- Updated README / USAGE / AGENTS to document the validation shared-first policy and new guidance
- Added `AAQueryContext` class and `AdobeAnalyticsClient.query_context()` factory method
  - Eliminates repetitive passing of rsid/date_from/date_to/segment across chained report and breakdown calls
  - Supports `ctx.report()` and `ctx.breakdown()` with stored defaults
  - Subclassable for domain-specific composite queries (e.g. page → prop12 → child breakdown chains)

### 2026-03-08 (v0.6.1)

- Expanded Adobe Analytics query/list support:
  - Added `scripts/query.py --list-aa-segments` with optional name filter and definition expansion
  - AA query params now accept inline `segment_definition` and `breakdown` objects
- Expanded Playwright validation helpers:
  - Added `run_page()` and `TagsLaunchOverride` for reusable page checks with optional Adobe Tags launch override
  - Added `run_with_launch_override()` compatibility wrapper for legacy `satelliteLib-*.js` replacement flows
- Added shared validation and sync helpers:
  - `megaton_lib.validation` now exposes JSON path-contract checks and reusable Playwright page/delivery capture utilities
  - `megaton_lib.audit.providers.tag_config.sync` adds exported custom-code tree apply helpers for Adobe Tags
  - `megaton_lib.audit.providers.target.activities` adds Adobe Target activity export helpers
- Expanded Adobe Tags (Reactor) library operations in `megaton_lib.audit.providers.tag_config.adobe_tags`:
  - Added `list_rule_revisions()`, `list_library_resources()`, `revise_library_rules()`, `find_dirty_origin_rules()`, `build_library()`, and `deploy_library()`
  - Added 409-conflict recovery in `revise_library_rules()` by removing existing revision copies and retrying revise
  - Added warning log when a library rule has no `origin` relationship and is skipped in dirty-origin detection
- Updated Adobe Target Recommendations apply behavior:
  - Added `AdobeTargetClient.put()` and switched `apply_recs()` to use `PUT` for `designs` updates (PATCH path kept for other resources)
  - `apply_recs()` now strips server-managed metadata keys from outbound PATCH/PUT payloads before sending
  - `detect_getoffer_scope()` now auto-detects design/template names from delivery `responseTokens` / `option.content` and scopes design export when available
- Fixed Adobe Tags export stability:
  - `export_property()` no longer crashes on export startup
  - Exported Adobe Tags resource files now use `id_slug` basenames to avoid silent overwrite when names collide
- Unified `site` alias resolution across CLI, batch execution, and Streamlit `input/params.json` loading
- Expanded tests for Adobe Tags provider and Target Recs apply path, including metadata-strip and designs PUT behavior

### 2026-03-06 (v0.6.0)

- Added shared Adobe IMS OAuth layer (`megaton_lib/audit/providers/adobe_auth.py`):
  - `AdobeOAuthClient` handles `client_credentials` token flow with disk cache and auto-refresh
  - Shared by Adobe Analytics, Adobe Tags (Reactor), and Adobe Target clients
  - AA client (`aa.py`) refactored to delegate auth to `AdobeOAuthClient`
- Added Adobe Target Recommendations provider set (`megaton_lib/audit/providers/target/`):
  - `client.py` — `AdobeTargetClient` with GET/PATCH, pagination, retry, 401 re-auth
  - `recs.py` — `export_recs()` and `apply_recs()` with per-resource filters, design sidecar merge, metadata stripping
  - `feeds.py` — `export_feeds()` with sensitive field auto-redaction
  - `getoffer_scope.py` — getOffer scope detection from delivery captures + scoped export logic
- Refined Target export/scope behavior:
  - `getoffer_scope.detect_getoffer_scope()` now parses `option.content` JSON (`recs.activity`) as fallback for criteria/algorithm/activity ID extraction
  - `export_getoffer_scope()` now skips unscoped export when no scope filters are detected (returns empty `export_summary`)
  - `feeds.export_feeds()` redaction now includes `username` keys
- Refined Target criteria export/apply endpoint behavior:
  - `export_recs()` now resolves criteria sub-type detail endpoints (e.g. `/criteria/popularity/{id}`) to export full criteria configuration
  - `apply_recs()` now uses PUT for resolved criteria sub-type endpoints and falls back to PATCH on generic `/criteria/{id}` when sub-type is unknown
- Fixed Target getOffer scoped export behavior:
  - `export_getoffer_scope(..., include_designs=True, designs_name_regex=...)` now always applies explicit `designs_name_regex` as override when provided
- Expanded Target design apply compatibility:
  - `apply_recs()` now resolves design sidecar templates from both flat layout and `designs/code/` layout (`<id>.<ext>`, `<stem>.<ext>`, `code/<stem>.<ext>`, `code/<id>.<ext>`)
- Extended Adobe Tags (Reactor) auth to support OAuth alongside legacy bearer token:
  - `AdobeTagsConfig.oauth` field added
  - Config loader supports both `oauth: { ... }` and `oauth: true` shorthand
- Improved Streamlit table UX:
  - Extracted table/date formatting logic into `app/ui/table_format.py`
  - Added configurable table display options (date format, thousands separators, decimals, per-column type hints)
  - Added `column_types` support in params schema/validator/query builder
  - Added config templates and ignore rules for local column-type hints
- Added and expanded tests for Adobe OAuth, Target providers, table formatting, and datetime axis detection

### 2026-03-06 (v0.5.0)

- Expanded Adobe Analytics support in Streamlit:
  - Company ID and RSID are now auto-discovered and selectable from dropdowns
  - Dimension and metric options are fetched from AA metadata APIs (with manual input fallback)
  - Removed Org ID input from the UI (uses runtime config/env as internal auth context)
- Hardened AA API handling in the shared client:
  - Added support for array-style JSON payloads from `/dimensions` and `/metrics`
  - Improved paging stop conditions (`lastPage` default handling + `totalPages/number` fallback)
  - Discovery requests no longer require proxy company header
  - Refactored duplicated dimension/metric catalog fetch logic into a shared implementation
- Reduced Streamlit validation side effects by removing per-rerun `importlib.reload()` of the params validator
- Updated site alias configuration for public-repo safety:
  - Added `configs/sites.example.json` template
  - Switched CLI alias loading to layered config resolution:
    `sites.example.json` < `sites.json` < `sites.local.json`
  - Stopped tracking `configs/sites.json` in Git; treat `sites.json` / `sites.local.json` as local files
- Updated docs (`README.md`, `docs/USAGE.md`, `docs/REFERENCE.md`, `AGENTS.md`) to reflect AA UI behavior and local alias config policy

### 2026-03-05 (v0.4.2)

- Expanded default credentials discovery in `megaton_lib.credentials`:
  - If `credentials/` is not found from CWD/parent traversal, fallback to package-parent `megaton-app/credentials/`
- Improved Streamlit query execution feedback:
  - Run button now shows running state and prevents duplicate clicks while processing
- Improved Streamlit chart UX:
  - Datetime-like columns are auto-selected as default X-axis candidates
  - Added optional series split (pivoted multi-series charts by second dimension)
  - Added top-series cap for readability when category cardinality is high
- Updated authentication resolution docs in `docs/REFERENCE.md` to include full fallback order
- Updated docs (`README.md`, `docs/USAGE.md`, `docs/REFERENCE.md`) for new Streamlit behavior

### 2026-03-03 (v0.4.1) — Consolidated updates since last GitHub release

- Fixed JSON mode output stability in `scripts/query.py`: non-JSON log lines emitted by underlying calls are now captured instead of contaminating stdout
- Added `warnings` payload support for JSON responses (`list_*` and sync query paths)
- Preserved captured warning lines even when query/list execution raises exceptions (available in error `details.warnings`)
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
  - `megaton_lib.ga4_helpers.collect_site_frames()` for shared per-site GA4 collection loops
  - `megaton_lib.ga4_helpers.run_report_data_or_empty()` for stable report-data extraction
  - `megaton_lib.ga4_helpers.run_report_merge()` for multi-report merges by shared keys
  - `megaton_lib.ga4_helpers.merge_dataframes()` for ordered DataFrame merges with optional int coercion
  - `megaton_lib.sheets.read_sheet_table()` for worksheet-to-DataFrame loading
  - `megaton_lib.sheets.load_pattern_map()` for loading regex maps from Sheets
  - `megaton_lib.sheets.replace_sheet_by_group_keys()` for monthly group refresh workflows
  - `megaton_lib.sheets.update_cells()` for batched A1 cell updates
- Added `megaton_lib.traffic`:
  - `normalize_domain()` for source-domain normalization
  - `ensure_trailing_slash()` for URL path normalization
  - `apply_source_normalization()` for regex-based source normalization
  - `classify_channel()` for channel reclassification heuristics
  - `reclassify_source_channel()` for paired source/channel reclassification with configurable regex rules
- Expanded meaningful branch coverage tests for `scripts/query.py` (batch/list/job/alias/error paths) to satisfy CI coverage gate

### 2026-02-25 (v0.4.0) — AI Agent DX improvements

- Added `--inline` CLI option: pass JSON params directly without a temp file
- Added site alias system (`configs/sites.json`): use `"site": "corp"` instead of full URLs/property IDs
- GA4 date columns now auto-convert to `YYYY-MM-DD` strings (instead of epoch ms in JSON output)
- Improved GA4 403 error messages: now shows property ID and suggests checking service account permissions
- CLI GSC queries now keep full URLs by default (`page_to_path=False`); use pipeline `transform: "page:path_only"` when needed

### 2026-02-21

- Added multilingual support to the Streamlit UI (Japanese and English)
- Unified internal representations in the filter/aggregation UI to be language-independent

### 2026-02-16

- Added support for parameterized BigQuery queries (`query_bq()` / `get_bq_client()`)
- Standardized GA4 result handling to use `result.df`
- Added analysis helpers and extracted shared GA4 helpers
- Structured public API input validation and batch error reporting

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
