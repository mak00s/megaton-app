# Changelog

Only user-impacting changes are listed here (feature additions, bug fixes, and behavior/spec changes). Minor wording edits are omitted.

## 2026-05-02 (v0.11.0)

- Added `megaton_lib.gmail_client`, a shared Gmail API wrapper for readonly message lookup, attachment extraction, OAuth token refresh, and draft creation.
- Added `megaton_lib.report_gmail_draft`, a reusable post-report Gmail draft helper and module CLI (`python -m megaton_lib.report_gmail_draft`) that reads report execution summaries, validates success, fills report templates, and creates Gmail drafts from `GMAIL_DRAFT_*` / `{PREFIX}_GMAIL_DRAFT_*` environment settings.
- Gmail OAuth token files written by the interactive helper are now saved with owner-only permissions, and draft creation uses the minimum `gmail.compose` scope.

## 2026-05-01 (v0.10.0)

- Added `analysis_tags_workspace_main()` for analysis repos that need the standard Adobe Tags workspace CLI plus repo-local credentials and per-account token caches.
- Added `build_repo_tags_config_factory()`, `account_token_cache_file()`, and `resolve_first_existing_path()` to reduce duplicated `tags/__main__.py` and credential factory code across CSK / WWS / DMS analysis repos.
- Added package extras for UI, notebook, Google, validation, audit, dev, and full checkout-local installs; documented that `scripts/` and `app/` remain checkout-local entrypoints while `pip install -e .` installs `megaton_lib`.
- Made `megaton_lib.query_runner` the shared source execution path behind `scripts/query.py`, and documented the BigQuery / Sheets helper boundaries to avoid parallel APIs drifting.
- Updated `save_sheet_table()` to use megaton's public `mg.sheet.*` formatting helpers for optional row/column sizing, gridline visibility, and tab color.
- Moved direct-gspread and Sheets batchUpdate helpers into `megaton_lib.gspread_lowlevel`, with short canonical helper names and compatibility re-exports from `megaton_lib.sheets`.
- Replaced query-runner monkey-patching with explicit query executor injection, added GSC `page_to_path` params support, and fixed BigQuery `ensure_table()` `created` metadata.
- Updated shared Playwright page helpers to use stealth defaults for Adobe Analytics validation traffic, with `stealth=False` and `user_agent=...` opt-outs on the common wrappers.

## 2026-04-25 (v0.9.0)

- Added library-scope Adobe Tags workspace helpers for analysis repo wrappers:
  - destructive `checkout`, non-destructive `pull`, drift `status`, fast local `status --since-pull`, explicit `add`, guarded `push`, build-only, and `full-export`
  - bounded remote snapshot parallelism via `TAGS_SNAPSHOT_WORKERS` or wrapper `--workers`
  - structured result JSON with schema, severity, exit code, grouped warnings, summary-only output, and stderr progress
- Added shared account env bootstrap and a reusable `tags_workspace_main()` runner for thin `python -m tags` wrappers.
- Added `.tag-conflicts.json` helpers and CLI support for conflict list/show/resolve, including baseline/local/remote diffs backed by saved `baseline_text`.
- Normalized `tags conflict --list --format json` to the workspace result schema, and made explicit `--account` bootstrap override stale account env values.
- Separated outside-scope local files from remote-removed files in workspace warnings; dry-run `push` now returns outside-scope findings as structured output, while apply mode still aborts before mutation.
- Documented agent gotchas, workspace mode values, schema-version compatibility expectations, and CSK snapshot-worker benchmark results.
- Exported the new workspace helpers from `megaton_lib.audit.providers.tag_config`.

## 2026-04-23

- Hardened Adobe Tags export/apply safety and operator clarity:
  - export now refreshes `.apply-baseline.json` for stale-base detection
  - apply now blocks stale-base conflicts by default while auto-skipping remote-only drift
  - auto-build Step 5 re-export now defaults to `rules,data-elements` to reduce metadata noise
  - CLI help/output now explains default workflow, re-export scope, and stale-base behavior without requiring README lookup
  - CLI now exits `3` when post-build marker verification fails and `4` on stale-base conflicts

## 2026-04-23 (v0.8.2)

- Fixed Adobe Target `fetch_activity` for XT / options-backed activities:
  - now uses Admin API v3 and falls back from `ab` to `xt` on 404, resolving the v1 `409 Cannot access activity with options in this version of API` failure
  - non-404 errors bubble up immediately without trying the alternate activity type
- Added `AdobeTargetClient.with_accept_header()` to spawn a sibling client that reuses auth / session / retry settings with a different `Accept` header

## 2026-04-11 (v0.8.1)

- Fixed CI compatibility for `tests/test_ga4_helpers.py`:
  - `TestToDatetimeCol.test_converts_date_column` no longer assumes the source column keeps pandas `object` dtype
  - accepts pandas string-backed dtypes while still verifying that `to_datetime_col()` returns a converted copy
  - restores GitHub Actions compatibility on Python 3.12 / newer pandas combinations

## 2026-04-10 (v0.8.0)

Summary since `v0.7.0`:

- Expanded shared validation/session APIs in `megaton_lib.validation`:
  - `AppMeasurementCapture` and `execute_appmeasurement_scenario()` for multi-step AA beacon scenarios
  - `run_page_session()` and `run_storefront_validation_session()` for longer Playwright flows with cookie/session reuse
  - Adobe credential/file helpers plus `run_aa_api_followup_verifier()` and `finalize_followup_verification()` for follow-up verification flows
  - auth-profile and pending-task helpers, with shared CLI entrypoint `python scripts/check_pending_verifications.py`
- Added Adobe Analytics operational helpers:
  - `ClassificationsClient` plus `verify_classification.py` CLI for export/import/verification workflows
  - `AdobeDataWarehouseClient` plus `dw.cli` helpers for scheduled-request search, describe, dry-run, and create flows
- Expanded tag-management support:
  - GTM / Adobe Tags shared export CLI helpers
  - Adobe Tags sidecar sync for both custom code and data-element settings
  - GTM audit export now reports `has_changes` from full-container diffs
- Expanded Adobe Target Recommendations support:
  - export prune mode for full unfiltered snapshots
  - improved criteria/design apply behavior and sidecar compatibility
- Added user-facing quality-of-life helpers:
  - `read_sheet_table(..., header_row=...)` and stricter sheet-loading options
  - report tracker dry-run write skipping
  - `megaton_lib.docs_sites` helpers for generated Markdown table maintenance
- Added broad regression coverage across validation, AA classifications, DW scheduling, GTM/Adobe Tags sync, sheets, and tracker flows

Detailed changes by date:

### 2026-04-10

- Expanded shared validation APIs in `megaton_lib.validation`:
  - `AppMeasurementCapture` plus `execute_appmeasurement_scenario()` for reusable multi-step AA beacon scenarios
  - `run_page_session()` as the higher-level browser/context/page session primitive for long Playwright flows
  - `run_storefront_validation_session()` plus storefront session cookie load/save helpers for storefront validation scripts
  - `aa_api.py` with `resolve_adobe_credentials_path()`, `build_adobe_analytics_client()`, and `run_aa_api_followup_verifier()`
  - `finalize_followup_verification()` for shared follow-up metadata/save/task-complete flows
- Expanded shared AA runner behavior:
  - AppMeasurement `b/ss` parsing now supports POST body payloads in addition to query strings
  - `run_aa_validation()` now supports named hooks such as `bootstrapPage`, `captureRuntime`, `storageState`, `viewport`, and `ignoreHttpsErrors`
  - AA beacon summaries now include `pageUrl`, `prop1`, `eVar1`, and `linkName`
- Expanded Adobe Tags export/apply support in `megaton_lib.audit.providers.tag_config`:
  - export now writes data-element settings sidecars
  - apply now supports both custom-code sidecars and data-element settings sidecars via `apply_exported_changes_tree()`
  - added generic component/data-element settings helpers for direct PATCH-based updates
- Added shared pending follow-up task management:
  - `scripts/check_pending_verifications.py` as the reusable CLI entrypoint for overdue/all/complete task flows
  - auth-profile helpers for named validation credentials in one local JSON store
- Expanded Adobe Target Recommendations export:
  - `export_recs(..., prune=True)` can delete stale files after a full unfiltered export
- Added regression tests for the new validation/session/follow-up APIs and Adobe Tags settings sync flows

### 2026-04-05

- Added Adobe Analytics Data Warehouse scheduling support in `megaton_lib.audit.providers.analytics.dw`:
  - `AdobeDataWarehouseClient` for scheduled request list/get/create/update and generated report metadata
  - `build_adobe_auth()` / `build_dw_client()` runtime helpers for Adobe credential resolution
  - template discovery helpers `find_template_requests()`, `resolve_template_request()`, and `summarize_template_detail()`
  - template clone helpers `build_cloned_request_body()`, `create_request_from_template()`, and `bulk_create_requests_from_template()`
  - CLI module `python -m megaton_lib.audit.providers.analytics.dw.cli` with `--find-template`, `--describe-template`, `--list`, `--status`, and manifest-based `--dry-run` / `--create`
- Exported `AdobeDataWarehouseClient` from `megaton_lib.audit.providers.analytics`
- Added regression tests for DW template filtering and clone payload generation

### 2026-04-04

- Added Adobe Analytics classifications support in `megaton_lib.audit.providers.analytics`:
  - `ClassificationsClient` for dataset discovery, export, import, column inspection, and post-upload verification
  - `verify_classification.py` CLI (`python -m megaton_lib.audit.providers.analytics.verify_classification`) for reflection checks using `--keys` or `--diff-tsv`
  - Level 2 report verification via `--report` flag (AA Reporting API breakdown check)
  - `--creds-file` option using shared `load_adobe_oauth_credentials` (supports `org_id` / `ims_org_id` / `imsOrgId`)
  - exported `ClassificationsClient` and `print_verify_results` from `megaton_lib.audit.providers.analytics`
- Fixed classifications verification edge cases:
  - CLI no longer crashes when `--token-cache` is omitted
  - dataset lookup now requires exact dimension matching, avoiding prefix collisions such as `evar2` vs `evar29`
  - no-traffic keys in Level 2 are SKIP (not NG) and do not affect exit code
- Expanded shared notebook/report helpers:
  - `read_sheet_table(..., header_row=...)` for non-zero header rows in Sheets inputs
  - report tracker dry-run write skipping for notebook jobs that should skip Sheets writes
- Added regression tests for classifications dimension matching and CLI default token-cache behavior

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
