"""megaton_lib – notebook/automation helper library for megaton-based reports.

Modules are imported by path (``from megaton_lib.<module> import ...``);
this package ``__init__`` intentionally does not eagerly import submodules
so optional dependencies (playwright, gspread, matplotlib, …) stay lazy.
The map below is the curated index of what lives where — keep it in sync
when adding or removing a module.

Query & clients:
    megaton_client   : get_ga4() factory + query_ga4/query_gsc/query_bq/query_aa
    ga4_helpers      : run_report_df, build_filter, to_datetime_col, to_numeric_cols
    gsc_utils        : Search Console DataFrame post-processing helpers
    bigquery_utils   : BigQuery load/append/replace + schema/query helpers
    query_runner     : execute params-style query configs in-process
    batch_runner     : run a directory of query configs in order
    job_manager      : file-based job state tracking

Params & config:
    params_validator : validate/normalize params.json schema
    params_diff      : canonicalize_json() for effective-diff checks
    site_aliases     : resolve site aliases in params
    cli_help         : argparse helpers for the script CLIs

Dates:
    dates            : SINGLE ENTRY POINT for all date/period helpers — import
                       dates from here (resolve_date, resolve_month, windows,
                       month ranges, parse_summary_tokens, ...)
    date_template    : (impl) resolve_date()/resolve_month() template parsing
    date_utils       : (impl) month ranges + month parsing/current-month helpers
    periods          : (impl) month-window parsing (quarters/years/relative months)
    tz_utils         : (impl) shared timezone resolution (stdlib-only)

Sheets & output:
    sheets           : high-level save/read/upsert helpers for Google Sheets
    gspread_lowlevel : low-level gspread + batchUpdate request wrappers
    table_utils      : regex-based mapping/classification helpers
    traffic          : source normalization and channel reclassification
    result_inspector : pipeline transforms + CSV summary for query results
    analysis         : show()/properties()/sites() display helpers
    docs_sites       : generated-markdown table helpers

Reporting & delivery:
    report_validation: ExecutionTracker for report runs + sheet caching
    report_gmail_draft: build Gmail drafts from execution summaries
    gmail_client     : Gmail API client wrapper
    credentials      : credential discovery/loading

Browser automation:
    playwright_browser : generic Playwright browser (ephemeral/persistent/CDP),
                         stealth, storage-state, canvas screenshots
    box_ui           : Box.com upload + shared-link UI automation
    notebook         : notebook init helpers

Subpackages:
    audit            : reusable GTM/Adobe Tags + GA4/AA audit framework
    validation       : Playwright page checks, AA beacon + GTM/Tags overrides
                       (validation.playwright_pages builds on playwright_browser)
"""
