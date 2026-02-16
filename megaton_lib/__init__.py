"""megaton_lib – notebook helper library for megaton-based reports.

Public modules
--------------
Core:
    megaton_client   : get_ga4() factory for Megaton instances
    ga4_helpers      : run_report_df, build_filter, to_datetime_col, to_numeric_cols
    date_template    : resolve_date() for parameter date strings
    sheets           : save_sheet_from_template() and sheet utilities

Date utilities:
    date_utils       : month_ranges_for_year, month_ranges_between, months_between

WITH report:
    articles         : aggregate_article_meta()
    with_report      : write_summary_sheets()

Corp Talks report:
    talks_scraping   : scrape_talk_cards, scrape_article_links, normalize_url, crawl_new_article_links
    talks_ga4        : preprocess_page_metrics, preprocess_top_pages, fetch_nav_clicks, attach_nav_metrics
    talks_report     : build_article_sheet, build_talks_m, build_monthly_rows, write_monthly_sheet
    talks_retention  : query_retention_summary, query_retention_day_curve, query_retention_depth

DEI Lab report:
    dei_ga4          : classify_source_channel, ensure_trailing_slash, build_page_metrics

SLQM report:
    slqm_ga4         : get_13month_start, ym_from_year_month, safe_merge_many, fillna_int, compute_sp_ratio
    slqm_analysis    : fetch_daily_metrics, fetch_page_metrics, fetch_channel_breakdown,
                       fetch_source_medium, fetch_landing_pages, fetch_session_quality,
                       fetch_new_vs_returning, fetch_page_transitions
"""
