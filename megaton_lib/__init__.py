"""megaton_lib – notebook helper library for megaton-based reports.

Public modules
--------------
Core:
    megaton_client   : get_ga4() factory for Megaton instances
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
    talks_report     : build_monthly_view, write_monthly_sheets, build_all_sheet
    talks_retention  : query_retention_summary, query_retention_day_curve, query_retention_depth
"""
