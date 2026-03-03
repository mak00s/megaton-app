"""megaton_lib – notebook helper library for megaton-based reports.

Public modules
--------------
Core:
    megaton_client   : get_ga4() factory for Megaton instances
    ga4_helpers      : run_report_df, build_filter, to_datetime_col, to_numeric_cols
    date_template    : resolve_date() for parameter date strings
    sheets           : save/read/upsert helpers for Google Sheets
    table_utils      : regex-based mapping/classification helpers
    gsc_utils        : Search Console DataFrame post-processing helpers
    traffic          : source normalization and channel reclassification

Date utilities:
    date_utils       : month ranges + month parsing/current-month filtering helpers
"""
