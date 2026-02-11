"""BigQuery retention analysis for Corp Talks.

Provides D7/D30 cohort retention, day-level retention curves,
and reading-depth distribution queries.
"""

from __future__ import annotations

import os

import pandas as pd

from .credentials import list_service_account_paths
from .date_utils import months_between


# ---------------------------------------------------------------------------
# BQ client initialization
# ---------------------------------------------------------------------------

def init_bq_client(project_id: str, *, creds_hint: str = "corp"):
    """Initialize a BigQuery client, auto-selecting credentials if needed.

    Returns a ``google.cloud.bigquery.Client`` instance.
    """
    from google.cloud import bigquery

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        paths = list_service_account_paths()
        if paths:
            match = [p for p in paths if creds_hint in os.path.basename(p).lower()]
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = match[0] if match else paths[0]

    return bigquery.Client(project=project_id)


def resolve_cohort_months(
    start_date: str,
    end_date: str,
    df_page: pd.DataFrame | None = None,
) -> list[str]:
    """Determine cohort months from date range (preferred) or DataFrame."""
    date_months = months_between(start_date, end_date)
    if date_months:
        return date_months
    if df_page is not None and "month" in df_page.columns:
        return sorted(df_page["month"].dropna().astype(str).unique().tolist())
    return []


# ---------------------------------------------------------------------------
# Retention queries
# ---------------------------------------------------------------------------

_LANG_MAP = {"J": "JP", "E": "EN"}


def _run_parameterized(bq, sql: str, params: dict, *, location: str = "asia-northeast1"):
    """Run a parameterized BQ query and return list of row dicts."""
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
        ]
    )
    return [dict(r) for r in bq.query(sql, job_config=job_config, location=location).result()]


def query_retention_summary(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    table_first: str,
    subsite: str,
    cohort_months: list[str],
) -> pd.DataFrame:
    """Query D7/D30 retention for each cohort month.

    Returns DataFrame with columns:
    month, language, new_users_first_ever, retained_d7_users, retained_d30_users,
    retention_d7, retention_d30.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang, first_page_path
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
returns AS (
  SELECT c.user_pseudo_id, c.first_lang,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 7 DAY)
            AND p.page_path != c.first_page_path, 1, 0)) AS returned_d7,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
            AND p.page_path != c.first_page_path, 1, 0)) AS returned_d30
  FROM cohort c
  LEFT JOIN `{project_id}.{dataset}.{table_pv}` p
    ON p.subsite = @subsite AND p.user_pseudo_id = c.user_pseudo_id
   AND p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
                           AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
  GROUP BY c.user_pseudo_id, c.first_lang
)
SELECT @cohort_month AS month, first_lang AS lang,
  COUNT(*) AS new_users_first_ever,
  SUM(returned_d7) AS retained_d7_users,
  SUM(returned_d30) AS retained_d30_users,
  SAFE_DIVIDE(SUM(returned_d7), COUNT(*)) AS retention_d7,
  SAFE_DIVIDE(SUM(returned_d30), COUNT(*)) AS retention_d30
FROM returns GROUP BY month, lang ORDER BY lang
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df["language"] = df["lang"].map(_LANG_MAP).fillna("")
    df = df.drop(columns=["lang"])
    return df[[
        "month", "language", "new_users_first_ever",
        "retained_d7_users", "retained_d30_users", "retention_d7", "retention_d30",
    ]].sort_values(["month", "language"], kind="mergesort").reset_index(drop=True)


def query_retention_day_curve(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    table_first: str,
    subsite: str,
    cohort_months: list[str],
) -> pd.DataFrame:
    """Query cumulative day-level retention curve (day 1..30).

    Returns DataFrame: month, language, day, new_users_first_ever, retained_users, retention.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang, first_page_path
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
first_return AS (
  SELECT c.user_pseudo_id, c.first_lang,
    MIN(DATE_DIFF(p.event_date_jst, c.first_date_jst, DAY)) AS first_return_day
  FROM cohort c
  JOIN `{project_id}.{dataset}.{table_pv}` p
    ON p.subsite = @subsite AND p.user_pseudo_id = c.user_pseudo_id
  WHERE p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
                           AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
    AND p.page_path != c.first_page_path
  GROUP BY c.user_pseudo_id, c.first_lang
),
cohort2 AS (
  SELECT c.user_pseudo_id, c.first_lang, f.first_return_day
  FROM cohort c LEFT JOIN first_return f
    ON f.user_pseudo_id = c.user_pseudo_id AND f.first_lang = c.first_lang
)
SELECT @cohort_month AS month, first_lang AS lang, day,
  COUNT(*) AS new_users_first_ever,
  COUNTIF(first_return_day IS NOT NULL AND first_return_day <= day) AS retained_users,
  SAFE_DIVIDE(COUNTIF(first_return_day IS NOT NULL AND first_return_day <= day), COUNT(*)) AS retention
FROM cohort2 CROSS JOIN UNNEST(GENERATE_ARRAY(1, 30)) AS day
GROUP BY month, lang, day ORDER BY lang, day
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df["language"] = df["lang"].map(_LANG_MAP).fillna("")
    df = df.drop(columns=["lang"])
    return df[[
        "month", "language", "day", "new_users_first_ever", "retained_users", "retention",
    ]].sort_values(["month", "language", "day"], kind="mergesort").reset_index(drop=True)


def query_retention_depth(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    table_first: str,
    subsite: str,
    cohort_months: list[str],
) -> pd.DataFrame:
    """Query reading-depth distribution (distinct pages within 30 days).

    Returns DataFrame: month, language, bucket, users, share.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
depth AS (
  SELECT c.user_pseudo_id, c.first_lang, COUNT(DISTINCT p.page_path) AS pages
  FROM cohort c
  JOIN `{project_id}.{dataset}.{table_pv}` p
    ON p.subsite = @subsite AND p.user_pseudo_id = c.user_pseudo_id
  WHERE p.event_date_jst BETWEEN c.first_date_jst
                           AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
  GROUP BY c.user_pseudo_id, c.first_lang
),
bucketed AS (
  SELECT first_lang AS lang,
    CASE WHEN pages >= 4 THEN '4+' ELSE CAST(pages AS STRING) END AS bucket
  FROM depth
)
SELECT @cohort_month AS month, lang, bucket,
  COUNT(*) AS users,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY lang)) AS share
FROM bucketed GROUP BY month, lang, bucket ORDER BY lang, bucket
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df["language"] = df["lang"].map(_LANG_MAP).fillna("")
    df = df.drop(columns=["lang"])
    return df[[
        "month", "language", "bucket", "users", "share",
    ]].sort_values(["month", "language", "bucket"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Dashboard sheet creation
# ---------------------------------------------------------------------------

def ensure_dashboard_sheet(mg, language: str) -> bool:
    """Create or update a ``retention_JP`` / ``retention_EN`` dashboard sheet.

    Returns True if a new sheet was created.
    """
    sheet_name = f"retention_{language}"
    sheets = list(getattr(mg.gs, "sheets", []) or [])
    created = False
    if sheet_name not in sheets:
        mg.sheets.create(sheet_name)
        created = True

    mg.sheets.select(sheet_name)
    ws = getattr(mg.gs.sheet, "_driver", None)
    if not ws or not hasattr(ws, "update"):
        print(f"[skip] {sheet_name}: worksheet driver does not support update()")
        return False

    ws.update("A1", [[f"定着ダッシュボード（{language}）"]])
    ws.update("A3", [["月次サマリ（_retention-m）"]])
    ws.update("A4", [[
        f"=QUERY('_retention-m'!A:G,\"select A,C,D,F,E,G where B='{language}' order by A desc\",1)"
    ]])
    ws.update("A6", [["D1-30 累積定着（ヒートマップ用, _retention-day）"]])
    ws.update("A7", [[
        f"=QUERY('_retention-day'!A:F,\"select A, max(F) where B='{language}' group by A pivot C order by A desc\",1)"
    ]])
    ws.update("A9", [["深さ（30日以内の読む本数分布, _retention-depth）"]])
    ws.update("A10", [[
        f"=QUERY('_retention-depth'!A:E,\"select A, max(E) where B='{language}' group by A pivot C order by A desc\",1)"
    ]])
    return created
