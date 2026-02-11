"""BigQuery retention analysis for Corp Talks.

Provides:
- D7/D30 cohort retention (new-user based)
- Monthly active revisit rate (prev-month visitors → this month)
- Day-level retention curves
- Reading-depth distribution (buckets: 1, 2, 3, 4-5, 6-9, 10+)
- Breakdowns by source/medium, device, and first-article tag
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


def _map_lang(df: pd.DataFrame) -> pd.DataFrame:
    """Map single-char ``lang`` column to ``language`` (JP/EN) and drop ``lang``."""
    df["language"] = df["lang"].map(_LANG_MAP).fillna("")
    return df.drop(columns=["lang"])


# ---------------------------------------------------------------------------
# 1. Retention summary (D7/D30) — cohort = first-ever visitors
# ---------------------------------------------------------------------------

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

    Retention = revisited *any* Talks page (including same page) within the window.
    Cohort = users whose first-ever Talks visit was in the cohort month.

    Returns DataFrame with columns:
    month, language, new_users_first_ever, retained_d7_users, retained_d30_users,
    retention_d7, retention_d30.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
returns AS (
  SELECT c.user_pseudo_id, c.first_lang,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 7 DAY), 1, 0)) AS returned_d7,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY), 1, 0)) AS returned_d30
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
    df = _map_lang(df)
    return df[[
        "month", "language", "new_users_first_ever",
        "retained_d7_users", "retained_d30_users", "retention_d7", "retention_d30",
    ]].sort_values(["month", "language"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. Monthly active revisit rate (prev month → this month)
# ---------------------------------------------------------------------------

def query_monthly_active_revisit(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    subsite: str,
    cohort_months: list[str],
) -> pd.DataFrame:
    """Monthly active revisit rate: fraction of prev-month visitors who return this month.

    For each target month M, the cohort is users who visited Talks in month M-1.
    Revisit = at least one page_view in Talks during month M.

    Returns DataFrame: month, language, prev_month_users, revisit_users, revisit_rate.
    """
    sql = f"""
WITH prev AS (
  SELECT DISTINCT user_pseudo_id, lang
  FROM `{project_id}.{dataset}.{table_pv}`
  WHERE subsite = @subsite
    AND FORMAT_DATE('%Y%m', event_date_jst) = @prev_month
),
curr AS (
  SELECT DISTINCT user_pseudo_id, lang
  FROM `{project_id}.{dataset}.{table_pv}`
  WHERE subsite = @subsite
    AND FORMAT_DATE('%Y%m', event_date_jst) = @target_month
)
SELECT @target_month AS month, p.lang,
  COUNT(DISTINCT p.user_pseudo_id) AS prev_month_users,
  COUNT(DISTINCT c.user_pseudo_id) AS revisit_users,
  SAFE_DIVIDE(COUNT(DISTINCT c.user_pseudo_id), COUNT(DISTINCT p.user_pseudo_id)) AS revisit_rate
FROM prev p
LEFT JOIN curr c ON p.user_pseudo_id = c.user_pseudo_id AND p.lang = c.lang
GROUP BY month, p.lang ORDER BY p.lang
"""
    rows = []
    for ym in cohort_months:
        # Calculate prev_month from ym (yyyymm)
        y, m = int(ym[:4]), int(ym[4:6])
        if m == 1:
            prev_ym = f"{y - 1}12"
        else:
            prev_ym = f"{y}{m - 1:02d}"
        rows.extend(_run_parameterized(
            bq, sql, {"subsite": subsite, "target_month": ym, "prev_month": prev_ym}
        ))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df = _map_lang(df)
    return df[[
        "month", "language", "prev_month_users", "revisit_users", "revisit_rate",
    ]].sort_values(["month", "language"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Day-level retention curve
# ---------------------------------------------------------------------------

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

    Retention = revisited any Talks page (including same page).

    Returns DataFrame: month, language, day, new_users_first_ever, retained_users, retention.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang
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
    df = _map_lang(df)
    return df[[
        "month", "language", "day", "new_users_first_ever", "retained_users", "retention",
    ]].sort_values(["month", "language", "day"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Reading depth (expanded buckets + avg pages)
# ---------------------------------------------------------------------------

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

    Buckets: 1, 2, 3, 4-5, 6-9, 10+.
    Also returns avg_pages per language/month.

    Returns DataFrame: month, language, bucket, users, share, avg_pages.
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
  SELECT first_lang AS lang, pages,
    CASE
      WHEN pages >= 10 THEN '10+'
      WHEN pages >= 6 THEN '6-9'
      WHEN pages >= 4 THEN '4-5'
      ELSE CAST(pages AS STRING)
    END AS bucket
  FROM depth
)
SELECT @cohort_month AS month, lang, bucket,
  COUNT(*) AS users,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY lang)) AS share,
  AVG(pages) AS avg_pages
FROM bucketed GROUP BY month, lang, bucket ORDER BY lang, bucket
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df = _map_lang(df)
    return df[[
        "month", "language", "bucket", "users", "share", "avg_pages",
    ]].sort_values(["month", "language", "bucket"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 5. Breakdown: source/medium
# ---------------------------------------------------------------------------

def query_retention_by_source(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    table_first: str,
    subsite: str,
    cohort_months: list[str],
) -> pd.DataFrame:
    """D30 retention broken down by first-visit source/medium.

    Returns DataFrame: month, language, source, medium, users, retained_d30, retention_d30.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang, first_source, first_medium
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
returns AS (
  SELECT c.user_pseudo_id, c.first_lang,
    IFNULL(c.first_source, '(direct)') AS source,
    IFNULL(c.first_medium, '(none)') AS medium,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY), 1, 0)) AS returned_d30
  FROM cohort c
  LEFT JOIN `{project_id}.{dataset}.{table_pv}` p
    ON p.subsite = @subsite AND p.user_pseudo_id = c.user_pseudo_id
   AND p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
                           AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
  GROUP BY c.user_pseudo_id, c.first_lang, source, medium
)
SELECT @cohort_month AS month, first_lang AS lang, source, medium,
  COUNT(*) AS users,
  SUM(returned_d30) AS retained_d30,
  SAFE_DIVIDE(SUM(returned_d30), COUNT(*)) AS retention_d30
FROM returns GROUP BY month, lang, source, medium
HAVING COUNT(*) >= 5
ORDER BY lang, users DESC
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df = _map_lang(df)
    return df[[
        "month", "language", "source", "medium", "users", "retained_d30", "retention_d30",
    ]].sort_values(["month", "language", "users"], ascending=[True, True, False],
                   kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 6. Breakdown: first-article tag (joined with _meta sheet)
# ---------------------------------------------------------------------------

def query_retention_by_tag(
    bq,
    *,
    project_id: str,
    dataset: str,
    table_pv: str,
    table_first: str,
    subsite: str,
    cohort_months: list[str],
    df_meta: pd.DataFrame,
) -> pd.DataFrame:
    """D30 retention broken down by first-article tag from _meta sheet.

    ``df_meta`` must have columns ``URL`` and ``Tag``.
    The URL column contains paths like ``/jp/company/talk/20250101.html``.

    Returns DataFrame: month, language, tag, users, retained_d30, retention_d30.
    """
    sql = f"""
WITH cohort AS (
  SELECT user_pseudo_id, first_date_jst, first_lang, first_page_path
  FROM `{project_id}.{dataset}.{table_first}`
  WHERE subsite = @subsite AND FORMAT_DATE('%Y%m', first_date_jst) = @cohort_month
),
returns AS (
  SELECT c.user_pseudo_id, c.first_lang, c.first_page_path,
    MAX(IF(p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
            AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY), 1, 0)) AS returned_d30
  FROM cohort c
  LEFT JOIN `{project_id}.{dataset}.{table_pv}` p
    ON p.subsite = @subsite AND p.user_pseudo_id = c.user_pseudo_id
   AND p.event_date_jst BETWEEN DATE_ADD(c.first_date_jst, INTERVAL 1 DAY)
                           AND DATE_ADD(c.first_date_jst, INTERVAL 30 DAY)
  GROUP BY c.user_pseudo_id, c.first_lang, c.first_page_path
)
SELECT @cohort_month AS month, first_lang AS lang, first_page_path,
  COUNT(*) AS users,
  SUM(returned_d30) AS retained_d30,
  SAFE_DIVIDE(SUM(returned_d30), COUNT(*)) AS retention_d30
FROM returns GROUP BY month, lang, first_page_path ORDER BY lang
"""
    rows = []
    for ym in cohort_months:
        rows.extend(_run_parameterized(bq, sql, {"subsite": subsite, "cohort_month": ym}))

    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    df = _map_lang(df)

    # Join with _meta to get Tag
    if df_meta is not None and "URL" in df_meta.columns and "Tag" in df_meta.columns:
        meta = df_meta[["URL", "Tag"]].copy()
        meta["URL"] = meta["URL"].astype(str).str.strip()
        meta["Tag"] = meta["Tag"].astype(str).str.strip()
        df = df.merge(meta, left_on="first_page_path", right_on="URL", how="left")
        df["tag"] = df["Tag"].fillna("(unknown)")
        df = df.drop(columns=["URL", "Tag", "first_page_path"])
    else:
        df["tag"] = "(unknown)"
        df = df.drop(columns=["first_page_path"])

    # Aggregate by tag
    agg = df.groupby(["month", "language", "tag"], as_index=False).agg(
        users=("users", "sum"),
        retained_d30=("retained_d30", "sum"),
    )
    agg["retention_d30"] = (agg["retained_d30"] / agg["users"]).where(agg["users"] > 0, 0.0)

    return agg[[
        "month", "language", "tag", "users", "retained_d30", "retention_d30",
    ]].sort_values(["month", "language", "users"], ascending=[True, True, False],
                   kind="mergesort").reset_index(drop=True)


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
    ws.update("A6", [["月次アクティブ再訪率（_revisit-m）"]])
    ws.update("A7", [[
        f"=QUERY('_revisit-m'!A:E,\"select A,C,D,E where B='{language}' order by A desc\",1)"
    ]])
    ws.update("A9", [["D1-30 累積定着（ヒートマップ用, _retention-day）"]])
    ws.update("A10", [[
        f"=QUERY('_retention-day'!A:F,\"select A, max(F) where B='{language}' group by A pivot C order by A desc\",1)"
    ]])
    ws.update("A12", [["深さ（30日以内の読む本数分布, _retention-depth）"]])
    ws.update("A13", [[
        f"=QUERY('_retention-depth'!A:F,\"select A, max(E) where B='{language}' group by A pivot C order by A desc\",1)"
    ]])
    ws.update("A15", [["流入元別 D30 維持率（_retention-source）"]])
    ws.update("A16", [[
        f"=QUERY('_retention-source'!A:G,\"select A,C,D,E,F,G where B='{language}' order by A desc, E desc\",1)"
    ]])
    ws.update("A18", [["タグ別 D30 維持率（_retention-tag）"]])
    ws.update("A19", [[
        f"=QUERY('_retention-tag'!A:F,\"select A,C,D,E,F where B='{language}' order by A desc, D desc\",1)"
    ]])
    return created
