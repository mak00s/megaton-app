"""Report output orchestration for Corp Talks.

Analogous to ``with_report.py`` — builds DataFrames for monthly / ARTICLE sheets
and writes them to Google Sheets.
"""

from __future__ import annotations

import pandas as pd

from .sheets import save_sheet_from_template
from .talks_scraping import normalize_meta_sheet


# ---------------------------------------------------------------------------
# Monthly sheets (yyyymm)
# ---------------------------------------------------------------------------

def build_monthly_view(
    df_page: pd.DataFrame,
    df_meta: pd.DataFrame,
) -> pd.DataFrame:
    """Merge page metrics with metadata for human-readable monthly sheets.

    *df_meta* should have columns ``URL, Title, Date`` (from ``_meta`` sheet).
    Returns a DataFrame with ``published_date`` and ``Title`` columns added.
    """
    df = df_page.merge(
        df_meta[["URL", "Title", "Date"]],
        left_on="page",
        right_on="URL",
        how="left",
    ).drop(columns=["URL"])

    df["published_date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.drop(columns=["Date"])
    df["language"] = df["language"].astype(str).str.upper()
    return df


def write_monthly_sheets(mg, df_month_view: pd.DataFrame) -> list[str]:
    """Write one sheet per ``yyyymm`` month found in *df_month_view*.

    Returns a list of written sheet names.
    """
    months = sorted(df_month_view["month"].dropna().astype(str).unique().tolist())
    written: list[str] = []

    for yyyymm in months:
        df_m = df_month_view[df_month_view["month"].astype(str) == str(yyyymm)].copy()
        if len(df_m) == 0:
            continue

        # Sort: JP first, then by published_date desc
        df_m["_lang_sort"] = df_m["language"].map({"JP": 2, "EN": 1}).fillna(0).astype(int)
        df_m = df_m.sort_values(
            ["_lang_sort", "published_date"],
            ascending=[False, False],
            kind="mergesort",
        ).drop(columns=["_lang_sort"])

        df_out = df_m[[
            "language", "published_date", "Title", "page",
            "pv", "sessions", "nav_clicks", "nav_rate",
            "entrances", "total_users", "new_users", "bounces", "footer_views",
        ]].copy()
        df_out = df_out.rename(columns={"Title": "title"})
        df_out["published_date"] = df_out["published_date"].dt.strftime("%Y-%m-%d")
        df_out["nav_rate"] = df_out["nav_rate"].round(6)

        save_sheet_from_template(
            mg, str(yyyymm), df_out,
            start_row=1,
            template_regex=r"^\d{6}$",
            save_kwargs={"freeze_header": True},
        )
        written.append(str(yyyymm))

    return written


# ---------------------------------------------------------------------------
# ARTICLE cumulative sheet
# ---------------------------------------------------------------------------

_TALK_PATH_REGEX = r"^/(en|jp)/company/talk/[^.]+\.html$"


def build_article_sheet(
    df_article_m: pd.DataFrame,
    df_meta: pd.DataFrame,
    *,
    path_regex: str = _TALK_PATH_REGEX,
) -> pd.DataFrame:
    """Build the ARTICLE cumulative sheet from accumulated ``_article-m`` data.

    Aggregates monthly rows in *df_article_m* across all months per page,
    merges with *df_meta* for title/tag/lang/date, and computes derived
    metrics (nav_rate, read_rate).

    This is a pure DataFrame transformation — no GA4 queries.
    """
    # Filter to article pages only (exclude Top pages)
    df = df_article_m.copy()
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "lang", "published_date", "tag", "title", "page",
            "uu_total", "new_users",
            "nav_clicks", "nav_rate", "read_rate",
        ])

    df["page"] = df["page"].astype(str).str.strip()
    df = df[df["page"].str.match(path_regex)].copy()

    # Aggregate across months per page (sum additive metrics)
    sum_cols = ["pv", "sessions", "nav_clicks", "total_users", "new_users", "footer_views"]
    for c in sum_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    df_agg = df.groupby("page", as_index=False)[sum_cols].sum()

    # Metadata
    meta = normalize_meta_sheet(df_meta)
    meta = meta.rename(columns={
        "URL": "page", "Title": "title", "Language": "lang",
        "Tag": "tag", "Date": "published_date",
    })[["page", "title", "lang", "tag", "published_date"]].copy()
    meta["lang"] = meta["lang"].astype(str).str.upper().str.strip()
    meta["published_date"] = pd.to_datetime(meta["published_date"], errors="coerce")
    meta = meta[meta["page"].astype(str).str.match(path_regex)].copy()
    meta = meta[(meta["title"].astype(str).str.strip() != "") & meta["published_date"].notna()].copy()

    # Merge
    df_all = meta.merge(df_agg, on="page", how="left")
    for c in sum_cols:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce").fillna(0).astype(int)

    df_all["nav_rate"] = (df_all["nav_clicks"] / df_all["sessions"]).where(df_all["sessions"] > 0, 0.0)
    df_all["uu_total"] = df_all["total_users"].fillna(0).astype(int)

    pv = pd.to_numeric(df_all["pv"], errors="coerce").fillna(0)
    fv = pd.to_numeric(df_all["footer_views"], errors="coerce").fillna(0)
    df_all["read_rate"] = 0.0
    mask = pv > 0
    df_all.loc[mask, "read_rate"] = (fv[mask] / pv[mask]).astype(float)

    # Sort: JP first, published_date desc
    df_all["_lang_sort"] = df_all["lang"].map({"JP": 2, "EN": 1}).fillna(0).astype(int)
    df_all = df_all.sort_values(
        ["_lang_sort", "published_date"], ascending=[False, False], kind="mergesort",
    ).drop(columns=["_lang_sort"])

    df_out = df_all[[
        "lang", "published_date", "tag", "title", "page",
        "uu_total", "new_users",
        "nav_clicks", "nav_rate", "read_rate",
    ]].copy()
    df_out["published_date"] = pd.to_datetime(df_out["published_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_out["nav_rate"] = df_out["nav_rate"].round(6)
    df_out["read_rate"] = df_out["read_rate"].round(6)

    return df_out


# ---------------------------------------------------------------------------
# _talks-m — monthly site-level trend
# ---------------------------------------------------------------------------

_ARTICLE_PATH_REGEX = r"^/(en|jp)/company/talk/\d{8}\.html$"


def _aggregate_nav_clicks(
    df_article_m: pd.DataFrame,
    target_months: list[str],
) -> pd.DataFrame:
    """Aggregate nav_clicks from ``_article-m`` by (month, lang) + ALL row."""
    if (
        len(df_article_m) == 0
        or not {"month", "page", "nav_clicks"}.issubset(df_article_m.columns)
        or len(target_months) == 0
    ):
        return pd.DataFrame(columns=["month", "lang", "nav_clicks"])

    df = df_article_m[["month", "page", "nav_clicks"]].copy()
    df["month"] = df["month"].astype(str).str.strip()
    df["page"] = df["page"].astype(str).str.strip()
    df = df[df["month"].isin(target_months)].copy()
    df = df[df["page"].str.match(_ARTICLE_PATH_REGEX)].copy()
    df["lang"] = df["page"].str.extract(r"^/(en|jp)/", expand=False).str.upper()
    df["nav_clicks"] = pd.to_numeric(df["nav_clicks"], errors="coerce").fillna(0).astype(int)
    by_lang = df.groupby(["month", "lang"], as_index=False)["nav_clicks"].sum()

    # ALL row (JP + EN)
    by_all = by_lang.groupby("month", as_index=False)["nav_clicks"].sum()
    by_all["lang"] = "ALL"
    return pd.concat([by_lang, by_all], ignore_index=True)


def _aggregate_retention(
    df_retention_m: pd.DataFrame,
    target_months: list[str],
) -> pd.DataFrame:
    """Extract retention_d7/d30 for *target_months* + compute ALL row."""
    need = {"month", "language", "new_users_first_ever", "retained_d7_users", "retained_d30_users"}
    if len(df_retention_m) == 0 or not need.issubset(df_retention_m.columns) or len(target_months) == 0:
        return pd.DataFrame(columns=["month", "lang", "retention_d7", "retention_d30"])

    tmp = df_retention_m.copy()
    tmp["month"] = tmp["month"].astype(str).str.strip()
    tmp = tmp[tmp["month"].isin(target_months)].copy()
    tmp["lang"] = tmp["language"].astype(str).str.upper().str.strip()
    for c in ["new_users_first_ever", "retained_d7_users", "retained_d30_users"]:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce").fillna(0).astype(int)
    tmp["retention_d7"] = pd.to_numeric(tmp.get("retention_d7"), errors="coerce")
    tmp["retention_d30"] = pd.to_numeric(tmp.get("retention_d30"), errors="coerce")
    out = tmp[["month", "lang", "new_users_first_ever", "retained_d7_users", "retained_d30_users", "retention_d7", "retention_d30"]].copy()

    # ALL row (weighted average)
    agg = out.groupby("month", as_index=False)[["new_users_first_ever", "retained_d7_users", "retained_d30_users"]].sum()
    agg["lang"] = "ALL"
    agg["retention_d7"] = (agg["retained_d7_users"] / agg["new_users_first_ever"]).where(agg["new_users_first_ever"] > 0, 0.0)
    agg["retention_d30"] = (agg["retained_d30_users"] / agg["new_users_first_ever"]).where(agg["new_users_first_ever"] > 0, 0.0)
    return pd.concat([out, agg], ignore_index=True)


def _aggregate_revisit(
    df_revisit_m: pd.DataFrame,
    target_months: list[str],
) -> pd.DataFrame:
    """Extract revisit_rate for *target_months* + compute ALL row."""
    if (
        len(df_revisit_m) == 0
        or not {"month", "language", "revisit_rate"}.issubset(df_revisit_m.columns)
        or len(target_months) == 0
    ):
        return pd.DataFrame(columns=["month", "lang", "revisit_rate"])

    rv = df_revisit_m.copy()
    rv["month"] = rv["month"].astype(str).str.strip()
    rv = rv[rv["month"].isin(target_months)].copy()
    rv["lang"] = rv["language"].astype(str).str.upper().str.strip()
    rv["revisit_rate"] = pd.to_numeric(rv["revisit_rate"], errors="coerce")

    if "prev_month_users" in rv.columns:
        rv["prev_month_users"] = pd.to_numeric(rv["prev_month_users"], errors="coerce").fillna(0).astype(int)
        rv["revisit_users"] = pd.to_numeric(rv["revisit_users"], errors="coerce").fillna(0).astype(int)
        agg = rv.groupby("month", as_index=False)[["prev_month_users", "revisit_users"]].sum()
        agg["lang"] = "ALL"
        agg["revisit_rate"] = (agg["revisit_users"] / agg["prev_month_users"]).where(agg["prev_month_users"] > 0, 0.0)
        return pd.concat([rv[["month", "lang", "revisit_rate"]], agg[["month", "lang", "revisit_rate"]]], ignore_index=True)

    return rv[["month", "lang", "revisit_rate"]].copy()


def build_talks_m(
    df_ga4_monthly: pd.DataFrame,
    *,
    df_article_m: pd.DataFrame,
    df_retention_m: pd.DataFrame,
    df_revisit_m: pd.DataFrame,
) -> pd.DataFrame:
    """Build the ``_talks-m`` DataFrame from GA4 monthly totals + sheet data.

    *df_ga4_monthly* has columns ``month, lang, pv, sessions, uu, new_users,
    footer_views, read_rate`` — produced by the notebook's GA4 query.

    The other DataFrames come from the accumulated sheets (``_article-m``,
    ``_retention-m``, ``_revisit-m``).

    Returns a DataFrame ready for upsert to the ``_talks-m`` sheet.
    Pure DataFrame transformation — no API calls.
    """
    if len(df_ga4_monthly) == 0:
        return pd.DataFrame(columns=[
            "month", "lang", "pv", "sessions", "uu", "new_users",
            "footer_views", "read_rate", "nav_clicks", "nav_rate",
            "retention_d7", "retention_d30", "revisit_rate",
        ])

    df_ga4_monthly = df_ga4_monthly.copy()
    if "month" in df_ga4_monthly.columns:
        df_ga4_monthly["month"] = df_ga4_monthly["month"].astype(str).str.strip()
    target_months = df_ga4_monthly["month"].unique().tolist()

    # Aggregate supplemental metrics for target months
    df_nav = _aggregate_nav_clicks(df_article_m, target_months)
    df_ret = _aggregate_retention(df_retention_m, target_months)
    df_rev = _aggregate_revisit(df_revisit_m, target_months)

    # Merge
    df = df_ga4_monthly.merge(df_nav, on=["month", "lang"], how="left")
    df["nav_clicks"] = pd.to_numeric(df.get("nav_clicks"), errors="coerce").fillna(0).astype(int)
    df["nav_rate"] = (df["nav_clicks"] / df["sessions"]).where(df["sessions"] > 0, 0.0)

    if len(df_ret) > 0:
        df = df.merge(
            df_ret[["month", "lang", "retention_d7", "retention_d30"]].drop_duplicates(subset=["month", "lang"]),
            on=["month", "lang"], how="left",
        )
    else:
        df["retention_d7"] = pd.NA
        df["retention_d30"] = pd.NA

    if len(df_rev) > 0:
        df = df.merge(
            df_rev[["month", "lang", "revisit_rate"]].drop_duplicates(subset=["month", "lang"]),
            on=["month", "lang"], how="left",
        )
    else:
        df["revisit_rate"] = pd.NA

    df["read_rate"] = pd.to_numeric(df["read_rate"], errors="coerce").fillna(0.0).astype(float)
    df["retention_d7"] = pd.to_numeric(df["retention_d7"], errors="coerce")
    df["retention_d30"] = pd.to_numeric(df["retention_d30"], errors="coerce")
    df["revisit_rate"] = pd.to_numeric(df["revisit_rate"], errors="coerce")

    df = df.sort_values(["month", "lang"], kind="mergesort").reset_index(drop=True)
    for c in ["nav_rate", "read_rate", "retention_d7", "retention_d30", "revisit_rate"]:
        df[c] = df[c].round(6)

    return df


# ---------------------------------------------------------------------------
# MONTHLY — human-readable pivot sheet
# ---------------------------------------------------------------------------

def build_monthly_rows(
    df_talks_m: pd.DataFrame,
    df_article_m: pd.DataFrame,
    *,
    talk_top_regex: str,
) -> tuple[list[list], list[str], list[str]]:
    """Build the rows for the MONTHLY pivot sheet.

    Returns ``(body_rows, year_row, month_row)`` ready for sheet output.

    *body_rows*: list of lists — Top section (UU/新規UU) + 記事 section.
    *year_row*: ``["", year1, "", year2, ...]``
    *month_row*: ``["指標", "1月", "2月", ...]``

    Pure DataFrame transformation — no API calls.
    """
    empty: tuple[list[list], list[str], list[str]] = ([], [], [])

    if len(df_talks_m) == 0 or not {"month", "lang"}.issubset(df_talks_m.columns):
        return empty

    df_all = df_talks_m[df_talks_m["lang"].astype(str).str.upper() == "ALL"].copy()
    if len(df_all) == 0:
        return empty

    df_all["month"] = df_all["month"].astype(str).str.strip()
    df_all = df_all[df_all["month"].str.match(r"^\d{6}$")].copy()
    df_all = df_all.sort_values("month", kind="mergesort")
    months = df_all["month"].drop_duplicates().tolist()
    if len(months) == 0:
        return empty

    # Top page UU/新規UU from _article-m
    df_top_monthly = pd.DataFrame(columns=["month", "total_users", "new_users"])
    if len(df_article_m) > 0 and {"month", "page", "total_users", "new_users"}.issubset(df_article_m.columns):
        top_mask = df_article_m["page"].astype(str).str.match(talk_top_regex)
        df_top = df_article_m[top_mask].copy()
        df_top["month"] = df_top["month"].astype(str).str.strip()
        for c in ["total_users", "new_users"]:
            df_top[c] = pd.to_numeric(df_top[c], errors="coerce").fillna(0).astype(int)
        df_top_monthly = df_top.groupby("month", as_index=False)[["total_users", "new_users"]].sum()

    # Helper: extract values per month
    def _extract(df: pd.DataFrame, col: str, kind: str) -> list:
        vals: list = []
        for ym in months:
            s = df.loc[df["month"] == ym, col] if col in df.columns else pd.Series(dtype=object)
            if len(s) == 0:
                vals.append("")
                continue
            v = pd.to_numeric(s.iloc[0], errors="coerce")
            if pd.isna(v):
                vals.append("")
            elif kind == "int":
                vals.append(int(v))
            else:
                vals.append(float(v))
        return vals

    empty_vals = [""] * len(months)

    # Top section
    top_rows = [
        ["Top", *empty_vals],
        ["UU", *_extract(df_top_monthly, "total_users", "int")],
        ["新規UU", *_extract(df_top_monthly, "new_users", "int")],
    ]

    # Article section
    article_defs = [
        ("UU", "uu", "int"),
        ("新規UU", "new_users", "int"),
        ("回遊率", "nav_rate", "float"),
        ("読了率", "read_rate", "float"),
        ("維持率D7", "retention_d7", "float"),
        ("維持率D30", "retention_d30", "float"),
        ("再訪率", "revisit_rate", "float"),
    ]
    article_rows = [["記事", *empty_vals]]
    for label, col, kind in article_defs:
        article_rows.append([label, *_extract(df_all, col, kind)])

    body_rows = top_rows + article_rows

    # Header rows
    year_row: list[str] = [""]
    prev_y = ""
    for ym in months:
        y = ym[:4]
        year_row.append(y if y != prev_y else "")
        prev_y = y

    month_row = ["指標"] + [f"{int(m[4:6])}月" for m in months]

    return body_rows, year_row, month_row


# ---------------------------------------------------------------------------
# MONTHLY — metric definitions from Markdown
# ---------------------------------------------------------------------------

def _col_to_index(col: str) -> int:
    """Convert Excel-style column letter(s) to 1-based index (A=1, Z=26, AA=27)."""
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def _index_to_col(n: int) -> str:
    """Convert 1-based index to Excel-style column letter(s)."""
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(ord("A") + r) + out
    return out


def _split_a1(cell: str) -> tuple[str, int]:
    """Parse an A1-style cell reference into (col_letter, row_number)."""
    s = str(cell).strip().upper()
    col = "".join(ch for ch in s if "A" <= ch <= "Z")
    row_s = "".join(ch for ch in s if ch.isdigit())
    if not col or not row_s:
        return "B", 17
    return col, int(row_s)


def read_monthly_definitions(md_path: str) -> list[list[str]]:
    """Read the §12-1 metric definitions table from *md_path*.

    Returns a list of ``[display_name, description]`` pairs.
    Empty list if the file or table is not found.
    """
    try:
        with open(md_path, encoding="utf-8") as f:
            md_text = f.read()
    except FileNotFoundError:
        return []

    lines = md_text.splitlines()
    in_section = False
    table_rows: list[list[str]] = []
    for ln in lines:
        t = ln.strip()
        if t.startswith("### §12-1."):
            in_section = True
            continue
        if in_section and t.startswith("### "):
            break
        if not in_section or not t.startswith("|"):
            continue

        cells = [c.strip() for c in t.strip("|").split("|")]
        if len(cells) < 2:
            continue
        # Skip Markdown separator row (|---|---|)
        if all(set(c.replace(" ", "")) <= set("-:") for c in cells):
            continue
        table_rows.append(cells[:2])

    if not table_rows:
        return []
    # Skip header row
    if table_rows[0][0] == "表示名" and table_rows[0][1] == "説明":
        table_rows = table_rows[1:]
    return table_rows


def write_monthly_definitions(ws, *, start_cell: str = "B17", md_path: str) -> bool:
    """Write metric definitions from *md_path* to the worksheet *ws*.

    *ws* is a gspread Worksheet (or compatible) that supports ``.update()``
    and ``.batch_clear()``.

    Returns ``True`` if written, ``False`` if skipped.
    """
    rows = read_monthly_definitions(md_path)
    if not rows:
        print("[skip] MONTHLY definitions: source table not found in", md_path)
        return False

    col, row = _split_a1(start_cell)
    col3 = _index_to_col(_col_to_index(col) + 2)
    clear_to = row + max(60, len(rows) + 8)
    clear_range = f"{col}{row}:{col3}{clear_to}"
    if hasattr(ws, "batch_clear"):
        ws.batch_clear([clear_range])

    out_rows = [["指標の定義", "", ""]]
    for name, desc in rows:
        out_rows.append([name, "", desc])
    ws.update(start_cell, out_rows)
    print(f"MONTHLY definitions updated: rows={len(out_rows)} at {start_cell}")
    return True


def write_monthly_sheet(
    mg,
    *,
    body_rows: list[list],
    year_row: list[str],
    month_row: list[str],
    md_path: str,
    definition_start_cell: str = "B17",
    definitions_only: bool = False,
) -> bool:
    """Write (or update definitions only) the MONTHLY pivot sheet.

    *mg* is the Megaton instance with a Sheets connection.
    *md_path* is the path to ``corp-talks.md`` for metric definitions.

    Returns ``True`` if output was written.
    """
    sheets = list(getattr(mg.gs, "sheets", []) or [])

    if definitions_only:
        if "MONTHLY" not in sheets:
            print("[skip] MONTHLY definitions: MONTHLY sheet not found")
            return False
        mg.sheets.select("MONTHLY")
        ws = getattr(mg.gs.sheet, "_driver", None)
        if ws and hasattr(ws, "update"):
            write_monthly_definitions(ws, start_cell=definition_start_cell, md_path=md_path)
            return True
        print("[skip] MONTHLY definitions: worksheet driver does not support update()")
        return False

    if len(body_rows) == 0:
        print("[skip] MONTHLY: no rows")
        return False

    if "MONTHLY" not in sheets:
        mg.sheets.create("MONTHLY")
    mg.sheets.select("MONTHLY")
    ws = getattr(mg.gs.sheet, "_driver", None)
    if not (ws and hasattr(ws, "update")):
        print("[skip] MONTHLY: worksheet driver does not support update()")
        return False

    try:
        if hasattr(ws, "clear"):
            ws.clear()
        else:
            mg.gs.sheet.clear()
    except Exception:
        pass

    ws.update("A1", [["Talksの推移"]])
    ws.update("A2", [year_row])
    ws.update("A3", [month_row])
    ws.update("A4", body_rows)
    write_monthly_definitions(ws, start_cell=definition_start_cell, md_path=md_path)
    print(f"MONTHLY updated: months={len(month_row)-1} rows={len(body_rows)}")
    return True
