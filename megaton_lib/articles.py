"""Article metadata helpers (GA4 custom dimensions).

This module centralizes notebook-side aggregation logic so reports stay small and
consistent across projects.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


def aggregate_article_meta(
    df_meta_raw: pd.DataFrame,
    *,
    preferred_langs: Iterable[str] = ("jp", "en"),
    title_joiner: str = " / ",
    lang_order: dict[str, int] | None = None,
    min_valid_year: int = 2000,
) -> pd.DataFrame:
    """Aggregate GA4 article meta rows into one row per article_id.

    Expects columns:
    - article_id (str)
    - article_title (str/nullable)
    - article_category (str/nullable)
    - article_date (str/nullable): e.g. "Nov. 3, 2025" or "2025/11/3" or "(not set)"
    - likes (numeric/str/nullable)
    - language (str/nullable): expected 'jp'/'en'/'cn' (post-processed)
    - pv (numeric): used to select the "best" row for fields that can vary

    Rules:
    - language: unique non-empty langs, ordered by ``lang_order``, joined with "/"
    - title: pick PV-max title per preferred language, join with ``title_joiner``
      fallback: PV-max valid title across all langs, then article_id
    - category: PV-max valid category, else category from PV-max row
    - date: pick date from PV-max row among valid parsed dates (year>=min_valid_year)
      (avoid 1900/.. noise)
    - likes: max numeric likes, coerced to int64 (missing -> 0)
    """
    if not isinstance(df_meta_raw, pd.DataFrame):
        raise TypeError("df_meta_raw must be a pandas DataFrame")

    required = {
        "article_id",
        "article_title",
        "article_category",
        "article_date",
        "likes",
        "language",
        "pv",
    }
    missing = sorted(required - set(df_meta_raw.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    order = lang_order or {"jp": 0, "en": 1, "cn": 2}

    df = df_meta_raw.copy()

    # likes: keep int semantics even if floats/strings/nulls are present.
    df.loc[:, "likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0).astype("int64")

    # Parse date into datetime64; keep original string column intact.
    ad_raw = df["article_date"].astype("string").str.strip()
    ad_raw = ad_raw.replace({"": pd.NA, "(not set)": pd.NA})
    df.loc[:, "article_date_dt"] = pd.to_datetime(ad_raw, format="mixed", dayfirst=False, errors="coerce")

    def _ymd(dt: pd.Timestamp) -> str:
        # Avoid platform-dependent strftime flags (e.g. %-m on Windows).
        return f"{dt.year}/{dt.month}/{dt.day}"

    def _agg(g: pd.DataFrame) -> pd.Series:
        pv = pd.to_numeric(g["pv"], errors="coerce").fillna(-1)

        langs = (
            g.loc[g["language"].notna() & (g["language"] != ""), "language"]
            .astype(str)
            .unique()
        )
        language = "/".join(sorted(langs, key=lambda x: order.get(x, 99))).upper()

        valid_title = g[g["article_title"].notna() & ~g["article_title"].isin(["", "(not set)"])]
        titles: list[str] = []
        for lang in preferred_langs:
            rows = valid_title[valid_title["language"] == lang]
            if len(rows) > 0:
                best = rows.loc[pd.to_numeric(rows["pv"], errors="coerce").fillna(-1).idxmax(), "article_title"]
                titles.append(str(best))
        if not titles and len(valid_title) > 0:
            best = valid_title.loc[pd.to_numeric(valid_title["pv"], errors="coerce").fillna(-1).idxmax(), "article_title"]
            titles.append(str(best))
        article_title = title_joiner.join(titles) if titles else str(g.name)

        valid_cat = g[g["article_category"].notna() & ~g["article_category"].isin(["", "(not set)"])]
        if len(valid_cat) > 0:
            article_category = valid_cat.loc[pd.to_numeric(valid_cat["pv"], errors="coerce").fillna(-1).idxmax(), "article_category"]
        else:
            article_category = g.loc[pv.idxmax(), "article_category"]

        valid_dates = g[g["article_date_dt"].notna() & (g["article_date_dt"].dt.year >= min_valid_year)]
        if len(valid_dates) > 0:
            best_dt = valid_dates.loc[pd.to_numeric(valid_dates["pv"], errors="coerce").fillna(-1).idxmax(), "article_date_dt"]
            article_date = _ymd(best_dt)
        else:
            article_date = ""

        likes = int(pd.to_numeric(g["likes"], errors="coerce").fillna(0).max())

        return pd.Series(
            {
                "article_title": article_title,
                "article_category": article_category,
                "article_date": article_date,
                "likes": likes,
                "language": language,
            }
        )

    # pandas>=2.1: include_groups=False avoids deprecation warning.
    try:
        out = df.groupby("article_id").apply(_agg, include_groups=False).reset_index()
    except TypeError:
        out = df.groupby("article_id").apply(_agg).reset_index()
    out.loc[:, "likes"] = pd.to_numeric(out["likes"], errors="coerce").fillna(0).astype("int64")
    return out
