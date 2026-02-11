"""Web scraping helpers for Corp Talks articles.

Extracts metadata from the Talks top page and inline links from article bodies.
"""

from __future__ import annotations

import datetime as dt
import re
from urllib.parse import urljoin, urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Text / date utilities
# ---------------------------------------------------------------------------

def collapse_whitespace(text: object) -> str:
    """Collapse all whitespace runs into a single space."""
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text).strip()


def parse_date_jp_en(text: str) -> str:
    """Parse JP / EN / numeric date strings to ``YYYY-MM-DD``.

    Handles:
    - JP: ``2025年11月27日``
    - EN: ``November 27, 2025``
    - Numeric: ``11/27/2025`` or ``11-27-2025``

    Returns empty string on failure.
    """
    # JP: 2025年11月27日
    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"

    # EN: November 27, 2025
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", text)
    if m:
        month_abbr = m.group(1).strip().lower()[:3]
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        mo = month_map.get(month_abbr)
        if mo:
            d, y = int(m.group(2)), int(m.group(3))
            return f"{y:04d}-{mo:02d}-{d:02d}"

    # Numeric fallback: 11/27/2025 or 11-27-2025
    m = re.search(r"(\d{1,2})\D+(\d{1,2})\D+(\d{4})", text)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"

    return ""


def normalize_url(value: object, hostname: str) -> str:
    """Normalize a URL to internal path or external URL (no fragment).

    Internal links (matching *hostname*) are returned as path only.
    External links are returned as full URL with fragment stripped.
    This replaces ``_normalize_link`` and ``_normalize_click_target`` in the
    notebook (which were 3 near-identical implementations).
    """
    if not isinstance(value, str):
        return ""
    s = value.strip()
    if not s:
        return ""
    # Already a path
    if s.startswith("/"):
        return s.split("#", 1)[0]
    sp = urlsplit(s)
    if sp.scheme in ("http", "https") and sp.netloc:
        host = sp.netloc.lower()
        if host == hostname:
            return sp.path or ""
        return f"{sp.scheme}://{sp.netloc}{sp.path}" + (f"?{sp.query}" if sp.query else "")
    return s.split("#", 1)[0]


# ---------------------------------------------------------------------------
# _meta: scrape Talk cards from the top page
# ---------------------------------------------------------------------------

def scrape_talk_cards(home_url: str, lang: str) -> pd.DataFrame:
    """Scrape Talk article cards from the corporate top page.

    Returns a DataFrame with columns: ``URL, Title, Language, Tag, Date``.
    """
    r = requests.get(home_url, timeout=20)
    r.encoding = r.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    code = lang.lower()
    anchors = (
        soup.select(f".talksList_item a[id^='{code}_talk_']")
        or soup.select(".talksList_item a")
    )

    rows: list[dict] = []
    seen: set[str] = set()
    for a in anchors:
        href = a.get("href") or ""
        if not href:
            continue
        abs_url = urljoin(home_url, href)
        path = urlsplit(abs_url).path or ""
        if not (path.startswith(f"/{code}/company/talk/") and path.endswith(".html")):
            continue
        if path in seen:
            continue
        seen.add(path)

        title_el = a.select_one(".details_title")
        title = collapse_whitespace((title_el or a).get_text(" ", strip=True))

        date_el = a.select_one(".details_date")
        raw_date = collapse_whitespace(date_el.get_text(" ", strip=True)) if date_el else ""
        date = parse_date_jp_en(raw_date)

        types = [collapse_whitespace(x.get_text(" ", strip=True)) for x in a.select(".details_type")]
        types = [t for t in types if t]
        tag = " / ".join(types)

        rows.append({"URL": path, "Title": title, "Language": lang.upper(), "Tag": tag, "Date": date})

    return pd.DataFrame(rows, columns=["URL", "Title", "Language", "Tag", "Date"])


def normalize_meta_sheet(obj) -> pd.DataFrame:
    """Normalize a ``_meta`` sheet object to a clean DataFrame.

    Accepts a DataFrame or list-of-dicts from ``mg.gs.sheet.data`` and returns
    a DataFrame with exactly ``[URL, Title, Language, Tag, Date]``.
    """
    expected = ["URL", "Title", "Language", "Tag", "Date"]
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    else:
        try:
            df = pd.DataFrame(obj)
        except Exception:
            df = pd.DataFrame()

    if set(expected).issubset(df.columns):
        df = df[expected].copy()
    elif df.shape[1] >= 5:
        df = df.iloc[:, :5].copy()
        df.columns = expected
    else:
        df = pd.DataFrame(columns=expected)

    for c in ["URL", "Title", "Language", "Tag"]:
        df[c] = df[c].astype(str).fillna("").str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["URL"] != ""].drop_duplicates("URL", keep="last").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# _link: scrape inline links from article body
# ---------------------------------------------------------------------------

def scrape_article_links(
    url: str,
    *,
    scope_selector: str = "div.article_contents",
    hostname: str = "corp.shiseido.com",
) -> pd.DataFrame:
    """Extract hyperlinks from the article body of the given URL.

    Returns a DataFrame with columns:
    ``crawled_at, from_url, from_path, href, to_url, to_host, to_path, text, context``

    Excludes: recommend section, same-page anchors, self-links, Talks top links.
    """
    r = requests.get(url, timeout=20)
    r.encoding = r.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    scope = soup.select_one(scope_selector)
    if not scope:
        return pd.DataFrame(
            columns=["crawled_at", "from_url", "from_path", "href",
                     "to_url", "to_host", "to_path", "text", "context"]
        )

    def _context(a_tag) -> str:
        cur = a_tag
        for _ in range(8):
            if cur is None:
                break
            if getattr(cur, "get", None):
                if cur.get("id"):
                    return f"#{cur.get('id')}"
                cls = cur.get("class") or []
                if cls:
                    return "." + ".".join(cls[:3])
            cur = getattr(cur, "parent", None)
        return ""

    from_path = urlsplit(url).path
    rows: list[dict] = []
    for a in scope.select("a[href]"):
        if a.find_parent(id="recommend") is not None:
            continue
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        to_url = urljoin(url, href)
        sp = urlsplit(to_url)
        if sp.fragment and (sp.path or "") == from_path:
            continue
        if (sp.path or "") == from_path:
            continue
        if re.fullmatch(r"/(en|jp)/company/talk/?", sp.path or ""):
            continue
        rows.append({
            "from_url": url,
            "from_path": from_path,
            "href": href,
            "to_url": to_url,
            "to_host": sp.netloc.lower(),
            "to_path": sp.path,
            "text": collapse_whitespace(a.get_text(" ", strip=True)),
            "context": _context(a),
        })

    df = pd.DataFrame(rows)
    jst = dt.timezone(dt.timedelta(hours=9))
    df.insert(0, "crawled_at", dt.datetime.now(jst).isoformat(timespec="seconds"))
    return df


def crawl_new_article_links(
    mg,
    df_page: pd.DataFrame,
    *,
    hostname: str = "corp.shiseido.com",
) -> pd.DataFrame:
    """Crawl article links for pages not yet in ``_link`` sheet.

    Returns a DataFrame with ``[fromPath, link, CrawledAt]`` ready for upsert.
    """
    try:
        existing_raw = pd.DataFrame(mg.sheets.read("_link") or [])
    except Exception:
        existing_raw = pd.DataFrame()

    if "fromPath" in existing_raw.columns:
        existing_fps = set(existing_raw["fromPath"].astype(str).str.strip())
    elif "from_path" in existing_raw.columns:
        existing_fps = set(existing_raw["from_path"].astype(str).str.strip())
    else:
        existing_fps = set()

    page_paths = df_page["page"].dropna().astype(str).drop_duplicates().tolist()
    targets = [p for p in page_paths if p not in existing_fps]
    if not targets:
        print("[skip] crawl: no new pages")
        return pd.DataFrame(columns=["fromPath", "link", "CrawledAt"])

    base = f"https://{hostname}"
    dfs: list[pd.DataFrame] = []
    for fp in targets:
        url = urljoin(base, fp)
        raw = scrape_article_links(url, hostname=hostname)
        if raw.empty:
            continue
        one = raw.copy()
        one["link"] = one.apply(lambda r: normalize_url(r.get("to_url", ""), hostname), axis=1)
        one = one[["from_path", "link", "crawled_at"]].rename(
            columns={"from_path": "fromPath", "crawled_at": "CrawledAt"}
        )
        dfs.append(one)

    if not dfs:
        print("[skip] _link: no rows")
        return pd.DataFrame(columns=["fromPath", "link", "CrawledAt"])

    df_new = pd.concat(dfs, ignore_index=True)
    df_new = df_new.drop_duplicates(subset=["fromPath", "link"], keep="first").reset_index(drop=True)
    return df_new
