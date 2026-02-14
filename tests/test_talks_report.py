"""Tests for megaton_lib.talks_report."""

import types

import pandas as pd

from megaton_lib.talks_report import (
    build_article_sheet,
    build_monthly_rows,
    build_monthly_view,
    build_talks_m,
    read_monthly_definitions,
    write_monthly_definitions,
    write_monthly_sheet,
)


class TestBuildMonthlyView:
    def test_merges_meta(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "language": ["jp"],
            "page": ["/jp/company/talk/20260101.html"],
            "pv": [100],
            "sessions": [80],
            "nav_clicks": [10],
            "nav_rate": [0.125],
            "entrances": [60],
            "total_users": [70],
            "new_users": [30],
            "bounces": [10],
            "footer_views": [5],
        })
        df_meta = pd.DataFrame({
            "URL": ["/jp/company/talk/20260101.html"],
            "Title": ["テスト記事"],
            "Language": ["JP"],
            "Tag": ["t"],
            "Date": ["2026-01-01"],
        })
        result = build_monthly_view(df_page, df_meta)
        assert "Title" in result.columns
        assert "published_date" in result.columns
        assert result.iloc[0]["Title"] == "テスト記事"
        assert result.iloc[0]["language"] == "JP"

    def test_no_meta_match(self):
        df_page = pd.DataFrame({
            "month": ["202601"],
            "language": ["en"],
            "page": ["/en/company/talk/nomatch.html"],
            "pv": [50],
            "sessions": [40],
            "nav_clicks": [0],
            "nav_rate": [0.0],
            "entrances": [30],
            "total_users": [35],
            "new_users": [15],
            "bounces": [5],
            "footer_views": [2],
        })
        df_meta = pd.DataFrame(columns=["URL", "Title", "Language", "Tag", "Date"])
        result = build_monthly_view(df_page, df_meta)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["Title"])


class TestBuildArticleSheet:
    """Tests for build_article_sheet (ARTICLE cumulative from _article-m)."""

    def _make_article_m(self) -> pd.DataFrame:
        """Create sample _article-m data spanning 2 months."""
        return pd.DataFrame({
            "month": ["202601", "202601", "202602", "202602"],
            "page": [
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
            ],
            "language": ["jp", "en", "jp", "en"],
            "pv": [100, 50, 120, 60],
            "sessions": [80, 40, 90, 45],
            "nav_clicks": [10, 5, 12, 6],
            "total_users": [70, 35, 85, 42],
            "new_users": [30, 15, 25, 10],
            "footer_views": [20, 10, 24, 12],
        })

    def _make_meta(self) -> pd.DataFrame:
        return pd.DataFrame({
            "URL": [
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
            ],
            "Title": ["JP記事", "EN Article"],
            "Language": ["JP", "EN"],
            "Tag": ["interview", "report"],
            "Date": ["2026-01-01", "2026-01-15"],
        })

    def test_aggregates_across_months(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert len(result) == 2
        # JP article: pv=100+120=220, sessions=80+90=170
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        assert jp["nav_clicks"] == 22  # 10+12
        assert jp["uu_total"] == 155   # 70+85 (sum of total_users)
        assert jp["nav_rate"] == round(22 / 170, 6)

    def test_read_rate(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        # footer_views=44, pv=220
        assert jp["read_rate"] == round(44 / 220, 6)

    def test_sort_jp_first(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert result.iloc[0]["lang"] == "JP"
        assert result.iloc[1]["lang"] == "EN"

    def test_empty_article_m(self):
        result = build_article_sheet(
            pd.DataFrame(columns=["month", "page", "pv", "sessions", "nav_clicks", "total_users", "new_users", "footer_views"]),
            self._make_meta(),
        )
        assert len(result) == 0

    def test_excludes_top_pages(self):
        """Top pages in _article-m should not contribute metrics to ARTICLE."""
        df = pd.concat([
            self._make_article_m(),
            pd.DataFrame({
                "month": ["202601"],
                "page": ["/jp/company/talk/"],
                "language": ["jp"],
                "pv": [500], "sessions": [400],
                "nav_clicks": [0], "total_users": [300],
                "new_users": [100], "footer_views": [0],
            }),
        ], ignore_index=True)
        result = build_article_sheet(df, self._make_meta())
        # Top page should not appear in output
        assert "/jp/company/talk/" not in result["page"].values
        # Article pages should still be present
        assert len(result) == 2

    def test_meta_columns(self):
        result = build_article_sheet(self._make_article_m(), self._make_meta())
        assert {"lang", "published_date", "tag", "title", "page", "uu_total"}.issubset(result.columns)
        assert "total_users" not in result.columns  # uu_total に統合済み
        jp = result[result["page"] == "/jp/company/talk/20260101.html"].iloc[0]
        assert jp["title"] == "JP記事"
        assert jp["tag"] == "interview"
        assert jp["published_date"] == "2026-01-01"


# ---------------------------------------------------------------------------
# build_talks_m
# ---------------------------------------------------------------------------

class TestBuildTalksM:
    """Tests for build_talks_m (_talks-m construction)."""

    def _make_ga4(self) -> pd.DataFrame:
        return pd.DataFrame({
            "month": ["202601", "202601", "202601"],
            "lang": ["JP", "EN", "ALL"],
            "pv": [100, 50, 150],
            "sessions": [80, 40, 120],
            "uu": [70, 35, 100],
            "new_users": [30, 15, 42],
            "pv_top": [25, 5, 30],
            "uu_top": [20, 4, 24],
            "pv_all": [125, 55, 180],
            "uu_all": [85, 39, 124],
            "new_users_all": [34, 16, 50],
            "footer_views": [20, 10, 30],
            "read_rate": [0.2, 0.2, 0.2],
        })

    def _make_article_m(self) -> pd.DataFrame:
        return pd.DataFrame({
            "month": ["202601", "202601"],
            "page": [
                "/jp/company/talk/20260101.html",
                "/en/company/talk/20260115.html",
            ],
            "nav_clicks": [10, 5],
        })

    def _make_revisit(self) -> pd.DataFrame:
        return pd.DataFrame({
            "month": ["202601", "202601"],
            "language": ["JP", "EN"],
            "prev_month_users": [50, 20],
            "revisit_users": [25, 10],
            "revisit_rate": [0.5, 0.5],
        })

    def test_basic_merge(self):
        result = build_talks_m(
            self._make_ga4(),
            df_article_m=self._make_article_m(),
            df_revisit_m=self._make_revisit(),
        )
        assert len(result) == 3  # JP, EN, ALL
        all_row = result[result["lang"] == "ALL"].iloc[0]
        assert all_row["nav_clicks"] == 15  # 10+5
        assert all_row["nav_rate"] == round(15 / 120, 6)

    def test_monthly_summary_columns(self):
        result = build_talks_m(
            self._make_ga4(),
            df_article_m=self._make_article_m(),
            df_revisit_m=self._make_revisit(),
        )
        all_row = result[result["lang"] == "ALL"].iloc[0]
        assert all_row["pv_top"] == 30
        assert all_row["uu_top"] == 24
        assert all_row["pv_all"] == 180
        assert all_row["uu_all"] == 124
        assert all_row["new_users_all"] == 50

    def test_empty_ga4(self):
        result = build_talks_m(
            pd.DataFrame(columns=["month", "lang", "pv", "sessions", "uu", "new_users", "footer_views", "read_rate"]),
            df_article_m=self._make_article_m(),
            df_revisit_m=self._make_revisit(),
        )
        assert len(result) == 0

    def test_missing_revisit(self):
        result = build_talks_m(
            self._make_ga4(),
            df_article_m=self._make_article_m(),
            df_revisit_m=pd.DataFrame(),
        )
        assert len(result) == 3
        assert pd.isna(result.iloc[0]["revisit_rate"])


# ---------------------------------------------------------------------------
# build_monthly_rows
# ---------------------------------------------------------------------------

class TestBuildMonthlyRows:
    """Tests for build_monthly_rows (MONTHLY pivot sheet)."""

    TALK_TOP_REGEX = r"^/(en|jp)/company/talk(/(index\.html)?)?$"

    def _make_talks_m(self) -> pd.DataFrame:
        return pd.DataFrame({
            "month": ["202601", "202602", "202601", "202602"],
            "lang": ["ALL", "ALL", "JP", "JP"],
            "pv": [100, 120, 70, 80],
            "uu": [100, 120, 70, 80],
            "new_users": [40, 50, 30, 35],
            "pv_top": [30, 35, 0, 0],
            "uu_top": [200, 220, 0, 0],
            "pv_all": [260, 300, 0, 0],
            "uu_all": [300, 340, 0, 0],
            "new_users_all": [120, 130, 0, 0],
            "nav_rate": [0.1, 0.12, 0.08, 0.09],
            "read_rate": [0.2, 0.22, 0.18, 0.19],
            "revisit_rate": [0.5, 0.52, 0.45, 0.48],
        })

    def _make_article_m(self) -> pd.DataFrame:
        return pd.DataFrame({
            "month": ["202601", "202602"],
            "page": ["/jp/company/talk/", "/jp/company/talk/"],
            "pv": [30, 35],
            "total_users": [200, 220],
            "new_users": [80, 90],
        })

    def test_structure(self):
        body, year_row, month_row = build_monthly_rows(
            self._make_talks_m(), self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        # 2 months
        assert len(month_row) == 3  # ["指標", "1月", "2月"]
        assert month_row[1] == "1月"
        assert month_row[2] == "2月"
        # body: Top(3) + 全体(7) = 10 rows
        assert len(body) == 10
        assert body[0][0] == "Top"
        assert body[3][0] == "全体"

    def test_top_values(self):
        body, _, _ = build_monthly_rows(
            self._make_talks_m(), self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        # Top PV row: [label, jan, feb]
        assert body[1][0] == "PV"
        assert body[1][1] == 30
        assert body[1][2] == 35
        # Top UU row
        assert body[2][0] == "UU"
        assert body[2][1] == 200
        assert body[2][2] == 220

    def test_all_values(self):
        body, _, _ = build_monthly_rows(
            self._make_talks_m(), self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        # 全体 PV row (index 4): from ALL rows
        assert body[4][0] == "PV"
        assert body[4][1] == 260
        assert body[4][2] == 300
        # 全体 UU row (index 5)
        assert body[5][0] == "UU"
        assert body[5][1] == 300
        assert body[5][2] == 340
        # 全体 新規UU row (index 6)
        assert body[6][0] == "新規UU"
        assert body[6][1] == 120
        assert body[6][2] == 130

    def test_fallback_when_new_columns_exist_but_empty(self):
        talks = self._make_talks_m().copy()
        # Simulate old _talks-m rows where new columns exist but are empty
        for c in ["pv_top", "uu_top", "pv_all", "uu_all", "new_users_all"]:
            talks[c] = pd.NA

        body, _, _ = build_monthly_rows(
            talks, self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        # Top fallback from _article-m
        assert body[1][1] == 30
        assert body[2][1] == 200
        # 全体 fallback from pv/uu/new_users
        assert body[4][1] == 100
        assert body[5][1] == 100
        assert body[6][1] == 40

    def test_empty_talks_m(self):
        body, year_row, month_row = build_monthly_rows(
            pd.DataFrame(), pd.DataFrame(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        assert body == []
        assert year_row == []
        assert month_row == []

    def test_year_row_groups(self):
        """Same year should only show once."""
        _, year_row, _ = build_monthly_rows(
            self._make_talks_m(), self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        # ["", "2026", ""] — second month same year is blank
        assert year_row[1] == "2026"
        assert year_row[2] == ""

    def test_inserts_annual_total_and_average_after_december(self):
        talks = pd.DataFrame({
            "month": ["202611", "202612", "202701"],
            "lang": ["ALL", "ALL", "ALL"],
            "pv_top": [100, 200, 300],
            "uu_top": [10, 30, 50],
            "pv_all": [1000, 1200, 1400],
            "uu_all": [100, 120, 140],
            "new_users_all": [40, 60, 80],
            "nav_rate": [0.1, 0.3, 0.2],
            "read_rate": [0.2, 0.4, 0.3],
            "revisit_rate": [0.5, 0.7, 0.6],
        })

        body, year_row, month_row = build_monthly_rows(
            talks, pd.DataFrame(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )

        # 指標 + 2026/11, 2026/12, 2026年合計, 2026年平均, 2027/1
        assert month_row == ["指標", "11月", "12月", "年合計", "年平均", "1月"]
        assert year_row == ["", "2026", "", "", "", "2027"]

        # Top PV: 100, 200, 300(2026合計), 150(2026平均), 300
        assert body[1] == ["PV", 100, 200, 300, 150, 300]
        # 全体 UU: 100, 120, 220(2026合計), 110(2026平均), 140
        assert body[5] == ["UU", 100, 120, 220, 110, 140]
        # Rate rows: annual total is blank, annual average is set
        assert body[7] == ["閲覧後回遊率", 0.1, 0.3, "", 0.2, 0.2]

    def test_rate_percent_string_values(self):
        """Rate columns should accept '%' strings loaded from Sheets."""
        talks = self._make_talks_m().copy()
        talks["nav_rate"] = ["10%", "12%", "8%", "9%"]
        talks["read_rate"] = ["20%", "22%", "18%", "19%"]
        talks["revisit_rate"] = ["50%", "52%", "45%", "48%"]

        body, _, _ = build_monthly_rows(
            talks, self._make_article_m(),
            talk_top_regex=self.TALK_TOP_REGEX,
        )
        assert body[7][1] == 0.1   # 閲覧後回遊率 (1月)
        assert body[8][1] == 0.2   # 読了率 (1月)
        assert body[9][1] == 0.5   # Talks再訪率 (1月)


# ---------------------------------------------------------------------------
# read_monthly_definitions
# ---------------------------------------------------------------------------

class TestReadMonthlyDefinitions:
    """Tests for read_monthly_definitions (Markdown → list)."""

    _MD = """\
### §12-1. MONTHLY 指標定義（シート注記用）

| 表示名 | 説明 |
|---|---|
| UU | ユニークユーザー数 |
| 新規UU | 初めて訪れたユーザー数 |

### §13. 次のセクション
"""

    def test_parses_rows(self, tmp_path):
        p = tmp_path / "corp-talks.md"
        p.write_text(self._MD, encoding="utf-8")
        rows = read_monthly_definitions(str(p))
        assert len(rows) == 2
        assert rows[0] == ["UU", "ユニークユーザー数"]
        assert rows[1] == ["新規UU", "初めて訪れたユーザー数"]

    def test_missing_file(self, tmp_path):
        rows = read_monthly_definitions(str(tmp_path / "nonexistent.md"))
        assert rows == []

    def test_no_section(self, tmp_path):
        p = tmp_path / "empty.md"
        p.write_text("# No section here\n", encoding="utf-8")
        rows = read_monthly_definitions(str(p))
        assert rows == []

    def test_stops_at_next_heading_any_level(self, tmp_path):
        md = """\
### §12-1. MONTHLY 指標定義（シート注記用）

| 表示名 | 説明 |
|---|---|
| PV | 表示回数の合計 |
| UU | 閲覧者数 |

## 出力シート一覧

| シート名 | キー |
|---|---|
| `_meta` | URL |
"""
        p = tmp_path / "corp-talks.md"
        p.write_text(md, encoding="utf-8")
        rows = read_monthly_definitions(str(p))
        assert rows == [
            ["PV", "表示回数の合計"],
            ["UU", "閲覧者数"],
        ]


# ---------------------------------------------------------------------------
# write_monthly_definitions
# ---------------------------------------------------------------------------

class TestWriteMonthlyDefinitions:
    """Tests for write_monthly_definitions (list → worksheet)."""

    _MD = """\
### §12-1. MONTHLY 指標定義（シート注記用）

| 表示名 | 説明 |
|---|---|
| UU | ユニークユーザー数 |

### §13. end
"""

    def test_writes_to_worksheet(self, tmp_path):
        p = tmp_path / "corp-talks.md"
        p.write_text(self._MD, encoding="utf-8")

        calls = {}
        ws = types.SimpleNamespace(
            batch_clear=lambda ranges: calls.update(cleared=ranges),
            update=lambda cell, data: calls.update(cell=cell, data=data),
        )
        result = write_monthly_definitions(ws, start_cell="A17", md_path=str(p))
        assert result is True
        assert "cleared" in calls
        assert calls["cell"] == "A17"
        assert calls["data"][0] == ["指標の定義"]
        assert calls["data"][1][0] == "UU：ユニークユーザー数"

    def test_skips_when_no_md(self, tmp_path):
        ws = types.SimpleNamespace()
        result = write_monthly_definitions(ws, md_path=str(tmp_path / "nope.md"))
        assert result is False


# ---------------------------------------------------------------------------
# write_monthly_sheet
# ---------------------------------------------------------------------------

class TestWriteMonthlySheet:
    """Tests for write_monthly_sheet (orchestration)."""

    _MD = """\
### §12-1. MONTHLY 指標定義（シート注記用）

| 表示名 | 説明 |
|---|---|
| UU | ユニークユーザー数 |

### §13. end
"""

    def _make_mg(self, *, has_monthly=True):
        sheets_list = ["_talks-m", "_article-m"]
        if has_monthly:
            sheets_list.append("MONTHLY")

        calls = {"updates": []}
        ws = types.SimpleNamespace(
            clear=lambda: None,
            batch_clear=lambda ranges: None,
            update=lambda cell, data: calls["updates"].append((cell, data)),
        )
        sheet_ns = types.SimpleNamespace(_driver=ws, data=[])
        gs = types.SimpleNamespace(sheets=sheets_list, sheet=sheet_ns)
        mg = types.SimpleNamespace(gs=gs)

        def select(name):
            mg.gs.sheet = sheet_ns
        def create(name):
            sheets_list.append(name)

        mg.sheets = types.SimpleNamespace(select=select, create=create)
        mg.sheet = sheet_ns
        return mg, calls

    def test_full_write(self, tmp_path):
        p = tmp_path / "corp-talks.md"
        p.write_text(self._MD, encoding="utf-8")
        mg, calls = self._make_mg()
        result = write_monthly_sheet(
            mg,
            body_rows=[["Top", 100], ["UU", 200]],
            year_row=["", "2026"],
            month_row=["指標", "1月"],
            md_path=str(p),
        )
        assert result is True
        cells_written = [c for c, _ in calls["updates"]]
        assert "A1" in cells_written
        assert "A4" in cells_written

    def test_definitions_only(self, tmp_path):
        p = tmp_path / "corp-talks.md"
        p.write_text(self._MD, encoding="utf-8")
        mg, calls = self._make_mg()
        result = write_monthly_sheet(
            mg,
            body_rows=[],
            year_row=[],
            month_row=[],
            md_path=str(p),
            definitions_only=True,
        )
        assert result is True

    def test_empty_rows_skips(self, tmp_path):
        mg, calls = self._make_mg()
        result = write_monthly_sheet(
            mg,
            body_rows=[],
            year_row=[],
            month_row=[],
            md_path=str(tmp_path / "nope.md"),
        )
        assert result is False

    def test_creates_sheet_if_missing(self, tmp_path):
        p = tmp_path / "corp-talks.md"
        p.write_text(self._MD, encoding="utf-8")
        mg, calls = self._make_mg(has_monthly=False)
        result = write_monthly_sheet(
            mg,
            body_rows=[["Top", 100]],
            year_row=["", "2026"],
            month_row=["指標", "1月"],
            md_path=str(p),
        )
        assert result is True
        assert "MONTHLY" in mg.gs.sheets
