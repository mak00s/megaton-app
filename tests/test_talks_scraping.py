"""Tests for megaton_lib.talks_scraping."""

import pandas as pd
import pytest

from megaton_lib.talks_scraping import (
    collapse_whitespace,
    normalize_meta_sheet,
    normalize_url,
    parse_date_jp_en,
)


class TestCollapseWhitespace:
    def test_basic(self):
        assert collapse_whitespace("  hello   world  ") == "hello world"

    def test_non_string(self):
        assert collapse_whitespace(None) == ""
        assert collapse_whitespace(42) == ""


class TestParseDateJpEn:
    def test_jp_format(self):
        assert parse_date_jp_en("2025年11月27日") == "2025-11-27"

    def test_en_format(self):
        assert parse_date_jp_en("November 27, 2025") == "2025-11-27"

    def test_en_short_month(self):
        assert parse_date_jp_en("Jan 5, 2026") == "2026-01-05"

    def test_numeric_slash(self):
        assert parse_date_jp_en("11/27/2025") == "2025-11-27"

    def test_numeric_dash(self):
        assert parse_date_jp_en("11-27-2025") == "2025-11-27"

    def test_invalid(self):
        assert parse_date_jp_en("no date here") == ""

    def test_empty(self):
        assert parse_date_jp_en("") == ""


class TestNormalizeUrl:
    HOST = "corp.shiseido.com"

    def test_internal_path(self):
        assert normalize_url("/jp/company/talk/20250101.html", self.HOST) == "/jp/company/talk/20250101.html"

    def test_internal_full_url(self):
        assert normalize_url("https://corp.shiseido.com/jp/page.html", self.HOST) == "/jp/page.html"

    def test_external_url(self):
        result = normalize_url("https://example.com/path?q=1#frag", self.HOST)
        assert result == "https://example.com/path?q=1"

    def test_strip_fragment_from_path(self):
        assert normalize_url("/page.html#section", self.HOST) == "/page.html"

    def test_empty(self):
        assert normalize_url("", self.HOST) == ""
        assert normalize_url(None, self.HOST) == ""


class TestNormalizeMetaSheet:
    def test_standard_columns(self):
        data = [
            {"URL": "/jp/talk/a.html", "Title": "A", "Language": "JP", "Tag": "t", "Date": "2025-01-01"},
        ]
        df = normalize_meta_sheet(data)
        assert list(df.columns) == ["URL", "Title", "Language", "Tag", "Date"]
        assert len(df) == 1

    def test_dedup_by_url_keep_last(self):
        data = [
            {"URL": "/same", "Title": "Old", "Language": "JP", "Tag": "t", "Date": "2025-01-01"},
            {"URL": "/same", "Title": "New", "Language": "EN", "Tag": "t2", "Date": "2025-02-01"},
        ]
        df = normalize_meta_sheet(data)
        assert len(df) == 1
        assert df.iloc[0]["URL"] == "/same"
        assert df.iloc[0]["Title"] == "New"

    def test_filters_empty_url(self):
        data = [
            {"URL": "", "Title": "X", "Language": "JP", "Tag": "", "Date": ""},
            {"URL": "/valid", "Title": "Y", "Language": "EN", "Tag": "", "Date": ""},
        ]
        df = normalize_meta_sheet(data)
        assert len(df) == 1
        assert df.iloc[0]["URL"] == "/valid"

    def test_positional_fallback(self):
        df_input = pd.DataFrame([
            ["/url", "title", "JP", "tag", "2025-01-01"],
        ])
        df = normalize_meta_sheet(df_input)
        assert df.iloc[0]["URL"] == "/url"

    def test_empty_input(self):
        df = normalize_meta_sheet([])
        assert list(df.columns) == ["URL", "Title", "Language", "Tag", "Date"]
        assert len(df) == 0

    def test_deduplicates_url_keeps_last(self):
        data = [
            {"URL": "/jp/talk/a.html", "Title": "Old", "Language": "JP", "Tag": "t1", "Date": "2025-01-01"},
            {"URL": "/jp/talk/a.html", "Title": "New", "Language": "JP", "Tag": "t2", "Date": "2025-02-01"},
            {"URL": "/en/talk/b.html", "Title": "B", "Language": "EN", "Tag": "t3", "Date": "2025-03-01"},
        ]
        df = normalize_meta_sheet(data)
        assert len(df) == 2
        row_a = df[df["URL"] == "/jp/talk/a.html"].iloc[0]
        assert row_a["Title"] == "New"  # last wins
