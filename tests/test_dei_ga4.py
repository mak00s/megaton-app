"""Tests for megaton_lib.dei_ga4."""

import pandas as pd
import pytest

from megaton_lib.dei_ga4 import (
    classify_source_channel,
    ensure_trailing_slash,
    build_page_metrics,
)


# --- classify_source_channel ---


class TestClassifySourceChannel:
    """Tests for classify_source_channel()."""

    def test_chatgpt(self):
        row = {"channel": "Referral", "source": "chatgpt.com"}
        assert classify_source_channel(row) == ("ChatGPT", "AI")

    def test_copilot_bing(self):
        row = {"channel": "Referral", "source": "bing.com"}
        assert classify_source_channel(row) == ("Copilot", "AI")

    def test_gemini(self):
        row = {"channel": "Referral", "source": "gemini.google.com"}
        assert classify_source_channel(row) == ("Gemini", "AI")

    def test_claude(self):
        row = {"channel": "Referral", "source": "claude.ai"}
        assert classify_source_channel(row) == ("Claude", "AI")

    def test_perplexity(self):
        row = {"channel": "Referral", "source": "perplexity.ai"}
        assert classify_source_channel(row) == ("Perplexity", "AI")

    def test_internal_sharepoint(self):
        row = {"channel": "Referral", "source": "sharepoint.com"}
        src, ch = classify_source_channel(row)
        assert ch == "Shiseido Internal"

    def test_internal_teams(self):
        row = {"channel": "Referral", "source": "teams.microsoft.com"}
        src, ch = classify_source_channel(row)
        assert ch == "Shiseido Internal"

    def test_organic_search(self):
        row = {"channel": "Organic Search", "source": "google.search"}
        src, ch = classify_source_channel(row)
        assert ch == "Organic Search"

    def test_twitter(self):
        row = {"channel": "Organic Social", "source": "t.co"}
        assert classify_source_channel(row) == ("Twitter", "Organic Social")

    def test_instagram(self):
        row = {"channel": "Organic Social", "source": "instagram.com"}
        assert classify_source_channel(row) == ("Instagram", "Organic Social")

    def test_facebook(self):
        row = {"channel": "Organic Social", "source": "facebook.com"}
        assert classify_source_channel(row) == ("Facebook", "Organic Social")

    def test_threads(self):
        row = {"channel": "Organic Social", "source": "threads.net"}
        assert classify_source_channel(row) == ("Threads", "Organic Social")

    def test_tiktok(self):
        row = {"channel": "Organic Social", "source": "tiktok.com"}
        assert classify_source_channel(row) == ("TikTok", "Organic Social")

    def test_referral_search_reclassified(self):
        row = {"channel": "Referral", "source": "search.yahoo.co.jp"}
        src, ch = classify_source_channel(row)
        assert ch == "Organic Search"

    def test_fallback(self):
        row = {"channel": "Direct", "source": "(direct)"}
        assert classify_source_channel(row) == ("(direct)", "Direct")

    def test_ai_via_medium(self):
        row = {"channel": "Other", "source": "unknown", "medium": "chatgpt"}
        assert classify_source_channel(row) == ("ChatGPT", "AI")


# --- ensure_trailing_slash ---


class TestEnsureTrailingSlash:
    """Tests for ensure_trailing_slash()."""

    def test_no_suffix(self):
        assert ensure_trailing_slash("/deilab/page1") == "/deilab/page1/"

    def test_already_slash(self):
        assert ensure_trailing_slash("/deilab/page1/") == "/deilab/page1/"

    def test_html(self):
        assert ensure_trailing_slash("/deilab/page1.html") == "/deilab/page1.html"


# --- build_page_metrics ---


class TestBuildPageMetrics:
    """Tests for build_page_metrics()."""

    def test_merge_basic(self):
        df_base = pd.DataFrame({"page": ["/a", "/b"], "uu": [10, 20]})
        df_extra = pd.DataFrame({"page": ["/a"], "footer_views": [5]})
        result = build_page_metrics(
            [("base", df_base), ("extra", df_extra)],
            merge_on="page",
            int_cols=["footer_views"],
        )
        assert len(result) == 2
        assert result.loc[result["page"] == "/a", "footer_views"].iloc[0] == 5
        assert result.loc[result["page"] == "/b", "footer_views"].iloc[0] == 0

    def test_empty_dfs(self):
        result = build_page_metrics([])
        assert result.empty

    def test_none_df_skipped(self):
        df_base = pd.DataFrame({"page": ["/a"], "uu": [10]})
        result = build_page_metrics(
            [("base", df_base), ("null", None)],
            merge_on="page",
        )
        assert len(result) == 1
        assert list(result.columns) == ["page", "uu"]
