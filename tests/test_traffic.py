import pandas as pd

from megaton_lib.traffic import (
    apply_source_normalization,
    classify_channel,
    ensure_trailing_slash,
    normalize_domain,
    reclassify_source_channel,
)


def test_normalize_domain_strips_scheme_and_www():
    assert normalize_domain("https://www.Example.com/a") == "example.com"


def test_apply_source_normalization_maps_by_regex():
    df = pd.DataFrame({"source": ["WWW.Google.Com", "unknown"]})
    out = apply_source_normalization(df, {r"google": "google"})
    assert out["source"].tolist() == ["google", "unknown"]


def test_classify_channel_ai():
    row = {"channel": "Referral", "medium": "referral", "source": "chatgpt.com"}
    assert classify_channel(row) == "AI"


def test_classify_channel_group_from_referral():
    row = {"channel": "Referral", "medium": "referral", "source": "foo.example.com"}
    assert classify_channel(row, group_domains={"example.com"}) == "Group"


def test_classify_channel_keeps_original_when_no_rule_matches():
    row = {"channel": "Organic Search", "medium": "organic", "source": "google"}
    assert classify_channel(row, group_domains={"example.com"}) == "Organic Search"


def test_ensure_trailing_slash_appends_only_when_needed():
    assert ensure_trailing_slash("/deilab/jp/actions") == "/deilab/jp/actions/"
    assert ensure_trailing_slash("/deilab/jp/actions/") == "/deilab/jp/actions/"
    assert ensure_trailing_slash("/deilab/jp/actions/page.html") == "/deilab/jp/actions/page.html"


def test_reclassify_source_channel_ai():
    row = {"channel": "Referral", "source": "chat.openai.com", "medium": "referral"}
    source, channel = reclassify_source_channel(row)
    assert source == "ChatGPT"
    assert channel == "AI"


def test_reclassify_source_channel_internal():
    row = {"channel": "Referral", "source": "teams.shiseido.co.jp", "medium": "referral"}
    source, channel = reclassify_source_channel(row)
    assert source == "teams.shiseido.co.jp"
    assert channel == "Shiseido Internal"


def test_reclassify_source_channel_fallback():
    row = {"channel": "Referral", "source": "example.com", "medium": "referral"}
    source, channel = reclassify_source_channel(row)
    assert source == "example.com"
    assert channel == "Referral"
