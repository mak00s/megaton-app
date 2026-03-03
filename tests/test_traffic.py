import pandas as pd

from megaton_lib.traffic import (
    apply_source_normalization,
    classify_channel,
    normalize_domain,
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
