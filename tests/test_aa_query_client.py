from __future__ import annotations

import pandas as pd

from megaton_lib.megaton_client import (
    get_aa_companies,
    get_aa_report_suites,
    get_aa_segments,
    query_aa,
)


def test_query_aa_normalizes_columns_and_end_date(monkeypatch):
    captured: dict[str, object] = {}

    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def get_report(self, **kwargs):
            captured.update(kwargs)
            return pd.DataFrame(
                {
                    "variables/daterangeday": ["Feb 17, 2026"],
                    "metrics/revenue": ["17034810"],
                    "metrics/orders": [1587],
                }
            )

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    out = query_aa(
        company_id="wacoal1",
        rsid="wacoal-all",
        start_date="2026-02-17",
        end_date="2026-02-17",
        dimension="daterangeday",
        metrics=["revenue", "orders"],
        segment=["s123"],
        limit=50000,
    )

    assert list(out.columns) == ["daterangeday", "revenue", "orders"]
    assert out.iloc[0]["daterangeday"] == "Feb 17, 2026"
    assert int(out.iloc[0]["revenue"]) == 17034810
    assert int(out.iloc[0]["orders"]) == 1587
    assert captured["date_to"] == "2026-02-18"
    assert captured["metrics"] == ["revenue", "orders"]


def test_query_aa_passes_inline_segment_and_breakdown(monkeypatch):
    captured: dict[str, object] = {}

    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def get_report(self, **kwargs):
            captured.update(kwargs)
            return pd.DataFrame(
                {
                    "variables/page": ["Page A"],
                    "metrics/revenue": [100],
                }
            )

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    out = query_aa(
        company_id="wacoal1",
        rsid="wacoal-all",
        start_date="2026-02-17",
        end_date="2026-02-17",
        dimension="page",
        metrics=["revenue"],
        segment_definition={"func": "segment"},
        breakdown={"dimension": "variables/page", "itemId": "123"},
    )

    assert list(out.columns) == ["page", "revenue"]
    assert captured["segment_definition"] == {"func": "segment"}
    assert captured["breakdown"] == {"dimension": "variables/page", "itemId": "123"}


def test_get_aa_report_suites_sorted(monkeypatch):
    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def list_report_suites(self, **kwargs):
            _ = kwargs
            return [
                {"rsid": "bbb", "name": "B Suite"},
                {"rsid": "aaa", "name": "A Suite"},
            ]

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    suites = get_aa_report_suites(company_id="wacoal1")
    assert [s["rsid"] for s in suites] == ["aaa", "bbb"]


def test_get_aa_companies_sorted(monkeypatch):
    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def list_companies(self):
            return [
                {"company_id": "bbb", "name": "B Company"},
                {"company_id": "aaa", "name": "A Company"},
            ]

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    companies = get_aa_companies()
    assert [c["company_id"] for c in companies] == ["aaa", "bbb"]


def test_get_aa_segments_sorted(monkeypatch):
    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def list_segments(self, **kwargs):
            _ = kwargs
            return [
                {"id": "s2", "name": "B Segment"},
                {"id": "s1", "name": "A Segment"},
            ]

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    segments = get_aa_segments(company_id="wacoal1", rsid="wacoal-all")
    assert [s["id"] for s in segments] == ["s1", "s2"]


def test_get_aa_segments_passes_name_and_definition(monkeypatch):
    captured: dict[str, object] = {}

    class _DummyClient:
        def __init__(self, config):
            self.config = config

        def list_segments(self, **kwargs):
            captured.update(kwargs)
            return [{"id": "s1", "name": "bot除外", "definition": {"func": "segment"}}]

    monkeypatch.setattr("megaton_lib.megaton_client.AdobeAnalyticsClient", _DummyClient)

    segments = get_aa_segments(
        company_id="wacoal1",
        rsid="wacoal-all",
        name="bot除外",
        include_definition=True,
    )

    assert segments[0]["id"] == "s1"
    assert captured["name"] == "bot除外"
    assert captured["include_definition"] is True
