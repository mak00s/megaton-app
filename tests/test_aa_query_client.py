from __future__ import annotations

import pandas as pd

from megaton_lib.megaton_client import get_aa_companies, get_aa_report_suites, query_aa


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
