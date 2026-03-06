from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from megaton_lib.audit.config import AdobeAnalyticsConfig
from megaton_lib.audit.providers.analytics.aa import AdobeAnalyticsClient


class _DummyResponse:
    def __init__(self, status_code: int, payload, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _DummySession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, method, url, headers=None, params=None, json=None, data=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
                "data": data,
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise RuntimeError("No more dummy responses")
        return self._responses.pop(0)


def _mock_token_post(*args, **kwargs):
    """Stub for adobe_auth.requests.post (token endpoint)."""
    return _DummyResponse(200, {"access_token": "tok", "expires_in": 3600})


@pytest.fixture
def aa_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ADOBE_CLIENT_ID", "client-id")
    monkeypatch.setenv("ADOBE_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("ADOBE_ORG_ID", "ORG@AdobeOrg")
    # Stub token request in adobe_auth module
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.requests.post",
        _mock_token_post,
    )
    return tmp_path


def _config(tmp_path: Path) -> AdobeAnalyticsConfig:
    return AdobeAnalyticsConfig(
        company_id="wacoal1",
        rsid="wacoal-all",
        dimension="daterangeday",
        metric="revenue",
        token_cache_file=str(tmp_path / ".token.json"),
    )


def test_get_report_paging(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            {
                "rows": [{"value": "Feb 17, 2026", "data": [100.0]}],
                "lastPage": False,
            },
        ),
        _DummyResponse(
            200,
            {
                "rows": [{"value": "Feb 18, 2026", "data": [200.0]}],
                "lastPage": True,
            },
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)

    df = client.get_report(
        rsid=cfg.rsid,
        dimension="daterangeday",
        metrics=["revenue"],
        date_from="2026-02-17",
        date_to="2026-02-19",
        limit=1,
    )

    assert len(df) == 2
    assert "variables/daterangeday" in df.columns
    assert "metrics/revenue" in df.columns


def test_get_report_retry_429(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(429, {"error_code": "429050", "error": "too many requests"}, {"Retry-After": "0"}),
        _DummyResponse(
            200,
            {
                "rows": [{"value": "Feb 17, 2026", "data": [17034810.0]}],
                "lastPage": True,
            },
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)

    monkeypatch.setattr("megaton_lib.audit.providers.analytics.aa.time.sleep", lambda _: None)

    df = client.get_report(
        rsid=cfg.rsid,
        dimension="daterangeday",
        metrics=["revenue"],
        date_from="2026-02-17",
        date_to="2026-02-18",
    )

    assert len(df) == 1
    assert int(round(pd.to_numeric(df["metrics/revenue"], errors="coerce").sum())) == 17034810


def test_fetch_dimension_metric_shape(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            {
                "rows": [{"value": "Feb 17, 2026", "data": [17034810.0]}],
                "lastPage": True,
            },
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)

    out = client.fetch_dimension_metric(
        start_date="2026-02-17",
        end_date="2026-02-18",
        dimension="daterangeday",
        metric="revenue",
    )

    assert list(out.columns) == ["site", "metric_value"]
    assert out.iloc[0]["site"] == "Feb 17, 2026"
    assert int(round(out.iloc[0]["metric_value"])) == 17034810


def test_list_dimensions_accepts_array_payload(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            [
                {"id": "variables/page", "name": "Page"},
                {"id": "variables/evar1", "name": "Site Name"},
            ],
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    out = client.list_dimensions(rsid=cfg.rsid)

    assert [d["id"] for d in out] == ["variables/page", "variables/evar1"]


def test_list_metrics_accepts_array_payload(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            [
                {"id": "metrics/revenue", "name": "Revenue"},
                {"id": "metrics/orders", "name": "Orders"},
            ],
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    out = client.list_metrics(rsid=cfg.rsid)

    assert [m["id"] for m in out] == ["metrics/revenue", "metrics/orders"]


def test_list_segments_accepts_content_payload(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            {
                "content": [
                    {"id": "s123", "name": "Segment A"},
                    {"id": "s456", "name": "Segment B"},
                ],
                "lastPage": True,
                "number": 0,
                "totalPages": 1,
            },
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    out = client.list_segments(rsid=cfg.rsid)

    assert [s["id"] for s in out] == ["s123", "s456"]
    assert dummy.calls[-1]["params"]["includeType"] == "all"


def test_list_report_suites_uses_total_pages_when_last_page_missing(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            {
                "content": [{"rsid": "suite-a", "name": "Suite A"}],
                "number": 0,
                "totalPages": 2,
            },
        ),
        _DummyResponse(
            200,
            {
                "content": [{"rsid": "suite-b", "name": "Suite B"}],
                "number": 1,
                "totalPages": 2,
            },
        ),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    suites = client.list_report_suites(limit=1000)

    assert [s["rsid"] for s in suites] == ["suite-a", "suite-b"]


def test_list_companies_does_not_send_proxy_company_header(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(200, {"imsOrgs": [{"companies": [{"globalCompanyId": "wacoal1"}]}]}),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    _ = client.list_companies()

    discovery_call = dummy.calls[-1]
    headers = discovery_call.get("headers") or {}
    assert "x-proxy-global-company-id" not in headers
