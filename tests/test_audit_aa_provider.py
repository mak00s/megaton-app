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


def test_get_report_supports_inline_segment_and_breakdown(aa_env, monkeypatch):
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(
            200,
            {
                "rows": [{"value": "Page A", "data": [100.0]}],
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
    segment_definition = {"func": "segment", "version": [1, 0, 0]}
    breakdown = {"dimension": "variables/page", "itemId": "12345"}
    df = client.get_report(
        rsid=cfg.rsid,
        dimension="page",
        metrics=["revenue"],
        date_from="2026-02-17",
        date_to="2026-02-18",
        segment=["s123"],
        segment_definition=segment_definition,
        breakdown=breakdown,
    )

    assert len(df) == 1
    request_json = dummy.calls[-1]["json"]
    assert request_json["globalFilters"][0] == {"type": "segment", "segmentId": "s123"}
    assert request_json["globalFilters"][1] == {
        "type": "segment",
        "segmentDefinition": segment_definition,
    }
    metric_container = request_json["metricContainer"]
    assert metric_container["metricFilters"] == [
        {
            "id": "0",
            "type": "breakdown",
            "dimension": "variables/page",
            "itemId": "12345",
        },
    ]
    assert metric_container["metrics"][0]["filters"] == ["0"]


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


def test_list_segments_supports_name_filter_and_definition(aa_env, monkeypatch):
    cfg = _config(aa_env)
    definition = {"func": "segment", "version": [1, 0, 0]}
    responses = [
        _DummyResponse(
            200,
            {
                "content": [
                    {
                        "id": "s123",
                        "name": "bot除外",
                        "description": "test description",
                        "definition": definition,
                    },
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
    out = client.list_segments(
        rsid=cfg.rsid,
        name="bot除外",
        include_definition=True,
    )

    assert out == [
        {
            "id": "s123",
            "name": "bot除外",
            "description": "test description",
            "definition": definition,
        },
    ]
    assert dummy.calls[-1]["params"]["name"] == "bot除外"
    assert dummy.calls[-1]["params"]["expansion"] == "definition"


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


# -- AAQueryContext tests --


def test_query_context_report_passes_stored_params(aa_env, monkeypatch):
    """query_context().report() should use stored rsid/date/segment."""
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(200, {
            "rows": [{"value": "Page A", "data": [5.0], "itemId": "111"}],
            "lastPage": True,
        }),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    seg = {"func": "segment", "version": [1, 0, 0], "container": {"func": "container", "context": "visits", "pred": {"func": "streq", "str": "v", "val": {"func": "attr", "name": "variables/evar99"}}}}
    ctx = client.query_context("test-rsid", "2026-03-01", "2026-03-31", segment=seg)

    df = ctx.report("page", ["occurrences"])
    assert not df.empty

    body = dummy.calls[0]["json"]
    assert body["rsid"] == "test-rsid"
    assert body["dimension"] == "variables/page"
    seg_filters = [f for f in body["globalFilters"] if f.get("type") == "segment"]
    assert len(seg_filters) == 1


def test_query_context_breakdown_builds_metric_filter(aa_env, monkeypatch):
    """query_context().breakdown() should add metricFilter for parent dimension."""
    cfg = _config(aa_env)
    responses = [
        _DummyResponse(200, {
            "rows": [{"value": "13:04:22", "data": [3.0], "itemId": "222"}],
            "lastPage": True,
        }),
    ]
    dummy = _DummySession(responses)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.aa.requests.Session",
        lambda: dummy,
    )

    client = AdobeAnalyticsClient(cfg)
    ctx = client.query_context("test-rsid", "2026-03-01", "2026-03-31")

    df = ctx.breakdown("page", 12345, "prop12", ["occurrences"])
    assert not df.empty

    body = dummy.calls[0]["json"]
    assert body["dimension"] == "variables/prop12"
    metric_filters = body["metricContainer"].get("metricFilters", [])
    assert len(metric_filters) == 1
    assert metric_filters[0]["dimension"] == "variables/page"
    assert metric_filters[0]["itemId"] == 12345
