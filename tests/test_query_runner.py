from __future__ import annotations

from pathlib import Path

import pandas as pd

from megaton_lib import query_runner


def _unused_query(*_args, **_kwargs):
    raise AssertionError("unexpected query executor")


def test_run_query_to_csv_writes_params_and_applies_pipeline(tmp_path: Path, monkeypatch):
    def fake_query_ga4(**kwargs):
        assert kwargs["property_id"] == "123"
        return pd.DataFrame(
            [
                {"customEvent:site": "b", "sessions": 2},
                {"customEvent:site": "a", "sessions": 5},
            ]
        )

    monkeypatch.setattr(query_runner.site_aliases, "resolve_site_alias", lambda raw: raw)
    monkeypatch.setattr(query_runner, "query_ga4", fake_query_ga4)

    params = {
        "schema_version": "1.0",
        "source": "ga4",
        "property_id": "123",
        "date_range": {"start": "today-7d", "end": "today"},
        "dimensions": ["customEvent:site"],
        "metrics": ["sessions"],
        "pipeline": {"sort": "sessions DESC"},
    }
    output_path = tmp_path / "out.csv"
    params_path = tmp_path / "params.json"

    query_runner.run_query_to_csv(
        params,
        output_path=output_path,
        params_path=params_path,
    )

    assert params_path.exists()
    out = pd.read_csv(output_path)
    assert out["customEvent:site"].tolist() == ["a", "b"]
    assert out["sessions"].tolist() == [5, 2]


def test_execute_query_params_uses_injected_executors_without_rebinding():
    original = query_runner.query_ga4

    def fake_query_ga4(**kwargs):
        assert kwargs["property_id"] == "123"
        return pd.DataFrame([{"date": "2026-04-25", "sessions": 1}])

    executors = query_runner.QueryExecutors(
        query_ga4=fake_query_ga4,
        query_gsc=_unused_query,
        query_aa=_unused_query,
        query_bq=_unused_query,
    )

    result = query_runner.execute_query_params(
        {
            "source": "ga4",
            "property_id": "123",
            "date_range": {"start": "2026-04-25", "end": "2026-04-25"},
            "dimensions": ["date"],
            "metrics": ["sessions"],
        },
        executors=executors,
    )

    assert len(result.df) == 1
    assert query_runner.query_ga4 is original


def test_execute_query_params_passes_gsc_page_to_path_from_params():
    called = {}

    def fake_query_gsc(**kwargs):
        called.update(kwargs)
        return pd.DataFrame([{"page": "/a", "clicks": 1}])

    executors = query_runner.QueryExecutors(
        query_ga4=_unused_query,
        query_gsc=fake_query_gsc,
        query_aa=_unused_query,
        query_bq=_unused_query,
    )

    query_runner.execute_query_params(
        {
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-04-25", "end": "2026-04-25"},
            "dimensions": ["page"],
            "page_to_path": True,
        },
        executors=executors,
    )

    assert called["page_to_path"] is True


def test_execute_query_params_gsc_page_to_path_kwarg_overrides_params():
    called = {}

    def fake_query_gsc(**kwargs):
        called.update(kwargs)
        return pd.DataFrame([{"page": "/a", "clicks": 1}])

    executors = query_runner.QueryExecutors(
        query_ga4=_unused_query,
        query_gsc=fake_query_gsc,
        query_aa=_unused_query,
        query_bq=_unused_query,
    )

    query_runner.execute_query_params(
        {
            "source": "gsc",
            "site_url": "https://example.com/",
            "date_range": {"start": "2026-04-25", "end": "2026-04-25"},
            "dimensions": ["page"],
            "page_to_path": False,
        },
        executors=executors,
        gsc_page_to_path=True,
    )

    assert called["page_to_path"] is True


def test_parse_gsc_filter():
    assert query_runner.parse_gsc_filter("page:contains:/jp;query:contains:brand") == [
        {"dimension": "page", "operator": "contains", "expression": "/jp"},
        {"dimension": "query", "operator": "contains", "expression": "brand"},
    ]


def test_aa_headers_include_inline_segments_and_breakdowns(monkeypatch):
    def fake_query_aa(**kwargs):
        assert kwargs["segment_definition"] == {"func": "segment"}
        assert kwargs["breakdown"] == {"dimension": "variables/page", "itemId": "123"}
        return pd.DataFrame([{"page": "A", "revenue": 1}])

    monkeypatch.setattr(query_runner, "query_aa", fake_query_aa)

    result = query_runner.execute_query_params(
        {
            "source": "aa",
            "company_id": "wacoal1",
            "rsid": "wacoal-all",
            "date_range": {"start": "2026-02-17", "end": "2026-02-17"},
            "dimension": "page",
            "metrics": ["revenue"],
            "segment_definition": {"func": "segment"},
            "breakdown": {"dimension": "variables/page", "itemId": "123"},
        }
    )

    assert "Inline segment definitions: 1" in result.header_lines
    assert "Breakdowns: 1" in result.header_lines
