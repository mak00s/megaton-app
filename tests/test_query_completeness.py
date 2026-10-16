import json
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton_lib import megaton_client as mc
from megaton_lib.params_validator import validate_params
from megaton_lib.query_runner import QueryExecutors, run_query_params
from scripts import query as cli


def metadata(status="complete", fetched=1, total=1, cap=10):
    return {"status": status, "fetched_rows": fetched, "row_limit": cap,
            "total_rows": total, "discarded_rows": 0, "limit_reached": fetched >= cap}


def result(evidence):
    return SimpleNamespace(df=pd.DataFrame({"sessions": [1]}),
                           completeness=SimpleNamespace(to_dict=lambda: evidence))


@pytest.mark.parametrize("status", ["truncated", "unknown"])
@pytest.mark.parametrize("source", ["ga4", "gsc"])
def test_incomplete_results_fail_closed_and_opt_in_retains_metadata(monkeypatch, status, source):
    evidence = metadata(status, fetched=10, total=None, cap=10)
    monkeypatch.setattr(mc, "run_query", lambda *a, **k: result(evidence))
    monkeypatch.setattr(mc, "get_megaton_for_property", lambda *a: object())
    monkeypatch.setattr(mc, "get_megaton_for_site", lambda *a: object())
    query = mc.query_ga4 if source == "ga4" else mc.query_gsc
    args = {"property_id": "123", "metrics": ["sessions"]} if source == "ga4" else {"site_url": "sc-domain:example.com"}
    args.update(start_date="2026-01-01", end_date="2026-01-31", dimensions=["date"])
    with pytest.raises(mc.QueryIncompleteError) as error:
        query(**args)
    assert error.value.completeness == evidence
    with pytest.warns(RuntimeWarning):
        frame = query(**args, allow_incomplete=True)
    assert frame.attrs["megaton_query_completeness"] == evidence


def test_exact_ga4_cap_with_known_total_is_complete():
    evidence = metadata(fetched=10, total=10, cap=10)
    frame = mc._checked_query_frame(result(evidence), allow_incomplete=False)
    assert frame.attrs["megaton_query_completeness"]["status"] == "complete"


@pytest.mark.parametrize("query,args", [
    (mc.query_ga4, {"property_id": "123", "metrics": ["sessions"]}),
    (mc.query_gsc, {"site_url": "sc-domain:example.com"}),
])
def test_bad_opt_in_is_rejected_before_account_lookup(monkeypatch, query, args):
    monkeypatch.setattr(mc, "get_megaton_for_property", lambda *a: pytest.fail("No API"))
    monkeypatch.setattr(mc, "get_megaton_for_site", lambda *a: pytest.fail("No API"))
    with pytest.raises(TypeError):
        query(**args, start_date="2026-01-01", end_date="2026-01-31",
              dimensions=["date"], allow_incomplete="true")


def test_explicit_credentials_uses_response_total(monkeypatch):
    from google.analytics import data_v1beta
    from megaton_lib import google_workspace

    response = SimpleNamespace(row_count=20, rows=[SimpleNamespace(
        dimension_values=[SimpleNamespace(value="20260101")], metric_values=[SimpleNamespace(value="1")])])
    client = SimpleNamespace(run_report=lambda *a: response)
    monkeypatch.setattr(data_v1beta, "BetaAnalyticsDataClient", lambda **k: client)
    monkeypatch.setattr(google_workspace, "build_service_account_credentials", lambda *a, **k: object())
    with pytest.raises(mc.QueryIncompleteError) as error:
        mc.query_ga4_with_credentials("123", "2026-01-01", "2026-01-31", ["date"], ["sessions"],
                                      credentials_path="unused", limit=1)
    assert error.value.completeness["status"] == "truncated"
    assert error.value.completeness["total_rows"] == 20


def test_async_result_keeps_evidence_outside_csv(tmp_path, monkeypatch, capsys):
    import sys
    from megaton_lib.job_manager import JobStore

    store = JobStore(tmp_path)
    job = store.create_job({"source": "ga4"}, "unused")
    frame = pd.DataFrame({"sessions": [1]})
    evidence = metadata("truncated", fetched=10, total=20, cap=10)
    frame.attrs["megaton_query_completeness"] = evidence
    monkeypatch.setattr(cli, "execute_query_from_params", lambda *a: (frame, []))
    assert cli.run_job(job["job_id"], store) == 0
    assert store.load_job(job["job_id"])["completeness"] == evidence
    monkeypatch.setenv("QUERY_JOB_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["query", "--json", "--result", job["job_id"]])
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["data"]["completeness"] == evidence


def test_async_incomplete_failure_keeps_evidence_without_artifact(tmp_path, monkeypatch):
    from megaton_lib.job_manager import JobStore

    store = JobStore(tmp_path)
    job = store.create_job({"source": "ga4"}, "unused")
    evidence = metadata("truncated", fetched=10, total=20, cap=10)
    def fail(*a):
        raise mc.QueryIncompleteError(evidence)
    monkeypatch.setattr(cli, "execute_query_from_params", fail)
    assert cli.run_job(job["job_id"], store) == 1
    assert store.load_job(job["job_id"])["status"] == "failed"
    assert store.load_job(job["job_id"])["completeness"] == evidence
    assert not store.artifact_path(job["job_id"]).exists()


def test_old_core_without_metadata_is_unknown_not_success():
    with pytest.raises(mc.QueryIncompleteError):
        mc._checked_query_frame(SimpleNamespace(df=pd.DataFrame({"x": [1]})), allow_incomplete=False)


def test_cli_incomplete_failure_has_safe_machine_evidence(monkeypatch, capsys):
    import sys

    evidence = metadata("truncated", fetched=10, total=20, cap=10)
    params = {"schema_version": "1.0", "source": "ga4", "property_id": "123",
              "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
              "dimensions": ["date"], "metrics": ["sessions"]}
    def fail(_):
        raise mc.QueryIncompleteError(evidence)
    monkeypatch.setattr(cli, "execute_query_from_params", fail)
    monkeypatch.setattr(sys, "argv", ["query", "--json", "--inline", json.dumps(params)])
    assert cli.main() == 1
    output = json.loads(capsys.readouterr().out)
    assert output["error_code"] == "INCOMPLETE_QUERY_RESULT"
    assert output["details"]["completeness"] == evidence


def test_gsc_reaggregation_does_not_hide_raw_cap(monkeypatch):
    from megaton.query import run_query as core_query
    from megaton.query_metadata import QueryCompleteness

    mg = Mock()
    raw = pd.DataFrame({"page": ["https://example.com/a?x=1", "https://example.com/a?x=2"],
                        "clicks": [1, 2], "impressions": [2, 4], "position": [1., 2.]})
    raw.attrs["megaton_query_completeness"] = QueryCompleteness(2, 2).to_dict()
    mg.search.run.return_value.df = raw
    monkeypatch.setattr(mc, "run_query", core_query)
    monkeypatch.setattr(mc, "get_megaton_for_site", lambda *a: mg)
    with pytest.raises(mc.QueryIncompleteError) as error:
        mc.query_gsc("sc-domain:example.com", "2026-01-01", "2026-01-31", ["page"], limit=2)
    assert error.value.completeness["fetched_rows"] == 2
    assert error.value.completeness["status"] == "unknown"


def test_pipeline_and_cli_json_keep_raw_evidence(capsys):
    evidence = metadata("truncated", fetched=10, total=20, cap=10)
    def query(**kwargs):
        assert kwargs["allow_incomplete"] is True
        frame = pd.DataFrame({"date": ["2026-01-01", "2026-01-01"], "sessions": [1, 2]})
        frame.attrs["megaton_query_completeness"] = evidence
        return frame
    params = {"schema_version": "1.0", "source": "ga4", "property_id": "123",
              "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
              "dimensions": ["date"], "metrics": ["sessions"], "allow_incomplete": True,
              "pipeline": {"group_by": "date", "aggregate": "sum:sessions"}}
    runners = QueryExecutors(query, query, query, query)
    value = run_query_params(params, executors=runners)
    assert len(value.df) == 1
    assert value.completeness == evidence
    cli.output_result(value.df, SimpleNamespace(output=None, json=True))
    assert json.loads(capsys.readouterr().out)["data"]["completeness"] == evidence


@pytest.mark.parametrize("value", ["true", 1, None])
def test_allow_incomplete_params_must_be_boolean(value):
    params = {"schema_version": "1.0", "source": "ga4", "property_id": "123",
              "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
              "dimensions": ["date"], "metrics": ["sessions"], "allow_incomplete": value}
    normalized, errors = validate_params(params)
    assert normalized is None
    assert any(e["path"] == "$.allow_incomplete" for e in errors)
