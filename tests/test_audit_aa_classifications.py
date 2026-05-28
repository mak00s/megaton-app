from __future__ import annotations

from unittest.mock import patch

import pytest
import requests

from megaton_lib.audit.providers.analytics.classifications import (
    ClassificationsClient,
    print_verify_results,
)
from megaton_lib.audit.providers.analytics.verify_classification import main as cli_main


class _DummyAuth:
    def get_headers(self, *, extra=None):
        return extra or {}


class _DummyResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code < 200 or self.status_code >= 300:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_find_dataset_id_requires_exact_dimension_match(monkeypatch):
    response = _DummyResponse(
        200,
        {
            "metrics": [
                {"id": "variables/evar29", "datasets": ["ds29"]},
                {"id": "variables/evar2", "datasets": ["ds2"]},
            ],
        },
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.get",
        lambda *args, **kwargs: response,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")

    assert client.find_dataset_id(rsid="wacoal-all", dimension="evar2") == "ds2"


def test_create_import_job_includes_notification_payload(monkeypatch):
    captured = {}

    def fake_post(url, *, headers=None, json=None, timeout=None):
        captured.update(
            {"url": url, "headers": headers, "json": json, "timeout": timeout}
        )
        return _DummyResponse(200, {"api_job_id": "job-1"})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.post",
        fake_post,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    job_id = client.create_import_job(
        "dataset-1",
        job_name="manual import",
        notification_emails=[" user@example.com ", "", "ops@example.com"],
        notification_states=["completed", "failed_validation"],
    )

    assert job_id == "job-1"
    assert captured["timeout"] == 30
    assert captured["url"].endswith(
        "/classifications/job/import/createApiJob/dataset-1"
    )
    assert captured["json"]["jobName"] == "manual import"
    assert captured["json"]["keyOptions"] == {"overwrite": True}
    assert captured["json"]["notifications"] == [
        {
            "method": "email",
            "state": "completed",
            "recipients": ["user@example.com", "ops@example.com"],
        },
        {
            "method": "email",
            "state": "failed_validation",
            "recipients": ["user@example.com", "ops@example.com"],
        },
    ]


def test_create_import_job_omits_notifications_without_email(monkeypatch):
    captured = {}

    def fake_post(_url, *, json=None, **_kwargs):
        captured["json"] = json
        return _DummyResponse(200, {"api_job_id": "job-1"})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.post",
        fake_post,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    client.create_import_job("dataset-1")

    assert "notifications" not in captured["json"]
    assert captured["json"]["keyOptions"] == {"overwrite": True}


def test_import_classification_chunked_retries_transient_upload_error(monkeypatch):
    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    calls = []

    def fake_create_import_job(dataset_id, **kwargs):
        job_id = f"job-{len([call for call in calls if call[0] == 'create']) + 1}"
        calls.append(("create", job_id, dataset_id, kwargs))
        return job_id

    upload_attempts = {"count": 0}

    def fake_upload_file(job_id, content, **kwargs):
        upload_attempts["count"] += 1
        calls.append(("upload", job_id, kwargs))
        if upload_attempts["count"] == 1:
            response = _DummyResponse(500, {"error": "transient"})
            error = requests.exceptions.HTTPError("HTTP 500")
            error.response = response
            raise error

    def fake_commit_job(job_id):
        calls.append(("commit", job_id))

    sleeps = []
    monkeypatch.setattr(client, "create_import_job", fake_create_import_job)
    monkeypatch.setattr(client, "upload_file", fake_upload_file)
    monkeypatch.setattr(client, "commit_job", fake_commit_job)
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )

    job_ids = client.import_classification_chunked(
        "dataset-1",
        "Key\tLabel\nA\tAlpha\n",
        chunk_rows=1,
        chunk_pause_seconds=0,
        max_attempts=2,
        retry_backoff_seconds=0.5,
        verbose=False,
    )

    assert job_ids == ["job-2"]
    assert [call[0] for call in calls] == ["create", "upload", "create", "upload", "commit"]
    assert sleeps == [0.5]


def test_import_helpers_forward_notification_options(monkeypatch):
    calls = []

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")

    def fake_create_import_job(dataset_id, **kwargs):
        calls.append(("create", dataset_id, kwargs))
        return f"job-{len(calls)}"

    monkeypatch.setattr(client, "create_import_job", fake_create_import_job)
    monkeypatch.setattr(
        client,
        "upload_file",
        lambda job_id, content, **kwargs: calls.append(("upload", job_id, kwargs)),
    )
    monkeypatch.setattr(
        client,
        "commit_job",
        lambda job_id: calls.append(("commit", job_id)),
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.time.sleep",
        lambda _seconds: None,
    )

    client.import_classification(
        "dataset-1",
        "Key\tLabel\nA\tAlpha\n",
        notification_emails=["ops@example.com"],
        notification_states=["completed"],
        verbose=False,
    )
    client.import_classification_chunked(
        "dataset-1",
        "Key\tLabel\nA\tAlpha\nB\tBeta\n",
        chunk_rows=1,
        chunk_pause_seconds=0,
        notification_emails=["ops@example.com"],
        notification_states=["failed_processing"],
        verbose=False,
    )

    create_calls = [call for call in calls if call[0] == "create"]
    assert create_calls[0][2]["notification_emails"] == ["ops@example.com"]
    assert create_calls[0][2]["notification_states"] == ["completed"]
    assert create_calls[1][2]["notification_emails"] == ["ops@example.com"]
    assert create_calls[1][2]["notification_states"] == ["failed_processing"]
    assert create_calls[2][2]["notification_emails"] == ["ops@example.com"]
    assert create_calls[2][2]["notification_states"] == ["failed_processing"]


def test_create_export_job_includes_date_filter_and_keys(monkeypatch):
    captured = {}

    def fake_post(url, *, headers=None, json=None, timeout=None):
        captured.update({"url": url, "json": json})
        return _DummyResponse(200, {"export_job_id": "exp-1"})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.post",
        fake_post,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    job_id = client.create_export_job(
        "dataset-1",
        date_filter_start="2024-01-01T00:00:00Z",
        keys=["A", "B"],
    )

    assert job_id == "exp-1"
    assert captured["json"]["dateFilterStart"] == "2024-01-01T00:00:00Z"
    assert captured["json"]["keys"] == ["A", "B"]


def test_create_export_job_defaults_date_filter_when_keys_only(monkeypatch):
    captured = {}

    def fake_post(_url, *, json=None, **_kwargs):
        captured["json"] = json
        return _DummyResponse(200, {"export_job_id": "exp-1"})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.post",
        fake_post,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    client.create_export_job("dataset-1", keys=["A"])

    # Adobe rejects keys without dateFilterStart, so the helper auto-fills
    # a wide window rather than letting the API 400 surface to callers.
    assert captured["json"]["dateFilterStart"] == "2015-01-01T00:00:00Z"
    assert captured["json"]["keys"] == ["A"]


def test_create_export_job_omits_date_filter_and_keys_by_default(monkeypatch):
    captured = {}

    def fake_post(_url, *, json=None, **_kwargs):
        captured["json"] = json
        return _DummyResponse(200, {"export_job_id": "exp-1"})

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.post",
        fake_post,
    )

    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    client.create_export_job("dataset-1")

    assert "dateFilterStart" not in captured["json"]
    assert "keys" not in captured["json"]


def test_resolve_classification_dim_uses_dimensions_slug_when_rsid_provided(monkeypatch):
    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    monkeypatch.setattr(
        client,
        "get_classification_columns",
        lambda dataset_id, verbose=False: ["StepD 202404-202603", "Step 202504-202603"],
    )

    def fake_get(url, *, headers=None, timeout=None):
        assert "/dimensions" in url
        assert "rsid=wacoal-all" in url
        return _DummyResponse(
            200,
            [
                {"id": "variables/evar30.bbb", "name": "Step 202304-202309"},
                {"id": "variables/evar30.stepd-202404202603", "name": "StepD 202404-202603"},
                {"id": "variables/evar29.foo", "name": "StepD 202404-202603"},  # wrong base
            ],
        )

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.get",
        fake_get,
    )

    resolved = client._resolve_classification_dim(
        "ds30", "StepD 202404-202603", "evar30", rsid="wacoal-all",
    )

    assert resolved == "variables/evar30.stepd-202404202603"


def test_resolve_classification_dim_falls_back_when_no_name_matches(monkeypatch, caplog):
    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    monkeypatch.setattr(
        client,
        "get_classification_columns",
        lambda dataset_id, verbose=False: ["alpha", "beta", "gamma"],
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.get",
        lambda *args, **kwargs: _DummyResponse(
            200,
            [{"id": "variables/evar29.unrelated", "name": "Other"}],
        ),
    )

    with caplog.at_level("WARNING", logger="megaton_lib.audit.providers.analytics.classifications"):
        resolved = client._resolve_classification_dim(
            "ds29", "beta", "evar29", rsid="wacoal-all",
        )

    # beta is index 1 (0-based) → 1-based "2"
    assert resolved == "variables/evar29.2"
    assert any(
        "No variables/evar29.* dimension matched" in record.message
        for record in caplog.records
    )


def test_resolve_classification_dim_warns_on_dimensions_api_error(monkeypatch, caplog):
    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    monkeypatch.setattr(
        client,
        "get_classification_columns",
        lambda dataset_id, verbose=False: ["alpha"],
    )

    def boom(*_args, **_kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.get",
        boom,
    )

    with caplog.at_level("WARNING", logger="megaton_lib.audit.providers.analytics.classifications"):
        resolved = client._resolve_classification_dim(
            "ds29", "alpha", "evar29", rsid="wacoal-all",
        )

    assert resolved == "variables/evar29.1"
    assert any(
        "/dimensions lookup failed" in record.message
        and "RuntimeError" in record.message
        for record in caplog.records
    )


def test_resolve_classification_dim_skips_dimensions_call_when_rsid_omitted(monkeypatch):
    client = ClassificationsClient(auth=_DummyAuth(), company_id="wacoal1")
    monkeypatch.setattr(
        client,
        "get_classification_columns",
        lambda dataset_id, verbose=False: ["alpha", "beta"],
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("requests.get should not be called when rsid is empty")

    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.requests.get",
        fail_if_called,
    )

    resolved = client._resolve_classification_dim("ds29", "beta", "evar29")

    assert resolved == "variables/evar29.2"


def test_cli_main_omits_empty_token_cache(monkeypatch):
    captured_auth_kwargs = {}

    class _DummyAdobeOAuthClient:
        def __init__(self, **kwargs):
            captured_auth_kwargs.update(kwargs)

    class _DummyClassificationsClient:
        def __init__(self, auth, company_id):
            self.auth = auth
            self.company_id = company_id

        def verify_column(self, *, rsid, dimension, column, expected):
            assert rsid == "suite"
            assert dimension == "evar2"
            assert column == "owner"
            assert expected == {"A1000": "社員"}
            return {
                "A1000": {"expected": "社員", "actual": "社員", "match": True},
            }

    # Patch at the module where cli_main will import from
    monkeypatch.setattr(
        "megaton_lib.audit.providers.adobe_auth.AdobeOAuthClient",
        _DummyAdobeOAuthClient,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.ClassificationsClient",
        _DummyClassificationsClient,
    )
    monkeypatch.setattr(
        "megaton_lib.audit.providers.analytics.classifications.print_verify_results",
        lambda results, **kwargs: None,
    )

    argv = [
        "verify_classification.py",
        "--company-id",
        "wacoal1",
        "--rsid",
        "suite",
        "--dimension",
        "evar2",
        "--column",
        "owner",
        "--keys",
        "A1000=社員",
    ]
    with patch("sys.argv", argv):
        with pytest.raises(SystemExit) as excinfo:
            cli_main()

    assert excinfo.value.code == 0
    assert "token_cache_file" not in captured_auth_kwargs
