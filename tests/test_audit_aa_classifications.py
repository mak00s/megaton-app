from __future__ import annotations

from unittest.mock import patch

import pytest

from megaton_lib.audit.providers.analytics.classifications import (
    ClassificationsClient,
    _cli_main,
)


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
        lambda results: None,
    )

    argv = [
        "classifications.py",
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
            _cli_main()

    assert excinfo.value.code == 0
    assert "token_cache_file" not in captured_auth_kwargs
