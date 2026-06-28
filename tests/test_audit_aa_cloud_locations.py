from __future__ import annotations

import pytest

from megaton_lib.audit.providers.analytics.cloud_locations.api import (
    AdobeCloudLocationsClient,
)
from megaton_lib.audit.providers.analytics.cloud_locations.manager import (
    build_gcs_iam_command,
    ensure_gcp_account,
    ensure_gcp_dw_location,
)


class _DummyAuth:
    def get_headers(self, *, extra=None):
        return {"Authorization": "Bearer token", **(extra or {})}

    def refresh_access_token(self):
        raise AssertionError("refresh_access_token should not be called")


class _DummyResponse:
    def __init__(self, status_code, *, text="", payload=None):
        self.status_code = status_code
        self.text = text
        self.headers = {}
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _DummySession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


class _DummyClient:
    def __init__(self, *, accounts=None, locations=None):
        self.accounts = accounts or []
        self.locations = locations or []
        self.created_accounts = []
        self.created_locations = []

    def list_accounts(self, **kwargs):
        self.list_accounts_kwargs = kwargs
        return {"content": self.accounts}

    def create_account(self, body):
        self.created_accounts.append(body)
        created = {
            **body,
            "uuid": "account-created",
            "accountProperties": {
                **body["accountProperties"],
                "email": "C-test@example.iam.gserviceaccount.com",
            },
            "deleted": False,
        }
        self.accounts.append(created)
        return created

    def list_locations(self, **kwargs):
        self.list_locations_kwargs = kwargs
        return {"content": self.locations}

    def create_location(self, body):
        self.created_locations.append(body)
        created = {**body, "uuid": "location-created", "deleted": False}
        self.locations.append(created)
        return created


def test_ensure_gcp_account_reuses_matching_account():
    client = _DummyClient(
        accounts=[
            {
                "type": "gcp",
                "name": "Shimizu GCS",
                "uuid": "account-1",
                "deleted": False,
                "accountProperties": {"projectId": "ajuma-8"},
            }
        ]
    )

    result = ensure_gcp_account(
        client,
        name="Shimizu GCS",
        project_id="ajuma-8",
    )

    assert result["created"] is False
    assert result["account"]["uuid"] == "account-1"
    assert client.created_accounts == []


class _PagedAccountsClient:
    """List client that paginates accounts in fixed-size pages."""

    def __init__(self, accounts, *, page_size=100):
        self._accounts = accounts
        self._page_size = page_size
        self.created_accounts = []

    def list_accounts(self, *, limit, page, **kwargs):
        start = page * self._page_size
        return {"content": self._accounts[start : start + self._page_size]}

    def create_account(self, body):
        self.created_accounts.append(body)
        return {**body, "uuid": "account-created"}


def test_ensure_gcp_account_finds_match_on_later_page():
    filler = [
        {
            "type": "gcp",
            "name": f"Filler {i}",
            "uuid": f"f-{i}",
            "deleted": False,
            "accountProperties": {"projectId": "x"},
        }
        for i in range(100)
    ]
    target = {
        "type": "gcp",
        "name": "Shimizu GCS",
        "uuid": "account-page2",
        "deleted": False,
        "accountProperties": {"projectId": "ajuma-8"},
    }
    client = _PagedAccountsClient(filler + [target])

    result = ensure_gcp_account(client, name="Shimizu GCS", project_id="ajuma-8")

    # the match is on page 1; the helper must not re-create it
    assert result["created"] is False
    assert result["account"]["uuid"] == "account-page2"
    assert client.created_accounts == []


def test_ensure_gcp_account_rejects_same_name_different_project():
    client = _DummyClient(
        accounts=[
            {
                "type": "gcp",
                "name": "Shimizu GCS",
                "uuid": "account-1",
                "deleted": False,
                "accountProperties": {"projectId": "other-project"},
            }
        ]
    )

    with pytest.raises(ValueError, match="other-project"):
        ensure_gcp_account(client, name="Shimizu GCS", project_id="ajuma-8")


def test_ensure_gcp_dw_location_creates_account_and_location():
    client = _DummyClient()

    result = ensure_gcp_dw_location(
        client,
        account_name="Shimizu GCS",
        project_id="ajuma-8",
        location_name="DMS AA DW",
        bucket="dms-aa",
        prefix="cx-v2/dw/",
        account_description="account desc",
        location_description="location desc",
        application_tag="cx-v2",
    )

    assert result["account_created"] is True
    assert result["location_created"] is True
    assert result["account_uuid"] == "account-created"
    assert result["export_location_uuid"] == "location-created"
    assert result["gcp_service_account"] == "C-test@example.iam.gserviceaccount.com"
    assert client.created_accounts == [
        {
            "type": "gcp",
            "accountProperties": {"projectId": "ajuma-8"},
            "name": "Shimizu GCS",
            "description": "account desc",
        }
    ]
    assert client.created_locations == [
        {
            "type": "gcp",
            "accountUuid": "account-created",
            "properties": {"bucket": "dms-aa", "prefix": "cx-v2/dw/"},
            "name": "DMS AA DW",
            "description": "location desc",
            "application": "DATA_WAREHOUSE",
            "applicationTag": "cx-v2",
        }
    ]
    assert "gs://dms-aa" in result["gcs_iam_command"]
    assert "roles/storage.objectCreator" in result["gcs_iam_command"]


def test_ensure_gcp_dw_location_dry_run_no_account_writes_nothing():
    client = _DummyClient()

    result = ensure_gcp_dw_location(
        client,
        account_name="Shimizu GCS",
        project_id="ajuma-8",
        location_name="DMS AA DW",
        bucket="dms-aa",
        prefix="cx-v2/dw/",
        dry_run=True,
    )

    assert result["applied"] is False
    assert result["mode"] == "dry_run"
    assert result["account_created"] is True
    assert result["location_created"] is True
    assert result["account_uuid"] == ""
    assert "note" in result
    # nothing was created
    assert client.created_accounts == []
    assert client.created_locations == []


def test_ensure_gcp_dw_location_dry_run_existing_account_plans_location_only():
    client = _DummyClient(
        accounts=[
            {
                "type": "gcp",
                "name": "Shimizu GCS",
                "uuid": "account-1",
                "deleted": False,
                "accountProperties": {
                    "projectId": "ajuma-8",
                    "email": "C-test@example.iam.gserviceaccount.com",
                },
            }
        ]
    )

    result = ensure_gcp_dw_location(
        client,
        account_name="Shimizu GCS",
        project_id="ajuma-8",
        location_name="DMS AA DW",
        bucket="dms-aa",
        prefix="cx-v2/dw/",
        dry_run=True,
    )

    assert result["applied"] is False
    assert result["account_created"] is False
    assert result["location_created"] is True  # planned
    assert result["account_uuid"] == "account-1"
    assert result["location"] is None
    # IAM command still surfaced from the existing account's service email
    assert "gs://dms-aa" in result["gcs_iam_command"]
    assert client.created_locations == []


def test_ensure_gcp_dw_location_reuses_matching_location():
    client = _DummyClient(
        accounts=[
            {
                "type": "gcp",
                "name": "Shimizu GCS",
                "uuid": "account-1",
                "deleted": False,
                "accountProperties": {
                    "projectId": "ajuma-8",
                    "email": "C-test@example.iam.gserviceaccount.com",
                },
            }
        ],
        locations=[
            {
                "type": "gcp",
                "accountUuid": "account-1",
                "properties": {"bucket": "dms-aa", "prefix": "cx-v2/dw/"},
                "name": "DMS AA DW",
                "application": "DATA_WAREHOUSE",
                "uuid": "location-1",
                "deleted": False,
            }
        ],
    )

    result = ensure_gcp_dw_location(
        client,
        account_name="Shimizu GCS",
        project_id="ajuma-8",
        location_name="DMS AA DW",
        bucket="dms-aa",
        prefix="cx-v2/dw/",
    )

    assert result["account_created"] is False
    assert result["location_created"] is False
    assert result["export_location_uuid"] == "location-1"
    assert client.created_accounts == []
    assert client.created_locations == []


def test_build_gcs_iam_command_quotes_values():
    command = build_gcs_iam_command(
        "dms-aa",
        "C-test@example.iam.gserviceaccount.com",
    )

    assert command == (
        "gcloud storage buckets add-iam-policy-binding gs://dms-aa "
        "--member=serviceAccount:C-test@example.iam.gserviceaccount.com "
        "--role=roles/storage.objectCreator"
    )


def test_delete_account_accepts_empty_204_response():
    client = AdobeCloudLocationsClient(auth=_DummyAuth(), company_id="company1")
    session = _DummySession(_DummyResponse(204))
    client.session = session

    assert client.delete_account("account-1") == {}
    assert session.requests[0]["method"] == "DELETE"
    assert session.requests[0]["url"].endswith(
        "/company1/export_locations/analytics/exportlocations/account/account-1"
    )


def test_delete_location_accepts_empty_204_response():
    client = AdobeCloudLocationsClient(auth=_DummyAuth(), company_id="company1")
    session = _DummySession(_DummyResponse(204))
    client.session = session

    assert client.delete_location("location-1") == {}
    assert session.requests[0]["method"] == "DELETE"
    assert session.requests[0]["url"].endswith(
        "/company1/export_locations/analytics/exportlocations/location/location-1"
    )
