"""Higher-level helpers for Adobe Analytics Cloud Locations."""

from __future__ import annotations

import shlex
from typing import Any

from .api import AdobeCloudLocationsClient


def _items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("content", [])
    if not isinstance(raw, list):
        raise RuntimeError(f"Unexpected Adobe Cloud Locations payload: {payload}")
    return [item for item in raw if isinstance(item, dict)]


def _active(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in items if not bool(item.get("deleted"))]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _account_project_id(account: dict[str, Any]) -> str:
    props = account.get("accountProperties", {})
    return _text(props.get("projectId")) if isinstance(props, dict) else ""


def _account_service_email(account: dict[str, Any]) -> str:
    props = account.get("accountProperties", {})
    return _text(props.get("email")) if isinstance(props, dict) else ""


def _same_location(
    location: dict[str, Any],
    *,
    account_uuid: str,
    name: str,
    bucket: str,
    prefix: str,
    application: str,
) -> bool:
    props = location.get("properties", {})
    if not isinstance(props, dict):
        return False
    return (
        _text(location.get("accountUuid")) == account_uuid
        and _text(location.get("name")) == name
        and _text(location.get("application")) == application
        and _text(props.get("bucket")) == bucket
        and _text(props.get("prefix")) == prefix
    )


def create_gcp_account(
    client: AdobeCloudLocationsClient,
    *,
    name: str,
    project_id: str,
    description: str = "",
    shared_to: str = "",
) -> dict[str, Any]:
    """Create an Adobe Analytics GCP Cloud Account."""
    body: dict[str, Any] = {
        "type": "gcp",
        "accountProperties": {"projectId": project_id},
        "name": name,
        "description": description,
    }
    if shared_to:
        body["sharedTo"] = shared_to
    return client.create_account(body)


def ensure_gcp_account(
    client: AdobeCloudLocationsClient,
    *,
    name: str,
    project_id: str,
    description: str = "",
    shared_to: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Return an existing matching GCP Cloud Account or create one.

    ``created`` reports whether a create is needed; ``applied`` reports whether
    the create was actually performed. In ``dry_run`` mode a needed create is
    not performed and ``account`` is ``None`` (it does not exist yet).
    """
    payload = client.list_accounts(account_type="gcp", limit=100, page=0)
    matches = [
        item
        for item in _active(_items(payload))
        if _text(item.get("name")) == name
    ]
    if len(matches) > 1:
        raise LookupError(f"Multiple active GCP Cloud Accounts named {name!r}")
    if matches:
        account = matches[0]
        existing_project_id = _account_project_id(account)
        if existing_project_id and existing_project_id != project_id:
            raise ValueError(
                f"GCP Cloud Account {name!r} uses project_id={existing_project_id!r}, "
                f"not {project_id!r}"
            )
        return {"created": False, "applied": False, "account": account}

    if dry_run:
        return {"created": True, "applied": False, "account": None}

    return {
        "created": True,
        "applied": True,
        "account": create_gcp_account(
            client,
            name=name,
            project_id=project_id,
            description=description,
            shared_to=shared_to,
        ),
    }


def create_gcp_location(
    client: AdobeCloudLocationsClient,
    *,
    account_uuid: str,
    name: str,
    bucket: str,
    prefix: str,
    description: str = "",
    application: str = "DATA_WAREHOUSE",
    application_tag: str = "",
    shared_to: str = "",
) -> dict[str, Any]:
    """Create an Adobe Analytics GCP Cloud Location."""
    body: dict[str, Any] = {
        "type": "gcp",
        "accountUuid": account_uuid,
        "properties": {"bucket": bucket, "prefix": prefix},
        "name": name,
        "description": description,
        "application": application,
    }
    if application_tag:
        body["applicationTag"] = application_tag
    if shared_to:
        body["sharedTo"] = shared_to
    return client.create_location(body)


def ensure_gcp_dw_location(
    client: AdobeCloudLocationsClient,
    *,
    account_name: str,
    project_id: str,
    location_name: str,
    bucket: str,
    prefix: str,
    account_description: str = "",
    location_description: str = "",
    application: str = "DATA_WAREHOUSE",
    application_tag: str = "",
    shared_to: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ensure a GCP Cloud Account and Data Warehouse Location exist.

    ``dry_run`` (the default for the CLI) performs no writes: it reports what
    would be created via the ``*_created`` flags with ``applied=False``. When
    the account itself does not yet exist, the location cannot be checked
    without it, so the location is reported as a planned create.
    """
    account_result = ensure_gcp_account(
        client,
        name=account_name,
        project_id=project_id,
        description=account_description,
        shared_to=shared_to,
        dry_run=dry_run,
    )
    account = account_result["account"]
    if account is None:
        # dry-run and the account does not exist yet: we cannot list locations
        # without an account uuid, so report both as planned creates.
        return {
            "account_created": True,
            "location_created": True,
            "applied": False,
            "mode": "dry_run",
            "account": None,
            "location": None,
            "account_uuid": "",
            "location_uuid": "",
            "export_location_uuid": "",
            "gcp_service_account": "",
            "gcs_iam_command": "",
            "note": (
                "Account does not exist yet; run with --apply to create it, "
                "then re-run to check/create the location."
            ),
        }
    account_uuid = _text(account.get("uuid"))
    if not account_uuid:
        raise RuntimeError(f"Adobe Cloud Account response did not include uuid: {account}")

    locations = _active(
        _items(
            client.list_locations(
                account_uuid=account_uuid,
                application=application,
                limit=100,
                page=0,
            )
        )
    )
    matches = [
        item
        for item in locations
        if _same_location(
            item,
            account_uuid=account_uuid,
            name=location_name,
            bucket=bucket,
            prefix=prefix,
            application=application,
        )
    ]
    same_name = [item for item in locations if _text(item.get("name")) == location_name]
    if len(matches) > 1:
        raise LookupError(f"Multiple active Cloud Locations match {location_name!r}")
    if not matches and same_name:
        raise ValueError(
            f"Cloud Location {location_name!r} exists but bucket/prefix/application differ"
        )

    if matches:
        location_created = False
        location = matches[0]
    elif dry_run:
        location_created = True
        location = None
    else:
        location_created = True
        location = create_gcp_location(
            client,
            account_uuid=account_uuid,
            name=location_name,
            bucket=bucket,
            prefix=prefix,
            description=location_description,
            application=application,
            application_tag=application_tag,
            shared_to=shared_to,
        )

    service_email = _account_service_email(account)
    location = location or {}
    return {
        "account_created": bool(account_result["created"]),
        "location_created": location_created,
        "applied": not dry_run,
        "mode": "dry_run" if dry_run else "apply",
        "account": account,
        "location": location or None,
        "account_uuid": account_uuid,
        "location_uuid": _text(location.get("uuid")),
        "export_location_uuid": _text(location.get("uuid")),
        "gcp_service_account": service_email,
        "gcs_iam_command": build_gcs_iam_command(bucket, service_email)
        if service_email
        else "",
    }


def build_gcs_iam_command(
    bucket: str,
    service_account_email: str,
    *,
    role: str = "roles/storage.objectCreator",
) -> str:
    """Return the gcloud command required to grant Adobe write access to GCS."""
    if not bucket.strip():
        raise ValueError("bucket is required")
    if not service_account_email.strip():
        raise ValueError("service_account_email is required")
    uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
    return " ".join(
        [
            "gcloud",
            "storage",
            "buckets",
            "add-iam-policy-binding",
            shlex.quote(uri),
            f"--member={shlex.quote('serviceAccount:' + service_account_email)}",
            f"--role={shlex.quote(role)}",
        ]
    )
