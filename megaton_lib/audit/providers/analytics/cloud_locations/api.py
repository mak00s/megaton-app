"""Adobe Analytics Cloud Locations API client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import email.utils
import random
import time
from typing import Any

import requests

from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
AA_API_BASE = "https://analytics.adobe.io/api"


@dataclass(slots=True)
class AdobeCloudLocationsClient:
    """Direct REST client for Adobe Analytics Cloud Accounts and Locations APIs."""

    auth: AdobeOAuthClient
    company_id: str
    max_retries: int = 5
    backoff_factor: float = 1.0
    jitter: float = 0.2
    timeout_sec: float = 60.0

    session: requests.Session = field(init=False)

    def __post_init__(self) -> None:
        self.company_id = str(self.company_id).strip()
        if not self.company_id:
            raise ValueError("company_id is required")
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if not (0 <= self.jitter < 1):
            raise ValueError("jitter must be in [0, 1)")
        self.session = requests.Session()

    def _api_headers(self) -> dict[str, str]:
        return self.auth.get_headers(
            extra={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-proxy-global-company-id": self.company_id,
            }
        )

    def _compute_wait(self, attempt_index: int, retry_after_sec: float | None) -> float:
        if retry_after_sec is not None and retry_after_sec >= 0:
            wait = retry_after_sec
        else:
            wait = self.backoff_factor * (2**attempt_index)
        if self.jitter:
            wait *= random.uniform(1.0 - self.jitter, 1.0 + self.jitter)
        return max(0.0, min(wait, 120.0))

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            return float(raw)
        try:
            dt = email.utils.parsedate_to_datetime(raw)
        except Exception:
            return None
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())

    @staticmethod
    def _safe_json(response: requests.Response) -> dict[str, Any] | list[Any] | None:
        try:
            payload = response.json()
        except Exception:
            return None
        if isinstance(payload, (dict, list)):
            return payload
        return None

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        allow_array_json: bool = False,
        allow_empty_success: bool = False,
    ) -> dict[str, Any]:
        refreshed = False
        headers = self._api_headers()
        url = f"{AA_API_BASE}/{self.company_id}{path}"

        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params={k: v for k, v in (params or {}).items() if v is not None},
                    json=json_body,
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries - 1:
                    raise RuntimeError(f"Adobe Cloud Locations request failed: {exc}") from exc
                time.sleep(self._compute_wait(attempt, None))
                continue

            payload = self._safe_json(response)
            status_code = response.status_code
            if isinstance(payload, dict):
                pseudo = payload.get("status_code")
                if status_code < 400 and isinstance(pseudo, int):
                    status_code = pseudo

            if 200 <= status_code < 300:
                if isinstance(payload, dict):
                    return payload
                if allow_array_json and isinstance(payload, list):
                    return {"content": payload}
                if allow_empty_success and not (response.text or "").strip():
                    return {}
                raise RuntimeError(
                    f"Adobe Cloud Locations API returned non-JSON response: HTTP {status_code}"
                )

            if status_code == 401 and not refreshed:
                self.auth.refresh_access_token()
                headers = self._api_headers()
                refreshed = True
                continue

            if status_code in _RETRYABLE_STATUS and attempt < self.max_retries - 1:
                retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                time.sleep(self._compute_wait(attempt, retry_after))
                continue

            detail = payload if payload is not None else response.text
            raise RuntimeError(
                f"Adobe Cloud Locations API request failed: HTTP {status_code}, response={detail}"
            )

        raise RuntimeError("Adobe Cloud Locations request retries exhausted")

    def list_accounts(
        self,
        *,
        account_type: str | None = None,
        created_by: str | None = None,
        limit: int = 100,
        page: int = 0,
    ) -> dict[str, Any]:
        return self._request_json(
            method="GET",
            path="/export_locations/analytics/exportlocations/account",
            params={
                "type": account_type,
                "createdBy": created_by,
                "limit": limit,
                "page": page,
            },
        )

    def get_account(self, account_uuid: str) -> dict[str, Any]:
        return self._request_json(
            method="GET",
            path=f"/export_locations/analytics/exportlocations/account/{account_uuid}",
        )

    def create_account(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._request_json(
            method="POST",
            path="/export_locations/analytics/exportlocations/account",
            json_body=body,
        )

    def update_account(self, account_uuid: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request_json(
            method="PUT",
            path=f"/export_locations/analytics/exportlocations/account/{account_uuid}",
            json_body=body,
        )

    def delete_account(self, account_uuid: str) -> dict[str, Any]:
        return self._request_json(
            method="DELETE",
            path=f"/export_locations/analytics/exportlocations/account/{account_uuid}",
            allow_empty_success=True,
        )

    def list_locations(
        self,
        *,
        account_uuid: str | None = None,
        application: str | None = None,
        limit: int = 100,
        page: int = 0,
    ) -> dict[str, Any]:
        return self._request_json(
            method="GET",
            path="/export_locations/analytics/exportlocations/location",
            params={
                "accountUuid": account_uuid,
                "application": application,
                "limit": limit,
                "page": page,
            },
        )

    def get_location(self, location_uuid: str) -> dict[str, Any]:
        return self._request_json(
            method="GET",
            path=f"/export_locations/analytics/exportlocations/location/{location_uuid}",
        )

    def create_location(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._request_json(
            method="POST",
            path="/export_locations/analytics/exportlocations/location",
            json_body=body,
        )

    def update_location(self, location_uuid: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request_json(
            method="PUT",
            path=f"/export_locations/analytics/exportlocations/location/{location_uuid}",
            json_body=body,
        )

    def delete_location(self, location_uuid: str) -> dict[str, Any]:
        return self._request_json(
            method="DELETE",
            path=f"/export_locations/analytics/exportlocations/location/{location_uuid}",
            allow_empty_success=True,
        )
