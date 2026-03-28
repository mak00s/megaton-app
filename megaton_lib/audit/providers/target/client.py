"""Adobe Target REST API client with retry and pagination.

Provides ``AdobeTargetClient`` for authenticated access to the Adobe Target
Recommendations and Feeds APIs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import email.utils
import random
import time
from typing import Any

import requests

from megaton_lib.audit.config import AdobeTargetConfig
from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class AdobeTargetClient:
    """Adobe Target API client with exponential backoff and auto-refresh.

    Mirrors the retry logic of ``AdobeAnalyticsClient``.
    """

    config: AdobeTargetConfig
    max_retries: int = 5
    backoff_factor: float = 1.0
    jitter: float = 0.2
    timeout_sec: float = 60.0

    _auth: AdobeOAuthClient = field(init=False, repr=False)
    session: requests.Session = field(init=False)

    def __post_init__(self) -> None:
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        oauth = self.config.oauth
        if oauth is not None:
            self._auth = AdobeOAuthClient(
                client_id=oauth.client_id or "",
                client_secret=oauth.client_secret or "",
                client_id_env=oauth.client_id_env,
                client_secret_env=oauth.client_secret_env,
                org_id=oauth.org_id or "",
                org_id_env=oauth.org_id_env,
                scopes=oauth.scopes,
                token_cache_file=oauth.token_cache_file,
            )
        else:
            # Default env vars
            self._auth = AdobeOAuthClient()

        self.session = requests.Session()

    @property
    def base_url(self) -> str:
        return f"{self.config.base_url.rstrip('/')}/{self.config.tenant_id}/target/recs"

    def _api_headers(self) -> dict[str, str]:
        return self._auth.get_headers(
            extra={
                "Accept": self.config.accept_header,
                "Content-Type": self.config.accept_header,
            },
        )

    # ---- HTTP helpers ----

    def get(self, endpoint: str, *, params: dict[str, Any] | None = None) -> dict[str, Any] | list[Any]:
        """Authenticated GET with retry."""
        url = f"{self.base_url}{endpoint}"
        return self._request("GET", url, params=params)

    def patch(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Authenticated PATCH with retry."""
        url = f"{self.base_url}{endpoint}"
        result = self._request("PATCH", url, json_body=payload)
        if isinstance(result, dict):
            return result
        return {}

    def put(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Authenticated PUT with retry.

        Required for design ``script`` updates — the Target Recs API
        ignores ``script`` in PATCH requests.
        """
        url = f"{self.base_url}{endpoint}"
        result = self._request("PUT", url, json_body=payload)
        if isinstance(result, dict):
            return result
        return {}

    # ---- pagination ----

    def get_all(
        self,
        endpoint: str,
        *,
        limit: int = 100,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all items from a paginated endpoint.

        The Target Recs API uses ``offset``/``limit`` pagination.
        """
        all_items: list[dict[str, Any]] = []
        offset = 0

        while True:
            params = {"offset": offset, "limit": limit}
            result = self.get(endpoint, params=params)

            if isinstance(result, list):
                items = result
            elif isinstance(result, dict):
                # Some endpoints wrap in an object
                items = result.get("list", result.get("items", []))
                if not isinstance(items, list):
                    items = [result]
            else:
                break

            all_items.extend(items)

            if max_items is not None and len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break

            if len(items) < limit:
                break

            offset += len(items)

        return all_items

    # ---- retry engine ----

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        refreshed = False
        headers = self._api_headers()

        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries - 1:
                    raise RuntimeError(f"Target API request failed (network): {exc}") from exc
                time.sleep(self._compute_wait(attempt, None))
                continue

            status = response.status_code

            if 200 <= status < 300:
                if not response.text.strip():
                    return {}
                payload = response.json()
                if isinstance(payload, (dict, list)):
                    return payload
                raise RuntimeError(f"Target API returned non-JSON: HTTP {status}")

            if status == 401 and not refreshed:
                self._auth.refresh_access_token()
                headers = self._api_headers()
                refreshed = True
                continue

            if status in _RETRYABLE_STATUS and attempt < self.max_retries - 1:
                retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                time.sleep(self._compute_wait(attempt, retry_after))
                continue

            raise RuntimeError(
                f"Target API request failed: HTTP {status}, url={url}, body={response.text}"
            )

        raise RuntimeError("Target API request retries exhausted")

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
