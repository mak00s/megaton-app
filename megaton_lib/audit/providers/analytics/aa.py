"""Adobe Analytics provider for audit tasks.

Custom AA 2.0 client with explicit exponential backoff and paging.
This is intentionally hosted in megaton-app first and can later be moved
into the core `megaton` package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import email.utils
import json
import os
from pathlib import Path
import random
import time
from typing import Any

import pandas as pd
import requests

from megaton_lib.audit.config import AdobeAnalyticsConfig


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class AdobeAnalyticsClient:
    """Adobe Analytics 2.0 API client (direct REST implementation)."""

    config: AdobeAnalyticsConfig
    max_retries: int = 5
    backoff_factor: float = 1.0
    jitter: float = 0.2
    timeout_sec: float = 60.0

    token_cache_file: Path = field(init=False)
    access_token: str = field(init=False)
    session: requests.Session = field(init=False)
    _client_id_val: str = field(init=False)
    _client_secret_val: str = field(init=False)
    _org_id_val: str = field(init=False)

    IMS_TOKEN_URL = "https://ims-na1.adobelogin.com/ims/token/v3"
    AA_API_BASE = "https://analytics.adobe.io/api"

    def __post_init__(self) -> None:
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if self.backoff_factor < 0:
            raise ValueError("backoff_factor must be >= 0")
        if not (0 <= self.jitter < 1):
            raise ValueError("jitter must be in [0, 1)")

        self._client_id_val = os.getenv(self.config.client_id_env, "").strip()
        if not self._client_id_val:
            raise RuntimeError(f"Adobe client id is missing: env {self.config.client_id_env}")

        self._client_secret_val = os.getenv(self.config.client_secret_env, "").strip()
        if not self._client_secret_val:
            raise RuntimeError(f"Adobe client secret is missing: env {self.config.client_secret_env}")

        self._org_id_val = self.config.org_id or os.getenv(self.config.org_id_env, "").strip()
        if not self._org_id_val:
            raise RuntimeError(
                "Adobe org_id is missing. "
                f"Set aa.org_id in config or env {self.config.org_id_env}.",
            )

        self.token_cache_file = Path(self.config.token_cache_file)
        self.token_cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.access_token = self._ensure_access_token()

    def _ensure_access_token(self, force_refresh: bool = False) -> str:
        if not force_refresh:
            cached = self._load_cached_token()
            if cached:
                return cached
        token_info = self._request_token()
        self._save_token(token_info)
        return str(token_info["access_token"])

    def refresh_access_token(self) -> str:
        self.access_token = self._ensure_access_token(force_refresh=True)
        return self.access_token

    def _load_cached_token(self) -> str | None:
        if not self.token_cache_file.exists():
            return None
        try:
            payload = json.loads(self.token_cache_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

        expires_at = float(payload.get("expires_at", 0))
        if expires_at - 60 <= time.time():
            return None

        token = payload.get("access_token")
        if isinstance(token, str) and token.strip():
            return token
        return None

    def _save_token(self, token_info: dict[str, Any]) -> None:
        payload = {
            "access_token": token_info.get("access_token"),
            "expires_at": time.time() + float(token_info.get("expires_in", 3600)),
        }
        self.token_cache_file.write_text(json.dumps(payload), encoding="utf-8")

    def _request_token(self) -> dict[str, Any]:
        data = {
            "client_id": self._client_id_val,
            "client_secret": self._client_secret_val,
            "grant_type": "client_credentials",
            "scope": self.config.scopes,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = self._request_json(
            method="POST",
            url=self.IMS_TOKEN_URL,
            headers=headers,
            data=data,
            authenticated=False,
            retry_on_401=False,
        )
        if "access_token" not in payload:
            raise RuntimeError("Invalid Adobe token response")
        return payload

    def _api_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self._client_id_val,
            "x-gw-ims-org-id": self._org_id_val,
            "x-proxy-global-company-id": self.config.company_id,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

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
        now = datetime.now(timezone.utc)
        return max(0.0, (dt - now).total_seconds())

    @staticmethod
    def _safe_json(response: requests.Response) -> dict[str, Any] | None:
        try:
            payload = response.json()
        except Exception:
            return None
        if isinstance(payload, dict):
            return payload
        return None

    @staticmethod
    def _extract_status(response: requests.Response, payload: dict[str, Any] | None) -> int:
        status = int(response.status_code)
        if payload is None:
            return status
        pseudo = payload.get("status_code")
        if isinstance(pseudo, int) and status < 400:
            return pseudo
        if isinstance(pseudo, str) and pseudo.isdigit() and status < 400:
            return int(pseudo)
        err_code = str(payload.get("error_code", ""))
        if status < 400 and err_code == "429050":
            return 429
        return status

    def _request_json(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        authenticated: bool,
        retry_on_401: bool,
    ) -> dict[str, Any]:
        refreshed = False

        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    data=data,
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries - 1:
                    raise RuntimeError(f"Adobe request failed (network): {exc}") from exc
                wait = self._compute_wait(attempt, None)
                time.sleep(wait)
                continue

            payload = self._safe_json(response)
            status = self._extract_status(response, payload)

            if 200 <= status < 300:
                if payload is not None:
                    return payload
                raise RuntimeError(f"Adobe API returned non-JSON response: HTTP {status}")

            if authenticated and retry_on_401 and status == 401 and not refreshed:
                self.refresh_access_token()
                headers = self._api_headers()
                refreshed = True
                continue

            if status in _RETRYABLE_STATUS and attempt < self.max_retries - 1:
                retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                wait = self._compute_wait(attempt, retry_after)
                time.sleep(wait)
                continue

            detail = payload if payload is not None else response.text
            raise RuntimeError(f"Adobe API request failed: HTTP {status}, response={detail}")

        raise RuntimeError("Adobe request retries exhausted")

    def get_report(
        self,
        *,
        rsid: str,
        dimension: str,
        metrics: list[str] | None,
        date_from: str | date | datetime,
        date_to: str | date | datetime,
        limit: int = 20000,
        n_results: int | None = None,
        segment: str | list[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch Adobe Analytics report with explicit page loop."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        dim_norm = str(dimension).strip().lower()
        if dim_norm.startswith("variables/"):
            dim_api = dim_norm
            dim_name = dim_norm.split("/", 1)[1]
        else:
            dim_name = dim_norm
            dim_api = f"variables/{dim_name}"

        metric_list = metrics or ["occurrences"]
        metric_ids = [m if m.startswith("metrics/") else f"metrics/{m}" for m in metric_list]

        global_filters: list[dict[str, Any]] = []
        if segment:
            segment_ids = [segment] if isinstance(segment, str) else list(segment)
            global_filters.extend({"type": "segment", "segmentId": seg_id} for seg_id in segment_ids)
        global_filters.append(
            {
                "type": "dateRange",
                "dateRange": f"{_normalize_datetime(date_from)}/{_normalize_datetime(date_to)}",
            }
        )

        request_body: dict[str, Any] = {
            "rsid": rsid,
            "globalFilters": global_filters,
            "metricContainer": {
                "metrics": [
                    {
                        "columnId": str(idx),
                        "id": metric_id,
                        "sort": "desc" if idx == 0 else None,
                    }
                    for idx, metric_id in enumerate(metric_ids)
                ],
            },
            "dimension": dim_api,
            "settings": {
                "countRepeatInstances": True,
                "limit": int(limit),
                "nonesBehavior": "return-nones",
                "page": 0,
            },
        }

        endpoint = f"{self.AA_API_BASE}/{self.config.company_id}/reports"
        params = {
            "allowRemoteLoad": "default",
            "useCache": True,
            "useResultsCache": False,
            "includeOberonXml": False,
            "includePlatformPredictiveObjects": False,
        }

        all_rows: list[dict[str, Any]] = []
        page = 0
        max_rows = float("inf") if n_results is None else float(max(n_results, 0))

        while True:
            request_body["settings"]["page"] = page
            response = self._request_json(
                method="POST",
                url=endpoint,
                headers=self._api_headers(),
                params=params,
                json_body=request_body,
                authenticated=True,
                retry_on_401=True,
            )

            page_rows = response.get("rows", [])
            if not isinstance(page_rows, list):
                raise RuntimeError(f"Unexpected Adobe report format: rows={type(page_rows)}")

            all_rows.extend(page_rows)
            if len(all_rows) >= max_rows:
                break

            last_page = bool(response.get("lastPage", True))
            if last_page:
                break
            page += 1

        if max_rows != float("inf") and len(all_rows) > int(max_rows):
            all_rows = all_rows[: int(max_rows)]

        if not all_rows:
            cols = [dim_api, *metric_ids]
            return pd.DataFrame(columns=cols)

        records: list[dict[str, Any]] = []
        for row in all_rows:
            if not isinstance(row, dict):
                continue
            values = row.get("data", [])
            if not isinstance(values, list):
                values = []

            record: dict[str, Any] = {dim_api: row.get("value", "")}
            for idx, metric_id in enumerate(metric_ids):
                record[metric_id] = values[idx] if idx < len(values) else 0
            if "itemId" in row:
                record["itemId"] = row["itemId"]
            records.append(record)

        df = pd.DataFrame(records)
        return df

    def fetch_dimension_metric(
        self,
        *,
        start_date: str,
        end_date: str,
        dimension: str,
        metric: str,
        limit: int = 50000,
    ) -> pd.DataFrame:
        """Fetch one dimension x one metric report from Adobe Analytics."""
        df = self.get_report(
            rsid=self.config.rsid,
            dimension=dimension,
            metrics=[metric],
            date_from=start_date,
            date_to=end_date,
            limit=limit,
        )
        if df.empty:
            return pd.DataFrame(columns=["site", "metric_value"])

        dim = dimension if str(dimension).startswith("variables/") else f"variables/{dimension}"
        metric_id = metric if str(metric).startswith("metrics/") else f"metrics/{metric}"

        out = df.copy()
        dim_col = dim if dim in out.columns else _resolve_dimension_col(out, dim)
        metric_col = metric_id if metric_id in out.columns else _resolve_metric_col(out, metric_id)

        out[metric_col] = pd.to_numeric(out[metric_col], errors="coerce").fillna(0.0)
        result = out.rename(columns={dim_col: "site", metric_col: "metric_value"})
        result["site"] = result["site"].fillna("").astype(str)
        result["metric_value"] = pd.to_numeric(result["metric_value"], errors="coerce").fillna(0.0)
        return result[["site", "metric_value"]]


def _resolve_dimension_col(df: pd.DataFrame, dim_name: str) -> str:
    target = dim_name.lower().replace("/", "")
    for col in df.columns:
        norm = str(col).lower().replace("/", "")
        if norm == target or norm.endswith(target):
            return str(col)
    return str(df.columns[0])


def _resolve_metric_col(df: pd.DataFrame, metric_name: str) -> str:
    base = metric_name.removeprefix("metrics/")
    candidates = {
        metric_name,
        base,
        base.lower(),
        base.upper(),
        base.replace("_", " "),
        base.title().replace("_", " "),
    }
    target = base.lower().replace("_", "")

    for col in df.columns:
        normalized = str(col).lower().replace("_", "").replace(" ", "")
        if col in candidates or normalized == target or normalized.endswith(target):
            return str(col)

    return str(df.columns[-1])


def _normalize_datetime(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S.000")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%dT00:00:00.000")

    text = str(value).strip()
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Invalid date value for Adobe API: {value}")
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def fetch_aa_site_metric(*, config: AdobeAnalyticsConfig, start_date: str, end_date: str) -> pd.DataFrame:
    """Convenience function for site-level AA metric extraction."""
    client = AdobeAnalyticsClient(config)
    return client.fetch_dimension_metric(
        start_date=start_date,
        end_date=end_date,
        dimension=config.dimension,
        metric=config.metric,
    )
