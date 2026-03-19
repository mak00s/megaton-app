"""Adobe Analytics provider for audit tasks.

Custom AA 2.0 client with explicit exponential backoff and paging.
This is intentionally hosted in megaton-app first and can later be moved
into the core `megaton` package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import email.utils
import random
import time
from typing import Any

import pandas as pd
import requests

from megaton_lib.audit.config import AdobeAnalyticsConfig
from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class AdobeAnalyticsClient:
    """Adobe Analytics 2.0 API client (direct REST implementation)."""

    config: AdobeAnalyticsConfig
    max_retries: int = 5
    backoff_factor: float = 1.0
    jitter: float = 0.2
    timeout_sec: float = 60.0

    _auth: AdobeOAuthClient = field(init=False, repr=False)
    session: requests.Session = field(init=False)

    AA_API_BASE = "https://analytics.adobe.io/api"

    def __post_init__(self) -> None:
        if self.max_retries < 1:
            raise ValueError("max_retries must be >= 1")
        if self.backoff_factor < 0:
            raise ValueError("backoff_factor must be >= 0")
        if not (0 <= self.jitter < 1):
            raise ValueError("jitter must be in [0, 1)")

        self._auth = AdobeOAuthClient(
            client_id_env=self.config.client_id_env,
            client_secret_env=self.config.client_secret_env,
            org_id=self.config.org_id or "",
            org_id_env=self.config.org_id_env,
            scopes=self.config.scopes,
            token_cache_file=self.config.token_cache_file,
        )
        self.session = requests.Session()

    @property
    def access_token(self) -> str:
        """Current access token (delegated to shared OAuth client)."""
        return self._auth.access_token

    def refresh_access_token(self) -> str:
        return self._auth.refresh_access_token()

    def _api_headers(self, *, include_company: bool = True) -> dict[str, str]:
        extra: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if include_company and self.config.company_id.strip():
            extra["x-proxy-global-company-id"] = self.config.company_id
        return self._auth.get_headers(extra=extra)

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
    def _safe_json(response: requests.Response) -> dict[str, Any] | list[Any] | None:
        try:
            payload = response.json()
        except Exception:
            return None
        if isinstance(payload, (dict, list)):
            return payload
        return None

    @staticmethod
    def _extract_status(
        response: requests.Response,
        payload: dict[str, Any] | list[Any] | None,
    ) -> int:
        status = int(response.status_code)
        if payload is None or not isinstance(payload, dict):
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
        allow_array_json: bool = False,
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
                if isinstance(payload, dict):
                    return payload
                if allow_array_json and isinstance(payload, list):
                    return {"content": payload, "lastPage": True, "number": 0, "totalPages": 1}
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
        segment_definition: dict[str, Any] | list[dict[str, Any]] | None = None,
        breakdown: dict[str, Any] | list[dict[str, Any]] | None = None,
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
        if segment_definition:
            definitions = (
                [segment_definition]
                if isinstance(segment_definition, dict)
                else list(segment_definition)
            )
            for definition in definitions:
                if not isinstance(definition, dict):
                    raise TypeError("segment_definition items must be objects.")
                global_filters.append(
                    {
                        "type": "segment",
                        "segmentDefinition": definition,
                    }
                )
        global_filters.append(
            {
                "type": "dateRange",
                "dateRange": f"{_normalize_datetime(date_from)}/{_normalize_datetime(date_to)}",
            }
        )

        metric_filters: list[dict[str, Any]] = []
        if breakdown:
            raw_breakdowns = [breakdown] if isinstance(breakdown, dict) else list(breakdown)
            seen_filter_ids: set[str] = set()
            for idx, item in enumerate(raw_breakdowns):
                if not isinstance(item, dict):
                    raise TypeError("breakdown items must be objects.")
                filter_item = dict(item)
                filter_id = str(filter_item.get("id") or idx).strip()
                if not filter_id:
                    raise ValueError("breakdown id cannot be empty.")
                if filter_id in seen_filter_ids:
                    raise ValueError(f"duplicate breakdown id: {filter_id}")
                seen_filter_ids.add(filter_id)

                filter_type = str(filter_item.get("type") or "breakdown").strip()
                if filter_type != "breakdown":
                    raise ValueError("breakdown type must be 'breakdown'.")
                dimension_name = str(filter_item.get("dimension") or "").strip()
                if not dimension_name:
                    raise ValueError("breakdown.dimension is required.")
                if "itemId" not in filter_item and "itemValue" not in filter_item:
                    raise ValueError("breakdown requires itemId or itemValue.")

                filter_item["id"] = filter_id
                filter_item["type"] = filter_type
                filter_item["dimension"] = dimension_name
                metric_filters.append(filter_item)

        metric_entries: list[dict[str, Any]] = []
        for idx, metric_id in enumerate(metric_ids):
            entry: dict[str, Any] = {
                "columnId": str(idx),
                "id": metric_id,
                "sort": "desc" if idx == 0 else None,
            }
            if metric_filters:
                entry["filters"] = [str(item["id"]) for item in metric_filters]
            metric_entries.append(entry)

        request_body: dict[str, Any] = {
            "rsid": rsid,
            "globalFilters": global_filters,
            "metricContainer": {
                "metrics": metric_entries,
            },
            "dimension": dim_api,
            "settings": {
                "countRepeatInstances": True,
                "limit": int(limit),
                "nonesBehavior": "return-nones",
                "page": 0,
            },
        }
        if metric_filters:
            request_body["metricContainer"]["metricFilters"] = metric_filters

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

            if not page_rows:
                break

            last_page = bool(response.get("lastPage", False))
            if last_page:
                break

            total_pages = response.get("totalPages")
            current_page = response.get("number")
            if (
                isinstance(total_pages, int)
                and isinstance(current_page, int)
                and current_page + 1 >= total_pages
            ):
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

    def list_report_suites(self, *, limit: int = 1000) -> list[dict[str, str]]:
        """List accessible report suites for the configured company."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        endpoint = f"{self.AA_API_BASE}/{self.config.company_id}/collections/suites"
        page = 0
        max_page_size = 200
        page_size = min(int(limit), max_page_size)
        suites: list[dict[str, str]] = []
        seen: set[str] = set()

        while True:
            response = self._request_json(
                method="GET",
                url=endpoint,
                headers=self._api_headers(),
                params={"limit": page_size, "page": page},
                authenticated=True,
                retry_on_401=True,
            )

            content = response.get("content", [])
            if not isinstance(content, list):
                raise RuntimeError(
                    f"Unexpected Adobe suites format: content={type(content)}",
                )

            for item in content:
                if not isinstance(item, dict):
                    continue
                rsid = str(item.get("rsid") or item.get("id") or "").strip()
                if not rsid or rsid in seen:
                    continue
                seen.add(rsid)
                suites.append(
                    {
                        "rsid": rsid,
                        "name": str(item.get("name") or rsid).strip(),
                    }
                )
                if len(suites) >= limit:
                    return suites

            if not content:
                break

            if bool(response.get("lastPage", False)):
                break

            total_pages = response.get("totalPages")
            current_page = response.get("number")
            if (
                isinstance(total_pages, int)
                and isinstance(current_page, int)
                and current_page + 1 >= total_pages
            ):
                break

            page += 1

        return suites

    def list_companies(self) -> list[dict[str, str]]:
        """List accessible Adobe Analytics companies from discovery API."""
        response = self._request_json(
            method="GET",
            url="https://analytics.adobe.io/discovery/me",
            headers=self._api_headers(include_company=False),
            authenticated=True,
            retry_on_401=True,
        )
        orgs = response.get("imsOrgs", [])
        if not isinstance(orgs, list):
            raise RuntimeError(f"Unexpected Adobe discovery format: imsOrgs={type(orgs)}")

        companies: list[dict[str, str]] = []
        seen: set[str] = set()
        for org in orgs:
            if not isinstance(org, dict):
                continue
            for company in org.get("companies", []) or []:
                if not isinstance(company, dict):
                    continue
                company_id = str(company.get("globalCompanyId") or "").strip()
                if not company_id or company_id in seen:
                    continue
                seen.add(company_id)
                companies.append(
                    {
                        "company_id": company_id,
                        "name": str(company.get("companyName") or company_id).strip(),
                    }
                )

        return companies

    def _list_catalog_items(
        self,
        *,
        endpoint_name: str,
        rsid: str,
        limit: int,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, str]]:
        """List AA catalog items (dimensions/metrics/segments) for the given RSID."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        endpoint = f"{self.AA_API_BASE}/{self.config.company_id}/{endpoint_name}"
        page = 0
        max_page_size = 200
        page_size = min(int(limit), max_page_size)
        items: list[dict[str, str]] = []
        seen: set[str] = set()

        while True:
            params = {"rsid": rsid, "limit": page_size, "page": page}
            if extra_params:
                params.update(extra_params)
            response = self._request_json(
                method="GET",
                url=endpoint,
                headers=self._api_headers(),
                params=params,
                authenticated=True,
                retry_on_401=True,
                allow_array_json=True,
            )
            content = response.get("content", [])
            if not isinstance(content, list):
                raise RuntimeError(
                    f"Unexpected Adobe {endpoint_name} format: content={type(content)}",
                )

            for item in content:
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("id") or "").strip()
                if not item_id or item_id in seen:
                    continue
                seen.add(item_id)
                items.append(
                    {
                        "id": item_id,
                        "name": str(item.get("name") or item_id).strip(),
                    }
                )
                if len(items) >= limit:
                    return items

            if not content:
                break
            if bool(response.get("lastPage", False)):
                break

            total_pages = response.get("totalPages")
            current_page = response.get("number")
            if (
                isinstance(total_pages, int)
                and isinstance(current_page, int)
                and current_page + 1 >= total_pages
            ):
                break

            page += 1

        return items

    def list_dimensions(self, *, rsid: str, limit: int = 2000) -> list[dict[str, str]]:
        """List report dimensions for the given RSID."""
        return self._list_catalog_items(
            endpoint_name="dimensions",
            rsid=rsid,
            limit=limit,
        )

    def list_metrics(self, *, rsid: str, limit: int = 2000) -> list[dict[str, str]]:
        """List report metrics for the given RSID."""
        return self._list_catalog_items(
            endpoint_name="metrics",
            rsid=rsid,
            limit=limit,
        )

    def list_segments(
        self,
        *,
        rsid: str,
        limit: int = 2000,
        name: str | None = None,
        include_definition: bool = False,
    ) -> list[dict[str, Any]]:
        """List available segments for the given RSID.

        When ``include_definition`` is enabled, Adobe returns the segment
        definition payload in addition to basic metadata. ``name`` applies the
        server-side name filter supported by the Segments API.
        """
        extra_params: dict[str, Any] = {"includeType": "all"}
        if name and str(name).strip():
            extra_params["name"] = str(name).strip()
        if include_definition:
            extra_params["expansion"] = "definition"

        endpoint = f"{self.AA_API_BASE}/{self.config.company_id}/segments"
        page = 0
        max_page_size = 200
        page_size = min(int(limit), max_page_size)
        items: list[dict[str, Any]] = []
        seen: set[str] = set()

        while True:
            params = {"rsid": rsid, "limit": page_size, "page": page, **extra_params}
            response = self._request_json(
                method="GET",
                url=endpoint,
                headers=self._api_headers(),
                params=params,
                authenticated=True,
                retry_on_401=True,
                allow_array_json=True,
            )
            content = response.get("content", [])
            if not isinstance(content, list):
                raise RuntimeError(
                    f"Unexpected Adobe segments format: content={type(content)}",
                )

            for item in content:
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("id") or "").strip()
                if not item_id or item_id in seen:
                    continue
                seen.add(item_id)
                segment: dict[str, Any] = {
                    "id": item_id,
                    "name": str(item.get("name") or item_id).strip(),
                }
                description = item.get("description")
                if description not in (None, ""):
                    segment["description"] = str(description)
                if "definition" in item:
                    segment["definition"] = item["definition"]
                items.append(segment)
                if len(items) >= limit:
                    return items

            if not content:
                break
            if bool(response.get("lastPage", False)):
                break

            total_pages = response.get("totalPages")
            current_page = response.get("number")
            if (
                isinstance(total_pages, int)
                and isinstance(current_page, int)
                and current_page + 1 >= total_pages
            ):
                break

            page += 1

        return items

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
