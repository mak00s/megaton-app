"""Shared megaton client used by both Streamlit and CLI.

Automatically discovers multiple service-account JSON files and routes to the
correct credential based on property_id / site_url.
"""
import logging
import os
from collections.abc import Mapping, Sequence
from megaton import start
import pandas as pd
from typing import Optional, TypedDict, TypeAlias
from megaton_lib.credentials import list_service_account_paths

logger = logging.getLogger(__name__)

# === Public type hints (API boundary) ===

FieldSpec: TypeAlias = str | tuple[str, str]


class GscDimensionFilter(TypedDict):
    dimension: str
    operator: str
    expression: str


class AuthContext(TypedDict):
    megaton_env_var: str
    megaton_env_value: str | None
    megaton_candidate_paths: list[str]
    google_application_credentials: str | None
    bq_creds_hint: str
    resolved_bq_creds_path: str | None
    resolved_bq_source: str | None

# === Registry (multi-credential management) ===

_instances: dict[str, object] = {}     # creds_path → Megaton instance
_property_map: dict[str, str] = {}     # property_id → creds_path
_site_map: dict[str, str] = {}         # site_url → creds_path
_registry_built = False


def _normalize_key(value: object) -> str:
    """Normalize a key for map lookups."""
    return str(value).strip()


def _normalize_fields(
    fields: Sequence[FieldSpec],
    *,
    name: str,
) -> list[FieldSpec]:
    """Validate and normalize field list into Megaton-compatible form."""
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
        raise TypeError(f"{name} must be a sequence of field specs.")
    normalized: list[FieldSpec] = []
    for item in fields:
        if isinstance(item, str):
            value = item.strip()
            if not value:
                raise ValueError(f"{name} contains an empty field name.")
            normalized.append(value)
            continue
        if (
            isinstance(item, tuple)
            and len(item) == 2
            and all(isinstance(v, str) and v.strip() for v in item)
        ):
            normalized.append((item[0].strip(), item[1].strip()))
            continue
        raise TypeError(
            f"{name} items must be str or tuple[str, str]. got={item!r}"
        )
    return normalized


def _normalize_gsc_dimension_filter(
    filters: Sequence[GscDimensionFilter] | None,
) -> list[GscDimensionFilter] | None:
    """Boundary validation for GSC dimension_filter."""
    if filters is None:
        return None
    if not isinstance(filters, Sequence) or isinstance(filters, (str, bytes)):
        raise TypeError("dimension_filter must be a sequence of filter dicts.")
    normalized: list[GscDimensionFilter] = []
    required = {"dimension", "operator", "expression"}
    for item in filters:
        if not isinstance(item, Mapping):
            raise TypeError(f"dimension_filter item must be mapping. got={item!r}")
        missing = required - set(item.keys())
        if missing:
            raise ValueError(f"dimension_filter item missing keys: {sorted(missing)}")
        normalized.append(
            GscDimensionFilter(
                dimension=str(item["dimension"]).strip(),
                operator=str(item["operator"]).strip(),
                expression=str(item["expression"]).strip(),
            )
        )
    return normalized


def _normalize_bq_params(
    params: Mapping[str, object] | None,
) -> dict[str, str | None] | None:
    """Normalize BQ query params to a string dict."""
    if params is None:
        return None
    if not isinstance(params, Mapping):
        raise TypeError("params must be a mapping of parameter name to value.")
    normalized: dict[str, str | None] = {}
    for key, value in params.items():
        k = str(key).strip()
        if not k:
            raise ValueError("params contains an empty parameter name.")
        normalized[k] = None if value is None else str(value)
    return normalized


def reset_registry() -> None:
    """Reset registry (used after env var changes or notebook re-runs)."""
    global _registry_built
    _property_map.clear()
    _site_map.clear()
    _instances.clear()
    _registry_built = False


def get_megaton(creds_path: str | None = None):
    """Get Megaton instance (singleton per creds_path).

    When creds_path=None, uses the first discovered credential file.
    """
    if creds_path is None:
        paths = list_service_account_paths()
        if not paths:
            raise FileNotFoundError(
                "No service account JSON found. "
                "Place a JSON file in credentials/ or set MEGATON_CREDS_PATH."
            )
        creds_path = paths[0]
    if creds_path not in _instances:
        _instances[creds_path] = start.Megaton(creds_path, headless=True)
    return _instances[creds_path]


def build_registry() -> None:
    """Build GA4 property/GSC site mappings across all credentials (once)."""
    global _registry_built
    if _registry_built:
        return

    paths = list_service_account_paths()

    for path in paths:
        mg = get_megaton(path)
        # GA4
        try:
            for acc in mg.ga["4"].accounts:
                for prop in acc.get("properties", []):
                    _property_map[_normalize_key(prop["id"])] = path
        except Exception as e:
            logger.debug("Skipping GA4 for %s: %s", path, e)
        # GSC
        try:
            sites = mg.search.get.sites()
            for site in sites:
                _site_map[_normalize_key(site)] = path
        except Exception as e:
            logger.debug("Skipping GSC for %s: %s", path, e)

    _registry_built = True


def get_megaton_for_property(property_id: str):
    """Return Megaton instance for the specified GA4 property."""
    key = _normalize_key(property_id)
    build_registry()
    creds_path = _property_map.get(key)
    if creds_path is None:
        # Rebuild once in case of stale cache (e.g., long notebook sessions).
        _property_map.clear()
        _site_map.clear()
        global _registry_built
        _registry_built = False
        build_registry()
        creds_path = _property_map.get(key)
    if creds_path is None:
        creds_dir = os.environ.get("MEGATON_CREDS_PATH", "(not set)")
        found_paths = list_service_account_paths()
        known_ids = sorted(_property_map.keys()) or ["(none)"]
        raise ValueError(
            f"No credential found for property_id: {property_id}\n"
            f"  MEGATON_CREDS_PATH: {creds_dir}\n"
            f"  Credential files found: {found_paths}\n"
            f"  Known property IDs: {known_ids}"
        )
    return get_megaton(creds_path)


def get_megaton_for_site(site_url: str):
    """Return Megaton instance for the specified GSC site."""
    key = _normalize_key(site_url)
    build_registry()
    creds_path = _site_map.get(key)
    if creds_path is None:
        _property_map.clear()
        _site_map.clear()
        global _registry_built
        _registry_built = False
        build_registry()
        creds_path = _site_map.get(key)
    if creds_path is None:
        raise ValueError(f"No credential found for site_url: {site_url}")
    return get_megaton(creds_path)


# === Initialized instance helpers (for notebooks) ===


def get_ga4(property_id: str):
    """Return an initialized Megaton instance for GA4.

    Credential is auto-selected and account/property are pre-selected.
    The ``mg.report.run()`` result supports method chaining.

    Usage::

        mg = get_ga4("254800682")
        mg.report.set.dates("2025-06-01", "2026-01-31")
        result = mg.report.run(d=["date", "landingPage"], m=["sessions"], show=False)
        result.clean_url("landingPage").group("date").sort("date")
        df = result.df
    """
    property_id = _normalize_key(property_id)
    mg = get_megaton_for_property(property_id)
    for acc in mg.ga["4"].accounts:
        for prop in acc.get("properties", []):
            if _normalize_key(prop["id"]) == property_id:
                mg.ga["4"].account.select(acc["id"])
                mg.ga["4"].property.select(property_id)
                return mg
    raise ValueError(
        f"Property {property_id} found in registry but not in accounts"
    )


def get_gsc(site_url: str):
    """Return an initialized Megaton instance for GSC.

    Credential is auto-selected and site is pre-selected.
    The ``mg.search.run()`` result supports method chaining.

    Usage::

        mg = get_gsc("https://example.com")
        mg.search.set.dates("2025-06-01", "2026-01-31")
        result = mg.search.run(dimensions=["query", "page"], limit=25000)
        result.decode().clean_url().filter_impressions(min=10)
        df = result.df
    """
    site_url = _normalize_key(site_url)
    mg = get_megaton_for_site(site_url)
    mg.search.use(site_url)
    return mg


# === GA4 ===

def get_ga4_properties() -> list:
    """Return merged GA4 properties from all credentials."""
    build_registry()
    result = []
    seen_ids: set[str] = set()
    for path in dict.fromkeys(_property_map.values()):
        mg = get_megaton(path)
        for acc in mg.ga["4"].accounts:
            for prop in acc.get("properties", []):
                if prop["id"] not in seen_ids:
                    seen_ids.add(prop["id"])
                    result.append({
                        "id": prop["id"],
                        "name": prop["name"],
                        "account_id": acc["id"],
                        "display": f"{prop['name']} ({prop['id']})"
                    })
    return result


def query_ga4(
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: Sequence[FieldSpec],
    metrics: Sequence[FieldSpec],
    filter_d: Optional[str] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    """Execute GA4 query (auto-routed).

    Args:
        property_id: GA4 property ID.
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
        dimensions: Dimensions as str or tuple(API field, alias).
            Example: ["date", ("sessionDefaultChannelGroup", "channel")]
        metrics: Metrics as str or tuple(API field, alias).
            Example: ["sessions", ("eventCount", "cv")]
        filter_d: Filter expression in "field==value;field!=value" form.
        limit: Max number of rows.
    """
    dimensions_l = _normalize_fields(dimensions, name="dimensions")
    metrics_l = _normalize_fields(metrics, name="metrics")
    mg = get_ga4(property_id)
    mg.report.set.dates(start_date, end_date)
    result = mg.report.run(
        d=dimensions_l,
        m=metrics_l,
        filter_d=filter_d if filter_d else None,
        limit=limit,
        show=False,
    )
    return result.df if result is not None else pd.DataFrame()


# === GSC ===

def get_gsc_sites() -> list:
    """Return merged GSC sites from all credentials."""
    build_registry()
    seen: set[str] = set()
    result = []
    for path in dict.fromkeys(_site_map.values()):
        mg = get_megaton(path)
        for site in mg.search.get.sites():
            if site not in seen:
                seen.add(site)
                result.append(site)
    return result


def query_gsc(
    site_url: str,
    start_date: str,
    end_date: str,
    dimensions: Sequence[FieldSpec],
    limit: int = 25000,
    dimension_filter: Sequence[GscDimensionFilter] | None = None,
    page_to_path: bool = True,
) -> pd.DataFrame:
    """Execute GSC query (auto-routed).

    Args:
        site_url: Search Console site URL.
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
        dimensions: Dimensions as str or tuple(API field, alias).
            Example: ["query", ("page", "url")]
        limit: Max number of rows.
        dimension_filter: List of filter objects.
            Example: [{"dimension": "query", "operator": "contains", "expression": "seo"}]
            Operators: contains, notContains, equals, notEquals, includingRegex, excludingRegex
        page_to_path: Convert page column to path-only (default: True).
            Full URL "https://example.com/path?q=1" -> "/path"
            Conversion happens after filter evaluation on full URL.
    """
    dimensions_l = _normalize_fields(dimensions, name="dimensions")
    dimension_filter_l = _normalize_gsc_dimension_filter(dimension_filter)
    mg = get_gsc(site_url)
    mg.search.set.dates(start_date, end_date)
    mg.search.run(
        dimensions=dimensions_l,
        metrics=["clicks", "impressions", "ctr", "position"],
        limit=limit,
        dimension_filter=dimension_filter_l
    )
    df = mg.search.data
    if page_to_path and df is not None and "page" in df.columns:
        from urllib.parse import urlparse
        df["page"] = df["page"].apply(lambda u: urlparse(u).path)
    return df


# === BigQuery ===

_bq_clients = {}
_bq_native_clients = {}


def get_bigquery(project_id: str):
    """Get BigQuery client via Megaton (legacy path)."""
    if project_id not in _bq_clients:
        mg = get_megaton()
        _bq_clients[project_id] = mg.launch_bigquery(project_id)
    return _bq_clients[project_id]


def _select_credential_path(*, creds_hint: str = "") -> str | None:
    """Pick one credential path, preferring filename match by hint."""
    paths = list_service_account_paths()
    if not paths:
        return None
    hint = (creds_hint or "").lower().strip()
    if hint:
        match = [p for p in paths if hint in os.path.basename(p).lower()]
        if match:
            return match[0]
    return paths[0]


def resolve_bq_creds_path(*, creds_hint: str = "corp") -> str | None:
    """Resolve credential JSON path for native BigQuery client.

    Resolution order:
    1) Use ``GOOGLE_APPLICATION_CREDENTIALS`` if set.
    2) Otherwise select from ``MEGATON_CREDS_PATH`` / ``credentials/*.json``:
       - Prefer filenames containing ``creds_hint``
       - Fallback to first file

    Returns:
        Resolved path, or ``None`` if no candidate exists.
    """
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if gac:
        return gac
    return _select_credential_path(creds_hint=creds_hint)


def ensure_bq_credentials(*, creds_hint: str = "corp") -> str | None:
    """Set ``GOOGLE_APPLICATION_CREDENTIALS`` if needed and return path."""
    resolved = resolve_bq_creds_path(creds_hint=creds_hint)
    if resolved and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = resolved
    return resolved


def describe_auth_context(*, creds_hint: str = "corp") -> AuthContext:
    """Return current auth-resolution context (for debug/ops checks)."""
    megaton_paths = list_service_account_paths()
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    resolved_bq = resolve_bq_creds_path(creds_hint=creds_hint)
    return {
        "megaton_env_var": "MEGATON_CREDS_PATH",
        "megaton_env_value": os.environ.get("MEGATON_CREDS_PATH", "").strip() or None,
        "megaton_candidate_paths": megaton_paths,
        "google_application_credentials": gac or None,
        "bq_creds_hint": creds_hint,
        "resolved_bq_creds_path": resolved_bq,
        "resolved_bq_source": (
            "GOOGLE_APPLICATION_CREDENTIALS" if gac
            else ("MEGATON_CREDS_PATH_or_credentials" if resolved_bq else None)
        ),
    }


def get_bq_client(project_id: str, *, creds_hint: str = "corp"):
    """Get ``google.cloud.bigquery.Client`` directly.

    Lightweight client that bypasses Megaton's BQ wrapper and supports full
    native APIs such as parameterized queries.

    Args:
        project_id: GCP project ID.
        creds_hint: Keyword matched against filenames under credentials/.
            Used when multiple credential files exist.

    Returns:
        google.cloud.bigquery.Client
    """
    # NOTE: cache key currently uses only project_id.
    # Different creds_hint values for same project are not expected.
    # If needed, switch key to (project_id, resolved_path).
    if project_id not in _bq_native_clients:
        from google.cloud import bigquery

        ensure_bq_credentials(creds_hint=creds_hint)

        _bq_native_clients[project_id] = bigquery.Client(project=project_id)
    return _bq_native_clients[project_id]


def get_bq_datasets(project_id: str) -> list:
    """Get list of BigQuery datasets."""
    bq = get_bigquery(project_id)
    return bq.datasets


def query_bq(
    project_id: str,
    sql: str,
    params: Mapping[str, object] | None = None,
    *,
    location: Optional[str] = None,
) -> pd.DataFrame:
    """Run a BigQuery query and return a DataFrame.

    Without params, uses Megaton BQ wrapper for backward compatibility.
    With params, uses ``google.cloud.bigquery.Client`` directly.

    Args:
        project_id: GCP project ID.
        sql: SQL query string. Parameters are referenced as ``@name``.
        params: Query params in ``{"name": "value"}`` form.
            Currently treated as STRING type.
        location: BQ job location. If None, client default is used.

    Returns:
        Query result DataFrame.
    """
    normalized_params = _normalize_bq_params(params)
    if normalized_params is None:
        bq = get_bigquery(project_id)
        return bq.run(sql, to_dataframe=True)

    from google.cloud import bigquery

    client = get_bq_client(project_id)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING", v)
            for k, v in normalized_params.items()
        ]
    )
    kwargs: dict = {"job_config": job_config}
    if location is not None:
        kwargs["location"] = location
    return client.query(sql, **kwargs).to_dataframe()


def save_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    df: pd.DataFrame,
    mode: str = "overwrite",
) -> dict:
    """Save data to BigQuery table.

    Args:
        project_id: GCP project ID.
        dataset_id: Dataset ID.
        table_id: Table ID.
        df: DataFrame to save.
        mode: "overwrite" or "append"

    Returns:
        dict with table reference and row count
    """
    from google.cloud import bigquery as bq_lib

    bq = get_bigquery(project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    disposition_map = {
        "overwrite": bq_lib.WriteDisposition.WRITE_TRUNCATE,
        "append": bq_lib.WriteDisposition.WRITE_APPEND,
    }
    if mode not in disposition_map:
        raise ValueError(f"Unsupported mode for BigQuery: {mode}")

    job_config = bq_lib.LoadJobConfig(
        write_disposition=disposition_map[mode],
        autodetect=True,
    )

    job = bq.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for completion.

    table = bq.client.get_table(table_ref)
    return {
        "table": table_ref,
        "row_count": table.num_rows,
    }


# === Google Sheets ===

def save_to_sheet(
    sheet_url: str,
    sheet_name: str,
    df: pd.DataFrame,
    mode: str = "overwrite",
    keys: Optional[list] = None
):
    """Save data to Google Sheets.

    Args:
        sheet_url: Spreadsheet URL.
        sheet_name: Sheet name.
        df: DataFrame to save.
        mode: "overwrite", "append", "upsert"
        keys: Key columns for upsert mode.
    """
    mg = get_megaton()
    mg.open.sheet(sheet_url)

    if mode == "overwrite":
        mg.save.to.sheet(sheet_name, df, freeze_header=True)
    elif mode == "append":
        mg.append.to.sheet(sheet_name, df)
    elif mode == "upsert":
        if not keys:
            raise ValueError("keys is required for upsert mode")
        mg.upsert.to.sheet(sheet_name, df, keys=keys)
