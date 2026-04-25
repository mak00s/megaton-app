"""Small BigQuery helpers shared by analysis jobs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


def fq_table(project_id: str, dataset_id: str, table_id: str) -> str:
    """Build a fully qualified BigQuery table id."""
    return f"{project_id}.{dataset_id}.{table_id}"


def resolve_table_id(table_id: str, project: str) -> str:
    """Resolve ``dataset.table`` or ``project.dataset.table`` to a full id."""
    if table_id.count(".") == 2:
        return table_id
    if table_id.count(".") == 1:
        return f"{project}.{table_id}"
    raise ValueError(f"Invalid BQ table id: {table_id}")


def build_scalar_query_parameters(
    params: Mapping[str, Any] | None,
    *,
    default_type: str = "STRING",
) -> list[Any] | None:
    """Build BigQuery scalar query parameters from a simple mapping.

    Values may be either raw values or ``(type, value)`` tuples.
    """
    if not params:
        return None
    from google.cloud import bigquery

    query_parameters = []
    for name, raw in params.items():
        if isinstance(raw, tuple) and len(raw) == 2:
            param_type, value = raw
        else:
            param_type, value = default_type, raw
        query_parameters.append(
            bigquery.ScalarQueryParameter(str(name), str(param_type), value)
        )
    return query_parameters


def build_query_job_config(
    params: Mapping[str, Any] | Sequence[Any] | None = None,
):
    """Build a QueryJobConfig for named scalar params or prebuilt params."""
    if not params:
        return None
    from google.cloud import bigquery

    if isinstance(params, Mapping):
        query_parameters = build_scalar_query_parameters(params)
    else:
        query_parameters = list(params)
    return bigquery.QueryJobConfig(query_parameters=query_parameters)


def query_df(
    client: Any,
    sql: str,
    *,
    params: Mapping[str, Any] | Sequence[Any] | None = None,
) -> pd.DataFrame:
    """Run a query and return a DataFrame."""
    job_config = build_query_job_config(params)
    return client.query(sql, job_config=job_config).to_dataframe()


def count_rows(
    client: Any,
    table_id: str,
    *,
    where_sql: str | None = None,
    params: Mapping[str, Any] | Sequence[Any] | None = None,
) -> int:
    """Count rows in a table, optionally with a trusted WHERE expression.

    ``where_sql`` is inserted as SQL text. Keep user values in BigQuery bind
    parameters via ``params`` rather than string concatenation.
    """
    where_clause = f"\nWHERE {where_sql}" if where_sql else ""
    sql = f"SELECT COUNT(*) AS c FROM `{table_id}`{where_clause}"
    job_config = build_query_job_config(params)
    rows = list(client.query(sql, job_config=job_config).result())
    return int(rows[0].c) if rows else 0


def load_dataframe(
    client: Any,
    df: pd.DataFrame,
    table_id: str,
    *,
    schema: Sequence[Any] | None = None,
    write_disposition: str = "WRITE_APPEND",
    create_disposition: str = "CREATE_IF_NEEDED",
    dry_run: bool = False,
) -> int:
    """Load a DataFrame into BigQuery and return the row count."""
    if dry_run:
        print(f"would_load_dataframe={table_id} rows={len(df)}")
        return int(len(df))
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        schema=list(schema) if schema is not None else None,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    return int(len(df))


def append_dataframe(
    client: Any,
    df: pd.DataFrame,
    table_id: str,
    *,
    schema: Sequence[Any] | None = None,
    dry_run: bool = False,
) -> int:
    """Append a DataFrame to BigQuery."""
    return load_dataframe(
        client,
        df,
        table_id,
        schema=schema,
        write_disposition="WRITE_APPEND",
        dry_run=dry_run,
    )


def replace_dataframe(
    client: Any,
    df: pd.DataFrame,
    table_id: str,
    *,
    schema: Sequence[Any] | None = None,
    dry_run: bool = False,
) -> int:
    """Replace a BigQuery table with a DataFrame."""
    return load_dataframe(
        client,
        df,
        table_id,
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        dry_run=dry_run,
    )


def load_csv_file(
    client: Any,
    csv_path: str | Path,
    table_id: str,
    *,
    schema: Sequence[Any] | None = None,
    skip_leading_rows: int = 1,
    write_disposition: str = "WRITE_APPEND",
    create_disposition: str = "CREATE_IF_NEEDED",
    dry_run: bool = False,
) -> int:
    """Load a CSV file into BigQuery and return the number of data rows."""
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig") as handle:
        expected_rows = sum(1 for _ in handle) - skip_leading_rows
    expected_rows = max(expected_rows, 0)
    if dry_run:
        print(f"would_load_csv={table_id} rows={expected_rows}")
        return expected_rows
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=skip_leading_rows,
        schema=list(schema) if schema is not None else None,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
    )
    with path.open("rb") as handle:
        client.load_table_from_file(handle, table_id, job_config=job_config).result()
    return expected_rows


def ensure_dataset(
    client: Any,
    dataset_id: str,
    *,
    project_id: str | None = None,
    location: str | None = None,
    dry_run: bool = False,
) -> str:
    """Create a dataset if missing and return its fully qualified id."""
    from google.cloud import bigquery

    project = project_id or client.project
    qualified = f"{project}.{dataset_id}"
    if dry_run:
        print(f"would_ensure_dataset={qualified} location={location or ''}".rstrip())
        return qualified
    dataset = bigquery.Dataset(qualified)
    if location:
        dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    return qualified


def build_schema(
    fields: Sequence[Any],
) -> list[Any]:
    """Build BigQuery SchemaField objects from tuples, mappings, or fields."""
    from google.cloud import bigquery

    schema = []
    for field in fields:
        if isinstance(field, bigquery.SchemaField):
            schema.append(field)
        elif isinstance(field, Mapping):
            schema.append(
                bigquery.SchemaField(
                    str(field["name"]),
                    str(field["type"]),
                    mode=str(field.get("mode", "NULLABLE")),
                    description=field.get("description"),
                )
            )
        else:
            name, field_type, *rest = field
            mode = rest[0] if rest else "NULLABLE"
            schema.append(bigquery.SchemaField(str(name), str(field_type), mode=str(mode)))
    return schema


def ensure_table(
    client: Any,
    table_id: str,
    *,
    schema: Sequence[Any] | None = None,
    dry_run: bool = False,
    allow_field_additions: bool = True,
) -> dict[str, Any]:
    """Create a table if missing and optionally append missing schema fields.

    This uses a ``get_table`` then ``create_table`` sequence and is not safe for
    concurrent creation of the same table by multiple processes.
    """
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    resolved_schema = build_schema(schema or [])
    if dry_run:
        return {
            "table": table_id,
            "created": False,
            "updated_schema": False,
            "added_fields": [field.name for field in resolved_schema],
            "dry_run": True,
        }

    try:
        existing = client.get_table(table_id)
        created = False
    except NotFound:
        table = bigquery.Table(table_id, schema=resolved_schema)
        client.create_table(table, exists_ok=False)
        existing = table
        created = True

    result = {
        "table": table_id,
        "created": created,
        "updated_schema": False,
        "added_fields": [],
    }
    if created or not resolved_schema or not allow_field_additions:
        return result

    existing_names = {field.name for field in existing.schema}
    missing = [field for field in resolved_schema if field.name not in existing_names]
    if missing:
        existing.schema = list(existing.schema) + missing
        client.update_table(existing, ["schema"])
        result["updated_schema"] = True
        result["added_fields"] = [field.name for field in missing]
    return result


def _table_spec_value(spec: Any, key: str, default: Any = None) -> Any:
    if isinstance(spec, Mapping):
        return spec.get(key, default)
    return getattr(spec, key, default)


def ensure_tables(
    client: Any,
    table_specs: Sequence[Any],
    *,
    project_id: str | None = None,
    dataset_id: str | None = None,
    dry_run: bool = False,
    allow_field_additions: bool = True,
) -> list[dict[str, Any]]:
    """Ensure multiple tables from specs with ``table_id`` and ``schema``."""
    results = []
    for spec in table_specs:
        raw_table_id = str(_table_spec_value(spec, "table_id"))
        if raw_table_id.count(".") == 2:
            table_id = raw_table_id
        elif dataset_id:
            table_id = fq_table(project_id or client.project, dataset_id, raw_table_id)
        else:
            table_id = resolve_table_id(raw_table_id, project_id or client.project)
        results.append(
            ensure_table(
                client,
                table_id,
                schema=_table_spec_value(spec, "schema", []),
                dry_run=dry_run,
                allow_field_additions=allow_field_additions,
            )
        )
    return results


def bootstrap_dataset_tables(
    client: Any,
    *,
    dataset_id: str,
    table_specs: Sequence[Any],
    project_id: str | None = None,
    location: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ensure a dataset and all tables, returning operation metadata."""
    dataset = ensure_dataset(
        client,
        dataset_id,
        project_id=project_id,
        location=location,
        dry_run=dry_run,
    )
    tables = ensure_tables(
        client,
        table_specs,
        project_id=project_id,
        dataset_id=dataset_id,
        dry_run=dry_run,
    )
    return {"dataset": dataset, "tables": tables, "dry_run": dry_run}


def append_run_log(
    client: Any,
    table_id: str,
    *,
    run_id: str,
    job_name: str,
    started_at: Any,
    finished_at: Any,
    status: str,
    details: Mapping[str, Any] | None = None,
    schema: Sequence[Any] | None = None,
    dry_run: bool = False,
) -> int:
    """Append a standard run_log row."""
    default_schema = [
        ("run_id", "STRING"),
        ("job_name", "STRING"),
        ("started_at", "TIMESTAMP"),
        ("finished_at", "TIMESTAMP"),
        ("status", "STRING"),
        ("details_json", "STRING"),
    ]
    row = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "job_name": job_name,
                "started_at": pd.Timestamp(started_at),
                "finished_at": pd.Timestamp(finished_at),
                "status": status,
                "details_json": json.dumps(
                    dict(details or {}),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        ]
    )
    return append_dataframe(
        client,
        row,
        table_id,
        schema=build_schema(schema or default_schema),
        dry_run=dry_run,
    )
