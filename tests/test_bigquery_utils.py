from __future__ import annotations

from pathlib import Path

from google.api_core.exceptions import NotFound

from megaton_lib import bigquery_utils


class _CountRowsJob:
    def result(self):
        return [type("Row", (), {"c": 3})()]


class _DummyClient:
    project = "proj"

    def __init__(self):
        self.queries = []
        self.loaded = []
        self.loaded_dfs = []
        self.created_datasets = []
        self.created_tables = []
        self.updated_tables = []
        self.existing_schema = []
        self.table_exists = True

    def query(self, sql, job_config=None):
        self.queries.append((sql, job_config))
        return _CountRowsJob()

    def load_table_from_file(self, handle, table_id, job_config=None):
        self.loaded.append((handle.read(), table_id, job_config))
        return type("LoadJob", (), {"result": lambda self: None})()

    def load_table_from_dataframe(self, df, table_id, job_config=None):
        self.loaded_dfs.append((df.copy(), table_id, job_config))
        return type("LoadJob", (), {"result": lambda self: None})()

    def create_dataset(self, dataset, exists_ok=False):
        self.created_datasets.append((dataset, exists_ok))
        return dataset

    def create_table(self, table, exists_ok=False):
        self.created_tables.append((table, exists_ok))
        return table

    def get_table(self, table_id):
        if not self.table_exists:
            raise NotFound("not found")
        return type("Table", (), {"schema": list(self.existing_schema)})()

    def update_table(self, table, fields):
        self.updated_tables.append((table, fields))
        return table


def test_resolve_table_id():
    assert bigquery_utils.resolve_table_id("dataset.table", "proj") == "proj.dataset.table"
    assert bigquery_utils.resolve_table_id("other.dataset.table", "proj") == "other.dataset.table"


def test_count_rows_builds_where_clause():
    client = _DummyClient()

    count = bigquery_utils.count_rows(
        client,
        "proj.dataset.table",
        where_sql="period = @period",
        params={"period": "202604"},
    )

    assert count == 3
    assert "WHERE period = @period" in client.queries[0][0]
    assert client.queries[0][1].query_parameters[0].name == "period"


def test_load_csv_file_returns_data_rows(tmp_path: Path):
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    client = _DummyClient()

    loaded = bigquery_utils.load_csv_file(client, csv_path, "proj.dataset.table")

    assert loaded == 2
    assert client.loaded[0][1] == "proj.dataset.table"


def test_load_csv_file_dry_run_counts_rows_without_loading(tmp_path: Path):
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    client = _DummyClient()

    loaded = bigquery_utils.load_csv_file(
        client,
        csv_path,
        "proj.dataset.table",
        dry_run=True,
    )

    assert loaded == 2
    assert client.loaded == []


def test_ensure_dataset_and_tables_add_missing_schema_fields():
    client = _DummyClient()
    client.existing_schema = bigquery_utils.build_schema([("id", "STRING")])

    result = bigquery_utils.bootstrap_dataset_tables(
        client,
        dataset_id="dataset",
        table_specs=[
            {
                "table_id": "events",
                "schema": [("id", "STRING"), ("value", "INTEGER")],
            }
        ],
        location="asia-northeast1",
    )

    assert result["dataset"] == "proj.dataset"
    assert client.created_datasets[0][0].location == "asia-northeast1"
    assert result["tables"][0]["created"] is False
    assert client.created_tables == []
    assert result["tables"][0]["added_fields"] == ["value"]
    assert client.updated_tables[0][1] == ["schema"]


def test_ensure_table_created_reflects_new_table_only():
    client = _DummyClient()
    client.table_exists = False

    result = bigquery_utils.ensure_table(
        client,
        "proj.dataset.events",
        schema=[("id", "STRING")],
    )

    assert result["created"] is True
    assert result["updated_schema"] is False
    created_table = client.created_tables[0][0]
    assert (created_table.project, created_table.dataset_id, created_table.table_id) == (
        "proj",
        "dataset",
        "events",
    )


def test_bootstrap_dataset_tables_dry_run():
    client = _DummyClient()

    result = bigquery_utils.bootstrap_dataset_tables(
        client,
        dataset_id="dataset",
        table_specs=[{"table_id": "events", "schema": [("id", "STRING")]}],
        location="asia-northeast1",
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["dataset"] == "proj.dataset"
    assert result["tables"][0]["dry_run"] is True
    assert result["tables"][0]["created"] is False
    assert client.created_datasets == []
    assert client.created_tables == []


def test_replace_append_and_run_log_load_dataframes():
    client = _DummyClient()
    df = bigquery_utils.pd.DataFrame({"id": ["1"]})

    replaced = bigquery_utils.replace_dataframe(
        client,
        df,
        "proj.dataset.events",
        schema=bigquery_utils.build_schema([("id", "STRING")]),
    )
    appended = bigquery_utils.append_dataframe(client, df, "proj.dataset.events")
    logged = bigquery_utils.append_run_log(
        client,
        "proj.dataset.run_log",
        run_id="run-1",
        job_name="job",
        started_at="2026-04-25T00:00:00Z",
        finished_at="2026-04-25T00:01:00Z",
        status="success",
        details={"rows": 1},
    )

    assert (replaced, appended, logged) == (1, 1, 1)
    assert client.loaded_dfs[0][2].write_disposition == "WRITE_TRUNCATE"
    assert client.loaded_dfs[1][2].write_disposition == "WRITE_APPEND"
    run_log_df = client.loaded_dfs[2][0]
    assert run_log_df.loc[0, "details_json"] == '{"rows": 1}'
