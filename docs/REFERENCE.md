# Technical Reference

For setup and how-to, see [USAGE.md](USAGE.md).

---

## CLI (`scripts/query.py`)

### Options

| Option | Description | Default |
|--------|------------|---------|
| `--params` | Schema-validated JSON input | `input/params.json` |
| `--submit` | Submit as async job | OFF |
| `--status <job_id>` | Show job status | - |
| `--cancel <job_id>` | Cancel queued/running job | - |
| `--result <job_id>` | Show job result | - |
| `--head <N>` | First N rows (with `--result`) | - |
| `--summary` | Summary stats (with `--result`) | OFF |
| `--transform` | Column transform (`col:func`) | - |
| `--where` | Row filter (pandas query) | - |
| `--sort` | Sort (`col DESC,col2 ASC`) | - |
| `--columns` | Column selection (comma-separated) | - |
| `--group-by` | Group columns (comma-separated) | - |
| `--aggregate` | Aggregation (`sum:clicks,mean:ctr`) | - |
| `--batch <dir>` | Batch execute all JSON in directory | - |
| `--list-jobs` | List jobs | OFF |
| `--job-limit` | Max jobs to list | 20 |
| `--list-ga4-properties` | List GA4 properties | OFF |
| `--list-gsc-sites` | List GSC sites | OFF |
| `--list-bq-datasets` | List BQ datasets | OFF |
| `--project` | GCP project for dataset listing | - |
| `--json` | JSON output | table |
| `--output` | Save to CSV file | - |

**Constraints:**
- `--params`: validates `schema_version: "1.0"` and source-key consistency
- `--params` sync execution: pipeline must be in params.json (CLI args not allowed)
- `--head` and `--summary`: require `--result`
- `--group-by` and `--aggregate`: must be used together
- `--summary`: exclusive with pipeline options

### Result Pipeline

Transforms query results (from `--result` or `--params` sync execution).

**Processing order (fixed):**
`read CSV → transform → where → group-by+aggregate → sort → columns → head → output`

#### Transform Functions (`--transform`)

| Function | Syntax | Description |
|----------|--------|-------------|
| `date_format` | `date:date_format` | YYYYMMDD → YYYY-MM-DD |
| `url_decode` | `page:url_decode` | Decode %xx |
| `path_only` | `page:path_only` | Extract path (remove domain) |
| `strip_qs` | `page:strip_qs` | Remove all query params |
| `strip_qs` | `page:strip_qs:id,ref` | Keep only specified params |

**strip_qs args:** Without args → remove all. With args → keep listed params only.

**Comma disambiguation:** In `page:strip_qs:id,ref`, `ref` is a strip_qs argument (not a new transform). Segments without colons are appended to the preceding transform.

#### Aggregate Functions

`sum`, `mean`, `count`, `min`, `max`, `median`

#### Pipeline Error Codes

| Code | Condition |
|------|-----------|
| `INVALID_TRANSFORM` | Invalid transform function/column/expression |
| `INVALID_WHERE` | Invalid where expression |
| `INVALID_SORT` | Invalid sort syntax or column |
| `INVALID_COLUMNS` | Non-existent column |
| `INVALID_AGGREGATE` | Invalid aggregate function/column |

### `--json` Response Format

Success:

```json
{
  "status": "ok",
  "mode": "query",
  "data": {}
}
```

- With `pipeline` in params.json: `data.pipeline` includes `input_rows` / `output_rows`
- `--result` jobs: use CLI args (`--where` / `--sort` etc.)

Failure:

```json
{
  "status": "error",
  "error_code": "PARAMS_VALIDATION_FAILED",
  "message": "Params validation failed.",
  "hint": "Fix params based on details[].",
  "details": {}
}
```

### Job Management

#### Job States

| Status | Description |
|--------|-------------|
| `queued` | Submitted to queue |
| `running` | In progress |
| `canceled` | Canceled |
| `succeeded` | Done (result CSV in `artifact_path`) |
| `failed` | Failed (details in `error`) |

#### Storage

| Path | Contents |
|------|----------|
| `output/jobs/records/<job_id>.json` | Job metadata |
| `output/jobs/logs/<job_id>.log` | Execution log |
| `output/jobs/artifacts/<job_id>.csv` | Result CSV |

### Batch Execution

`--batch <dir>` runs all JSON files in the directory in filename order.
Each config runs independently; failures don't stop the rest.

```bash
python scripts/query.py --batch configs/weekly/ --json
```

```json
{
  "status": "ok",
  "total": 3, "succeeded": 2, "failed": 1, "skipped": 0,
  "results": [
    {"config": "01_gsc.json", "status": "ok", "row_count": 500},
    {"config": "02_ga4.json", "status": "ok", "row_count": 120},
    {"config": "03_bq.json", "status": "error", "error": "..."}
  ],
  "elapsed_sec": 12.34
}
```

---

## JSON Parameter Schema

- Schema file: `schemas/query-params.schema.json`
- `schema_version` required (currently `"1.0"`)
- Only keys allowed for the specified `source` (`additionalProperties: false`)
- Shared by Streamlit UI and CLI

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | ✓ | `"1.0"` |
| `source` | string | ✓ | `"ga4"`, `"gsc"`, `"bigquery"` |
| `property_id` | string | GA4 | GA4 property ID |
| `site_url` | string | GSC | Search Console site URL |
| `project_id` | string | BQ | GCP project ID |
| `sql` | string | BQ | SQL to execute |
| `date_range.start` | string | GA4/GSC | Start date (YYYY-MM-DD or template) |
| `date_range.end` | string | GA4/GSC | End date (YYYY-MM-DD or template) |
| `dimensions` | array | - | Dimension list |
| `metrics` | array | GA4 | Metric list |
| `filter_d` | string | - | GA4 filter (`field==value` format) |
| `filter` | string | - | GSC filter (`dim:op:expr` format) |
| `limit` | number | - | Row limit (max 100,000) |
| `pipeline` | object | - | Post-fetch pipeline (see below) |
| `save` | object | - | Save destination (see below) |

### Examples

**GA4:**
```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {"start": "2026-01-28", "end": "2026-02-03"},
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"],
  "filter_d": "sessionDefaultChannelGroup==Organic Search",
  "limit": 1000
}
```

**GSC:**
```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
  "dimensions": ["query"],
  "filter": "query:contains:keyword",
  "limit": 1000
}
```

**BigQuery:**
```json
{
  "schema_version": "1.0",
  "source": "bigquery",
  "project_id": "my-gcp-project",
  "sql": "SELECT event_date, COUNT(*) as cnt FROM `project.dataset.events_*` GROUP BY 1"
}
```

### Filter Syntax

**GA4** (`filter_d`): `field==value` format, semicolon-separated for multiple

```
"filter_d": "sessionDefaultChannelGroup==Organic Search;country==Japan"
```

**GSC** (`filter`): `dimension:operator:expression` format, semicolon-separated

```
"filter": "query:contains:keyword;page:includingRegex:/blog/"
```

GSC operators: `contains`, `notContains`, `equals`, `notEquals`, `includingRegex`, `excludingRegex`

### Pipeline Field

Post-fetch processing. Available for all sources.

| Field | Type | Description |
|-------|------|-------------|
| `pipeline.transform` | string | Column transform (`col:func`) |
| `pipeline.where` | string | Row filter (pandas query) |
| `pipeline.sort` | string | Sort (`col DESC,col2 ASC`) |
| `pipeline.columns` | string | Column selection |
| `pipeline.group_by` | string | Group columns |
| `pipeline.aggregate` | string | Aggregation (`sum:clicks,mean:ctr`) |
| `pipeline.head` | integer | First N rows |

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.example.com/",
  "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
  "dimensions": ["query", "page"],
  "limit": 25000,
  "pipeline": {
    "transform": "page:url_decode,page:strip_qs,page:path_only",
    "where": "clicks > 10",
    "group_by": "page",
    "aggregate": "sum:clicks,sum:impressions",
    "sort": "sum_clicks DESC",
    "head": 50
  }
}
```

### Save Field

Save destination for query results (post-pipeline). Available for all sources.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `save.to` | string | ✓ | `csv`, `sheets`, `bigquery` |
| `save.mode` | string | | `overwrite` (default), `append`, `upsert` |
| `save.path` | string | CSV | File path |
| `save.sheet_url` | string | Sheets | Spreadsheet URL |
| `save.sheet_name` | string | | Sheet name (default: `data`) |
| `save.project_id` | string | BQ | GCP project ID |
| `save.dataset` | string | BQ | Dataset ID |
| `save.table` | string | BQ | Table ID |
| `save.keys` | string[] | upsert | Upsert key columns |

| Mode | CSV | Sheets | BigQuery |
|------|-----|--------|----------|
| overwrite | ✓ | ✓ | ✓ |
| append | ✓ | ✓ | ✓ |
| upsert | - | ✓ | - |

### Date Templates

`date_range.start` / `date_range.end` accept template expressions resolved at validation time.

| Template | Meaning |
|----------|---------|
| `today` | Current date |
| `today-Nd` | N days ago (e.g., `today-7d`) |
| `today+Nd` | N days ahead (e.g., `today+3d`) |
| `month-start` | First day of current month |
| `month-end` | Last day of current month |
| `prev-month-start` | First day of previous month |
| `prev-month-end` | Last day of previous month |
| `week-start` | Monday of current week |
| `YYYY-MM-DD` | Absolute date (pass-through) |

---

## megaton API

### Initialization

```python
from megaton_lib.megaton_client import get_ga4, get_gsc

mg = get_ga4("PROPERTY_ID")     # Auto-selects credentials + account/property
mg = get_gsc("https://example.com/")  # Auto-selects credentials + site
```

`get_ga4()` / `get_gsc()` return a megaton instance.
`mg.report.run()` → `ReportResult`, `mg.search.run()` → `SearchResult` (chainable).

**Low-level init:**
```python
from megaton import start
from megaton_lib.credentials import resolve_service_account_path
mg = start.Megaton(resolve_service_account_path(), headless=True)
```

### GA4

```python
mg = get_ga4("PROPERTY_ID")
mg.report.set.dates("2026-01-01", "2026-01-31")
result = mg.report.run(d=["date"], m=["sessions"], filter_d="...", show=False)
result.clean_url("landingPage").group("date").sort("date")
df = result.df
```

#### Dimensions / Metrics

```python
# Basic: list of strings
d = ["date", "sessionDefaultChannelGroup"]
m = ["sessions", "activeUsers"]

# Rename columns: tuple (API_name, display_name)
d = ["date", ("sessionDefaultChannelGroup", "channel")]
m = [("sessions", "sessions_count"), ("activeUsers", "uu")]
```

**Common dimensions:**

| API Name | Description |
|----------|-------------|
| `date` | Date |
| `sessionDefaultChannelGroup` | Channel (Organic Search, etc.) |
| `sessionSource` | Source |
| `sessionMedium` | Medium |
| `pagePath` | Page path |
| `landingPage` | Landing page |
| `deviceCategory` | Device (desktop/mobile/tablet) |
| `country` | Country |

**Common metrics:**

| API Name | Description |
|----------|-------------|
| `sessions` | Sessions |
| `activeUsers` | Active users |
| `newUsers` | New users |
| `screenPageViews` | Page views |
| `bounceRate` | Bounce rate |
| `averageSessionDuration` | Average session duration |
| `conversions` | Conversions |

#### Filter Syntax

String format: `<field><operator><value>`, semicolon-separated for AND.

```python
filter_d="sessionDefaultChannelGroup==Organic Search;country==Japan"
filter_m="sessions>100"  # Metric filter
```

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equals | `country==Japan` |
| `!=` | Not equals | `country!=Japan` |
| `=@` | Contains | `pagePath=@/blog/` |
| `!@` | Not contains | `pagePath!@/admin/` |
| `=~` | Regex match | `pagePath=~^/products/` |
| `!~` | Regex not match | `pagePath!~/test/` |
| `>`, `>=`, `<`, `<=` | Numeric | `sessions>100` |

#### Sort Syntax

String format. Prefix `-` for descending.

```python
mg.report.run(d=["date"], m=["sessions"], sort="date", show=False)       # Ascending
mg.report.run(d=["date"], m=["sessions"], sort="-sessions", show=False)  # Descending
```

#### ReportResult Methods

| Method | Description |
|--------|-------------|
| `.clean_url(dim)` | Normalize URL (decode, strip query, lowercase) |
| `.group(by, method='sum')` | Group and aggregate |
| `.sort(by, ascending=True)` | Sort |
| `.fill(to='(not set)')` | Fill missing values |
| `.to_int(metrics)` | Convert metrics to int |
| `.replace(dim, by)` | Replace values |
| `.normalize(dim, by)` | Normalize values (overwrite) |
| `.categorize(dim, by, into)` | Add category column |
| `.classify(dim, by)` | Normalize + aggregate |
| `.df` | Get final DataFrame |

### Search Console

```python
mg = get_gsc("https://example.com/")
mg.search.set.dates("2026-01-01", "2026-01-31")
result = mg.search.run(dimensions=["query", "page"], limit=25000)
result.decode().clean_url().normalize_queries().filter_impressions(min=10)
df = result.df
```

**Filter:**
```python
mg.search.run(
    dimensions=["query", "page"],
    dimension_filter=[
        {"dimension": "query", "operator": "contains", "expression": "keyword"},
    ]
)
```

| Operator | Description |
|----------|-------------|
| `contains` | Contains |
| `notContains` | Not contains |
| `equals` | Equals |
| `notEquals` | Not equals |
| `includingRegex` | Regex match |
| `excludingRegex` | Regex not match |

**Dimensions:** `query`, `page`, `country`, `device`, `date`
**Metrics:** `clicks`, `impressions`, `ctr` (0-1), `position`

#### SearchResult Methods

| Method | Description |
|--------|-------------|
| `.decode()` | URL decode (%xx → char) |
| `.clean_url(dim='page')` | Normalize URL |
| `.remove_params(keep=None)` | Remove URL query params |
| `.normalize_queries()` | Normalize query whitespace, merge duplicates |
| `.filter_clicks(min, max)` | Filter by clicks |
| `.filter_impressions(min, max)` | Filter by impressions |
| `.filter_ctr(min, max)` | Filter by CTR |
| `.filter_position(min, max)` | Filter by position |
| `.aggregate(by)` | Manual aggregation |
| `.normalize(dim, by)` | Normalize values |
| `.categorize(dim, by, into)` | Add category column |
| `.classify(dim, by)` | Normalize + aggregate |
| `.apply_if(cond, method)` | Conditional method application |
| `.df` | Get final DataFrame |

### Google Sheets

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# Read
mg.sheets.select("sheet_name")
df = mg.sheet.df()

# Write (overwrite)
mg.save.to.sheet("sheet_name", df, sort_by="date", auto_width=True)

# Append
mg.append.to.sheet("sheet_name", df)

# Upsert (merge by key)
mg.upsert.to.sheet("sheet_name", df, keys=["date", "channel"])

# Cell operations
mg.sheet.cell.set("A1", "value")
mg.sheet.range.set("A1:C3", [["a", "b", "c"], ...])
mg.sheet.clear()
```

Sheet management: `mg.sheets.list()`, `mg.sheets.create("name")`, `mg.sheets.delete("name")`

### BigQuery

```python
bq = mg.launch_bigquery("my-gcp-project")

# Query
df = bq.run("SELECT * FROM `project.dataset.table` LIMIT 100", to_dataframe=True)

# Browse
bq.datasets                    # ['dataset1', 'dataset2']
bq.dataset.select("my_dataset")
bq.dataset.tables              # ['table1', 'table2']
```

---

## Authentication

### Service Account JSON

- Location: `credentials/` directory
- Git: excluded (`.gitignore`)
- Recommended: set `MEGATON_CREDS_PATH` env var (file or directory containing JSON)

### Resolution Rules

**GA4 / GSC (via Megaton):**

`get_ga4(property_id)` / `get_gsc(site_url)` auto-select credentials:
1. `MEGATON_CREDS_PATH` points to a file → use that file
2. `MEGATON_CREDS_PATH` points to a directory → `*.json` files (sorted by filename)
3. Not set → search `credentials/*.json` (with parent directory traversal)

**BigQuery (native client):**

`query_bq()` uses `google.cloud.bigquery.Client`:
1. `GOOGLE_APPLICATION_CREDENTIALS` if set
2. Otherwise, select from `MEGATON_CREDS_PATH` / `credentials/*.json`
   - `creds_hint` parameter matches filename substring
   - Falls back to first candidate

### In Notebooks

```python
from megaton_lib.notebook import init; init()
```

`init()` searches upward for `credentials/` to resolve project root and set `MEGATON_CREDS_PATH`.

### Verify Resolution

```python
from megaton_lib.megaton_client import describe_auth_context
info = describe_auth_context(creds_hint="corp")
# info["resolved_bq_creds_path"], info["resolved_bq_source"]
```

---

## External Links

- [megaton on GitHub](https://github.com/mak00s/megaton)
- [Streamlit Documentation](https://docs.streamlit.io/)
