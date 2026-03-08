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
- `site` alias resolution (`configs/sites*.json`) is applied consistently in CLI `--params`, `--batch`, and Streamlit `input/params.json` handoff
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

---

## Audit CLI (`scripts/audit.py`)

Reusable audit runner for shared features `1-9`:

1. project config model
2. common runner
3. GTM extraction
4. Adobe Tags extraction
5. GA4 extraction
6. AA extraction
7. site mapping audit
8. JSON/CSV reporting
9. unified CLI

Project-specific logic (`10-12`) should stay in each analysis repository.

### Commands

| Command | Description |
|---|---|
| `site-mapping` | Tag mapping vs GA4/AA observed values |
| `export-tag-config` | Export current tag mapping snapshot |

### Common options

| Option | Description |
|---|---|
| `--project` | Project ID or config path |
| `--config-root` | Config directory (default: `configs/audit/projects`) |
| `--output` | Output directory |
| `--json` | JSON output |

### `site-mapping` options

| Option | Description |
|---|---|
| `--days` | Period length when start/end are omitted |
| `--start-date` | Start date (`YYYY-MM-DD`) |
| `--end-date` | End date (`YYYY-MM-DD`) |
| `--with-aa` | Include Adobe Analytics comparison |

### Project config

- Directory: `configs/audit/projects/`
- Example: `configs/audit/projects/example.json`
- Keys:
  - `tag_source` (`gtm` or `adobe_tags`)
  - `ga4` (`property_id`, dimensions, metrics)
  - optional `aa` (`company_id`, `rsid`, `dimension`, `metric`)
  - optional `fallback_mapping_path` (markdown table fallback)

### Audit Runtime Environment Variables

| Area | Variables |
|---|---|
| Adobe Tags | `ADOBE_TAGS_API_KEY`, `ADOBE_TAGS_BEARER_TOKEN`, optional `ADOBE_TAGS_IMS_ORG_ID` |
| Adobe Analytics | `ADOBE_CLIENT_ID`, `ADOBE_CLIENT_SECRET`, `ADOBE_ORG_ID` |

Notes:
- AA integration uses a built-in Adobe Analytics 2.0 REST client (OAuth + retry/backoff + paging).
- GTM access uses service-account credentials resolved by `MEGATON_CREDS_PATH` / `credentials/`.

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
| `source` | string | ✓ | `"ga4"`, `"gsc"`, `"aa"`, `"bigquery"` |
| `site` | string | - | Site alias defined in `configs/sites.local.json` / `configs/sites.json` (template: `configs/sites.example.json`) |
| `property_id` | string | GA4 | GA4 property ID |
| `site_url` | string | GSC | Search Console site URL |
| `company_id` | string | AA | Adobe global company ID |
| `org_id` | string | - | Adobe Org ID (optional override) |
| `rsid` | string | AA | Adobe report suite ID |
| `dimension` | string | AA | Adobe dimension (e.g. `daterangeday`) |
| `project_id` | string | BQ | GCP project ID |
| `sql` | string | BQ | SQL to execute |
| `date_range.start` | string | GA4/GSC/AA | Start date (YYYY-MM-DD or template) |
| `date_range.end` | string | GA4/GSC/AA | End date (YYYY-MM-DD or template) |
| `dimensions` | array | - | Dimension list |
| `metrics` | array | GA4/AA | Metric list |
| `segment` | string/array | - | Adobe segment ID(s) |
| `filter_d` | string | - | GA4 filter (`field==value` format) |
| `filter` | string | - | GSC filter (`dim:op:expr` format) |
| `limit` | number | - | Row limit (max 100,000) |
| `column_types` | object | - | Table display hints (`date`, `int`, `float`, `currency`, `percent`, `text`) |
| `pipeline` | object | - | Post-fetch pipeline (see below) |
| `save` | object | - | Save destination (see below) |

`site` is resolved by CLI before validation.
- `source: "ga4"`: `site` -> `property_id`
- `source: "gsc"`: `site` -> `site_url`
- `source: "aa"`: `site` -> `rsid` / `company_id` (if configured in `configs/sites*.json`)
- If `property_id` / `site_url` is already set, it takes precedence over `site`.
- Alias file precedence: `configs/sites.example.json` < `configs/sites.json` < `configs/sites.local.json`

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

**GA4 (with alias):**
```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "site": "corp",
  "date_range": {"start": "today-7d", "end": "today"},
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"],
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

**Adobe Analytics (AA):**
```json
{
  "schema_version": "1.0",
  "source": "aa",
  "company_id": "wacoal1",
  "rsid": "wacoal-all",
  "date_range": {"start": "2026-02-17", "end": "2026-02-17"},
  "dimension": "daterangeday",
  "metrics": ["revenue", "orders"],
  "segment": ["s1234567890"],
  "limit": 50000
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

### Spike Investigation Template (GA4)

Use this when investigating a short traffic spike by page and channel.

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "site": "corp",
  "date_range": {"start": "2026-01-01", "end": "2026-01-12"},
  "dimensions": ["date", "landingPage", "sessionDefaultChannelGroup"],
  "metrics": ["sessions", "activeUsers"],
  "filter_d": "landingPage=@/jp/company/",
  "limit": 50000,
  "pipeline": {"sort": "date ASC"}
}
```

Then compare:
- baseline window (example: `2026-01-01` to `2026-01-05`)
- spike window (example: `2026-01-06` to `2026-01-08`)
- delta metrics by `landingPage`, `sessionDefaultChannelGroup`, and their combination

Note: delta computation is not a built-in one-shot CLI feature. Export rows (`--output`) and compute comparisons in pandas / notebook / BI tool.

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

## Date Utilities (`megaton_lib.date_utils`)

### Month Range Helpers

| Function | Returns |
|----------|---------|
| `month_ranges_for_year(year)` | List of `("YYYY-MM-01", "YYYY-MM-last")` for each month |
| `month_ranges_between(start, end)` | Per-month clamped `(start, end)` pairs inside the window |
| `months_between(start, end)` | `["YYYYMM", ...]` month keys in range |

### Timezone/Relative Month Helpers

| Function | Description |
|----------|-------------|
| `now_in_tz(tz="Asia/Tokyo")` | Current timezone-aware datetime |
| `previous_month_range(reference=None, tz="Asia/Tokyo")` | Previous month start/end (`YYYY-MM-DD`) |
| `month_start_months_ago(months_ago, reference=None, tz="Asia/Tokyo")` | Month start for N months ago |
| `previous_year_start(reference=None, tz="Asia/Tokyo")` | Jan 1 of previous year |
| `month_suffix_months_ago(months_ago, reference=None, tz="Asia/Tokyo", fmt="%Y.%m")` | Month label for N months ago |

### DataFrame Month Parsing/Filtering

| Function | Description |
|----------|-------------|
| `parse_year_month_series(series)` | Parse flexible month values (`202301`, `2023-01`, `2023/1`, `2023年1月`) into month-start datetime |
| `drop_current_month_rows(df, month_col, tz="Asia/Tokyo")` | Remove rows matching current month key (`YYYYMM`) |
| `select_recent_months(df, month_col, months=13)` | Keep recent N months from the max month in the column |

Notes:
- `drop_current_month_rows()` compares against `YYYYMM` string values. Normalize your month column first when source data is datetime, slash/dash strings, or numeric `YYYYMM`.
- For numeric/heterogeneous month columns, run `parse_year_month_series()` first, then filter on the parsed datetime column.

Example:

```python
import pandas as pd
from megaton_lib.date_utils import (
    parse_year_month_series,
    select_recent_months,
    drop_current_month_rows,
)

df = pd.DataFrame({"month_raw": [202401, "2024-02", "2024年3月"], "value": [10, 12, 9]})
df["month_dt"] = parse_year_month_series(df["month_raw"])
df = select_recent_months(df, month_col="month_dt", months=13)
df["month_ym"] = df["month_dt"].dt.strftime("%Y%m")
df = drop_current_month_rows(df, month_col="month_ym")
```

---

## Library Helper Modules

### GA4 Helpers (`megaton_lib.ga4_helpers`)

| Function | Description |
|----------|-------------|
| `run_report_df(mg, dimensions, metrics, ...)` | Execute `mg.report.run()` and return `result.df` |
| `merge_dataframes(frames, on, how="left", int_cols=None)` | Merge DataFrames in order, skipping `None`/empty frames |
| `collect_site_frames(mg, sites_df, fetch_fn, ...)` | Loop site settings and collect non-empty per-site frames |
| `report_data_or_empty(mg, expected_cols)` | Return `mg.report.data` with guaranteed columns/order |
| `run_report_data_or_empty(mg, dimensions, metrics, expected_cols, ...)` | Run report and return `report.data` in stable schema |
| `run_report_merge(mg, reports, on, how="outer", fillna_value=None)` | Run multiple report specs and merge by shared keys |
| `build_filter(*parts)` | Build GA4 filter string from non-empty parts |
| `to_datetime_col(df, col="date")` | Convert a date-like column to datetime |
| `to_numeric_cols(df, cols, fillna=None, as_int=False)` | Convert selected columns to numeric |

### GSC Helpers (`megaton_lib.gsc_utils`)

| Function | Description |
|----------|-------------|
| `aggregate_search_console_data(df_raw)` | Normalize `page` and aggregate clicks/impressions/position |
| `deduplicate_queries(df_in)` | Merge whitespace-variant queries by `month+clinic+page` |
| `filter_by_clinic_thresholds(df, threshold_df)` | Apply clinic-specific min impressions / max position filters |
| `force_text_on_numeric_column(df, column="query")` | Prefix apostrophe for numeric-like strings (Sheets safety) |

Notes:
- `aggregate_search_console_data()` and `deduplicate_queries()` assume numeric metrics (`clicks`, `impressions`, `position`) or numeric-castable values.
- `filter_by_clinic_thresholds()` evaluates only clinics present in `threshold_df` and excludes undefined clinics by design.

### Table Mapping Helpers (`megaton_lib.table_utils`)

| Function | Description |
|----------|-------------|
| `apply_pattern_map(df, column, mapping, output_col=None, default_unmatched=None)` | Regex map values in a column |
| `classify_by_pattern_map(df, mapping, source_col, output_col="category", default_label="other")` | Classify with explicit fallback label |

### Sheets Helpers (`megaton_lib.sheets`)

| Function | Description |
|----------|-------------|
| `read_sheet_table(mg, sheet_url, sheet_name)` | Read a worksheet into DataFrame (trimmed headers, empty rows dropped) |
| `load_pattern_map(mg, sheet_url, sheet_name, key_col, value_col)` | Load mapping table as `{pattern: value}` dict |
| `upsert_or_skip(mg, name, df, keys, ...)` | Skip empty input, otherwise upsert to worksheet |
| `replace_sheet_by_group_keys(mg, sheet_url, sheet_name, ...)` | Replace matching group rows (e.g. month+clinic) and overwrite |
| `update_cells(mg, sheet_url, sheet_name, values)` | Update multiple A1 cells |
| `save_sheet_from_template(mg, sheet_name, df, ...)` | Write with template-based sheet creation |

### Traffic Helpers (`megaton_lib.traffic`)

| Function | Description |
|----------|-------------|
| `normalize_domain(value)` | Normalize host text (strip scheme/path/www, lowercase) |
| `ensure_trailing_slash(path, preserve_suffixes=(".html", "/"))` | Append `/` unless path already has a preserved suffix |
| `apply_source_normalization(df, source_map, source_col="source")` | Apply regex map to normalize source values |
| `classify_channel(row, ...)` | Reclassify channel using source/medium heuristics (AI/Map/Group etc.) |
| `reclassify_source_channel(row, ...)` | Reclassify source + channel pair and return `(source, channel)` |

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

## Streamlit UI Behavior

### Query Execution UX

- Clicking **Run** switches the button label to `Running...` and disables repeat clicks until completion
- Sidebar shows a fetching status while query execution is in progress

### Chart Tab

- Supports `X Axis`, `Y Axis`, and optional `Series`
- If a datetime-like column exists (for example `date`), it is preselected as default `X Axis`
- `Y Axis` candidates are numeric columns only
- When `Series` is set, chart data is pivoted (`index=X`, `columns=Series`, `values=Y`) with `sum` aggregation
- If series cardinality is high, only top 20 series are shown (with an in-app notice)

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
3. Not set → use `credentials/` if it exists in current working directory
4. If not found, walk up parent directories for `credentials/`
5. If still not found, fallback to `credentials/` next to this package (`megaton-app/credentials/`)

**BigQuery (native client):**

`query_bq()` uses `google.cloud.bigquery.Client`:
1. `GOOGLE_APPLICATION_CREDENTIALS` if set
2. Otherwise, select from `MEGATON_CREDS_PATH` / `credentials/*.json`
   - `creds_hint` parameter matches filename substring
   - `credentials/` resolution follows the same order as GA4/GSC (CWD → parent walk → package-parent fallback)
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
