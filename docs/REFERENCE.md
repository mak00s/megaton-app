# Technical Reference

For setup and how-to, see [USAGE.md](USAGE.md).

---

## CLI (`scripts/query.py`)

`scripts/query.py` delegates params-style source execution to
`megaton_lib.query_runner`. Keep source branching, GSC filter parsing, AA
segment/breakdown handling, and result header construction in
`megaton_lib.query_runner` so CLI, notebooks, and agent wrappers share one
contract.

### Options

| Option | Description | Default |
|--------|------------|---------|
| `--params` | Schema-validated JSON input | `input/params.json` |
| `--inline` | Inline JSON params string (takes precedence over `--params`) | - |
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
| `--batch <path>` | Batch execute all JSON in a directory, or one JSON file | - |
| `--list-jobs` | List jobs | OFF |
| `--job-limit` | Max jobs to list | 20 |
| `--list-ga4-properties` | List GA4 properties | OFF |
| `--list-gsc-sites` | List GSC sites | OFF |
| `--list-bq-datasets` | List BQ datasets | OFF |
| `--list-aa-segments` | List Adobe Analytics segments | OFF |
| `--project` | GCP project for dataset listing | - |
| `--aa-company-id` | Adobe company ID for segment listing | - |
| `--aa-rsid` | Adobe RSID for segment listing | - |
| `--aa-org-id` | Adobe Org ID override for segment listing | - |
| `--aa-segment-name` | Segment name filter for segment listing | - |
| `--aa-segment-definition` | Include segment definitions in listing output | OFF |
| `--json` | JSON output | table |
| `--output` | Save to CSV file | - |

**Constraints:**
- `--params`: validates `schema_version: "1.0"` and source-key consistency
- `--inline`: validates the same schema as `--params` and is used first when both are present
- `site` alias resolution (`configs/sites*.json`) is applied consistently in CLI `--params`, `--batch`, and Streamlit `input/params.json` handoff
- direct query execution (`--params` / `--inline`): pipeline must be in the JSON payload (CLI args not allowed)
- `--head` and `--summary`: require `--result`
- `--group-by` and `--aggregate`: must be used together
- `--summary`: exclusive with pipeline options
- `--list-aa-segments`: requires `--aa-company-id` and `--aa-rsid`

### AA Segment Listing

Use this when you need Adobe segment IDs, descriptions, or raw definitions.

```bash
python scripts/query.py \
  --list-aa-segments \
  --aa-company-id wacoal1 \
  --aa-rsid wacoal-all \
  --aa-segment-name "bot除外" \
  --aa-segment-definition \
  --json
```

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
| `export-tag-config` | Export tag config snapshot to a local directory. GTM exports the full container and reports `has_changes`; Adobe Tags exports the current mapping snapshot. |

### Common options

| Option | Description |
|---|---|
| `--project` | Project ID or config path |
| `--config-root` | Config directory (default: `configs/audit/projects`) |
| `--output` | Output directory |
| `--json` | JSON output |

Notes:
- `export-tag-config` requires `--output`
- `site-mapping` can print a console summary or emit JSON/CSV artifacts under `--output`

### `site-mapping` options

| Option | Description |
|---|---|
| `--days` | Period length when start/end are omitted |
| `--start-date` | Start date (`YYYY-MM-DD`) |
| `--end-date` | End date (`YYYY-MM-DD`) |
| `--with-aa` | Include Adobe Analytics comparison |

### Project config

- Directory: `configs/audit/projects/`
- Format: YAML or JSON (e.g., `shiseido.yaml`, `shibuya-shibuya.yaml`)
- Keys:
  - `tag_source` (`gtm` or `adobe_tags`)
  - `ga4` (`property_id`, dimensions, metrics)
  - optional `aa` (`company_id`, `rsid`, `dimension`, `metric`)
  - optional `fallback_mapping_path` (markdown table fallback)
  - GTM-specific: `export_resources` (default: all) to limit exported resource types

### Audit Runtime Environment Variables

| Area | Variables |
|---|---|
| Adobe Tags | `ADOBE_TAGS_API_KEY`, `ADOBE_TAGS_BEARER_TOKEN`, optional `ADOBE_TAGS_IMS_ORG_ID` |
| Adobe Analytics | `ADOBE_CLIENT_ID`, `ADOBE_CLIENT_SECRET`, `ADOBE_ORG_ID` |

Notes:
- AA integration uses a built-in Adobe Analytics 2.0 REST client (OAuth + retry/backoff + paging).
- GTM access uses service-account credentials resolved by `MEGATON_CREDS_PATH` / `credentials/`.
- Adobe Analytics can also auto-detect OAuth JSON files in `ADOBE_CREDS_PATH` or `credentials/`.
- Adobe OAuth JSON shape: `client_id`, `client_secret`, `org_id` (or `ims_org_id`), optional `scopes`.

### Adobe Analytics Classifications CLI

Module entrypoint:

```bash
python -m megaton_lib.audit.providers.analytics.verify_classification ...
```

Use this when you need to verify whether uploaded classification values have been reflected in Adobe Analytics.

| Option | Description |
|---|---|
| `--company-id` | Adobe Analytics company ID |
| `--rsid` | Report suite ID |
| `--dimension` | AA dimension such as `evar29` or `prop10` |
| `--column` | Classification column name to verify |
| `--keys` | Comma-separated `key=value` pairs |
| `--diff-tsv` | TSV file containing `Key` and target column |
| `--sample` | Randomly sample N keys from `--diff-tsv` |
| `--creds-file` | JSON file with `client_id`, `client_secret`, `org_id` |
| `--org-id` | Optional Adobe Org ID override |
| `--token-cache` | Optional token cache file override |
| `--report` | Also verify via AA Reporting API breakdown (Level 2) |
| `--report-sample` | Keys to spot-check in report mode (default: 10) |

Constraints:
- `--keys` and `--diff-tsv` are mutually exclusive
- `--diff-tsv` must contain a `Key` column and the target `--column`
- dimension matching is exact (`evar2` does not match `evar29`)
- omitting `--token-cache` falls back to the auth client's default cache path

Examples:

```bash
python -m megaton_lib.audit.providers.analytics.verify_classification \
  --company-id wacoal1 \
  --rsid wacoal-all \
  --dimension evar29 \
  --column "関係者" \
  --keys A100012345=社員,A100067890=業者
```

```bash
python -m megaton_lib.audit.providers.analytics.verify_classification \
  --company-id wacoal1 \
  --rsid wacoal-all \
  --dimension evar29 \
  --column "関係者" \
  --diff-tsv output/classification_diff.tsv \
  --sample 20
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

### Report Validation / ExecutionTracker

`megaton_lib.report_validation` は notebook のレポート実行を追跡するユーティリティ。

#### `init_report_tracker(report_name, *, write_enabled=True, **window_values)`

`ExecutionTracker` を初期化する。`MEGATON_RUN_SUMMARY_PATH` 環境変数が設定されている場合、実行サマリー JSON を書き出す。

**パラメータ:**
- `report_name` (str) - レポート名（サマリーに記録）
- `write_enabled` (bool) - `False` にすると全 Sheets 書き込みをスキップ。`WRITE_TO_SHEETS` パラメータと組み合わせて使う
- `**window_values` - レポート期間情報（`last_month_from`, `last_month_to` 等）

#### `finish_report_tracker(tracker, *, status="skipped", notes=None, errors=None)`

実行サマリーを確定し、コンソールに表示する。GitHub Actions では job summary に出力される。

#### `ExecutionTracker` の Sheets 書き込みメソッド

| メソッド | 説明 |
|---|---|
| `tracker.save_sheet(mg, gs_url=, sheet_name=, df=)` | 全行上書き |
| `tracker.upsert_sheet(mg, gs_url=, sheet_name=, df=, keys=)` | キーで upsert |
| `tracker.save_sheet_from_template(mg, gs_url=, sheet_name=, df=)` | テンプレートシートから複製して保存 |
| `tracker.replace_sheet_groups(mg, gs_url=, sheet_name=, df_new=, remove_group_keys=)` | グループ単位で置換 |
| `tracker.duplicate_sheet(mg, gs_url=, source_sheet_name=, new_sheet_name=)` | シート複製 |
| `tracker.update_sheet_cells(mg, gs_url=, cells_to_update=)` | セル更新 |
| `tracker.append_sheet(mg, gs_url=, sheet_name=, df=)` | 追記 |

全メソッドは `write_enabled=False` 時にスキップし、run summary に `mode="skipped_write"` として記録する。

### Pending Verification CLI

Module entrypoint:

```bash
python scripts/check_pending_verifications.py ...
```

Use this when validation flows register Adobe Analytics follow-up checks and you need to inspect or complete the shared pending-task store.

| Option | Description |
|---|---|
| `--file` | Pending task JSON path (default: `validation/pending_aa_verifications.json`) |
| `--all` | Show all pending tasks instead of overdue-only |
| `--complete <TASK_ID>` | Mark one task as completed |
| `--result` | Completion result label (default: `verified`) |
| `--notes` | Optional completion notes |
| `--add` | Add one pending task interactively or from flags |
| `--id` / `--description` / `--verification-file` | Core fields used with `--add` |
| `--verification-type` / `--aa-verifier` | Extra metadata for follow-up routing |
| `--expected key=value` | Repeatable expected-value metadata for `--add` |
| `--delay-minutes` | Fixed delay instead of next AA batch time |
| `--quiet`, `-q` | Suppress the “no overdue tasks” message |

Examples:

```bash
python scripts/check_pending_verifications.py --all
```

```bash
python scripts/check_pending_verifications.py --complete task-123 --result verified
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

`--batch <path>` runs all JSON files in the directory in filename order, or one JSON file directly.
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
| `segment_definition` | object/array | - | Adobe inline segment definition(s) |
| `breakdown` | object/array | - | Adobe Reports API breakdown filter object(s) |
| `filter_d` | string | - | GA4 filter (`field==value` format) |
| `filter` | string | - | GSC filter (`dim:op:expr` format) |
| `page_to_path` | boolean | GSC | Convert GSC page URLs to paths before returning rows |
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

**Adobe Analytics (AA, inline segment + breakdown):**
```json
{
  "schema_version": "1.0",
  "source": "aa",
  "company_id": "wacoal1",
  "rsid": "wacoal-all",
  "date_range": {"start": "2026-03-01", "end": "2026-03-03"},
  "dimension": "lasttouchchannel",
  "metrics": ["visits"],
  "segment_definition": {
    "func": "segment",
    "version": [1, 0, 0],
    "container": {
      "func": "container",
      "context": "hits",
      "pred": {
        "func": "container",
        "context": "hits",
        "pred": {
          "str": "Direct",
          "val": {"func": "attr", "name": "variables/lasttouchchannel"},
          "description": "ラストタッチチャネル",
          "func": "streq"
        }
      }
    }
  },
  "breakdown": {
    "dimension": "variables/geocountry",
    "itemId": "4007560033"
  },
  "limit": 10
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

Set `"page_to_path": true` for GSC params when callers need page URLs converted
to paths during fetch. The default remains `false`.

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

Megaton-instance helpers use a `megaton` object (`mg`) and are the default for
notebook/report flows. Direct-gspread helpers use a `gspread.Spreadsheet`
object and are intended for jobs that deliberately bypass `megaton`.

#### Megaton-instance helpers

| Function | Description |
|----------|-------------|
| `read_sheet_table(mg, sheet_url, sheet_name, header_row=0)` | Read a worksheet into DataFrame (trimmed headers, empty rows dropped) |
| `load_pattern_map(mg, sheet_url, sheet_name, key_col, value_col)` | Load mapping table as `{pattern: value}` dict |
| `upsert_or_skip(mg, name, df, keys, ...)` | Skip empty input, otherwise upsert to worksheet |
| `replace_sheet_by_group_keys(mg, sheet_url, sheet_name, ...)` | Replace matching group rows (e.g. month+clinic) and overwrite |
| `update_cells(mg, sheet_url, sheet_name, values)` | Update multiple A1 cells |
| `save_sheet_table(mg, sheet_url, sheet_name, df, min_rows=None, min_cols=None, hide_gridlines=None, tab_color=None, ...)` | Save a DataFrame and optionally format the selected sheet via `mg.sheet.*` |
| `save_sheet_from_template(mg, sheet_name, df, ...)` | Write with template-based sheet creation |

#### Direct-gspread helpers

These low-level helpers live in `megaton_lib.gspread_lowlevel`. They are
available from `megaton_lib.sheets` only as compatibility re-exports. New code
should import them from `megaton_lib.gspread_lowlevel`.

| Function | Description |
|----------|-------------|
| `open_spreadsheet(spreadsheet_id, credentials_path, scopes=None)` | Open a spreadsheet with a service-account JSON |
| `overwrite_worksheet(spreadsheet, sheet_name, df, ...)` | Clear and overwrite one worksheet from a DataFrame |
| `append_rows(spreadsheet, sheet_name, rows, ...)` | Append raw row values |
| `ensure_sheet_exists(spreadsheet, sheet_name, ...)` | Create a worksheet when missing |
| `get_sheet_id(spreadsheet, sheet_name)` | Resolve worksheet title to numeric `sheetId` |
| `set_frozen_rows(spreadsheet, sheet_name, count)` | Set frozen row count via batchUpdate |
| `ensure_min_dimensions(spreadsheet, sheet_name, ...)` | Append rows/columns to reach minimum dimensions |

Notes:
- `read_sheet_table(..., header_row=...)` can start from a non-zero header row when source sheets include title/comment rows
- `read_sheet_df(..., strict=True)` is available when callers need header/shape validation instead of permissive fallback loading
- sheet formatting in `save_sheet_table()` uses megaton's public API:
  `mg.sheet.resize()`, `mg.sheet.gridlines.hide()/show()`, and
  `mg.sheet.tab.color()`
- `min_rows` and `min_cols` expand the sheet to at least those dimensions;
  they do not shrink existing sheets
- legacy `*_gspread_*` names such as `open_gspread_spreadsheet()` and
  `overwrite_gspread_worksheet()` remain as compatibility aliases; prefer
  the short names above for new low-level batchUpdate code

### Docs Table Helpers (`megaton_lib.docs_sites`)

| Function | Description |
|----------|-------------|
| `format_md_value(value)` | Normalize Python values into Markdown table cell text |
| `render_markdown_table(headers, rows)` | Render a Markdown table string from headers and rows |
| `replace_generated_section(text, name, content)` | Replace one named generated block between `BEGIN/END GENERATED` markers |
| `update_generated_markdown(path, sections)` | Update multiple generated Markdown sections in one file |

### Traffic Helpers (`megaton_lib.traffic`)

| Function | Description |
|----------|-------------|
| `normalize_domain(value)` | Normalize host text (strip scheme/path/www, lowercase) |
| `ensure_trailing_slash(path, preserve_suffixes=(".html", "/"))` | Append `/` unless path already has a preserved suffix |
| `apply_source_normalization(df, source_map, source_col="source")` | Apply regex map to normalize source values |
| `classify_channel(row, ...)` | Reclassify channel using source/medium heuristics (AI/Map/Group etc.) |
| `reclassify_source_channel(row, ...)` | Reclassify source + channel pair and return `(source, channel)` |

### Validation Helpers (`megaton_lib.validation`)

| Function / Class | Description |
|------------------|-------------|
| `resolve_path(obj, path)` | Resolve dotted JSON-like path and return `(exists, value)` |
| `check_rule(data, rule)` | Evaluate one contract rule (`path`, `type`, `nonEmpty`, `minItems`) |
| `validate_contract(data, contract)` | Run a contract file and return aggregate check report |
| `capture_console_args(msg)` | Safely serialize Playwright console arguments |
| `select_headers(headers)` | Keep stable request/response headers for debug output |
| `extract_mbox_names(payload)` | Extract Target delivery mbox names from request payload |
| `PageEventCapture(...)` | Collect Playwright console logs, page errors, failed requests, and Target delivery calls |
| `GtmPreviewOverride(...)` | Describe how GTM container requests should be routed to a workspace preview |
| `TagsLaunchOverride(...)` | Describe how Adobe Tags launch assets should be replaced during Playwright runs |
| `build_gtm_preview_override(config, ...)` | Build GTM preview override config from Tag Assistant URL or raw params |
| `configure_gtm_preview_override(page, override)` | Attach Playwright routes that append GTM preview parameters to container requests |
| `describe_gtm_preview_override(override)` | Return stable metadata dict for saved GTM preview runs (omits auth token) |
| `build_tags_launch_override(config, ...)` | Build `TagsLaunchOverride` from config mapping with dev/region overrides |
| `describe_tags_launch_override(override)` | Return stable metadata dict for saved Tags override runs |
| `configure_tags_launch_override(page, url, override)` | Attach Playwright routes that swap Adobe Tags assets for one page/origin |
| `run_page_session(...)` | Open a browser/context/page session with cookie preload, launch options, and optional override routing |
| `run_page(...)` | Open a Playwright page with optional basic auth, HTTPS ignore, and Tags override support |
| `capture_storage_state(...)` | Open a temporary Playwright context, run a callback, and return `storage_state` |
| `run_page_with_bootstrapped_state(...)` | Capture auth/bootstrap `storage_state` once, then open a second page run with that state |
| `run_with_basic_auth_page(...)` | Open a page with BASIC auth and run a callback in Playwright |
| `run_with_launch_override(...)` | Backward-compatible helper for legacy `satelliteLib-*.js` replacement |
| `capture_selector_state(page, selectors)` | Snapshot selector existence/opacity/child counts for page validation |
| `wait_for_any_selector(page, selectors, ...)` | Poll until any selector appears and optionally wait for a settle period |
| `click_selector_if_visible(page, selector, ...)` | Click a selector only when it exists and is visible |
| `scroll_selector_region_to_end(page, selector, ...)` | Scroll a container element to its end and report whether it existed |
| `enable_selector(page, selector, ...)` | Clear disabled state from a selector and report whether it existed |
| `set_checkbox_checked(page, selector, ...)` | Enable and check a checkbox-like selector when it exists |
| `scroll_selector_into_view(page, selector, ...)` | Scroll a selector into view and report whether the element existed |
| `capture_satellite_info(page)` | Snapshot `_satellite` presence/build metadata from the current page |
| `parse_appmeasurement_url(url, post_data=None)` | Parse AppMeasurement params from a `b/ss` URL and optional POST body |
| `AppMeasurementCapture()` | Collector object with `attach()`, `checkpoint()`, `since()`, `collect_after()`, `snapshot()`, `clear()`, and `wait_until_ready()`; supports custom request parsers |
| `execute_appmeasurement_scenario(page, capture, steps)` | Run declarative AppMeasurement steps and collect per-step incremental beacons |
| `extract_appmeasurement_request(request)` | Parse one Playwright request into AppMeasurement params when it is a `b/ss` beacon |
| `attach_appmeasurement_capture(page, sink)` | Attach a request listener that appends parsed `b/ss` beacons into a list |
| `slice_appmeasurement_beacons(beacons, start_index)` | Return a shallow copy of parsed beacons captured after an offset |
| `wait_for_appmeasurement_ready(page, beacons, ...)` | Wait until a beacon fires or `_satellite` and `s` are ready |
| `run_aa_validation(config)` | Shared AA beacon runner; supports named hooks such as `bootstrapPage` and `captureRuntime` in config |
| `resolve_adobe_credentials_path(...)` | Resolve an Adobe credential JSON from an explicit path or candidate list |
| `build_adobe_analytics_client(...)` | Build an Adobe Analytics API client from local credential JSON plus env fallbacks |
| `run_aa_api_followup_verifier(...)` | Load a verification JSON, run a verifier callback, and finalize the pending task |
| `build_validation_run_metadata(...)` | Build standard metadata dict (execution mode, project, scenario, timestamps) |
| `write_validation_json(path, report)` | Write validation report JSON with parent dir creation |
| `load_auth_profile_store(path)` | Load a local JSON credential store for named auth profiles |
| `resolve_auth_profile(store_or_path, profile_name, ...)` | Resolve one named auth profile and optionally require specific fields |
| `load_storefront_session_cookies(path)` | Load persisted storefront session cookies from JSON when present |
| `save_storefront_session_cookies(page, path)` | Persist current browser-context cookies for later storefront sessions |
| `run_storefront_validation_session(...)` | Run a storefront browser session with cookie preload plus shared validation setup |
| `finalize_followup_verification(...)` | Attach follow-up metadata, save the verification JSON, and complete the matching pending task |

Notes:
- Playwright page wrappers default to `stealth=True`: they use a real Chrome
  user-agent, disable Chromium automation blink features, and hide
  `navigator.webdriver` so Adobe Analytics validation traffic is not dropped
  by common bot rules.
- Pass `stealth=False` when validating bot detection itself or when a test
  intentionally needs Playwright's default headless fingerprint.
- Pass `user_agent="..."` to override the default user-agent while keeping the
  common wrapper lifecycle.

#### Adobe Tags Launch Override

Use `TagsLaunchOverride` when you need to test a site against a different Adobe Tags
build without changing the site code.

```python
from megaton_lib.validation import TagsLaunchOverride, run_page

override = TagsLaunchOverride(
    launch_url="https://assets.adobedtm.com/<company>/<property>/launch-xxxx-development.js",
    mode="auto",
)

def validate(page):
    page.goto("https://example.com/", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    return {
        "url": page.url,
        "has_satellite": page.evaluate("() => typeof _satellite !== 'undefined'"),
    }

result = run_page(
    "https://example.com/",
    tags_override=override,
    callback=validate,
)
```

Mode guide:

- `auto`: enable both strategies below. Use this as the default when sites are mixed.
- `legacy_satellite`: replace `satelliteLib-*.js` references in HTML responses.
- `launch_env`: intercept `launch-*staging*.js` / `launch-*development*.js` requests and serve `launch_url`.

Important fields:

- `launch_url`: full `assets.adobedtm.com/.../launch-*.js` URL to test.
- `env_patterns`: which environment names should be matched in `launch_env` mode. Default is `("staging", "development")`.
- `abort_old_property_assets`: if `True`, abort other requests under the same property asset prefix so old chunks do not run beside the override build.

Notes:

- `run_page_session(...)` is the higher-level session primitive for long browser flows. Use it when the callback owns navigation across many steps and you need cookie preload, `channel`, or `slow_mo`.
- `run_page(...)` does not navigate for you. Call `page.goto(...)` inside `callback` or `setup`.
- `run_page(...)`, `capture_storage_state(...)`, and `run_page_with_bootstrapped_state(...)` all accept the same session options as `run_page_session(...)`, including `channel`, `slow_mo`, `cookies`, and `context_setup`.
- `run_with_launch_override(...)` remains available for older callers, but new code should prefer `run_page(..., tags_override=TagsLaunchOverride(...))`.
- `run_with_launch_override(...)` performs one initial `page.goto(...)` before your callback runs. Do not call `page.goto(...)` again for the same URL unless you intentionally want a second load.
- `run_with_basic_auth_page(...)` is still useful when you only need BASIC auth and no Tags replacement.
- `legacy_satellite` HTML rewriting is scoped to the initial `run_page(url=...)` origin. If your callback later moves to another origin, that later HTML will not be rewritten by the original route.

#### GTM Preview Override

Use `GtmPreviewOverride` when you need to validate a site against an unpublished GTM workspace draft.

```python
from megaton_lib.validation import GtmPreviewOverride, run_page

override = GtmPreviewOverride(
    container_id="GTM-TJKK7S5",
    auth_token="...",
    preview_id="env-361",
)

def validate(page):
    page.goto("https://corp.shiseido.com/jp/rd/safety/ingredients/", wait_until="networkidle", timeout=90000)
    page.wait_for_timeout(3000)
    return {"url": page.url}

result = run_page(
    "https://corp.shiseido.com/jp/rd/safety/ingredients/",
    gtm_preview=override,
    callback=validate,
)
```

Notes:

- Prefer `build_gtm_preview_override({"previewUrl": "<tag assistant url>"}, require=True)` when you already have a Tag Assistant preview link.
- GTM preview support rewrites matching `gtm.js` / `ns.html` requests to include `gtm_auth`, `gtm_preview`, and `gtm_cookies_win`.
- The preview auth token is intentionally not persisted in validation metadata.

### Adobe Tags Bootstrap (`megaton_lib.audit.providers.tag_config`)

| Function | Description |
|---|---|
| `adobe_tags_output_root(property_id)` | Return the canonical local output path for one Adobe Tags property |
| `bootstrap_account_env(account="", project_root=".", known_accounts=("csk", "wws", "dms"), account_hints=None, property_id="", library_id="", git_remote_url="")` | Resolve an analysis account, load `.env.<account>`, and set `ACCOUNT` for thin wrappers |
| `account_token_cache_file(account="", project_root=".", token_cache_dir="credentials")` | Return an account-namespaced Adobe OAuth token cache path such as `.adobe_token_cache.csk.json` |
| `resolve_first_existing_path(explicit="", project_root=".", candidates=())` | Resolve an explicit path or the first existing candidate, preserving relative-to-project-root semantics |
| `load_env_file(path, override=False)` | Load `KEY=VALUE` pairs from a file into `os.environ`; defaults to setdefault semantics |
| `seed_adobe_oauth_env(...)` | Resolve Adobe OAuth credentials from args, env, JSON file, and payload dict. Sets resolved values into `os.environ`. |
| `build_tags_config(...)` | Build `AdobeTagsConfig` after resolving OAuth settings via `seed_adobe_oauth_env` |
| `build_repo_tags_config_factory(...)` | Build a repo-local `tags_config_factory` that loads `ENV_FILE`, resolves credential candidates, and uses per-account token caches |

Notes:

- `bootstrap_account_env` resolution order: explicit account → `ACCOUNT` env var → `[tool.megaton].default_account` / `[tool.tags].default_account` → account hints → the only matching `.env.<account>` file
- when an explicit account is passed, the selected `.env.<account>` overrides existing env values and `ACCOUNT` is set to the resolved account; inferred accounts preserve existing env values with setdefault semantics
- `account_hints` can map accounts to `property_ids`, `library_ids`, `remote_contains`, and `path_contains` / `cwd_contains`; wrappers can build this from repo-local `config.py` constants such as `CSK_PROPERTY_ID`
- default known accounts are `csk`, `wws`, and `dms`; pass `known_accounts=...` from wrappers if a repo needs a different allow-list
- wrappers should call `bootstrap_account_env(args.account)` before building AA / AT / Adobe Tags clients so Makefile targets and direct `python -m ...` invocations share the same env bootstrap
- `seed_adobe_oauth_env` resolution order: explicit args → env vars → `creds_file` JSON → `payload` dict
- `creds_file` accepts a path to a JSON file with `client_id`, `client_secret`, `org_id` keys (loaded via `load_adobe_oauth_credentials`)
- `build_tags_config` passes `creds_file` through to `seed_adobe_oauth_env`
- `build_repo_tags_config_factory` is the preferred factory for analysis repos that keep credentials under repo-local `key/` or shared `credentials/` directories
- account token caches are namespaced by `ACCOUNT` to avoid OAuth churn when switching between CSK / WWS / DMS
- `adobe_tags_output_root(property_id)` defaults to `adobe-tags/<property_id>` under the chosen project root
- `load_env_file` silently skips if the file does not exist

### Adobe Tags / GTM CLI Entrypoints (`megaton_lib.audit.providers.tag_config`)

These helpers are intended for thin wrapper scripts in analysis repos.

| Function | Description |
|---|---|
| `tags_export_main(...)` | Reusable Adobe Tags export CLI with env-driven resource/filter resolution |
| `tags_apply_main(...)` | Reusable Adobe Tags apply/build CLI with dry-run default |
| `tags_workspace_main(...)` | Reusable library-scope Adobe Tags CLI for thin analysis repo `python -m tags` wrappers |
| `analysis_tags_workspace_main(...)` | Convenience wrapper for analysis repos: standard workspace CLI plus repo credential candidates and account token cache setup |
| `gtm_export_main(...)` | Reusable GTM container export CLI |

#### `tags_export_main(...)`

- resolves property IDs from `--property-id`, `property_ids=...`, or `TAGS_PROPERTY_ID`
- preloads the first config so `.env` values are available before reading export filter env vars
- reads `TAGS_EXPORT_RESOURCES`, `TAGS_RULE_NAME_CONTAINS`, `TAGS_RULE_ENABLED_ONLY`, `TAGS_DE_ENABLED_ONLY`
- writes under `project_root / adobe_tags_output_root(property_id)`
- refreshes `.apply-baseline.json` under the same output root for later stale-base checks

#### `tags_apply_main(...)`

- defaults to dry-run unless `--apply` is passed
- resolves dev build settings from `TAGS_DEV_LIBRARY_ID` and `TAGS_DEV_LAUNCH_URL`
- runs the build workflow automatically when a dev library ID is available and `--skip-build` is not set
- falls back to apply-only mode when no library ID is configured
- uses `.apply-baseline.json` when present to detect stale-base conflicts
- skips `remote_only` drift automatically and aborts on `conflict` unless `--allow-stale-base` is passed

#### `tags_workspace_main(...)`

- supports `checkout`, `pull`, `status`, `add`, `push`, `build`, `full-export`, and `conflict`
- global flags include `--account`, `--property-id`, `--library-id`, `--root`, `--workers`, `--summary-only`, `--verbose`, and `--format json`
- `status --since-pull` uses only local baseline files and makes no Adobe API calls
- `conflict --list`, `conflict --show <path>`, and `conflict --resolve <path> --use local|remote|baseline [--apply]` read `.tag-conflicts.json` and do not require Adobe credentials
- `conflict --list --format json` emits the same workspace result envelope as other JSON commands, with conflict records under `details.conflicts`
- conflict commands bootstrap `.env.<account>` before resolving the default workspace root unless `--root` is explicitly provided
- `push --apply` runs local `status --since-pull --summary-only` before and after the push unless `--no-local-status-hooks` is used
- wrapper entrypoint example:

```python
from pathlib import Path

import config
from megaton_lib.audit.providers.tag_config import analysis_tags_workspace_main

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    analysis_tags_workspace_main(
        project_root=PROJECT_ROOT,
        account_hints=config.ACCOUNT_HINTS,
        known_accounts=("wws",),
        credentials_candidates=[PROJECT_ROOT / "key/adobe_credentials.json"],
        token_cache_dir=PROJECT_ROOT / "key",
        account_default="wws",
        org_id=config.ADOBE_ORG_ID,
    )
```

#### `gtm_export_main(...)`

- resolves container ID from `--container-id`, `container_public_id=...`, or `GTM_CONTAINER_PUBLIC_ID`
- accepts `--resources` or `GTM_EXPORT_RESOURCES`
- defaults output to `gtm/<container_id>` under the chosen project root

### Adobe Tags Build Workflow Helpers (`megaton_lib.audit.providers.tag_config`)

| Function | Description |
|---|---|
| `collect_changed_resources(config, root, dry_run, allow_stale_base=False)` | Apply exported sidecars, collect changed rule / data-element origins, and enforce stale-base conflict checks |
| `run_build_workflow(config, ..., apply, verify_asset_url=None, markers=None, re_export_resources=None, allow_stale_base=False, verify_retries=5, build_wait_timeout=300, build_poll_interval=5)` | Apply changes, refresh library revisions, build, wait for completion, optionally verify, then re-export |
| `verify_build_markers(asset_url, markers, max_retries=5)` | Search the built asset corpus for marker strings with retry/backoff for CDN propagation |

Notes:

- `run_build_workflow(...)` waits for the Adobe Tags build to reach a terminal status before returning
- the default Step 5 re-export scope is `rules,data-elements`; override with `TAGS_REEXPORT_RESOURCES`
- returns `0` for success, dry-run, or no-op
- returns `2` when the build completes in a non-success terminal state
- returns `3` when marker verification fails after a successful build
- CLI exits `4` on stale-base conflicts unless `--allow-stale-base` is used
- raises `ValueError` when `markers` are provided without `verify_asset_url`

### Adobe Tags Sync Helpers (`megaton_lib.audit.providers.tag_config`)

| Function | Description |
|----------|-------------|
| `slugify_component_name(name)` | Convert component names to stable ASCII slugs for file matching |
| `find_component_id(code_file)` | Resolve Reactor component/data-element ID from exported custom-code file path |
| `find_data_element_id(settings_file)` | Resolve a data-element ID from an exported `*.settings.json` sidecar |
| `get_component_settings(config, component_id)` | Fetch one rule component or data element with parsed Reactor settings |
| `apply_component_settings(config, component_id, new_settings, dry_run=True)` | PATCH full settings for one rule component or data element |
| `delete_resource(config, resource_id, dry_run=True)` | Delete a rule component or data element via the Reactor API |
| `remove_library_resources(config, library_id, resource_type, resource_ids, dry_run=True)` | Remove resource revisions from a library relationship |
| `apply_custom_code_tree(config, root, dry_run=True)` | Apply all exported `*.custom-code.*` files under a property tree |
| `apply_data_element_settings_tree(config, root, dry_run=True)` | Apply exported data-element `*.settings.json` sidecars via direct Reactor PATCH |
| `apply_exported_changes_tree(config, root, dry_run=True)` | Apply both custom-code sidecars and data-element settings sidecars under a property tree |

### Adobe Tags Library-Scope Workspace Helpers (`megaton_lib.audit.providers.tag_config.workspace`)

These helpers are intended for analysis-repo wrappers that want a safer day-to-day
Adobe Tags workflow than raw full-property export/apply.

| Function | Description |
|---|---|
| `checkout_library_scope(config, root, library_id, force=False, snapshot_workers=None)` | Destructively overwrite local managed files from the current remote library scope; if managed local files already exist, callers should require `force=True` |
| `pull_library_scope(config, root, library_id, snapshot_workers=None, summary_only=False, verbose=False)` | Non-destructively sync remote-only library-scope changes into local files |
| `status_library_scope(config, root, library_id, since_pull=False, summary_only=False, verbose=False, snapshot_workers=None)` | Report library-scope drift without mutating local files; `since_pull=True` uses only local baseline files and makes no Adobe API calls |
| `add_to_library_scope(config, root, library_id, from_paths, apply)` | Resolve local exported new resources from paths and explicitly add them to the library |
| `push_library_scope(config, root, library_id, apply, verify_asset_url=None, allow_stale_base=False, skip_build=False, markers=None, snapshot_workers=None)` | Guarded PATCH + library refresh + optional build flow for existing in-library resources |
| `build_library_scope(config, library_id, verify_asset_url=None, markers=None)` | Build-only helper for one Adobe Tags library |
| `full_export_property(config, output_root, resources=None)` | Export a full property mirror to a non-canonical output path |
| `list_workspace_conflicts(root)` | Read `.tag-conflicts.json` for wrapper `tags conflict --list --format json` commands |
| `render_workspace_conflict(root, path)` | Render saved conflict diffs for wrapper `tags conflict --show <path>` commands |
| `resolve_workspace_conflict(root, path, use, apply=False)` | Resolve one saved conflict by keeping local text or writing saved remote/baseline text; dry-run by default |
| `workspace_result_exit_code(result)` | Return the stable process exit code for a workspace result dict |

Notes:

- these helpers assume the analysis repo treats **library membership** as the working scope
- remote snapshot fetches use a bounded thread pool; default parallelism is 10 and can be adjusted with `TAGS_SNAPSHOT_WORKERS` or wrapper flags such as `--workers N` mapped to `snapshot_workers`
- CSK benchmark saturated around 20 workers for a roughly 170-resource library scope; keep the generic default at 10 and set `TAGS_SNAPSHOT_WORKERS=20` only in CSK-specific env/examples
- `checkout` is destructive and should require repo-local `--force` whenever managed local files already exist
- `pull` uses 3-way compare semantics:
  - remote-only drift is applied
  - local-only edits are kept
  - local+remote edits are left as conflicts
  - local files whose parent resources are outside the current library scope are reported with the user-facing label `outside_library_scope_files`, not as remote-removed files
- `status` should surface library-scope drift in both file counts and resource/type counts so migration-state repos are understandable
- `status` supports a fast local `since_pull` mode for repeated verification after local cleanup; use normal remote status when remote drift matters
- baseline manifests store `baseline_text` for managed resources so conflict artifacts can render true baseline/local/remote diffs
- remote `pull` / `status` writes `.tag-conflicts.json` when conflicts are found; wrappers can expose `tags conflict --list`, `tags conflict --show <path>`, and `tags conflict --resolve <path> --use local|remote|baseline --apply` from that artifact
- wrappers can implement `--format json` by emitting the returned result dict to stdout; workspace progress and summaries are written to stderr so JSON stdout stays clean
- `summary_only=True` prints counts without warnings/next hints, and non-verbose outside-scope warnings are grouped by resource type
- workspace result JSON includes stable top-level keys such as `schema_version`, `command`, `ok`, `exit_code`, `severity`, `summary`, `details`, `next`, and `elapsed`; `mode` is included when a command has a meaningful variant
- workspace exit code contract: `0` ok, `1` wrapper/runtime error, `2` conflicts, `3` stale remote or remote-removed-with-local-edits, `4` outside-library-scope resources/files
- `mode` is command-specific: `status` uses `remote` or `since_pull`; `push` uses `dry_run`, `skip_build`, or `apply`; conflict resolve uses `resolve`; commands without a distinct variant omit `mode`
- `schema_version=1` is additive-compatible: consumers should ignore unknown fields. If a future change removes or renames existing fields, bump to `schema_version=2`; wrappers should support v1 until their minimum `megaton_lib` version requires v2
- `add` and `push` are intended to be compare-and-abort workflows
- `push` should not auto-add non-member origins; analysis repos should keep that explicit via `add`
- dry-run `push` reports outside-scope local resources as result JSON with exit code `4`; apply mode still aborts before mutation
- each helper prints step progress plus `Summary / Warnings / Next`

#### Validation Auth Profiles

Use auth profiles when one local JSON file stores multiple named login credentials or tenant settings for validation flows.

Accepted store shapes:

```json
{
  "profiles": {
    "stg": {"username": "user@example.com", "password": "secret"},
    "prod": {"username": "user2@example.com", "password": "secret2"}
  }
}
```

or a flat mapping:

```json
{
  "stg": {"username": "user@example.com", "password": "secret"},
  "prod": {"username": "user2@example.com", "password": "secret2"}
}
```

Example:

```python
from megaton_lib.validation import resolve_auth_profile

profile = resolve_auth_profile(
    "credentials/storefront_profiles.json",
    "stg",
    required_fields=("username", "password"),
)
```

### Adobe Target Recommendations Helpers (`megaton_lib.audit.providers.target`)

| Function | Description |
|----------|-------------|
| `AdobeTargetClient(...)` | Authenticated Target REST client with retry / re-auth helpers |
| `export_recs(client, output_root, ..., prune=False)` | Export recommendations resources with optional per-resource filters |
| `apply_recs(client, source_root, ..., dry_run=True)` | Diff local exported resources against remote state and apply changes |
| `export_feeds(client, output_root, ...)` | Export feed definitions with sensitive-field redaction |
| `detect_getoffer_scope(delivery_payload)` | Detect Recommendation scope hints from delivery payloads |
| `export_getoffer_scope(client, output_root, ...)` | Export a narrowed Recommendations snapshot based on detected scope |

Notes:
- `export_recs(..., prune=True)` is only allowed for full unfiltered exports; filtered exports raise a `ValueError` because prune would delete files outside the queried subset
- design updates are applied with `PUT`, and criteria updates use subtype `PUT` when a specific criteria endpoint is available
- design sidecars are resolved from both flat layouts (`<id>.html`) and `code/<stem>.html` style layouts during apply

### Adobe Target Activity Helpers (`megaton_lib.audit.providers.target`)

| Function | Description |
|----------|-------------|
| `parse_ids(raw)` | Parse comma-separated activity IDs into `list[int]` |
| `resolve_activity_ids(index_path, raw_ids="")` | Resolve activity IDs from explicit input or exported `index.json` |
| `fetch_activity(client, tenant_id, activity_id)` | Fetch one activity detail JSON (AB or XT/options) |
| `export_activities(client, tenant_id, output_root, activity_ids)` | Export selected Target activities and write `index.json` |

### Adobe Analytics Classifications (`megaton_lib.audit.providers.analytics`)

| Symbol | Description |
|---|---|
| `ClassificationsClient(auth, company_id, ...)` | Adobe Analytics Classifications API client for dataset discovery, export, import, and verify flows |
| `print_verify_results(results)` | Print `verify_column()` output as a compact table |

#### `ClassificationsClient`

| Method | Description |
|---|---|
| `find_dataset_id(rsid, dimension)` | Resolve the classification dataset ID for one exact dimension name |
| `create_export_job(dataset_id, ...)` | Start an export job |
| `download_export_file(job_id)` | Download the TSV for a completed export job |
| `export_classification(dataset_id, ...)` | Export TSV with create → poll → download |
| `create_import_job(dataset_id, ...)` | Start an import job |
| `upload_file(job_id, content, ...)` | Upload TSV content or a local file path |
| `commit_job(job_id)` | Commit an import job |
| `import_classification(dataset_id, content, ...)` | Import TSV with create → upload → commit and return the import job ID |
| `get_classification_columns(dataset_id)` | Discover current classification column names from an export header |
| `export_column_as_dict(dataset_id, column)` | Return `{Key: value}` mapping for one column |
| `verify_column(rsid, dimension, column, expected)` | Compare expected values against current AA values |

Example:

```python
from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient
from megaton_lib.audit.providers.analytics import (
    ClassificationsClient,
    print_verify_results,
)

auth = AdobeOAuthClient()
client = ClassificationsClient(auth=auth, company_id="wacoal1")

results = client.verify_column(
    rsid="wacoal-all",
    dimension="evar29",
    column="関係者",
    expected={
        "A100012345": "社員",
        "A100067890": "業者",
    },
)
print_verify_results(results)
```

### Adobe Analytics Data Warehouse (`megaton_lib.audit.providers.analytics.dw`)

| Symbol | Description |
|---|---|
| `AdobeDataWarehouseClient(auth, company_id, ...)` | Adobe Analytics Data Warehouse scheduled request API client |
| `build_adobe_auth(...)` | Build shared Adobe OAuth auth from explicit args, creds file, and env fallback |
| `build_dw_client(company_id, ...)` | Build `AdobeDataWarehouseClient` from Adobe credential inputs |
| `find_template_requests(client, ...)` | Search scheduled request summaries by `rsid`, created/updated date, status, and name/owner filters |
| `resolve_template_request(client, ...)` | Resolve one template request detail by UUID or filtered search |
| `summarize_template_detail(template_detail)` | Build a compact template summary for CLI/display use |
| `build_cloned_request_body(template_detail, ...)` | Build a create/update payload from one template detail payload |
| `create_request_from_template(client, ...)` | Create one scheduled request from a template |
| `bulk_create_requests_from_template(client, ...)` | Create or preview multiple scheduled requests from one template |

#### `AdobeDataWarehouseClient`

| Method | Description |
|---|---|
| `list_scheduled_requests(rsid, ...)` | List scheduled request summaries for one RSID |
| `get_scheduled_request(scheduled_request_uuid)` | Fetch one scheduled request detail payload |
| `create_scheduled_request(body)` | Create one scheduled request |
| `update_scheduled_request(scheduled_request_uuid, body)` | Update one scheduled request |
| `list_reports(...)` | List generated report metadata |
| `get_report(report_uuid)` | Fetch one generated report metadata payload |
| `resend_report(report_uuid)` | Request resend for one generated report |

#### Template discovery notes

- `rsid` is required for scheduled request listing
- date filters on list APIs target request `createdDate` / `updatedDate`, not report target range
- if more than 100 requests share the same `updatedDate`, template discovery stops with an explicit error because the Adobe list API exposes `limit` but no safe page cursor for that tie bucket
- if you need to distinguish by report range, output basename, or segment, fetch the detail payload and inspect:
  - `request.reportParameters.reportRange`
  - `request.outputFile.outputFileBasename`
  - `request.reportParameters.segmentList`
  - `delivery.exportLocationUUID`

#### CLI

Module entrypoint:

```bash
python -m megaton_lib.audit.providers.analytics.dw.cli ...
```

Examples:

```bash
python -m megaton_lib.audit.providers.analytics.dw.cli \
  --company-id wacoal1 \
  --find-template \
  --rsid wacoal-all \
  --name-contains tmpl_step_ \
  --updated-after 2026-01-01T00:00:00Z
```

```bash
python -m megaton_lib.audit.providers.analytics.dw.cli \
  --company-id wacoal1 \
  --describe-template \
  --scheduled-request-uuid 12345678-90ab-cdef-1234-567890abcdef
```

```bash
python -m megaton_lib.audit.providers.analytics.dw.cli \
  --manifest input/dw_manifest.json \
  --dry-run
```

```bash
python -m megaton_lib.audit.providers.analytics.dw.cli \
  --manifest input/dw_manifest.json \
  --create
```

Notes:

- the first scheduled request must already exist in Adobe UI
- create flows require `exportLocationUUID`, which is obtained from the template request detail payload
- daily operations should prefer a fixed template UUID; search mode is mainly for bootstrap and investigation
- `--describe-template` accepts either `--scheduled-request-uuid` or an `--rsid` plus search filters

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

For params-style GA4/GSC/AA/BQ execution, use `megaton_lib.query_runner` or
`scripts/query.py`. For legacy/high-level BigQuery reads and UI/CLI save
compatibility, use `megaton_lib.megaton_client.query_bq()` and `save_to_bq()`.
For native-client jobs that already own a `google.cloud.bigquery.Client`, use
`megaton_lib.bigquery_utils`.

`bigquery_utils.count_rows(where_sql=...)` inserts `where_sql` as trusted SQL
text; keep user values in bind parameters via `params`. `ensure_table()`
returns `created: true` only when a table was newly created, not when an
existing table was verified.

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

`query_bq()` uses `google.cloud.bigquery.Client` when `params` is provided or `force_native=True`:
1. `GOOGLE_APPLICATION_CREDENTIALS` if set
2. Otherwise, select from `MEGATON_CREDS_PATH` / `credentials/*.json`
   - `creds_hint` parameter matches filename substring
   - `credentials/` resolution follows the same order as GA4/GSC (CWD → parent walk → package-parent fallback)
   - Falls back to first candidate

Example:

```python
df = query_bq(
    "my-project",
    "SELECT 1",
    location="asia-northeast1",
    force_native=True,
    creds_hint="shibuya",
)
```

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
