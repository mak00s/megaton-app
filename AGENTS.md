# megaton-app

Toolkit for fetching, processing, and visualizing data from GA4, Search Console, and BigQuery.

## Directory Structure

```
megaton-app/
├── megaton_lib/           # Shared library (used by other repos via pip install -e)
│   ├── megaton_client.py  #   GA4/GSC/BQ init & query execution (core module)
│   ├── credentials.py     #   Service account auto-detection
│   ├── ga4_helpers.py     #   GA4 API helpers
│   ├── sheets.py          #   Google Sheets read/write
│   ├── analysis.py        #   show() and analysis utilities
│   ├── params_validator.py#   JSON parameter schema validation
│   ├── job_manager.py     #   Async job management
│   ├── batch_runner.py    #   Batch execution
│   ├── result_inspector.py#   Pipeline processing (where/sort/group etc.)
│   ├── date_template.py   #   Date template resolution (today-7d etc.)
│   ├── periods.py         #   Period utilities
│   ├── date_utils.py      #   Monthly range generation
│   ├── params_diff.py     #   params.json diff detection
│   └── notebook.py        #   Notebook initialization helper
├── scripts/
│   ├── query.py           # Unified CLI (auto-routes GA4/GSC/BQ by source)
│   └── run_notebook.py    # Run notebooks from CLI with parameter override
├── app/
│   ├── streamlit_app.py   # Streamlit UI main
│   ├── i18n.py            # JA/EN translation (t() function)
│   └── ui/                # UI components
│       ├── params_utils.py    # Filter row DataFrame operations
│       ├── query_builders.py  # params.json builder
│       └── ga4_fields.py      # GA4 dimension/metric definitions
├── schemas/               # JSON schema
├── credentials/           # Service account JSON (.gitignore)
├── input/                 # params.json (Streamlit UI <-> Agent handoff ONLY)
├── output/                # Query results & job artifacts
├── configs/               # Batch execution JSON configs
├── tests/                 # pytest (337 tests)
└── docs/                  # USAGE.md, REFERENCE.md, CHANGELOG.md
```

## Available Resources

Credentials in `credentials/` provide access to the following. To refresh:
`python scripts/query.py --list-gsc-sites --json` / `--list-ga4-properties --json`

### GSC Sites

| Site URL | Notes |
|---|---|
| `https://corp.shiseido.com/` | 資生堂企業情報サイト (full domain) |
| `https://corp.shiseido.com/slqm/jp/` | SLQM (prefix property) |
| `https://shibuya-kyousei.gr.jp/` | 渋谷矯正歯科 (gr.jp) |
| `https://www.shibuyakyousei.jp/` | 渋谷矯正歯科 |
| `https://www.shinjukushinbi.com/` | 新宿 |
| `https://www.yokohamakyousei.com/` | 横浜 |
| `https://www.nambakyousei.com/` | 難波 |
| `https://www.hakatakyousei.com/` | 博多 |
| `https://umeda-cure.jp/` | 梅田 |
| `https://tenjinkyousei.com/` | 天神 |
| `https://ikebukurokyousei.com/` | 池袋 |
| `https://tokyo-cure.jp/` | 東京八重洲 |
| `https://sendai-cure.jp/` | 仙台 |
| `https://sapporo-cure.jp/` | 札幌 |

### GA4 Properties

| Property ID | Name | Site URL |
|---|---|---|
| `334854563` | 【新】資生堂企業情報サイト - GA4 | `corp.shiseido.com` |
| `411947632` | GA4 - Shiseido ALL - Prod | (all Shiseido sites) |
| `404579165` | 資生堂150年史 | `corp.shiseido.com/150th/` |
| `432304087` | corp_to_shiseido.co.jp | (redirect tracking) |
| `254470346` | 渋谷 - GA4 | `www.shibuyakyousei.jp` |
| `341415410` | shibuya-kyousei.gr.jp | `shibuya-kyousei.gr.jp` |
| `254477007` | 新宿 - GA4 | `www.shinjukushinbi.com` |
| `254800682` | 横浜 - GA4 | `www.yokohamakyousei.com` |
| `254475127` | 難波 - GA4 | `www.nambakyousei.com` |
| `254818909` | 博多 - GA4 | `www.hakatakyousei.com` |
| `250423487` | 梅田 - GA4 | `umeda-cure.jp` |
| `251231302` | 天神 - GA4 | `tenjinkyousei.com` |
| `253611291` | 池袋 - GA4 | `ikebukurokyousei.com` |
| `254467517` | 東京八重洲 - GA4 | `tokyo-cure.jp` |
| `266373360` | 仙台 - GA4 | `sendai-cure.jp` |
| `251616452` | 札幌 - GA4 | `sapporo-cure.jp` |
| `283927309` | WITH - GA4 | `with-orthodontics.com` |
| `492311970` | kyousei-clinic.jp + dentamap | `kyousei-clinic.jp` |

## Site Aliases

Instead of remembering full URLs and property IDs, use short aliases defined in `configs/sites.json`:

```json
{"schema_version":"1.0","source":"gsc","site":"corp","date_range":{"start":"today-7d","end":"today"},"dimensions":["query"]}
```

The `"site"` field auto-resolves to `site_url` (GSC) or `property_id` (GA4).

Available aliases: `corp`, `shibuya`, `shibuya-gr`, `shinjuku`, `yokohama`, `namba`, `hakata`, `umeda`, `tenjin`, `ikebukuro`, `yaesu`, `sendai`, `sapporo`

## How to Fetch Data

### CLI (recommended for agents)

`--params` accepts **any JSON file path**. Write your query to a file and pass its path.

> **Important**: `input/params.json` is the Streamlit UI handoff file (polled every 2s).
> For CLI-only queries, write to a **separate file** (e.g., `output/my_query.json`).

```bash
# Write query JSON to a file, then execute
python scripts/query.py --params output/my_query.json --json

# Save result to CSV
python scripts/query.py --params output/my_query.json --output output/result.csv

# Async jobs
python scripts/query.py --submit --params output/my_query.json
python scripts/query.py --result <job_id> --head 20

# List available properties/sites
python scripts/query.py --list-ga4-properties --json
python scripts/query.py --list-gsc-sites --json
```

### Query JSON format

All queries require `schema_version: "1.0"` and a `source` field.

**GSC query:**
```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://corp.shiseido.com/",
  "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
  "dimensions": ["query", "date"],
  "filter": "page:equals:https://corp.shiseido.com/jp/news/detail.html?n=00000000002077",
  "limit": 25000
}
```

**GA4 query:**
```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "334854563",
  "date_range": {"start": "today-7d", "end": "today"},
  "dimensions": ["date", "sessionDefaultChannelGroup"],
  "metrics": ["sessions", "activeUsers"],
  "filter_d": "sessionDefaultChannelGroup==Organic Search",
  "limit": 1000
}
```

### GSC Filter Syntax

Format: `dimension:operator:expression` (semicolon-separated for multiple filters)

| Operator | Example |
|---|---|
| `contains` | `query:contains:不正ログイン` |
| `notContains` | `query:notContains:brand` |
| `equals` | `page:equals:https://example.com/path?q=1` |
| `notEquals` | `page:notEquals:https://example.com/` |
| `includingRegex` | `page:includingRegex:/blog/.*2026` |
| `excludingRegex` | `query:excludingRegex:^$` |

Multiple filters (AND): `query:contains:keyword;page:includingRegex:/blog/`

### Post-fetch Pipeline

Add `pipeline` to the query JSON to filter/sort/aggregate results.

```json
{
  "pipeline": {
    "sort": "clicks DESC",
    "where": "clicks > 10",
    "head": 30
  }
}
```

Full pipeline options: `transform`, `where`, `group_by` + `aggregate`, `sort`, `columns`, `head`.
Processing order (fixed): transform → where → group-by+aggregate → sort → columns → head.
Details: [docs/REFERENCE.md](docs/REFERENCE.md#result-pipeline)

### Direct Python

```python
from megaton_lib.megaton_client import query_ga4, query_gsc
from megaton_lib.analysis import show

df = query_ga4("PROPERTY_ID", "2026-01-01", "2026-01-31",
               dimensions=["date", "sessionDefaultChannelGroup"],
               metrics=["sessions"],
               filter_d="sessionDefaultChannelGroup==Organic Search")
show(df)                              # Display first 20 rows
show(df, save="output/result.csv")    # Save to CSV + display
```

### Streamlit UI Integration

Write to `input/params.json` -> UI auto-syncs every 2 seconds.
`schema_version: "1.0"` required. Only fields defined for the `source` are allowed.

## Recipes

### Investigate traffic to a specific URL

Find which queries drove traffic to a page, broken down by date:

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://corp.shiseido.com/",
  "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
  "dimensions": ["query", "date"],
  "filter": "page:equals:https://corp.shiseido.com/jp/news/detail.html?n=00000000002077",
  "limit": 25000,
  "pipeline": {"sort": "clicks DESC", "head": 50}
}
```

To find the spike date first, use `"dimensions": ["date"]` without `"query"`.

### Find top queries for a site

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.shibuyakyousei.jp/",
  "date_range": {"start": "today-28d", "end": "today-3d"},
  "dimensions": ["query"],
  "limit": 1000,
  "pipeline": {"sort": "clicks DESC", "head": 30}
}
```

### Compare traffic by page

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "https://www.shibuyakyousei.jp/",
  "date_range": {"start": "today-28d", "end": "today-3d"},
  "dimensions": ["page"],
  "limit": 5000,
  "pipeline": {"sort": "clicks DESC", "head": 30}
}
```

### GA4 channel trend

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {"start": "today-30d", "end": "today"},
  "dimensions": ["date", "sessionDefaultChannelGroup"],
  "metrics": ["sessions", "activeUsers"],
  "limit": 5000,
  "pipeline": {"sort": "date ASC"}
}
```

### GA4 landing page performance (Organic Search only)

```json
{
  "schema_version": "1.0",
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {"start": "today-30d", "end": "today"},
  "dimensions": ["landingPage"],
  "metrics": ["sessions", "activeUsers", "keyEvents"],
  "filter_d": "sessionDefaultChannelGroup==Organic Search",
  "limit": 5000,
  "pipeline": {"sort": "sessions DESC", "head": 30}
}
```

## Rules

1. **Use `show()`**: Never `print(df.to_string())`. For large results, use `save=` to write CSV
2. **Prefer CLI**: Use `scripts/query.py` directly when human confirmation is not needed
3. **Don't overwrite `input/params.json`**: Use a separate file for CLI queries
4. **Jupytext**: Edit `.py` files -> `jupytext --sync notebooks/**/*.ipynb`
5. **Run tests**: After changing `megaton_lib/` or `app/`, run `python -m pytest -q`
6. **BQ location**: Always pass `query_bq(..., location="asia-northeast1")`
7. **Keep megaton_lib/ generic**: `app/` is for UI-specific code only

## Tests

```bash
python -m pytest -q                    # All tests (337 passed)
python -m pytest -q -m unit           # Unit only
python -m pytest -q --cov=scripts.query --cov-report=term-missing  # Coverage
```

Tests use API mocks (SimpleNamespace pattern) with no external dependencies.

## Documentation

| Document | Language | Contents |
|----------|----------|----------|
| [docs/USAGE.md](docs/USAGE.md) | Japanese | Setup, quick start, recipes (will be split into JA/EN later) |
| [docs/REFERENCE.md](docs/REFERENCE.md) | English | JSON schema, all CLI options, pipeline, megaton API, auth |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | English | Change history |
