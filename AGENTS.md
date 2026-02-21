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
├── input/                 # params.json (UI <-> Agent handoff)
├── output/                # Query results & job artifacts
├── configs/               # Batch execution JSON configs
├── tests/                 # pytest (337 tests)
└── docs/                  # USAGE.md, REFERENCE.md, CHANGELOG.md
```

## How to Fetch Data

### CLI (recommended)

```bash
# Sync execution (auto-routes by source field)
python scripts/query.py --params input/params.json

# JSON output
python scripts/query.py --params input/params.json --json

# Async jobs
python scripts/query.py --submit --params input/params.json
python scripts/query.py --result <job_id> --head 20

# List available properties/sites
python scripts/query.py --list-ga4-properties
python scripts/query.py --list-gsc-sites
```

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

## Rules

1. **Use `show()`**: Never `print(df.to_string())`. For large results, use `save=` to write CSV
2. **Prefer CLI**: Use `scripts/query.py` directly when human confirmation is not needed
3. **Jupytext**: Edit `.py` files -> `jupytext --sync notebooks/**/*.ipynb`
4. **Run tests**: After changing `megaton_lib/` or `app/`, run `python -m pytest -q`
5. **BQ location**: Always pass `query_bq(..., location="asia-northeast1")`
6. **Keep megaton_lib/ generic**: `app/` is for UI-specific code only

## Tests

```bash
python -m pytest -q                    # All tests (337 passed)
python -m pytest -q -m unit           # Unit only
python -m pytest -q --cov=scripts.query --cov-report=term-missing  # Coverage
```

Tests use API mocks (SimpleNamespace pattern) with no external dependencies.

## Documentation

| Document | Contents |
|----------|----------|
| [docs/USAGE.md](docs/USAGE.md) | Setup, quick start, recipes |
| [docs/REFERENCE.md](docs/REFERENCE.md) | JSON schema, all CLI options, pipeline, megaton API, auth |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | Change history |
