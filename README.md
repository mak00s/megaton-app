# megaton-app

A toolkit for fetching and analyzing data from GA4, Search Console, and BigQuery.
Use it through three interfaces: Notebook, CLI, and Streamlit UI.

## Setup

```bash
pip install -r requirements.txt
# Place service account JSON file(s) in credentials/
# Multiple files are supported; routing is resolved by property_id
```

## Quick Start

```bash
# Streamlit UI
streamlit run app/streamlit_app.py

# CLI (source in params.json is auto-routed to GA4/GSC/BigQuery)
python scripts/query.py --params input/params.json

# Tests
pytest
```

See [docs/USAGE.md](docs/USAGE.md) for detailed usage.

## Structure

```
megaton-app/
├── megaton_lib/        # Shared library (reusable from other repos via pip install -e)
├── scripts/            # CLI tools (query.py, run_notebook.py)
├── app/                # Streamlit UI
├── notebooks/          # Jupyter notebooks (Jupytext .py <-> .ipynb)
├── credentials/        # Service account JSON files (not tracked by Git)
├── configs/            # JSON configs for batch runs
├── input/              # AI Agent -> UI parameter handoff
├── output/             # Query results and job artifacts
├── tests/              # pytest (CI requires >=90% coverage)
└── docs/               # Detailed documentation
```

## Documentation

| Document | Description |
|-------------|------|
| [docs/USAGE.md](docs/USAGE.md) | Usage guide for Notebook, CLI, and Streamlit UI |
| [docs/REFERENCE.md](docs/REFERENCE.md) | JSON schema, megaton API, auth, and pipeline reference |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | Project change history |

## Related Repositories

| Repository | Role |
|---|---|
| [megaton](https://github.com/mak00s/megaton) | GA4/GSC/Sheets API wrapper (PyPI package) |
| [megaton-notebooks](https://github.com/mak00s/megaton-notebooks) | Notebook collection for scheduled reporting |
