from __future__ import annotations

import sys
from pathlib import Path

# Some environments invoke `pytest` via an installed entrypoint script. In that
# case, the project root is not guaranteed to be on sys.path, and imports like
# `import app` / `import scripts` / `import lib` may fail. Ensure the repo root
# is importable for tests.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _infer_layer_from_nodeid(nodeid: str) -> str:
    """Infer test layer marker from test file path."""
    file_name = nodeid.split("::", 1)[0].replace("\\", "/").split("/")[-1]

    integration_files = {
        "test_app_engines.py",
        "test_batch.py",
        "test_query_branch_coverage.py",
        "test_query_nonjson_and_main_branches.py",
        "test_query_success_flows.py",
        "test_run_notebook.py",
    }
    contract_files = {
        "test_params_validator.py",
        "test_query_builders.py",
        "test_query_core_helpers.py",
        "test_query_json_errors.py",
    }

    if file_name in integration_files:
        return "integration"
    if file_name in contract_files:
        return "contract"
    return "unit"


def pytest_collection_modifyitems(items):
    """Attach one of unit/contract/integration markers to every test item."""
    import pytest

    for item in items:
        layer = _infer_layer_from_nodeid(item.nodeid)
        item.add_marker(getattr(pytest.mark, layer))
