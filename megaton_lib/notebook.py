"""Notebook initialization helpers.

Common setup for using project libs from under ``notebooks/``.
In your setup cell, just run ``from megaton_lib.notebook import init`` then
``init()`` to:
- add project root to ``sys.path``
- set ``MEGATON_CREDS_PATH``
- reload project modules
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def init() -> Path:
    """Detect project root and initialize paths/env/modules.

    Returns:
        Project root path.
    """
    root = _find_project_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    os.environ["MEGATON_CREDS_PATH"] = str(root / "credentials")

    _reload_lib()

    return root


def _find_project_root() -> Path:
    """Find project root by searching upward for a ``credentials/`` directory."""
    d = Path.cwd()
    while d != d.parent:
        if (d / "credentials").exists():
            return d
        d = d.parent
    raise RuntimeError("Project root not found (no 'credentials/' directory in parents)")


def _reload_lib() -> None:
    """Reload project modules in dependency order."""
    import megaton_lib.date_template
    import megaton_lib.credentials
    import megaton_lib.megaton_client
    import megaton_lib.analysis
    import megaton_lib.sheets
    import megaton_lib.periods

    importlib.reload(megaton_lib.date_template)
    importlib.reload(megaton_lib.credentials)
    importlib.reload(megaton_lib.megaton_client)
    importlib.reload(megaton_lib.analysis)
    importlib.reload(megaton_lib.sheets)
    importlib.reload(megaton_lib.periods)

    from megaton_lib.megaton_client import reset_registry
    reset_registry()
