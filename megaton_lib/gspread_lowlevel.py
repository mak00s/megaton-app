"""Re-export shim over ``megaton.gsheet_lowlevel`` (since megaton 2.1.0).

The full stateless Sheets implementation (retry core with quota-403 handling
and nested-retry suppression, Retrying proxies, cell/serial parsers,
batchUpdate request builders, one-shot I/O helpers) was promoted into the
megaton package so ONE implementation serves both entry styles — see
docs/sheets-consolidation.md. Existing import sites keep working through this
shim; new code may import ``megaton.gsheet_lowlevel`` directly.

To monkeypatch internals in tests, patch ``megaton.gsheet_lowlevel``
attributes (the implementation home), not this shim.
"""

from __future__ import annotations

import megaton.gsheet_lowlevel as _core
from megaton.gsheet_lowlevel import *  # noqa: F401,F403

__all__ = list(_core.__all__)


def __getattr__(name):
    # Delegate remaining (incl. underscore) names to the implementation home.
    return getattr(_core, name)
