"""Common scrape error vocabulary for browser/HTTP collectors.

Consumers can raise or translate to ``ScrapeError`` when they are ready to
standardize retry/notification policy across repos. This module intentionally
does not prescribe handling behavior; it only names coarse failure kinds.
"""

from __future__ import annotations

from typing import Literal

ScrapeErrorKind = Literal["rejected", "throttled", "dom_changed", "login_required", "empty", "unknown"]


class ScrapeError(RuntimeError):
    """Structured scrape failure with a shared coarse ``kind`` vocabulary."""

    def __init__(self, message: str, *, kind: ScrapeErrorKind = "unknown") -> None:
        super().__init__(message)
        self.kind = kind
