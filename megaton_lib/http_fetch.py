"""HTTP fetch helpers for scrapers: one retry policy, one UA, one timeout.

Keeps scraping code small and consistent — swap to async/Playwright later by
touching only this module. ``fetch_*`` raise on final failure; ``safe_fetch_*``
return None instead (for optional enrichment fetches where a miss is fine).

BeautifulSoup is imported lazily: only ``fetch_html``/``safe_fetch_html`` need
it (install ``beautifulsoup4`` + ``lxml``, or the ``scrape`` extra).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = 15

_MAX_ATTEMPTS = 3
_BACKOFF_MULTIPLIER = 1.5
_BACKOFF_MIN_S = 1.0
_BACKOFF_MAX_S = 8.0


def _with_retry(func, *args, sleep=time.sleep, **kwargs):
    """Run ``func`` retrying ``requests.RequestException`` with expo backoff."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except requests.RequestException as exc:
            if attempt >= _MAX_ATTEMPTS:
                raise
            wait = min(max(_BACKOFF_MULTIPLIER * (2 ** (attempt - 1)), _BACKOFF_MIN_S), _BACKOFF_MAX_S)
            logger.debug("HTTP retry %d/%d in %.1fs: %s", attempt, _MAX_ATTEMPTS, wait, exc)
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover


def fetch_text(url: str, *, timeout: int = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_UA) -> str:
    def _get() -> str:
        resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
        resp.raise_for_status()
        return resp.text

    return _with_retry(_get)


def fetch_html(url: str, *, timeout: int = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_UA):
    """GET ``url`` and parse with BeautifulSoup (lxml). Returns ``BeautifulSoup``."""
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "fetch_html requires beautifulsoup4 + lxml: pip install beautifulsoup4 lxml"
        ) from exc

    def _get():
        resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
        resp.raise_for_status()
        return BeautifulSoup(resp.content, "lxml")

    return _with_retry(_get)


def fetch_json(
    url: str,
    *,
    method: str = "GET",
    payload: Any | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_UA,
) -> Any:
    headers = {"User-Agent": user_agent, "Content-Type": "application/json"}

    def _req() -> Any:
        if method.upper() == "POST":
            resp = requests.post(url, json=payload or {}, headers=headers, timeout=timeout)
        else:
            resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    return _with_retry(_req)


def safe_fetch_text(url: str, **kwargs) -> str | None:
    """Like ``fetch_text`` but returns None on failure instead of raising."""
    try:
        return fetch_text(url, **kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.debug("safe_fetch_text failed for %s: %s", url, exc)
        return None


def safe_fetch_html(url: str, **kwargs):
    """Like ``fetch_html`` but returns None on failure instead of raising."""
    try:
        return fetch_html(url, **kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.debug("safe_fetch_html failed for %s: %s", url, exc)
        return None


def safe_fetch_json(url: str, **kwargs) -> Any | None:
    try:
        return fetch_json(url, **kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.debug("safe_fetch_json failed for %s: %s", url, exc)
        return None
