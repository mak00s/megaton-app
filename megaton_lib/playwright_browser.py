"""Shared Playwright browser helpers for scraping workflows.

Provides a context manager and a one-shot scrape helper. Use this when
a target site has no HTTP/JSON endpoint and must be driven through a
real browser (e.g. SPA-heavy pages, lazy-loaded data, anti-bot walls).

Three launch modes are supported:

- **Ephemeral** (default): ``playwright.chromium.launch()`` + fresh
  ``new_context()``. Cookies and storage do not persist between runs.
- **Persistent**: when ``user_data_dir`` is supplied,
  ``launch_persistent_context()`` is used so cookies / localStorage
  survive across runs. Useful for sites that gate behind a login the
  user completes manually once.
- **CDP-attach** (``connected_browser_page``): connect to an existing
  Chrome instance via the DevTools Protocol. Combined with
  ``launch_chrome_with_debug_port`` this supports half-automatic flows
  where the user logs in (passkey, SMS) in a real Chrome window once
  and the script attaches afterwards.

``playwright`` is an optional dependency — install via the ``playwright``
extras group and run ``playwright install chromium`` before use.
"""

from __future__ import annotations

import logging
import sys
import socket
import subprocess
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from playwright.sync_api import BrowserContext, Page

logger = logging.getLogger(__name__)


_PLAYWRIGHT_MISSING_MSG = (
    "playwright is not installed; install the 'playwright' extra "
    "(e.g. `pip install megaton-app[playwright]`) and run "
    "`playwright install chromium`"
)


def _load_sync_playwright() -> Any:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - exercised when extras missing
        raise RuntimeError(_PLAYWRIGHT_MISSING_MSG) from exc
    return sync_playwright


@contextmanager
def browser_page(
    *,
    headless: bool = True,
    locale: str = "ja-JP",
    user_data_dir: str | Path | None = None,
    user_agent: str | None = None,
    timezone_id: str | None = None,
    viewport: Mapping[str, int] | None = None,
    context_kwargs: Mapping[str, Any] | None = None,
) -> Iterator[Page]:
    """Yield a Playwright ``Page`` and clean up the browser on exit.

    Args:
        headless: Launch Chromium headless. Default ``True``.
        locale: Browser locale; passed to the context.
        user_data_dir: If set, use a persistent context rooted at this
            directory so cookies / localStorage survive across runs.
            The directory is created if missing.
        user_agent: Optional UA override. Recommended when the site
            rejects ``HeadlessChrome``.
        timezone_id: Optional timezone (e.g. ``"Asia/Tokyo"``).
        viewport: Optional viewport mapping (``{"width": ..., "height": ...}``).
        context_kwargs: Extra kwargs merged into the context constructor.
            Lets callers pass uncommon options without expanding the API.

    Raises:
        RuntimeError: If ``playwright`` is not installed.
    """
    sync_playwright = _load_sync_playwright()

    extra: dict[str, Any] = {"locale": locale}
    if user_agent is not None:
        extra["user_agent"] = user_agent
    if timezone_id is not None:
        extra["timezone_id"] = timezone_id
    if viewport is not None:
        extra["viewport"] = dict(viewport)
    if context_kwargs:
        extra.update(context_kwargs)

    with sync_playwright() as pw:
        if user_data_dir is not None:
            profile = Path(user_data_dir)
            profile.mkdir(parents=True, exist_ok=True)
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(profile),
                headless=headless,
                **extra,
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                yield page
            finally:
                context.close()
        else:
            browser = pw.chromium.launch(headless=headless)
            try:
                context = browser.new_context(**extra)
                try:
                    page = context.new_page()
                    yield page
                finally:
                    context.close()
            finally:
                browser.close()


def scrape_with_playwright(
    url: str,
    *,
    handler: Callable[[Page], Any],
    headless: bool = True,
    locale: str = "ja-JP",
    user_data_dir: str | Path | None = None,
    user_agent: str | None = None,
    timezone_id: str | None = None,
    viewport: Mapping[str, int] | None = None,
    wait_selector: str | None = None,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 15_000,
    context_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Open ``url`` in a fresh browser, run ``handler(page)``, return its result.

    ``handler`` is responsible for any clicking / scrolling / waiting and
    returning a normalized record. Use this for one-off integrations
    where a JSON endpoint is unavailable.
    """
    with browser_page(
        headless=headless,
        locale=locale,
        user_data_dir=user_data_dir,
        user_agent=user_agent,
        timezone_id=timezone_id,
        viewport=viewport,
        context_kwargs=context_kwargs,
    ) as page:
        page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        if wait_selector:
            page.wait_for_selector(wait_selector, timeout=timeout_ms)
        return handler(page)


def is_port_open(port: int, *, host: str = "127.0.0.1", timeout: float = 0.2) -> bool:
    """Return ``True`` if a TCP connection to ``host:port`` succeeds within ``timeout``.

    Useful for probing whether a Chrome instance is already listening on
    its remote-debugging port before deciding to launch a new one.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def launch_chrome_with_debug_port(
    *,
    url: str,
    user_data_dir: str | Path,
    debug_port: int,
    wait_seconds: float = 2.0,
) -> None:
    """Launch Google Chrome with ``--remote-debugging-port`` for CDP attach.

    macOS-only: uses ``open -na "Google Chrome" --args ...``. Creates
    ``user_data_dir`` if missing and sleeps ``wait_seconds`` to let
    Chrome bind to the port before the caller attempts to connect.

    Pair with :func:`connected_browser_page` for half-automatic flows
    where the user completes a login (passkey, SMS, captcha) in a real
    Chrome window before scripts take over.
    """
    if sys.platform != "darwin":
        raise RuntimeError("launch_chrome_with_debug_port() is macOS-only and requires the `open` command.")

    profile = Path(user_data_dir)
    profile.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "open",
            "-na",
            "Google Chrome",
            "--args",
            f"--remote-debugging-port={debug_port}",
            f"--user-data-dir={profile}",
            "--new-window",
            url,
        ],
        check=True,
    )
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def find_or_open_page(
    context: BrowserContext,
    url: str,
    *,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 30_000,
) -> Page:
    """Return an existing page in ``context`` whose URL starts with ``url``, else open a new one.

    When attaching to a real Chrome via CDP, the user may already have
    the target page open from a prior step (e.g. after a passkey login).
    This helper reuses that page when possible, only navigating when no
    matching tab is found.
    """
    for page in context.pages:
        if page.url.startswith(url):
            return page
    page = context.new_page()
    page.goto(url, wait_until=wait_until, timeout=timeout_ms)
    return page


@contextmanager
def connected_browser_page(
    cdp_url: str,
    *,
    target_url: str | None = None,
    bring_to_front: bool = True,
) -> Iterator[Page]:
    """Connect to an existing Chrome via CDP and yield a ``Page``.

    Args:
        cdp_url: DevTools endpoint, e.g. ``"http://127.0.0.1:9222"``.
        target_url: If set, locate or open a page whose URL starts with
            this value. Otherwise yield the last page in the first
            context (matches expense's historical behavior).
        bring_to_front: Call ``page.bring_to_front()`` after attaching.

    On exit, the Playwright Browser handle is closed — this only
    disconnects the CDP session and does **not** kill the underlying
    Chrome process.
    """
    sync_playwright = _load_sync_playwright()
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(cdp_url)
        try:
            context = browser.contexts[0]
            if target_url is not None:
                page = find_or_open_page(context, target_url)
            else:
                page = context.pages[-1] if context.pages else context.new_page()
            if bring_to_front:
                page.bring_to_front()
            yield page
        finally:
            browser.close()
