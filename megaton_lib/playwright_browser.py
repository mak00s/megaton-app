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


def save_page_storage_state(page: Page, storage_state_path: str | Path) -> Path:
    """Persist the current Playwright context storage_state and return the path.

    Use this when a caller must decide the safe save timing itself, such as
    after a successful login but not after a failed login attempt.
    """
    state_path = Path(storage_state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    page.context.storage_state(path=str(state_path))
    return state_path


@contextmanager
def browser_page(
    *,
    headless: bool = True,
    locale: str = "ja-JP",
    user_data_dir: str | Path | None = None,
    storage_state_path: str | Path | None = None,
    save_storage_state: bool = False,
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
        storage_state_path: Optional Playwright storage_state JSON path.
            If the file exists and ``user_data_dir`` is not set, it is
            loaded into the fresh context. When ``save_storage_state`` is
            true, the current context state is written back to this path
            before the context is closed.
        save_storage_state: Persist the context storage state on exit.
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
    state_path = Path(storage_state_path) if storage_state_path is not None else None
    if state_path is not None and state_path.exists() and user_data_dir is None:
        extra["storage_state"] = str(state_path)
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
                if save_storage_state and state_path is not None:
                    save_page_storage_state(page, state_path)
                context.close()
        else:
            browser = pw.chromium.launch(headless=headless)
            try:
                context = browser.new_context(**extra)
                try:
                    page = context.new_page()
                    yield page
                finally:
                    if save_storage_state and state_path is not None:
                        state_path.parent.mkdir(parents=True, exist_ok=True)
                        context.storage_state(path=str(state_path))
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
    storage_state_path: str | Path | None = None,
    save_storage_state: bool = False,
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
        storage_state_path=storage_state_path,
        save_storage_state=save_storage_state,
        user_agent=user_agent,
        timezone_id=timezone_id,
        viewport=viewport,
        context_kwargs=context_kwargs,
    ) as page:
        page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        if wait_selector:
            page.wait_for_selector(wait_selector, timeout=timeout_ms)
        return handler(page)


class CanvasClipScreenshotter:
    """Capture fixed clip rectangles relative to a page canvas.

    This is useful for report workflows where the source is an authenticated
    browser UI such as Google Sheets or Looker Studio and the stable contract is
    "open URL, wait for canvas, crop x/y/width/height from that canvas".
    """

    def __init__(
        self,
        *,
        screenshot_dir: str | Path,
        storage_state_path: str | Path | None = None,
        headless: bool = True,
        user_data_dir: str | Path | None = None,
        login_timeout_ms: int = 60_000,
        width: int = 1400,
        height: int = 830,
        locale: str = "ja-JP",
        timezone_id: str | None = "Asia/Tokyo",
        canvas_selector: str = "canvas",
        ready_url_prefix: str | None = "https://docs.google.com/spreadsheets/",
        login_url_part: str = "accounts.google.com",
    ) -> None:
        self.screenshot_dir = Path(screenshot_dir).expanduser()
        self.storage_state_path = (
            Path(storage_state_path).expanduser() if storage_state_path is not None else None
        )
        self.headless = bool(headless)
        self.user_data_dir = Path(user_data_dir).expanduser() if user_data_dir is not None else None
        self.login_timeout_ms = int(login_timeout_ms)
        self.width = int(width)
        self.height = int(height)
        self.locale = locale
        self.timezone_id = timezone_id
        self.canvas_selector = canvas_selector
        self.ready_url_prefix = ready_url_prefix
        self.login_url_part = login_url_part
        self._page_context: Any = None
        self.page: Page | None = None

    def __enter__(self) -> "CanvasClipScreenshotter":
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        if self.storage_state_path is not None:
            self.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        self._page_context = browser_page(
            headless=self.headless,
            locale=self.locale,
            user_data_dir=self.user_data_dir,
            storage_state_path=self.storage_state_path,
            save_storage_state=self.storage_state_path is not None,
            timezone_id=self.timezone_id,
            viewport={"width": self.width, "height": self.height},
        )
        self.page = self._page_context.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._page_context is not None:
            self._page_context.__exit__(exc_type, exc, tb)
            self._page_context = None
            self.page = None

    def open(
        self,
        url: str,
        *,
        wait_until: str = "domcontentloaded",
        timeout_ms: int = 30_000,
        settle_ms: int = 800,
    ) -> None:
        page = self._require_page()
        page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        page.wait_for_timeout(settle_ms)
        self._wait_until_ready()

    def click_zoom_menu(
        self,
        percent: str,
        *,
        menu_selector: str = 'div[aria-label^="ズーム"], div[aria-label^="Zoom"]',
        item_selector_template: str = 'div.goog-menuitem-content:text("{percent}")',
        timeout_ms: int = 10_000,
        settle_ms: int = 300,
    ) -> None:
        page = self._require_page()
        page.locator(menu_selector).first.click(timeout=timeout_ms)
        page.wait_for_timeout(settle_ms)
        page.locator(item_selector_template.format(percent=percent)).first.click(timeout=timeout_ms)
        page.wait_for_timeout(settle_ms)

    def screenshot_canvas_clip(
        self,
        *,
        path: str | Path,
        offset: Mapping[str, int | float],
        url: str | None = None,
        zoom: str | None = None,
        pre_wait_ms: int = 500,
    ) -> str:
        page = self._require_page()
        if url is not None:
            self.open(url)
        if zoom:
            self.click_zoom_menu(zoom)
        page.wait_for_timeout(pre_wait_ms)
        page.wait_for_selector(self.canvas_selector, state="attached", timeout=30_000)
        canvas = page.locator(self.canvas_selector).first
        canvas.scroll_into_view_if_needed()
        box = canvas.bounding_box()
        if not box:
            raise RuntimeError(f"Canvas bounding box not found for selector: {self.canvas_selector}")

        save_to = self.screenshot_dir / Path(path).name
        clip_area = {
            "x": float(box["x"] + offset["x"]),
            "y": float(box["y"] + offset["y"]),
            "width": float(offset["width"]),
            "height": float(offset["height"]),
        }
        page.screenshot(path=str(save_to), clip=clip_area)
        return str(save_to)

    def screenshot_pair_same_page(
        self,
        *,
        first_path: str | Path,
        first_offset: Mapping[str, int | float],
        second_path: str | Path,
        second_offset: Mapping[str, int | float],
        delay_ms: int = 250,
    ) -> tuple[str, str]:
        first_saved = self.screenshot_canvas_clip(path=first_path, offset=first_offset, pre_wait_ms=0)
        page = self._require_page()
        page.wait_for_timeout(delay_ms)
        second_saved = self.screenshot_canvas_clip(path=second_path, offset=second_offset, pre_wait_ms=0)
        return first_saved, second_saved

    def _require_page(self) -> Page:
        if self.page is None:
            raise RuntimeError("CanvasClipScreenshotter must be used as a context manager.")
        return self.page

    def _wait_until_ready(self) -> None:
        page = self._require_page()
        ready_page = self._find_ready_page()
        if ready_page is not None:
            self.page = ready_page
            self.page.bring_to_front()
            self._wait_for_canvas()
            return

        if self.login_url_part not in page.url:
            self._wait_for_canvas()
            return

        if self.headless:
            raise RuntimeError(
                "Login is required for canvas screenshot capture. "
                "Refresh storage state in a headful run, then rerun headless."
            )

        logger.info("Login required. Complete login in the opened browser window.")
        deadline = time.time() + (self.login_timeout_ms / 1000)
        while time.time() < deadline:
            ready_page = self._find_ready_page()
            if ready_page is not None:
                self.page = ready_page
                self.page.bring_to_front()
                self._wait_for_canvas()
                return
            page.wait_for_timeout(500)
        raise RuntimeError("Timed out waiting for canvas page after login.")

    def _find_ready_page(self) -> Page | None:
        page = self._require_page()
        if self.ready_url_prefix is None:
            return None
        for candidate in page.context.pages:
            if candidate.url.startswith(self.ready_url_prefix):
                return candidate
        return None

    def _wait_for_canvas(self) -> None:
        page = self._require_page()
        try:
            page.wait_for_selector(self.canvas_selector, state="attached", timeout=10_000)
            page.wait_for_timeout(300)
        except Exception:
            pass


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
