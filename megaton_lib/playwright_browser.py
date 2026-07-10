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

import contextlib
import datetime as dt
import json
import logging
import re
import socket
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from megaton_lib.tz_utils import JST

if TYPE_CHECKING:
    from playwright.async_api import Page as AsyncPage
    from playwright.sync_api import BrowserContext, Page

logger = logging.getLogger(__name__)


_PLAYWRIGHT_MISSING_MSG = (
    "playwright is not installed; install the 'playwright' extra "
    "(e.g. `pip install megaton-app[playwright]`) and run "
    "`playwright install chromium`"
)

# Launch arg + init script that hide the headless-automation fingerprint.
# ``--disable-blink-features=AutomationControlled`` stops Chromium from
# auto-setting ``navigator.webdriver``; the init script is a redundant
# belt-and-braces override that also covers frames spawned later. This is
# the canonical signal bot-detection (incl. Adobe Analytics Bot Rules)
# keys off. Note: ``stealth`` here does NOT set a user-agent — pass
# ``user_agent`` explicitly when a site also screens UA strings.
_STEALTH_LAUNCH_ARG = "--disable-blink-features=AutomationControlled"
_STEALTH_INIT_SCRIPT = (
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
)


def _load_sync_playwright() -> Any:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - exercised when extras missing
        raise RuntimeError(_PLAYWRIGHT_MISSING_MSG) from exc
    return sync_playwright


def _load_async_playwright() -> Any:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - exercised when extras missing
        raise RuntimeError(_PLAYWRIGHT_MISSING_MSG) from exc
    return async_playwright


def save_page_storage_state(page: Page, storage_state_path: str | Path) -> Path:
    """Persist the current Playwright context storage_state and return the path.

    Use this when a caller must decide the safe save timing itself, such as
    after a successful login but not after a failed login attempt.
    """
    state_path = Path(storage_state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    page.context.storage_state(path=str(state_path))
    return state_path


async def async_save_page_storage_state(page: AsyncPage, storage_state_path: str | Path) -> Path:
    """Async variant of :func:`save_page_storage_state`."""
    state_path = Path(storage_state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    await page.context.storage_state(path=str(state_path))
    return state_path


def load_storage_state(storage_state_path: str | Path) -> dict[str, Any]:
    """Read and validate a Playwright storage_state JSON file.

    The read-side counterpart to :func:`save_page_storage_state`. Both
    ``browser_page`` (via ``storage_state_path``) and
    ``new_context(storage_state=...)`` can consume a path directly, but
    this helper validates the file up front (exists + is a JSON object)
    and returns the parsed dict so callers can inspect or merge it before
    handing it to a context.

    Raises:
        FileNotFoundError: If the path does not exist.
        ValueError: If the file is not a JSON object.
    """
    path = Path(storage_state_path)
    if not path.exists():
        raise FileNotFoundError(f"storage_state file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(
            f"storage_state must be a JSON object, got {type(data).__name__}: {path}"
        )
    return data


def wait_for_url_not_contains(
    page: Page,
    url_part: str,
    *,
    timeout_ms: int = 60_000,
    poll_ms: int = 1_000,
    wait_load_state: str | None = "domcontentloaded",
) -> bool:
    """Poll until ``url_part`` is no longer present in ``page.url``.

    This is intentionally small and generic for browser flows where a user may
    need to complete Google/Box/SAML login in a headed window before automation
    can continue.
    """
    deadline = time.monotonic() + max(timeout_ms, 1) / 1000
    poll_ms = max(poll_ms, 1)
    while True:
        if url_part not in page.url:
            if wait_load_state:
                _wait_for_load_state(page, wait_load_state, timeout_ms=_remaining_timeout_ms(deadline))
            return True
        remaining_ms = _remaining_timeout_ms(deadline)
        if remaining_ms <= 0:
            return False
        page.wait_for_timeout(min(poll_ms, remaining_ms))


async def async_wait_for_url_not_contains(
    page: AsyncPage,
    url_part: str,
    *,
    timeout_ms: int = 60_000,
    poll_ms: int = 1_000,
    wait_load_state: str | None = "domcontentloaded",
) -> bool:
    """Async variant of :func:`wait_for_url_not_contains`."""
    deadline = time.monotonic() + max(timeout_ms, 1) / 1000
    poll_ms = max(poll_ms, 1)
    while True:
        if url_part not in page.url:
            if wait_load_state:
                await _async_wait_for_load_state(
                    page,
                    wait_load_state,
                    timeout_ms=_remaining_timeout_ms(deadline),
                )
            return True
        remaining_ms = _remaining_timeout_ms(deadline)
        if remaining_ms <= 0:
            return False
        await page.wait_for_timeout(min(poll_ms, remaining_ms))


WaitUntil = Literal["commit", "domcontentloaded", "load", "networkidle"]


def _failure_artifact_base(label: str, dir: str | Path) -> Path:
    out_dir = Path(dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    name = f"{label}-{dt.datetime.now(JST).strftime('%m%d_%H%M%S')}"
    return out_dir / name


def activate_app(app_name: str = "Google Chrome") -> None:
    """Best-effort foregrounding for a macOS app; no-op elsewhere."""
    if sys.platform != "darwin":
        return
    with contextlib.suppress(Exception):
        subprocess.run(
            ["osascript", "-e", f'tell application "{app_name}" to activate'],
            check=False,
        )


def _async_timeout_error():
    try:
        from playwright.async_api import TimeoutError as PWTimeoutError
    except ImportError as exc:  # pragma: no cover - exercised when extras missing
        raise RuntimeError(_PLAYWRIGHT_MISSING_MSG) from exc
    return PWTimeoutError


def is_transient_playwright_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "target page, context or browser has been closed",
            "frame was detached",
            "net::err_aborted",
            "net::err_ssl_protocol_error",
            "execution context was destroyed",
            "interrupted by another navigation",
        )
    )


async def goto_with_retries(
    page: AsyncPage,
    url: str,
    *,
    goto: Callable[..., Awaitable[bool]] | None = None,
    timeouts: tuple[int, ...] = (30_000,),
    wait_until: WaitUntil | None = "domcontentloaded",
) -> bool:
    """Try ``url`` once per entry in ``timeouts``.

    With a ``goto`` callback (a wrapper returning bool, e.g. one that swallows
    transient errors) a failed attempt returns False after the last timeout.
    Without one, ``page.goto`` is called directly and a navigation failure
    RAISES instead of returning False — the two modes are intentionally
    asymmetric.
    """
    for timeout in timeouts:
        if goto is not None:
            ok = await goto(url, timeout=timeout, wait_until=wait_until)
        else:
            await page.goto(url, timeout=timeout, wait_until=wait_until)
            ok = True
        if ok:
            return True
    return False


async def click_with_retries(
    locator,
    *,
    attempts: int = 3,
    timeout: int = 3000,
    on_retry: Callable[[], Awaitable[None]] | None = None,
    force_on_last: bool = False,
    js_on_last: bool = False,
) -> bool:
    PWTimeoutError = _async_timeout_error()
    for attempt in range(attempts):
        try:
            await locator.click(timeout=timeout)
            return True
        except PWTimeoutError:
            if attempt == attempts - 1:
                if force_on_last:
                    try:
                        await locator.click(timeout=timeout, force=True)
                        return True
                    except PWTimeoutError:
                        pass
                if js_on_last:
                    handle = await locator.element_handle()
                    if handle is not None:
                        await handle.evaluate("(el) => el.click()")
                        return True
                raise
            if on_retry is not None:
                await on_retry()
    return False


async def settle_page(
    page: AsyncPage,
    *,
    state: WaitUntil = "domcontentloaded",
    timeout: int = 3_000,
    delay_ms: int = 0,
) -> bool:
    PWTimeoutError = _async_timeout_error()
    try:
        await page.wait_for_load_state(state=state, timeout=timeout)
        settled = True
    except PWTimeoutError:
        settled = False
    if delay_ms:
        await page.wait_for_timeout(delay_ms)
    return settled


async def wait_for_url_change(page: AsyncPage, previous_url: str, *, timeout: int = 1_500) -> bool:
    """Wait until the current page URL differs from previous_url."""
    PWTimeoutError = _async_timeout_error()
    try:
        await page.wait_for_function(
            "url => window.location.href !== url",
            arg=previous_url,
            timeout=timeout,
        )
        return True
    except PWTimeoutError:
        return page.url != previous_url


async def save_failure_artifact(page: AsyncPage | None, label: str, dir: str | Path) -> Path | None:
    """Save a screenshot + HTML + URL on failure for post-mortem.

    The timestamp is JST (not runner-local) so CI artifacts sort with the
    operator's timezone. Filenames are built by string concatenation, not
    ``with_suffix``: labels can legitimately contain dots (e.g. an object
    repr) and ``with_suffix`` would replace everything after the last one.
    """
    try:
        if page is None:
            return None
        base = _failure_artifact_base(label, dir)
        await page.screenshot(path=f"{base}.png", timeout=5000)
        with contextlib.suppress(Exception):
            Path(f"{base}.html").write_text(await page.content())
        Path(f"{base}.url.txt").write_text(page.url)
        return base
    except Exception:
        return None


def save_failure_artifact_sync(
    page: Page | None,
    label: str,
    dir: str | Path,
    *,
    full_page: bool = True,
) -> Path | None:
    """Sync variant of :func:`save_failure_artifact`.

    ``full_page`` defaults to True to preserve consumer repos that used full
    page diagnostic screenshots before this helper existed.
    """
    try:
        if page is None:
            return None
        base = _failure_artifact_base(label, dir)
        page.screenshot(path=f"{base}.png", full_page=full_page, timeout=5000)
        with contextlib.suppress(Exception):
            Path(f"{base}.html").write_text(page.content())
        Path(f"{base}.url.txt").write_text(page.url)
        return base
    except Exception:
        return None


def _remaining_timeout_ms(deadline: float) -> int:
    return max(0, int((deadline - time.monotonic()) * 1000))


def _wait_for_load_state(page: Page, state: str, *, timeout_ms: int) -> None:
    try:
        page.wait_for_load_state(state, timeout=max(timeout_ms, 1))
    except TypeError:
        page.wait_for_load_state(state)


async def _async_wait_for_load_state(page: AsyncPage, state: str, *, timeout_ms: int) -> None:
    try:
        await page.wait_for_load_state(state, timeout=max(timeout_ms, 1))
    except TypeError:
        await page.wait_for_load_state(state)


@contextmanager
def browser_page(
    *,
    headless: bool = True,
    locale: str | None = "ja-JP",
    browser_channel: str | None = None,
    launch_args: list[str] | None = None,
    slow_mo: int = 0,
    stealth: bool = False,
    user_data_dir: str | Path | None = None,
    storage_state_path: str | Path | None = None,
    save_storage_state: bool = False,
    user_agent: str | None = None,
    timezone_id: str | None = None,
    viewport: Mapping[str, int] | None = None,
    accept_downloads: bool | None = None,
    device_name: str | None = None,
    context_kwargs: Mapping[str, Any] | None = None,
) -> Iterator[Page]:
    """Yield a Playwright ``Page`` and clean up the browser on exit.

    Args:
        headless: Launch Chromium headless. Default ``True``.
        locale: Browser locale; passed to the context. Pass ``None`` to
            leave the locale unset (Playwright default).
        slow_mo: Slow each Playwright operation by this many milliseconds.
        stealth: Hide the automation fingerprint (``navigator.webdriver``)
            via a launch flag + init script. Does NOT set a user-agent —
            pass ``user_agent`` when a site also screens UA strings.
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

    state_path = Path(storage_state_path) if storage_state_path is not None else None

    with sync_playwright() as pw:
        extra = _build_context_options(
            devices=pw.devices,
            locale=locale,
            device_name=device_name,
            storage_state_path=state_path,
            use_storage_state=user_data_dir is None,
            user_agent=user_agent,
            timezone_id=timezone_id,
            viewport=viewport,
            accept_downloads=accept_downloads,
            context_kwargs=context_kwargs,
        )
        launch_kwargs = _build_launch_options(
            headless=headless,
            browser_channel=browser_channel,
            launch_args=launch_args,
            slow_mo=slow_mo,
            stealth=stealth,
        )
        if user_data_dir is not None:
            profile = Path(user_data_dir)
            profile.mkdir(parents=True, exist_ok=True)
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(profile),
                **launch_kwargs,
                **extra,
            )
            try:
                if stealth:
                    context.add_init_script(_STEALTH_INIT_SCRIPT)
                page = context.pages[0] if context.pages else context.new_page()
                yield page
            finally:
                if save_storage_state and state_path is not None:
                    save_page_storage_state(page, state_path)
                context.close()
        else:
            browser = _launch_sync_browser(pw.chromium, launch_kwargs)
            try:
                context = browser.new_context(**extra)
                try:
                    if stealth:
                        context.add_init_script(_STEALTH_INIT_SCRIPT)
                    page = context.new_page()
                    yield page
                finally:
                    if save_storage_state and state_path is not None:
                        state_path.parent.mkdir(parents=True, exist_ok=True)
                        context.storage_state(path=str(state_path))
                    context.close()
            finally:
                browser.close()


@asynccontextmanager
async def async_browser_page(
    *,
    headless: bool = True,
    locale: str | None = "ja-JP",
    browser_channel: str | None = None,
    launch_args: list[str] | None = None,
    slow_mo: int = 0,
    stealth: bool = False,
    user_data_dir: str | Path | None = None,
    storage_state_path: str | Path | None = None,
    save_storage_state: bool = False,
    user_agent: str | None = None,
    timezone_id: str | None = None,
    viewport: Mapping[str, int] | None = None,
    accept_downloads: bool | None = None,
    device_name: str | None = None,
    context_kwargs: Mapping[str, Any] | None = None,
) -> Iterator[AsyncPage]:
    """Async variant of :func:`browser_page` for existing async Playwright flows."""
    async_playwright = _load_async_playwright()
    state_path = Path(storage_state_path) if storage_state_path is not None else None

    async with async_playwright() as pw:
        extra = _build_context_options(
            devices=pw.devices,
            locale=locale,
            device_name=device_name,
            storage_state_path=state_path,
            use_storage_state=user_data_dir is None,
            user_agent=user_agent,
            timezone_id=timezone_id,
            viewport=viewport,
            accept_downloads=accept_downloads,
            context_kwargs=context_kwargs,
        )
        launch_kwargs = _build_launch_options(
            headless=headless,
            browser_channel=browser_channel,
            launch_args=launch_args,
            slow_mo=slow_mo,
            stealth=stealth,
        )
        if user_data_dir is not None:
            profile = Path(user_data_dir)
            profile.mkdir(parents=True, exist_ok=True)
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(profile),
                **launch_kwargs,
                **extra,
            )
            try:
                if stealth:
                    await context.add_init_script(_STEALTH_INIT_SCRIPT)
                page = context.pages[0] if context.pages else await context.new_page()
                yield page
            finally:
                if save_storage_state and state_path is not None:
                    await async_save_page_storage_state(page, state_path)
                await context.close()
        else:
            browser = await _launch_async_browser(pw.chromium, launch_kwargs)
            try:
                context = await browser.new_context(**extra)
                try:
                    if stealth:
                        await context.add_init_script(_STEALTH_INIT_SCRIPT)
                    page = await context.new_page()
                    yield page
                finally:
                    if save_storage_state and state_path is not None:
                        await async_save_page_storage_state(page, state_path)
                    await context.close()
            finally:
                await browser.close()


async def open_async_browser_context(
    playwright: Any,
    *,
    headless: bool = True,
    browser_channel: str | None = None,
    launch_args: list[str] | None = None,
    user_data_dir: str | Path | None = None,
    storage_state_path: str | Path | None = None,
    device_name: str | None = None,
    locale: str | None = "ja-JP",
    timezone_id: str | None = None,
    viewport: Mapping[str, int] | None = None,
    accept_downloads: bool | None = None,
    context_kwargs: Mapping[str, Any] | None = None,
    cdp_url: str | None = None,
) -> Any:
    """Open an async BrowserContext using the shared launch/CDP policy.

    The caller owns the passed Playwright instance and must close the returned
    context and call ``playwright.stop()``. Stopping that caller-owned instance
    also releases a CDP transport created by this helper; no separate Browser
    handle is required. This shape supports long-lived task objects such as
    poimak4's runner while keeping launch configuration centralized.
    """
    extra = _build_context_options(
        devices=playwright.devices,
        locale=locale,
        device_name=device_name,
        storage_state_path=Path(storage_state_path) if storage_state_path else None,
        use_storage_state=user_data_dir is None,
        user_agent=None,
        timezone_id=timezone_id,
        viewport=viewport,
        accept_downloads=accept_downloads,
        context_kwargs=context_kwargs,
    )
    if cdp_url:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
        if browser.contexts:
            return browser.contexts[0]
        return await browser.new_context(**extra)
    launch_kwargs = _build_launch_options(
        headless=headless,
        browser_channel=browser_channel,
        launch_args=launch_args,
    )
    if user_data_dir is not None:
        profile = Path(user_data_dir)
        profile.mkdir(parents=True, exist_ok=True)
        return await playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile),
            **launch_kwargs,
            **extra,
        )
    browser = await _launch_async_browser(playwright.chromium, launch_kwargs)
    return await browser.new_context(**extra)


def _build_launch_options(
    *,
    headless: bool,
    browser_channel: str | None,
    launch_args: list[str] | None,
    slow_mo: int = 0,
    stealth: bool = False,
) -> dict[str, Any]:
    options: dict[str, Any] = {"headless": headless}
    if browser_channel:
        options["channel"] = browser_channel
    if slow_mo:
        options["slow_mo"] = slow_mo
    args = list(launch_args) if launch_args else []
    if stealth:
        args.append(_STEALTH_LAUNCH_ARG)
    if args:
        options["args"] = args
    return options


def _build_context_options(
    *,
    devices: Mapping[str, Mapping[str, Any]],
    locale: str | None,
    device_name: str | None,
    storage_state_path: Path | None,
    use_storage_state: bool,
    user_agent: str | None,
    timezone_id: str | None,
    viewport: Mapping[str, int] | None,
    accept_downloads: bool | None,
    context_kwargs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    extra: dict[str, Any] = dict(devices[device_name]) if device_name else {}
    extra.pop("default_browser_type", None)
    if locale is not None:
        extra["locale"] = locale
    if storage_state_path is not None and storage_state_path.exists() and use_storage_state:
        extra["storage_state"] = str(storage_state_path)
    if user_agent is not None:
        extra["user_agent"] = user_agent
    if timezone_id is not None:
        extra["timezone_id"] = timezone_id
    if viewport is not None:
        extra["viewport"] = dict(viewport)
    if accept_downloads is not None:
        extra["accept_downloads"] = bool(accept_downloads)
    if context_kwargs:
        extra.update(context_kwargs)
    return extra


async def _launch_async_browser(chromium: Any, launch_kwargs: Mapping[str, Any]) -> Any:
    try:
        return await chromium.launch(**launch_kwargs)
    except Exception as exc:
        if "channel" not in launch_kwargs:
            raise
        fallback = dict(launch_kwargs)
        channel = fallback.pop("channel")
        logger.warning(
            "Could not launch Chromium channel %s; falling back to bundled Chromium: %s",
            channel,
            exc,
            exc_info=True,
        )
        return await chromium.launch(**fallback)


def _launch_sync_browser(chromium: Any, launch_kwargs: Mapping[str, Any]) -> Any:
    try:
        return chromium.launch(**launch_kwargs)
    except Exception as exc:
        if "channel" not in launch_kwargs:
            raise
        fallback = dict(launch_kwargs)
        channel = fallback.pop("channel")
        logger.warning(
            "Could not launch Chromium channel %s; falling back to bundled Chromium: %s",
            channel,
            exc,
            exc_info=True,
        )
        return chromium.launch(**fallback)


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


def ensure_chrome_cdp(
    *,
    port: int,
    user_data_dir: str | Path,
    start_url: str = "about:blank",
    timeout: float = 10.0,
    allow_unverified: bool = False,
) -> str:
    """Ensure a normal Chrome instance is reachable over CDP; return its URL.

    Idempotent: an existing listener is reused only when its command line has
    an exact ``--user-data-dir`` match. Otherwise Chrome is launched WITHOUT
    Playwright automation flags (for sites that reject automated Chrome) and
    polled until the debug port is ready. Ownership checks fail closed unless
    ``allow_unverified`` is explicitly enabled.
    """
    url = f"http://127.0.0.1:{port}"
    profile_dir = Path(user_data_dir).resolve()
    if _cdp_ready(url):
        assert_cdp_profile_owner(
            url,
            profile_dir,
            allow_unverified=allow_unverified,
        )
        return url

    profile_dir.mkdir(parents=True, exist_ok=True)
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    subprocess.Popen(
        [
            chrome,
            "--remote-debugging-address=127.0.0.1",
            f"--remote-debugging-port={port}",
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--new-window",
            start_url,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _cdp_ready(url):
            assert_cdp_profile_owner(
                url,
                profile_dir,
                allow_unverified=allow_unverified,
            )
            return url
        time.sleep(0.25)
    raise RuntimeError(f"Chrome CDP did not become ready: {url}")


def _cdp_ready(url: str) -> bool:
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"{url}/json/version", timeout=0.5) as res:
            return res.status == 200
    except (OSError, urllib.error.URLError):
        return False


def cdp_command_uses_profile(command: str, user_data_dir: str | Path) -> bool:
    """Return whether a process command has the exact Chrome profile argument."""
    expected = re.escape(str(Path(user_data_dir).resolve()))
    pattern = rf"(?:^|\s)--user-data-dir(?:=|\s+)(?P<q>['\"]?){expected}(?P=q)(?=\s|$)"
    return re.search(pattern, command) is not None


def local_cdp_listener_commands(cdp_url: str) -> list[str]:
    """Return local listener command lines for a loopback CDP endpoint."""
    parsed = urlsplit(cdp_url)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError(f"CDP endpoint is not local: {cdp_url}")
    if parsed.port is None:
        raise ValueError(f"CDP endpoint has no port: {cdp_url}")

    try:
        pids = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{parsed.port}", "-sTCP:LISTEN", "-t"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.split()
        commands = []
        for pid in pids:
            command = subprocess.run(
                ["ps", "-ww", "-o", "command=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            ).stdout.strip()
            if command:
                commands.append(command)
        return commands
    except (OSError, subprocess.SubprocessError):
        return []


def assert_cdp_profile_owner(
    cdp_url: str,
    user_data_dir: str | Path,
    *,
    allow_unverified: bool = False,
    allow_remote: bool = False,
) -> None:
    """Require a CDP endpoint to belong to the expected Chrome profile.

    Local listeners are matched by an exact ``--user-data-dir`` process
    argument. Remote endpoints cannot be verified from the local process table
    and therefore require explicit ``allow_remote=True``. Missing process
    metadata fails closed unless ``allow_unverified=True`` is explicitly set,
    which also covers intentionally trusted SSH tunnel listeners.
    """
    parsed = urlsplit(cdp_url)
    is_local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if not is_local:
        if allow_remote:
            return
        raise RuntimeError(
            f"cannot verify remote CDP profile owner: {cdp_url}; "
            "set allow_remote=True only for an explicitly trusted endpoint"
        )

    commands = local_cdp_listener_commands(cdp_url)
    if any(cdp_command_uses_profile(command, user_data_dir) for command in commands):
        return
    if allow_unverified:
        return
    expected = Path(user_data_dir).resolve()
    if not commands:
        raise RuntimeError(
            f"cannot identify the process listening at {cdp_url}; expected profile: {expected}"
        )
    raise RuntimeError(
        f"CDP endpoint {cdp_url} belongs to a different Chrome profile; expected: {expected}"
    )


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


def _norm_cdp_hosts(host: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if not host:
        return []
    return [host] if isinstance(host, str) else list(host)


def cdp_url_matches_host(url: str, hosts: str | list[str] | tuple[str, ...] | None) -> bool:
    """Return whether ``url`` belongs to one of the exact hosts/subdomains."""
    hostname = (urlsplit(url or "").hostname or "").lower().rstrip(".")
    if not hostname:
        return False
    for host in _norm_cdp_hosts(hosts):
        normalized = host.lower().rstrip(".")
        if hostname == normalized or hostname.endswith(f".{normalized}"):
            return True
    return False


def plan_cdp_active_and_duplicates(
    urls: list[str],
    match: str | list[str],
    cleanup_host: str | list[str] | None,
) -> tuple[int | None, list[int]]:
    """Pick the best CDP tab and same-site duplicates to close.

    ``match`` may be priority ordered. This avoids attaching to a stale broad
    domain tab when a more specific authenticated page is already open.
    """
    hosts = _norm_cdp_hosts(cleanup_host)
    patterns = [match] if isinstance(match, str) else list(match)
    active: int | None = None
    for pattern in patterns:
        hits = [i for i, url in enumerate(urls) if pattern in (url or "")]
        if hits:
            active = hits[0]
            break
    if active is None:
        return None, []
    matching_any = {i for i, url in enumerate(urls) if any(pattern in (url or "") for pattern in patterns)}
    close = matching_any - {active}
    if hosts:
        close |= {i for i, url in enumerate(urls) if i != active and cdp_url_matches_host(url, hosts)}
    return active, sorted(close)


def cdp_host_pages(
    pages: list[Any],
    cleanup_host: str | list[str] | None,
    *,
    kept_page: Any | None = None,
    keep_kept: bool = True,
) -> list[Any]:
    hosts = _norm_cdp_hosts(cleanup_host)
    if not hosts:
        return []
    return [
        page
        for page in pages
        if (not keep_kept or page is not kept_page)
        and cdp_url_matches_host(getattr(page, "url", "") or "", hosts)
    ]


# Compatibility aliases for consumers released before these helpers became public.
_cdp_host_match = cdp_url_matches_host
_plan_cdp_active_and_dups = plan_cdp_active_and_duplicates
_cdp_host_pages = cdp_host_pages


def _close_cdp_pages(pages: list[Any]) -> int:
    closed = 0
    interrupt: BaseException | None = None
    for page in pages:
        try:
            page.close()
            closed += 1
        except (KeyboardInterrupt, SystemExit) as exc:
            interrupt = exc
        except Exception:  # noqa: BLE001 - tab cleanup must be best-effort
            logger.debug("[cdp] could not close tab %s", getattr(page, "url", "?"), exc_info=True)
    if interrupt is not None:
        raise interrupt
    return closed


def _find_or_open_cdp_page(
    browser: Any,
    *,
    target_url: str | None,
    match: str | list[str] | None,
    cleanup_host: str | list[str] | None,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 30_000,
) -> tuple[Page, int]:
    contexts = browser.contexts
    context = contexts[0] if contexts else browser.new_context()
    pages = [page for ctx in contexts for page in ctx.pages]
    if not pages:
        pages = list(context.pages)
    if match is not None:
        urls = [getattr(page, "url", "") or "" for page in pages]
        active_i, dup_i = plan_cdp_active_and_duplicates(urls, match, cleanup_host)
        if active_i is not None:
            active = pages[active_i]
            closed = _close_cdp_pages([pages[i] for i in dup_i])
            logger.info("[cdp] attached to tab %s%s", getattr(active, "url", "?"),
                        f" (closed {closed} stale tab(s))" if closed else "")
            return active, closed
        if target_url is None:
            # A caller that asked for a match and gave no URL to open must not
            # be handed an arbitrary tab — a stale/wrong tab parsed as if fresh
            # is exactly the failure mode CDP scrapers guard against.
            raise RuntimeError(
                f"no open tab matching {match!r}. Open the target site and log "
                "in, then re-run (or pass target_url to open it automatically)."
            )
    elif target_url is not None:
        for existing_page in pages:
            if (getattr(existing_page, "url", "") or "").startswith(target_url):
                return existing_page, 0
    if target_url is not None:
        page = context.new_page()
        page.goto(target_url, wait_until=wait_until, timeout=timeout_ms)
        stale = cdp_host_pages(pages, cleanup_host, keep_kept=False)
        closed = _close_cdp_pages(stale)
        logger.info("[cdp] no matching tab; opened %s%s", target_url,
                    f" (closed {closed} stale tab(s))" if closed else "")
        return page, closed
    if pages:
        return pages[-1], 0
    return context.new_page(), 0


@contextmanager
def connected_browser_page(
    cdp_url: str,
    *,
    target_url: str | None = None,
    match: str | list[str] | None = None,
    cleanup_host: str | list[str] | None = None,
    keep_open_on_success: bool = True,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 30_000,
    bring_to_front: bool = True,
) -> Iterator[Page]:
    """Connect to an existing Chrome via CDP and yield a ``Page``.

    Args:
        cdp_url: DevTools endpoint, e.g. ``"http://127.0.0.1:9222"``.
        target_url: If set, locate or open a page whose URL starts with
            this value. Otherwise yield the last page in the first
            context (matches expense's historical behavior).
        match: URL substring or priority-ordered substrings used to pick an
            existing tab. Defaults to ``target_url`` for backward compatibility.
        cleanup_host: Host/domain substring(s) whose duplicate/stale tabs should
            be closed before and after the yielded block.
        keep_open_on_success: Keep the yielded same-host tab after successful
            completion; on exceptions it is also pruned.
        bring_to_front: Call ``page.bring_to_front()`` after attaching.

    On exit, the Playwright Browser handle is closed — this only
    disconnects the CDP session and does **not** kill the underlying
    Chrome process.
    """
    sync_playwright = _load_sync_playwright()
    with sync_playwright() as pw:
        try:
            browser = pw.chromium.connect_over_cdp(cdp_url)
        except Exception as exc:  # noqa: BLE001 - surface an actionable hint
            raise RuntimeError(
                f"could not attach to Chrome at {cdp_url}. Launch it with "
                "--remote-debugging-port and log in first."
            ) from exc
        page = None
        success = False
        try:
            page, closed = _find_or_open_cdp_page(
                browser,
                target_url=target_url,
                match=match,
                cleanup_host=cleanup_host,
                wait_until=wait_until,
                timeout_ms=timeout_ms,
            )
            if bring_to_front:
                with contextlib.suppress(Exception):  # foregrounding is best-effort
                    page.bring_to_front()
            yield page
            success = True
        finally:
            try:
                pages = [pg for ctx in browser.contexts for pg in ctx.pages]
                stale = cdp_host_pages(
                    pages,
                    cleanup_host,
                    kept_page=page,
                    keep_kept=success and keep_open_on_success,
                )
                closed = _close_cdp_pages(stale)
                if closed:
                    logger.info("[cdp] cleanup: closed %d stale tab(s)", closed)
            except Exception:  # noqa: BLE001 - cleanup must not mask caller errors
                logger.debug("[cdp] cleanup failed", exc_info=True)
            browser.close()


async def _async_close_cdp_pages(pages: list[Any]) -> int:
    closed = 0
    interrupt: BaseException | None = None
    for page in pages:
        try:
            await page.close()
            closed += 1
        except (KeyboardInterrupt, SystemExit) as exc:
            interrupt = exc
        except Exception:  # noqa: BLE001 - tab cleanup must be best-effort
            logger.debug("[cdp] could not close async tab %s", getattr(page, "url", "?"), exc_info=True)
    if interrupt is not None:
        raise interrupt
    return closed


async def _async_find_or_open_cdp_page(
    browser: Any,
    *,
    target_url: str | None,
    match: str | list[str] | None,
    cleanup_host: str | list[str] | None,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 30_000,
) -> tuple[AsyncPage, int]:
    contexts = browser.contexts
    context = contexts[0] if contexts else await browser.new_context()
    pages = [page for ctx in contexts for page in ctx.pages]
    if not pages:
        pages = list(context.pages)
    if match is not None:
        urls = [getattr(page, "url", "") or "" for page in pages]
        active_i, dup_i = plan_cdp_active_and_duplicates(urls, match, cleanup_host)
        if active_i is not None:
            return pages[active_i], await _async_close_cdp_pages([pages[i] for i in dup_i])
        if target_url is None:
            raise RuntimeError(
                f"no open tab matching {match!r}. Open the target site and log "
                "in, then re-run (or pass target_url to open it automatically)."
            )
    elif target_url is not None:
        for existing_page in pages:
            if (getattr(existing_page, "url", "") or "").startswith(target_url):
                return existing_page, 0
    if target_url is not None:
        page = await context.new_page()
        await page.goto(target_url, wait_until=wait_until, timeout=timeout_ms)
        stale = cdp_host_pages(pages, cleanup_host, keep_kept=False)
        return page, await _async_close_cdp_pages(stale)
    if pages:
        return pages[-1], 0
    return await context.new_page(), 0


@asynccontextmanager
async def async_connected_browser_page(
    cdp_url: str,
    *,
    target_url: str | None = None,
    match: str | list[str] | None = None,
    cleanup_host: str | list[str] | None = None,
    keep_open_on_success: bool = True,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 30_000,
    bring_to_front: bool = True,
) -> Iterator[AsyncPage]:
    """Async CDP attach with the same stale-tab guarantees as the sync API."""
    async_playwright = _load_async_playwright()
    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(cdp_url)
        except Exception as exc:  # noqa: BLE001 - surface an actionable hint
            raise RuntimeError(
                f"could not attach to Chrome at {cdp_url}. Launch it with "
                "--remote-debugging-port and log in first."
            ) from exc
        page = None
        success = False
        try:
            page, _closed = await _async_find_or_open_cdp_page(
                browser,
                target_url=target_url,
                match=match,
                cleanup_host=cleanup_host,
                wait_until=wait_until,
                timeout_ms=timeout_ms,
            )
            if bring_to_front:
                with contextlib.suppress(Exception):
                    await page.bring_to_front()
            yield page
            success = True
        finally:
            try:
                pages = [pg for ctx in browser.contexts for pg in ctx.pages]
                stale = cdp_host_pages(
                    pages,
                    cleanup_host,
                    kept_page=page,
                    keep_kept=success and keep_open_on_success,
                )
                await _async_close_cdp_pages(stale)
            except Exception:  # noqa: BLE001 - cleanup must not mask caller errors
                logger.debug("[cdp] async cleanup failed", exc_info=True)
            with contextlib.suppress(Exception):
                await browser.close()
