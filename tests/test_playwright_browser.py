from __future__ import annotations

import builtins
import importlib
import socket
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from megaton_lib import playwright_browser


class FakePage:
    def __init__(self, url: str = "about:blank") -> None:
        self.url = url
        self.goto_calls: list[dict] = []
        self.wait_selector_calls: list[dict] = []
        self.brought_to_front = 0

    def goto(self, url, *, wait_until, timeout):
        self.url = url
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})

    def wait_for_selector(self, selector, *, timeout):
        self.wait_selector_calls.append({"selector": selector, "timeout": timeout})

    def bring_to_front(self):
        self.brought_to_front += 1


class FakeContext:
    def __init__(self, page: FakePage, *, prepopulated: bool = False) -> None:
        self.pages = [page] if prepopulated else []
        self._page = page
        self.closed = False
        self.kwargs: dict = {}

    def new_page(self):
        return self._page

    def close(self):
        self.closed = True


class FakeBrowser:
    def __init__(self, context: FakeContext) -> None:
        self._context = context
        self.closed = False
        self.new_context_kwargs: dict = {}

    def new_context(self, **kwargs):
        self.new_context_kwargs = kwargs
        return self._context

    def close(self):
        self.closed = True


class FakeChromium:
    def __init__(self) -> None:
        self.page = FakePage()
        self.context = FakeContext(self.page)
        self.persistent_context = FakeContext(self.page, prepopulated=True)
        self.browser = FakeBrowser(self.context)
        self.launch_kwargs: dict = {}
        self.launch_persistent_kwargs: dict = {}

    def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        return self.browser

    def launch_persistent_context(self, **kwargs):
        self.launch_persistent_kwargs = kwargs
        return self.persistent_context


class FakePlaywright:
    def __init__(self) -> None:
        self.chromium = FakeChromium()


@contextmanager
def _fake_sync_playwright_cm():
    yield FAKE_PW


FAKE_PW = FakePlaywright()


def _install_fake_sync_playwright(monkeypatch):
    """Make _load_sync_playwright return a fake sync_playwright()."""
    global FAKE_PW
    FAKE_PW = FakePlaywright()

    def fake_loader():
        return _fake_sync_playwright_cm

    monkeypatch.setattr(playwright_browser, "_load_sync_playwright", fake_loader)
    return FAKE_PW


def test_lazy_import_raises_runtime_error_when_playwright_missing(monkeypatch):
    """_load_sync_playwright should raise RuntimeError with a helpful hint."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("playwright"):
            raise ImportError("No module named 'playwright'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="playwright is not installed"):
        playwright_browser._load_sync_playwright()


def test_module_imports_without_playwright(monkeypatch):
    """Importing megaton_lib.playwright_browser must not require playwright."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("playwright"):
            raise ImportError("No module named 'playwright'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    sys.modules.pop("megaton_lib.playwright_browser", None)
    try:
        mod = importlib.import_module("megaton_lib.playwright_browser")
    finally:
        # Restore for subsequent tests.
        monkeypatch.setattr(builtins, "__import__", real_import)
        sys.modules.pop("megaton_lib.playwright_browser", None)
        importlib.import_module("megaton_lib.playwright_browser")

    assert hasattr(mod, "browser_page")
    assert hasattr(mod, "scrape_with_playwright")


def test_browser_page_default_uses_launch_and_new_context(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page() as page:
        assert page is pw.chromium.page

    assert pw.chromium.launch_kwargs == {"headless": True}
    assert pw.chromium.launch_persistent_kwargs == {}
    assert pw.chromium.browser.new_context_kwargs == {"locale": "ja-JP"}
    assert pw.chromium.context.closed is True
    assert pw.chromium.browser.closed is True


def test_browser_page_passes_optional_context_options(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page(
        headless=False,
        locale="en-US",
        user_agent="UA/1.0",
        timezone_id="Asia/Tokyo",
        viewport={"width": 1280, "height": 720},
        context_kwargs={"ignore_https_errors": True},
    ):
        pass

    assert pw.chromium.launch_kwargs == {"headless": False}
    assert pw.chromium.browser.new_context_kwargs == {
        "locale": "en-US",
        "user_agent": "UA/1.0",
        "timezone_id": "Asia/Tokyo",
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": True,
    }


def test_browser_page_with_user_data_dir_uses_persistent_context(monkeypatch, tmp_path):
    pw = _install_fake_sync_playwright(monkeypatch)
    profile = tmp_path / "profile"

    with playwright_browser.browser_page(user_data_dir=profile) as page:
        assert page is pw.chromium.page

    assert profile.exists()
    assert pw.chromium.launch_kwargs == {}
    assert pw.chromium.launch_persistent_kwargs == {
        "user_data_dir": str(profile),
        "headless": True,
        "locale": "ja-JP",
    }
    assert pw.chromium.persistent_context.closed is True


def test_browser_page_cleans_up_on_handler_exception(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with pytest.raises(ValueError):
        with playwright_browser.browser_page():
            raise ValueError("boom")

    assert pw.chromium.context.closed is True
    assert pw.chromium.browser.closed is True


def test_scrape_with_playwright_navigates_and_calls_handler(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)
    handler = MagicMock(return_value={"ok": True})

    result = playwright_browser.scrape_with_playwright(
        "https://example.test/quote",
        handler=handler,
        wait_selector=".price",
        timeout_ms=5000,
    )

    assert result == {"ok": True}
    handler.assert_called_once_with(pw.chromium.page)
    assert pw.chromium.page.goto_calls == [
        {"url": "https://example.test/quote", "wait_until": "domcontentloaded", "timeout": 5000}
    ]
    assert pw.chromium.page.wait_selector_calls == [{"selector": ".price", "timeout": 5000}]


def test_scrape_with_playwright_skips_wait_selector_when_none(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)
    handler = MagicMock(return_value=[])

    playwright_browser.scrape_with_playwright(
        "https://example.test/list",
        handler=handler,
    )

    assert pw.chromium.page.wait_selector_calls == []
    assert pw.chromium.page.goto_calls[0]["wait_until"] == "domcontentloaded"


# ---- is_port_open ----


def test_is_port_open_returns_true_for_listening_port():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        assert playwright_browser.is_port_open(port) is True
    finally:
        server.close()


def test_is_port_open_returns_false_for_closed_port():
    # Bind to find a free port, then close so it's reliably unbound.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    port = server.getsockname()[1]
    server.close()
    assert playwright_browser.is_port_open(port, timeout=0.1) is False


# ---- launch_chrome_with_debug_port ----


def test_launch_chrome_with_debug_port_invokes_open_command(monkeypatch, tmp_path):
    calls: list = []

    def fake_run(args, *, check):
        calls.append({"args": args, "check": check})

    sleeps: list = []
    monkeypatch.setattr(playwright_browser.sys, "platform", "darwin")
    monkeypatch.setattr(playwright_browser.subprocess, "run", fake_run)
    monkeypatch.setattr(playwright_browser.time, "sleep", lambda s: sleeps.append(s))

    profile = tmp_path / "profile"
    playwright_browser.launch_chrome_with_debug_port(
        url="https://example.test/login",
        user_data_dir=profile,
        debug_port=9231,
        wait_seconds=1.5,
    )

    assert profile.exists()
    assert calls[0]["check"] is True
    args = calls[0]["args"]
    assert args[:4] == ["open", "-na", "Google Chrome", "--args"]
    assert f"--remote-debugging-port=9231" in args
    assert f"--user-data-dir={profile}" in args
    assert args[-1] == "https://example.test/login"
    assert sleeps == [1.5]


def test_launch_chrome_with_debug_port_skips_sleep_when_zero(monkeypatch, tmp_path):
    monkeypatch.setattr(playwright_browser.sys, "platform", "darwin")
    monkeypatch.setattr(playwright_browser.subprocess, "run", lambda *a, **k: None)
    slept: list = []
    monkeypatch.setattr(playwright_browser.time, "sleep", lambda s: slept.append(s))

    playwright_browser.launch_chrome_with_debug_port(
        url="https://example.test/",
        user_data_dir=tmp_path / "p",
        debug_port=9000,
        wait_seconds=0,
    )
    assert slept == []


def test_launch_chrome_with_debug_port_rejects_non_macos(monkeypatch, tmp_path):
    monkeypatch.setattr(playwright_browser.sys, "platform", "linux")

    with pytest.raises(RuntimeError, match="macOS-only"):
        playwright_browser.launch_chrome_with_debug_port(
            url="https://example.test/",
            user_data_dir=tmp_path / "p",
            debug_port=9000,
        )


# ---- find_or_open_page ----


class FakeCDPContext:
    def __init__(self, pages: list[FakePage]) -> None:
        self.pages = pages
        self._new_pages_created = 0

    def new_page(self):
        self._new_pages_created += 1
        page = FakePage()
        self.pages.append(page)
        return page


def test_find_or_open_page_returns_existing_page_matching_prefix():
    p1 = FakePage(url="https://example.test/dashboard")
    p2 = FakePage(url="https://example.test/login?next=/bills")
    ctx = FakeCDPContext([p1, p2])

    result = playwright_browser.find_or_open_page(ctx, "https://example.test/login")

    assert result is p2
    assert ctx._new_pages_created == 0


def test_find_or_open_page_opens_new_when_no_match():
    ctx = FakeCDPContext([FakePage(url="https://other.test/")])

    result = playwright_browser.find_or_open_page(
        ctx,
        "https://example.test/target",
        timeout_ms=10_000,
    )

    assert ctx._new_pages_created == 1
    assert result.goto_calls == [
        {"url": "https://example.test/target", "wait_until": "domcontentloaded", "timeout": 10_000}
    ]


# ---- connected_browser_page ----


class FakeCDPBrowser:
    def __init__(self, contexts: list[FakeCDPContext]) -> None:
        self.contexts = contexts
        self.closed = False

    def close(self):
        self.closed = True


class FakeCDPChromium:
    def __init__(self, browser: FakeCDPBrowser) -> None:
        self._browser = browser
        self.connect_calls: list[str] = []

    def connect_over_cdp(self, cdp_url):
        self.connect_calls.append(cdp_url)
        return self._browser


def _install_fake_cdp(monkeypatch, contexts):
    browser = FakeCDPBrowser(contexts)
    chromium = FakeCDPChromium(browser)

    class FakePw:
        pass

    fake_pw = FakePw()
    fake_pw.chromium = chromium

    @contextmanager
    def cm():
        yield fake_pw

    monkeypatch.setattr(playwright_browser, "_load_sync_playwright", lambda: cm)
    return browser, chromium


def test_connected_browser_page_uses_target_url_match(monkeypatch):
    existing = FakePage(url="https://example.test/account")
    ctx = FakeCDPContext([existing])
    browser, chromium = _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231",
        target_url="https://example.test/account",
    ) as page:
        assert page is existing
        assert page.brought_to_front == 1

    assert chromium.connect_calls == ["http://127.0.0.1:9231"]
    assert browser.closed is True


def test_connected_browser_page_falls_back_to_last_page_when_no_target(monkeypatch):
    p1 = FakePage(url="https://a.test/")
    p2 = FakePage(url="https://b.test/")
    ctx = FakeCDPContext([p1, p2])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page("http://127.0.0.1:9231") as page:
        assert page is p2


def test_connected_browser_page_opens_new_when_context_empty(monkeypatch):
    ctx = FakeCDPContext([])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page("http://127.0.0.1:9231") as page:
        assert ctx._new_pages_created == 1
        assert page is ctx.pages[-1]


def test_connected_browser_page_respects_bring_to_front_false(monkeypatch):
    existing = FakePage(url="https://example.test/")
    ctx = FakeCDPContext([existing])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231",
        target_url="https://example.test/",
        bring_to_front=False,
    ) as page:
        assert page.brought_to_front == 0


def test_connected_browser_page_closes_browser_on_exception(monkeypatch):
    ctx = FakeCDPContext([FakePage(url="https://example.test/")])
    browser, _ = _install_fake_cdp(monkeypatch, [ctx])

    with pytest.raises(RuntimeError):
        with playwright_browser.connected_browser_page("http://127.0.0.1:9231"):
            raise RuntimeError("boom")

    assert browser.closed is True
