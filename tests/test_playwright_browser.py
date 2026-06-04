from __future__ import annotations

import builtins
import importlib
import socket
import sys
from contextlib import asynccontextmanager, contextmanager
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

    def wait_for_timeout(self, ms):
        pass

    def wait_for_load_state(self, state):
        self.wait_load_state = state

    def bring_to_front(self):
        self.brought_to_front += 1


class FakeContext:
    def __init__(self, page: FakePage, *, prepopulated: bool = False) -> None:
        self.pages = [page] if prepopulated else []
        self._page = page
        self._page.context = self
        self.closed = False
        self.kwargs: dict = {}
        self.saved_storage_state_path = ""
        self.init_scripts: list[str] = []

    def new_page(self):
        return self._page

    def close(self):
        self.closed = True

    def storage_state(self, *, path):
        self.saved_storage_state_path = path

    def add_init_script(self, script: str):
        self.init_scripts.append(script)


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
        self.devices = {}


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
    assert hasattr(mod, "async_browser_page")
    assert hasattr(mod, "scrape_with_playwright")
    assert hasattr(mod, "CanvasClipScreenshotter")


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


def test_browser_page_stealth_adds_launch_arg_and_init_script(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page(stealth=True):
        pass

    assert pw.chromium.launch_kwargs["args"] == [
        "--disable-blink-features=AutomationControlled"
    ]
    assert pw.chromium.context.init_scripts == [
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    ]


def test_browser_page_no_stealth_means_no_arg_or_init_script(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page():
        pass

    assert "args" not in pw.chromium.launch_kwargs
    assert pw.chromium.context.init_scripts == []


def test_browser_page_slow_mo_passed_to_launch(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page(slow_mo=250):
        pass

    assert pw.chromium.launch_kwargs["slow_mo"] == 250


def test_browser_page_locale_none_omits_locale(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)

    with playwright_browser.browser_page(locale=None):
        pass

    assert "locale" not in pw.chromium.browser.new_context_kwargs


def test_browser_page_passes_channel_args_accept_downloads_and_device(monkeypatch):
    pw = _install_fake_sync_playwright(monkeypatch)
    pw.devices["iPhone 13 Mini"] = {"viewport": {"width": 375, "height": 812}, "is_mobile": True}

    with playwright_browser.browser_page(
        browser_channel="chrome",
        launch_args=["--no-sandbox"],
        device_name="iPhone 13 Mini",
        accept_downloads=True,
        viewport={"width": 390, "height": 844},
    ):
        pass

    assert pw.chromium.launch_kwargs == {
        "headless": True,
        "channel": "chrome",
        "args": ["--no-sandbox"],
    }
    assert pw.chromium.browser.new_context_kwargs == {
        "viewport": {"width": 390, "height": 844},
        "is_mobile": True,
        "locale": "ja-JP",
        "accept_downloads": True,
    }


def test_launch_sync_browser_fallback_logs_original_error(caplog):
    class FallbackChromium:
        def __init__(self) -> None:
            self.calls = []
            self.browser = object()

        def launch(self, **kwargs):
            self.calls.append(kwargs)
            if "channel" in kwargs:
                raise RuntimeError("chrome missing")
            return self.browser

    chromium = FallbackChromium()

    with caplog.at_level("WARNING", logger="megaton_lib.playwright_browser"):
        result = playwright_browser._launch_sync_browser(
            chromium,
            {"headless": True, "channel": "chrome"},
        )

    assert result is chromium.browser
    assert chromium.calls == [{"headless": True, "channel": "chrome"}, {"headless": True}]
    assert "chrome missing" in caplog.text
    assert caplog.records[0].exc_info is not None


def test_launch_sync_browser_without_channel_reraises():
    class BrokenChromium:
        def launch(self, **kwargs):
            raise RuntimeError("bundled broken")

    with pytest.raises(RuntimeError, match="bundled broken"):
        playwright_browser._launch_sync_browser(BrokenChromium(), {"headless": True})


def test_save_storage_state_creates_parent_and_returns_path(tmp_path):
    page = FakePage()
    context = FakeContext(page)
    page.context = context
    state_path = tmp_path / "tokens" / "state.json"

    result = playwright_browser.save_page_storage_state(page, state_path)

    assert result == state_path
    assert state_path.parent.exists()
    assert context.saved_storage_state_path == str(state_path)


def test_browser_page_loads_and_saves_storage_state(monkeypatch, tmp_path):
    pw = _install_fake_sync_playwright(monkeypatch)
    state_path = tmp_path / "tokens" / "state.json"
    state_path.parent.mkdir()
    state_path.write_text("{}", encoding="utf-8")

    with playwright_browser.browser_page(
        storage_state_path=state_path,
        save_storage_state=True,
    ):
        pass

    assert pw.chromium.browser.new_context_kwargs == {
        "locale": "ja-JP",
        "storage_state": str(state_path),
    }
    assert pw.chromium.context.saved_storage_state_path == str(state_path)


def test_browser_page_creates_parent_when_saving_new_storage_state(monkeypatch, tmp_path):
    pw = _install_fake_sync_playwright(monkeypatch)
    state_path = tmp_path / "new" / "state.json"

    with playwright_browser.browser_page(
        storage_state_path=state_path,
        save_storage_state=True,
    ):
        pass

    assert pw.chromium.browser.new_context_kwargs == {"locale": "ja-JP"}
    assert state_path.parent.exists()
    assert pw.chromium.context.saved_storage_state_path == str(state_path)


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


def test_browser_page_with_user_data_dir_can_save_storage_state(monkeypatch, tmp_path):
    pw = _install_fake_sync_playwright(monkeypatch)
    profile = tmp_path / "profile"
    state_path = tmp_path / "tokens" / "state.json"
    state_path.parent.mkdir()
    state_path.write_text("{}", encoding="utf-8")

    with playwright_browser.browser_page(
        user_data_dir=profile,
        storage_state_path=state_path,
        save_storage_state=True,
    ):
        pass

    assert pw.chromium.launch_persistent_kwargs == {
        "user_data_dir": str(profile),
        "headless": True,
        "locale": "ja-JP",
    }
    assert pw.chromium.persistent_context.saved_storage_state_path == str(state_path)


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


# ---- async_browser_page ----


class AsyncFakePage:
    def __init__(self) -> None:
        self.context = None
        self.url = "about:blank"
        self.waited_load_state = ""

    async def wait_for_timeout(self, ms):
        pass

    async def wait_for_load_state(self, state):
        self.waited_load_state = state


class AsyncFakeContext:
    def __init__(self, page: AsyncFakePage, *, prepopulated: bool = False) -> None:
        self.pages = [page] if prepopulated else []
        self._page = page
        self._page.context = self
        self.closed = False
        self.saved_storage_state_path = ""

    async def new_page(self):
        return self._page

    async def close(self):
        self.closed = True

    async def storage_state(self, *, path):
        self.saved_storage_state_path = path


class AsyncFakeBrowser:
    def __init__(self, context: AsyncFakeContext) -> None:
        self._context = context
        self.closed = False
        self.new_context_kwargs: dict = {}

    async def new_context(self, **kwargs):
        self.new_context_kwargs = kwargs
        return self._context

    async def close(self):
        self.closed = True


class AsyncFakeChromium:
    def __init__(self) -> None:
        self.page = AsyncFakePage()
        self.persistent_page = AsyncFakePage()
        self.context = AsyncFakeContext(self.page)
        self.persistent_context = AsyncFakeContext(self.persistent_page, prepopulated=True)
        self.browser = AsyncFakeBrowser(self.context)
        self.launch_kwargs: dict = {}
        self.launch_persistent_kwargs: dict = {}

    async def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        return self.browser

    async def launch_persistent_context(self, **kwargs):
        self.launch_persistent_kwargs = kwargs
        return self.persistent_context


class AsyncFakePlaywright:
    def __init__(self) -> None:
        self.chromium = AsyncFakeChromium()
        self.devices = {"Pixel": {"viewport": {"width": 393, "height": 851}, "is_mobile": True}}


@asynccontextmanager
async def _fake_async_playwright_cm():
    yield ASYNC_FAKE_PW


ASYNC_FAKE_PW = AsyncFakePlaywright()


def _install_fake_async_playwright(monkeypatch):
    global ASYNC_FAKE_PW
    ASYNC_FAKE_PW = AsyncFakePlaywright()
    monkeypatch.setattr(playwright_browser, "_load_async_playwright", lambda: _fake_async_playwright_cm)
    return ASYNC_FAKE_PW


def test_async_browser_page_passes_options_and_closes(monkeypatch):
    pw = _install_fake_async_playwright(monkeypatch)

    async def run():
        async with playwright_browser.async_browser_page(
            headless=False,
            browser_channel="chrome",
            launch_args=["--disable-popup-blocking"],
            device_name="Pixel",
            accept_downloads=True,
            timezone_id="Asia/Tokyo",
        ) as page:
            assert page is pw.chromium.page

    import asyncio

    asyncio.run(run())

    assert pw.chromium.launch_kwargs == {
        "headless": False,
        "channel": "chrome",
        "args": ["--disable-popup-blocking"],
    }
    assert pw.chromium.browser.new_context_kwargs == {
        "viewport": {"width": 393, "height": 851},
        "is_mobile": True,
        "locale": "ja-JP",
        "timezone_id": "Asia/Tokyo",
        "accept_downloads": True,
    }
    assert pw.chromium.context.closed is True
    assert pw.chromium.browser.closed is True


def test_launch_async_browser_fallback_logs_original_error(caplog):
    class AsyncFallbackChromium:
        def __init__(self) -> None:
            self.calls = []
            self.browser = object()

        async def launch(self, **kwargs):
            self.calls.append(kwargs)
            if "channel" in kwargs:
                raise RuntimeError("chrome missing")
            return self.browser

    async def run():
        chromium = AsyncFallbackChromium()
        with caplog.at_level("WARNING", logger="megaton_lib.playwright_browser"):
            result = await playwright_browser._launch_async_browser(
                chromium,
                {"headless": True, "channel": "chrome"},
            )
        return chromium, result

    import asyncio

    chromium, result = asyncio.run(run())

    assert result is chromium.browser
    assert chromium.calls == [{"headless": True, "channel": "chrome"}, {"headless": True}]
    assert "chrome missing" in caplog.text
    assert caplog.records[0].exc_info is not None


def test_async_browser_page_can_save_storage_state(monkeypatch, tmp_path):
    pw = _install_fake_async_playwright(monkeypatch)
    state_path = tmp_path / "tokens" / "state.json"

    async def run():
        async with playwright_browser.async_browser_page(
            storage_state_path=state_path,
            save_storage_state=True,
        ):
            pass

    import asyncio

    asyncio.run(run())

    assert state_path.parent.exists()
    assert pw.chromium.context.saved_storage_state_path == str(state_path)


def test_wait_for_url_not_contains_returns_true_after_login():
    page = FakePage(url="https://app.example.test/dashboard")

    assert playwright_browser.wait_for_url_not_contains(page, "accounts.google.com") is True
    assert page.wait_load_state == "domcontentloaded"


def test_wait_for_url_not_contains_caps_poll_to_remaining_timeout(monkeypatch):
    clock = {"now": 0.0}

    class RecordingPage(FakePage):
        def __init__(self) -> None:
            super().__init__(url="https://accounts.google.com/signin")
            self.wait_timeout_calls: list[int] = []

        def wait_for_timeout(self, ms):
            self.wait_timeout_calls.append(ms)
            clock["now"] += ms / 1000

    monkeypatch.setattr(playwright_browser.time, "monotonic", lambda: clock["now"])
    page = RecordingPage()

    assert playwright_browser.wait_for_url_not_contains(
        page,
        "accounts.google.com",
        timeout_ms=5,
        poll_ms=100,
    ) is False
    assert page.wait_timeout_calls == [5]


def test_wait_for_url_not_contains_passes_remaining_timeout_to_load_state(monkeypatch):
    class RecordingPage(FakePage):
        def wait_for_load_state(self, state, *, timeout):
            self.wait_load_state = state
            self.wait_load_timeout = timeout

    monkeypatch.setattr(playwright_browser.time, "monotonic", lambda: 10.0)
    page = RecordingPage(url="https://app.example.test/dashboard")

    assert playwright_browser.wait_for_url_not_contains(
        page,
        "accounts.google.com",
        timeout_ms=1000,
    ) is True
    assert page.wait_load_state == "domcontentloaded"
    assert page.wait_load_timeout == 1000


def test_async_wait_for_url_not_contains_returns_false_on_timeout():
    page = AsyncFakePage()
    page.url = "https://accounts.google.com/signin"

    async def run():
        return await playwright_browser.async_wait_for_url_not_contains(
            page,
            "accounts.google.com",
            timeout_ms=1,
            poll_ms=1,
        )

    import asyncio

    assert asyncio.run(run()) is False


# ---- CanvasClipScreenshotter ----


class FakeCanvasLocator:
    def __init__(self, box: dict | None) -> None:
        self.first = self
        self.box = box
        self.scrolled = False
        self.clicks: list[dict] = []

    def click(self, *, timeout):
        self.clicks.append({"timeout": timeout})

    def scroll_into_view_if_needed(self):
        self.scrolled = True

    def bounding_box(self):
        return self.box


class FakeCanvasPage:
    def __init__(self, url: str = "about:blank", box: dict | None = None) -> None:
        self.url = url
        self.context = None
        self.box = box or {"x": 10, "y": 20, "width": 1000, "height": 600}
        self.goto_calls: list[dict] = []
        self.wait_timeout_calls: list[int] = []
        self.wait_selector_calls: list[dict] = []
        self.screenshot_calls: list[dict] = []
        self.locators: dict[str, FakeCanvasLocator] = {}
        self.brought_to_front = 0

    def goto(self, url, *, wait_until, timeout):
        self.url = url
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})

    def wait_for_timeout(self, ms):
        self.wait_timeout_calls.append(ms)

    def wait_for_selector(self, selector, *, state=None, timeout):
        self.wait_selector_calls.append({"selector": selector, "state": state, "timeout": timeout})

    def locator(self, selector):
        if selector not in self.locators:
            self.locators[selector] = FakeCanvasLocator(self.box)
        return self.locators[selector]

    def screenshot(self, *, path, clip):
        self.screenshot_calls.append({"path": path, "clip": clip})

    def bring_to_front(self):
        self.brought_to_front += 1


class FakeCanvasContext:
    def __init__(self, page: FakeCanvasPage) -> None:
        self.pages = [page]
        page.context = self


@contextmanager
def _fake_browser_page_context(page: FakeCanvasPage):
    yield page


def test_canvas_clip_screenshotter_captures_canvas_relative_clip(monkeypatch, tmp_path):
    page = FakeCanvasPage(url="https://docs.google.com/spreadsheets/d/sheet")
    FakeCanvasContext(page)
    monkeypatch.setattr(
        playwright_browser,
        "browser_page",
        lambda **kwargs: _fake_browser_page_context(page),
    )

    with playwright_browser.CanvasClipScreenshotter(screenshot_dir=tmp_path) as shotter:
        saved = shotter.screenshot_canvas_clip(
            url="https://docs.google.com/spreadsheets/d/sheet#gid=1",
            path="clip.png",
            offset={"x": 5, "y": 7, "width": 300, "height": 120},
        )

    assert saved == str(tmp_path / "clip.png")
    assert page.goto_calls == [
        {
            "url": "https://docs.google.com/spreadsheets/d/sheet#gid=1",
            "wait_until": "domcontentloaded",
            "timeout": 30_000,
        }
    ]
    assert page.screenshot_calls == [
        {
            "path": str(tmp_path / "clip.png"),
            "clip": {"x": 15.0, "y": 27.0, "width": 300.0, "height": 120.0},
        }
    ]


def test_canvas_clip_screenshotter_fails_closed_when_headless_login_required(monkeypatch, tmp_path):
    page = FakeCanvasPage(url="https://accounts.google.com/signin")
    page.context = FakeCanvasContext(page)
    monkeypatch.setattr(
        playwright_browser,
        "browser_page",
        lambda **kwargs: _fake_browser_page_context(page),
    )

    with pytest.raises(RuntimeError, match="Login is required"):
        with playwright_browser.CanvasClipScreenshotter(screenshot_dir=tmp_path, headless=True) as shotter:
            shotter.open("https://accounts.google.com/signin")


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
    assert "--remote-debugging-port=9231" in args
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
