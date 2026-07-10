from __future__ import annotations

import asyncio
import builtins
import importlib
import socket
import sys
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from megaton_lib import playwright_browser


class FakePage:
    def __init__(self, url: str = "about:blank") -> None:
        self.url = url
        self.goto_calls: list[dict] = []
        self.wait_selector_calls: list[dict] = []
        self.brought_to_front = 0
        self.closed = False
        self.close_error: BaseException | None = None

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

    def close(self):
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


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


def test_load_storage_state_reads_valid_json(tmp_path):
    path = tmp_path / "state.json"
    path.write_text('{"cookies": [{"name": "s"}], "origins": []}', encoding="utf-8")

    state = playwright_browser.load_storage_state(path)

    assert state == {"cookies": [{"name": "s"}], "origins": []}


def test_load_storage_state_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        playwright_browser.load_storage_state(tmp_path / "nope.json")


def test_load_storage_state_non_object_raises(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a JSON object"):
        playwright_browser.load_storage_state(path)


def test_load_storage_state_roundtrips_with_save(tmp_path):
    # save_page_storage_state writes via page.context.storage_state(path=...);
    # load_storage_state should read back an equivalent object.
    path = tmp_path / "rt.json"
    path.write_text('{"cookies": [], "origins": [{"origin": "https://x.test"}]}', encoding="utf-8")
    assert playwright_browser.load_storage_state(path)["origins"][0]["origin"] == "https://x.test"


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

    @property
    def contexts(self):
        return [self._context]

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
        self.connect_calls: list[str] = []

    async def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        return self.browser

    async def launch_persistent_context(self, **kwargs):
        self.launch_persistent_kwargs = kwargs
        return self.persistent_context

    async def connect_over_cdp(self, cdp_url):
        self.connect_calls.append(cdp_url)
        return self.browser


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


def test_open_async_browser_context_uses_shared_device_and_cdp_policy():
    pw = AsyncFakePlaywright()

    context = asyncio.run(
        playwright_browser.open_async_browser_context(
            pw,
            cdp_url="http://127.0.0.1:9222",
            device_name="Pixel",
            timezone_id="Asia/Tokyo",
        )
    )

    assert context is pw.chromium.context
    assert pw.chromium.connect_calls == ["http://127.0.0.1:9222"]


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


def test_connected_browser_page_target_url_keeps_startswith_semantics(monkeypatch):
    embedded = FakePage(url="https://other.test/?next=https://example.test/account")
    ctx = FakeCDPContext([embedded])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231",
        target_url="https://example.test/account",
    ) as page:
        assert page is not embedded
        assert page.goto_calls == [
            {"url": "https://example.test/account", "wait_until": "domcontentloaded", "timeout": 30_000}
        ]

    assert ctx._new_pages_created == 1


def test_cdp_host_cleanup_matches_hostname_not_query_string():
    assert playwright_browser.cdp_url_matches_host(
        "https://member.example.test/account", "example.test"
    )
    assert not playwright_browser.cdp_url_matches_host(
        "https://unrelated.test/?next=https://example.test/account", "example.test"
    )


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


def test_connected_browser_page_priority_match_closes_same_host_duplicates(monkeypatch):
    login = FakePage(url="https://broker.example/login")
    holdings = FakePage(url="https://broker.example/member/holdings")
    help_page = FakePage(url="https://broker.example/help")
    other = FakePage(url="https://other.example/")
    ctx = FakeCDPContext([login, holdings, help_page, other])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231",
        target_url="https://broker.example/login",
        match=["member/holdings", "broker.example"],
        cleanup_host="broker.example",
    ) as page:
        assert page is holdings
        assert holdings.closed is False

    assert login.closed is True
    assert help_page.closed is True
    assert other.closed is False
    assert holdings.closed is False


def test_connected_browser_page_prunes_duplicates_across_multiple_hosts(monkeypatch):
    holdings = FakePage(url="https://trade.kabu.co.jp/member/holdings")
    stale_login = FakePage(url="https://kabu.com/login")
    other = FakePage(url="https://other.example/")
    ctx = FakeCDPContext([stale_login, holdings, other])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231",
        match="member/holdings",
        cleanup_host=["kabu.co.jp", "kabu.com"],
    ) as page:
        assert page is holdings

    assert stale_login.closed is True
    assert holdings.closed is False
    assert other.closed is False


def test_connected_browser_page_prunes_kept_page_on_failure(monkeypatch):
    page = FakePage(url="https://broker.example/member/holdings")
    ctx = FakeCDPContext([page])
    _install_fake_cdp(monkeypatch, [ctx])

    with pytest.raises(RuntimeError):
        with playwright_browser.connected_browser_page(
            "http://127.0.0.1:9231",
            match="member/holdings",
            cleanup_host="broker.example",
        ):
            raise RuntimeError("boom")

    assert page.closed is True


def test_close_cdp_pages_reraises_interrupt_after_best_effort_cleanup():
    interrupted = FakePage(url="https://broker.example/interrupt")
    interrupted.close_error = KeyboardInterrupt()
    later = FakePage(url="https://broker.example/later")

    with pytest.raises(KeyboardInterrupt):
        playwright_browser._close_cdp_pages([interrupted, later])

    assert later.closed is True


def test_connected_browser_page_closes_browser_on_exception(monkeypatch):
    ctx = FakeCDPContext([FakePage(url="https://example.test/")])
    browser, _ = _install_fake_cdp(monkeypatch, [ctx])

    with pytest.raises(RuntimeError):
        with playwright_browser.connected_browser_page("http://127.0.0.1:9231"):
            raise RuntimeError("boom")

    assert browser.closed is True


def test_is_transient_playwright_error_matches_existing_tokens():
    assert playwright_browser.is_transient_playwright_error(RuntimeError("Frame was detached"))
    assert playwright_browser.is_transient_playwright_error(RuntimeError("net::ERR_ABORTED"))
    assert not playwright_browser.is_transient_playwright_error(RuntimeError("selector missing"))


def test_click_with_retries_runs_retry_hook_then_succeeds():
    import asyncio
    from playwright.async_api import TimeoutError as PWTimeoutError

    calls: list[str] = []

    class Locator:
        attempts = 0

        async def click(self, **_kw):
            self.attempts += 1
            if self.attempts == 1:
                raise PWTimeoutError("timeout")
            calls.append("click")

    async def retry():
        calls.append("retry")

    assert asyncio.run(playwright_browser.click_with_retries(Locator(), on_retry=retry))
    assert calls == ["retry", "click"]


def test_settle_page_returns_false_on_timeout_and_still_delays():
    import asyncio
    from playwright.async_api import TimeoutError as PWTimeoutError

    class Page:
        delayed = 0

        async def wait_for_load_state(self, **_kw):
            raise PWTimeoutError("timeout")

        async def wait_for_timeout(self, delay):
            self.delayed = delay

    page = Page()

    assert asyncio.run(playwright_browser.settle_page(page, timeout=1, delay_ms=123)) is False
    assert page.delayed == 123


def test_wait_for_url_change_falls_back_to_current_url():
    import asyncio
    from playwright.async_api import TimeoutError as PWTimeoutError

    class Page:
        url = "after"

        async def wait_for_function(self, *_a, **_kw):
            raise PWTimeoutError("timeout")

    assert asyncio.run(playwright_browser.wait_for_url_change(Page(), "before"))


def test_save_failure_artifact_writes_png_html_and_url(tmp_path):
    import asyncio

    class Page:
        url = "https://example.test/fail"

        async def screenshot(self, *, path: str, timeout: int):
            Path(path).write_bytes(b"png")

        async def content(self):
            return "<html>fail</html>"

    base = asyncio.run(playwright_browser.save_failure_artifact(Page(), "Case-x", tmp_path))

    assert base is not None
    assert Path(f"{base}.png").read_bytes() == b"png"
    assert Path(f"{base}.html").read_text() == "<html>fail</html>"
    assert Path(f"{base}.url.txt").read_text() == "https://example.test/fail"


def test_save_failure_artifact_keeps_dotted_labels_and_jst_timestamp(tmp_path, monkeypatch):
    """Labels can contain dots (e.g. an object repr like
    ``Forest-<tests.gendama.Gendama object at 0x1>``); the filename must keep
    the full label + timestamp instead of letting with_suffix eat everything
    after the last dot. The timestamp is JST regardless of runner timezone."""
    import asyncio
    import datetime as real_dt

    class Page:
        url = "https://example.test/fail"

        async def screenshot(self, *, path: str, timeout: int):
            Path(path).write_bytes(b"png")

        async def content(self):
            return "<html>fail</html>"

    label = "Forest-<tests.gendama.Gendama object at 0x1>"
    base = asyncio.run(playwright_browser.save_failure_artifact(Page(), label, tmp_path))

    assert base is not None
    assert base.name.startswith(label + "-")
    ts = base.name.removeprefix(label + "-")
    assert Path(f"{base}.png").exists()
    assert Path(f"{base}.url.txt").read_text() == "https://example.test/fail"
    # timestamp parses and is JST-now (not naive runner-local)
    from megaton_lib.tz_utils import JST

    parsed = real_dt.datetime.strptime(ts, "%m%d_%H%M%S")
    now_jst = real_dt.datetime.now(JST)
    assert (parsed.month, parsed.day) == (now_jst.month, now_jst.day)


def test_save_failure_artifact_sync_writes_png_html_url_and_keeps_dotted_label(tmp_path):
    import datetime as real_dt

    class Page:
        url = "https://example.test/sync-fail"

        def __init__(self):
            self.full_page = None

        def screenshot(self, *, path: str, full_page: bool, timeout: int):
            self.full_page = full_page
            Path(path).write_bytes(b"sync-png")

        def content(self):
            return "<html>sync fail</html>"

    page = Page()
    label = "expense.provider.with.dot"
    base = playwright_browser.save_failure_artifact_sync(page, label, tmp_path)

    assert base is not None
    assert base.name.startswith(label + "-")
    assert page.full_page is True
    assert Path(f"{base}.png").read_bytes() == b"sync-png"
    assert Path(f"{base}.html").read_text() == "<html>sync fail</html>"
    assert Path(f"{base}.url.txt").read_text() == "https://example.test/sync-fail"

    from megaton_lib.tz_utils import JST

    ts = base.name.removeprefix(label + "-")
    parsed = real_dt.datetime.strptime(ts, "%m%d_%H%M%S")
    now_jst = real_dt.datetime.now(JST)
    assert (parsed.month, parsed.day) == (now_jst.month, now_jst.day)


def test_save_failure_artifact_sync_returns_none_on_screenshot_failure(tmp_path):
    class Page:
        url = "https://example.test/sync-fail"

        def screenshot(self, *, path: str, full_page: bool, timeout: int):
            raise RuntimeError("screen failed")

        def content(self):  # pragma: no cover - screenshot failure aborts capture
            return "<html>sync fail</html>"

    assert playwright_browser.save_failure_artifact_sync(Page(), "sync.case", tmp_path) is None


def test_activate_app_runs_osascript_on_darwin(monkeypatch):
    calls = []
    monkeypatch.setattr(playwright_browser.sys, "platform", "darwin")
    monkeypatch.setattr(
        playwright_browser.subprocess,
        "run",
        lambda cmd, **kw: calls.append((cmd, kw)),
    )

    playwright_browser.activate_app("Calculator")

    assert calls == [
        (
            ["osascript", "-e", 'tell application "Calculator" to activate'],
            {"check": False},
        )
    ]


def test_activate_app_noops_off_darwin(monkeypatch):
    calls = []
    monkeypatch.setattr(playwright_browser.sys, "platform", "linux")
    monkeypatch.setattr(
        playwright_browser.subprocess,
        "run",
        lambda cmd, **kw: calls.append((cmd, kw)),
    )

    playwright_browser.activate_app("Google Chrome")

    assert calls == []


def test_activate_app_suppresses_osascript_failures(monkeypatch):
    monkeypatch.setattr(playwright_browser.sys, "platform", "darwin")

    def fail(*_a, **_kw):
        raise RuntimeError("osascript failed")

    monkeypatch.setattr(playwright_browser.subprocess, "run", fail)

    assert playwright_browser.activate_app("Google Chrome") is None


def test_connected_browser_page_raises_when_match_misses_without_target(monkeypatch):
    """match指定+ヒットなし+target_urlなし → 任意タブへのfallbackではなくraise。
    stale/無関係タブを新鮮なページとして解析する事故を防ぐ (minkabu semantics)."""
    p1 = FakePage(url="https://unrelated.test/")
    ctx = FakeCDPContext([p1])
    _install_fake_cdp(monkeypatch, [ctx])

    with pytest.raises(RuntimeError, match="no open tab matching"):
        with playwright_browser.connected_browser_page(
            "http://127.0.0.1:9231", match=["holdings", "member."]
        ):
            pass
    assert not p1.closed


def test_connected_browser_page_suppresses_bring_to_front_errors(monkeypatch):
    page = FakePage(url="https://a.test/holdings")

    def boom():
        raise RuntimeError("no window")

    page.bring_to_front = boom
    ctx = FakeCDPContext([page])
    _install_fake_cdp(monkeypatch, [ctx])

    with playwright_browser.connected_browser_page(
        "http://127.0.0.1:9231", match="holdings"
    ) as got:
        assert got is page  # bring_to_front failure must not abort the attach


def test_connected_browser_page_wraps_connect_failure_with_hint(monkeypatch):
    class FailingChromium:
        def connect_over_cdp(self, cdp_url):
            raise ConnectionError("refused")

    class FakePw:
        chromium = FailingChromium()

    @contextmanager
    def cm():
        yield FakePw()

    monkeypatch.setattr(playwright_browser, "_load_sync_playwright", lambda: cm)

    with pytest.raises(RuntimeError, match="remote-debugging-port"):
        with playwright_browser.connected_browser_page("http://127.0.0.1:9299"):
            pass


def test_async_connected_browser_page_selects_and_cleans_tabs(monkeypatch):
    class Page:
        def __init__(self, url):
            self.url = url
            self.closed = False
            self.front = 0

        async def close(self):
            self.closed = True

        async def bring_to_front(self):
            self.front += 1

    keep = Page("https://member.example.test/account")
    stale = Page("https://login.example.test/")

    class Context:
        pages = [stale, keep]

        async def new_page(self):
            page = Page("about:blank")
            self.pages.append(page)
            return page

    context = Context()

    class Browser:
        contexts = [context]
        closed = False

        async def close(self):
            self.closed = True

    browser = Browser()

    class Chromium:
        async def connect_over_cdp(self, _url):
            return browser

    class Playwright:
        chromium = Chromium()

    @asynccontextmanager
    async def manager():
        yield Playwright()

    monkeypatch.setattr(playwright_browser, "_load_async_playwright", lambda: manager)

    async def run():
        async with playwright_browser.async_connected_browser_page(
            "http://127.0.0.1:9222",
            match="/account",
            cleanup_host="example.test",
        ) as page:
            assert page is keep

    asyncio.run(run())

    assert keep.front == 1
    assert keep.closed is False
    assert stale.closed is True
    assert browser.closed is True


def test_ensure_chrome_cdp_reuses_running_instance(monkeypatch, tmp_path):
    monkeypatch.setattr(playwright_browser, "_cdp_ready", lambda url: True)

    def must_not_launch(*a, **k):  # pragma: no cover
        raise AssertionError("Chrome must not be launched when CDP is already up")

    monkeypatch.setattr(playwright_browser.subprocess, "Popen", must_not_launch)
    url = playwright_browser.ensure_chrome_cdp(port=9222, user_data_dir=tmp_path / "p")
    assert url == "http://127.0.0.1:9222"


def test_ensure_chrome_cdp_launches_and_polls_until_ready(monkeypatch, tmp_path):
    calls = {"popen": [], "ready": 0}

    def fake_ready(url):
        calls["ready"] += 1
        return calls["ready"] >= 3  # 2回目まで未起動、3回目でready

    monkeypatch.setattr(playwright_browser, "_cdp_ready", fake_ready)
    monkeypatch.setattr(
        playwright_browser.subprocess, "Popen",
        lambda cmd, **kw: calls["popen"].append(cmd),
    )
    monkeypatch.setattr(playwright_browser.time, "sleep", lambda s: None)

    url = playwright_browser.ensure_chrome_cdp(
        port=9250, user_data_dir=tmp_path / "profile", start_url="about:blank"
    )
    assert url == "http://127.0.0.1:9250"
    assert len(calls["popen"]) == 1
    cmd = calls["popen"][0]
    assert "--remote-debugging-port=9250" in cmd
    assert "--no-first-run" in cmd
    assert (tmp_path / "profile").exists()


def test_ensure_chrome_cdp_raises_when_port_never_ready(monkeypatch, tmp_path):
    monkeypatch.setattr(playwright_browser, "_cdp_ready", lambda url: False)
    monkeypatch.setattr(playwright_browser.subprocess, "Popen", lambda cmd, **kw: None)
    monkeypatch.setattr(playwright_browser.time, "sleep", lambda s: None)
    times = iter([0.0, 0.1, 11.0, 12.0])
    monkeypatch.setattr(playwright_browser.time, "time", lambda: next(times))

    with pytest.raises(RuntimeError, match="did not become ready"):
        playwright_browser.ensure_chrome_cdp(port=9251, user_data_dir=tmp_path / "p", timeout=10.0)
