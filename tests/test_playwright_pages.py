from __future__ import annotations

from megaton_lib.validation.playwright_pages import (
    capture_selector_state,
    run_with_basic_auth_page,
)


class FakePage:
    def __init__(self) -> None:
        self.url = "https://example.test/page"
        self._title = "Example"
        self.goto_calls = []
        self.wait_calls = []

    def goto(self, url: str, **kwargs) -> None:
        self.goto_calls.append((url, kwargs))

    def wait_for_timeout(self, wait_ms: int) -> None:
        self.wait_calls.append(wait_ms)

    def evaluate(self, _script: str, selectors: list[str]) -> dict:
        return {
            selectors[0]: {"exists": True, "opacity": "1", "childCount": 2},
            selectors[1]: {"exists": False, "opacity": None, "childCount": 0},
        }

    def title(self) -> str:
        return self._title


class FakeContext:
    def __init__(self, page: FakePage, credentials: dict) -> None:
        self.page_obj = page
        self.credentials = credentials
        self.closed = False

    def new_page(self) -> FakePage:
        return self.page_obj

    def close(self) -> None:
        self.closed = True


class FakeBrowser:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.context = None
        self.closed = False

    def new_context(self, **kwargs) -> FakeContext:
        self.context = FakeContext(self.page, kwargs["http_credentials"])
        return self.context

    def close(self) -> None:
        self.closed = True


class FakeChromium:
    def __init__(self, browser: FakeBrowser) -> None:
        self.browser = browser
        self.launch_calls = []

    def launch(self, *, headless: bool) -> FakeBrowser:
        self.launch_calls.append(headless)
        return self.browser


class FakePlaywright:
    def __init__(self, chromium: FakeChromium) -> None:
        self.chromium = chromium


class FakePlaywrightManager:
    def __init__(self, playwright: FakePlaywright) -> None:
        self.playwright = playwright

    def __enter__(self) -> FakePlaywright:
        return self.playwright

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_capture_selector_state_uses_page_metadata():
    page = FakePage()
    result = capture_selector_state(page, ["#a", "#b"])
    assert result["url"] == "https://example.test/page"
    assert result["title"] == "Example"
    assert result["checks"]["#a"]["childCount"] == 2
    assert result["checks"]["#b"]["exists"] is False


def test_run_with_basic_auth_page_opens_and_closes(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    result = run_with_basic_auth_page(
        "https://example.test/page",
        "user",
        "pass",
        wait_ms=250,
        setup=lambda opened_page: opened_page.wait_for_timeout(10),
        callback=lambda opened_page: {"title": opened_page.title()},
    )

    assert result == {"title": "Example"}
    assert chromium.launch_calls == [True]
    assert browser.context.credentials == {"username": "user", "password": "pass"}
    assert page.goto_calls[0][0] == "https://example.test/page"
    assert page.wait_calls == [10, 250]
    assert browser.context.closed is True
    assert browser.closed is True
