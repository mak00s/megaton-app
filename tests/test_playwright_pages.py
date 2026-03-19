from __future__ import annotations

from megaton_lib.validation.playwright_pages import (
    TagsLaunchOverride,
    capture_selector_state,
    configure_tags_launch_override,
    run_page,
    run_with_basic_auth_page,
    run_with_launch_override,
)


class FakePage:
    def __init__(self) -> None:
        self.url = "https://example.test/page"
        self._title = "Example"
        self.goto_calls = []
        self.route_calls = []
        self.wait_calls = []

    def goto(self, url: str, **kwargs) -> None:
        self.goto_calls.append((url, kwargs))

    def route(self, pattern: str, handler) -> None:
        self.route_calls.append((pattern, handler))

    def wait_for_timeout(self, wait_ms: int) -> None:
        self.wait_calls.append(wait_ms)

    def evaluate(self, _script: str, selectors: list[str]) -> dict:
        return {
            selectors[0]: {"exists": True, "opacity": "1", "childCount": 2},
            selectors[1]: {"exists": False, "opacity": None, "childCount": 0},
        }

    def title(self) -> str:
        return self._title


class FakeRequest:
    def __init__(self, url: str, resource_type: str = "script") -> None:
        self.url = url
        self.resource_type = resource_type


class FakeRoute:
    def __init__(self) -> None:
        self.actions = []

    def continue_(self) -> None:
        self.actions.append(("continue", None))

    def abort(self) -> None:
        self.actions.append(("abort", None))

    def fulfill(self, **kwargs) -> None:
        self.actions.append(("fulfill", kwargs))


class FakeContext:
    def __init__(self, page: FakePage, options: dict) -> None:
        self.page_obj = page
        self.options = options
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
        self.context = FakeContext(self.page, kwargs)
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
    assert browser.context.options["http_credentials"] == {
        "username": "user",
        "password": "pass",
    }
    assert page.goto_calls[0][0] == "https://example.test/page"
    assert page.wait_calls == [10, 250]
    assert browser.context.closed is True
    assert browser.closed is True


def test_configure_tags_launch_override_auto_registers_expected_routes():
    page = FakePage()

    configure_tags_launch_override(
        page,
        "https://example.test/page",
        TagsLaunchOverride(
            launch_url=(
                "https://assets.adobedtm.com/6463582a3ff4/"
                "f7b418109cbc/launch-809bb5e4ca24-development.js"
            ),
            mode="auto",
        ),
    )

    assert [pattern for pattern, _handler in page.route_calls] == [
        "https://example.test/**",
        "**/launch-*staging*.js",
        "**/launch-*development*.js",
    ]


def test_run_page_applies_override_before_callback(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    result = run_page(
        "https://example.test/page",
        ignore_https_errors=True,
        tags_override=TagsLaunchOverride(
            launch_url=(
                "https://assets.adobedtm.com/6463582a3ff4/"
                "f7b418109cbc/launch-809bb5e4ca24-development.js"
            ),
            mode="launch_env",
        ),
        callback=lambda opened_page: len(opened_page.route_calls),
    )

    assert result == 2
    assert browser.context.options["ignore_https_errors"] is True
    assert [pattern for pattern, _handler in page.route_calls] == [
        "**/launch-*staging*.js",
        "**/launch-*development*.js",
    ]


def test_abort_old_property_assets_only_blocks_competing_launch_assets():
    page = FakePage()

    configure_tags_launch_override(
        page,
        "https://example.test/page",
        TagsLaunchOverride(
            launch_url=(
                "https://assets.adobedtm.com/6463582a3ff4/"
                "f7b418109cbc/launch-809bb5e4ca24-development.js"
            ),
            mode="launch_env",
            abort_old_property_assets=True,
        ),
    )

    property_handler = page.route_calls[-1][1]

    old_launch_route = FakeRoute()
    property_handler(
        old_launch_route,
        FakeRequest(
            "https://assets.adobedtm.com/6463582a3ff4/"
            "f7b418109cbc/launch-legacy-production.js",
        ),
    )
    assert old_launch_route.actions == [("abort", None)]

    source_route = FakeRoute()
    property_handler(
        source_route,
        FakeRequest(
            "https://assets.adobedtm.com/6463582a3ff4/"
            "f7b418109cbc/RC1234567890-source.js",
        ),
    )
    assert source_route.actions == [("continue", None)]


def test_run_with_launch_override_keeps_legacy_wrapper_behavior(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    result = run_with_launch_override(
        "https://example.test/page",
        (
            "https://assets.adobedtm.com/6463582a3ff4/"
            "f7b418109cbc/launch-809bb5e4ca24-development.js"
        ),
        ignore_https_errors=True,
        callback=lambda opened_page: {"routes": len(opened_page.route_calls)},
    )

    assert result == {"routes": 1}
    assert page.goto_calls[0][0] == "https://example.test/page"
    assert browser.context.options["ignore_https_errors"] is True
    assert [pattern for pattern, _handler in page.route_calls] == [
        "https://example.test/**",
    ]
