from __future__ import annotations

from megaton_lib.validation.playwright_pages import (
    DEFAULT_STEALTH_USER_AGENT,
    GtmPreviewOverride,
    TagsLaunchOverride,
    build_gtm_preview_override,
    capture_storage_state,
    capture_satellite_info,
    capture_selector_state,
    click_selector_if_visible,
    configure_gtm_preview_override,
    configure_tags_launch_override,
    enable_selector,
    run_page,
    run_page_session,
    run_page_with_bootstrapped_state,
    run_with_basic_auth_page,
    run_with_launch_override,
    scroll_selector_region_to_end,
    scroll_selector_into_view,
    set_checkbox_checked,
    wait_for_any_selector,
)


class FakeElement:
    def __init__(self, *, visible: bool = True) -> None:
        self.visible = visible

    def is_visible(self) -> bool:
        return self.visible


class FakePage:
    def __init__(self) -> None:
        self.url = "https://example.test/page"
        self._title = "Example"
        self.goto_calls = []
        self.route_calls = []
        self.wait_calls = []
        self.selector_map: dict[str, object | None] = {}
        self.scroll_calls: list[dict[str, str]] = []
        self.click_calls: list[tuple[str, bool]] = []
        self.enabled_selectors: list[str] = []
        self.region_scrolls: list[str] = []

    def goto(self, url: str, **kwargs) -> None:
        self.goto_calls.append((url, kwargs))

    def route(self, pattern: str, handler) -> None:
        self.route_calls.append((pattern, handler))

    def wait_for_timeout(self, wait_ms: int) -> None:
        self.wait_calls.append(wait_ms)

    def click(self, selector: str, *, force: bool = False) -> None:
        self.click_calls.append((selector, force))

    def evaluate(self, _script: str, payload=None):
        if isinstance(payload, list):
            return {
                payload[0]: {"exists": True, "opacity": "1", "childCount": 2},
                payload[1]: {"exists": False, "opacity": None, "childCount": 0},
            }
        if isinstance(payload, dict) and "selector" in payload:
            selector = payload["selector"]
            if self.selector_map.get(selector) is None:
                return False
            self.scroll_calls.append(
                {"selector": selector, "block": payload.get("block", "center")}
            )
            return True
        if isinstance(payload, str):
            if self.selector_map.get(payload) is None:
                return False
            if "scrollTop = el.scrollHeight" in _script:
                self.region_scrolls.append(payload)
                return True
            self.enabled_selectors.append(payload)
            return True
        return None

    def title(self) -> str:
        return self._title

    def query_selector(self, selector: str):
        return self.selector_map.get(selector)


class FakeRequest:
    def __init__(self, url: str, resource_type: str = "script") -> None:
        self.url = url
        self.resource_type = resource_type


class FakeRoute:
    def __init__(self) -> None:
        self.actions = []

    def continue_(self, **kwargs) -> None:
        self.actions.append(("continue", kwargs or None))

    def abort(self) -> None:
        self.actions.append(("abort", None))

    def fulfill(self, **kwargs) -> None:
        self.actions.append(("fulfill", kwargs))


class FakeContext:
    def __init__(self, page: FakePage, options: dict) -> None:
        self.page_obj = page
        self.options = options
        self.closed = False
        self.added_cookies = []
        self.init_scripts: list[str] = []

    def new_page(self) -> FakePage:
        self.page_obj.context = self
        return self.page_obj

    def close(self) -> None:
        self.closed = True

    def add_cookies(self, cookies) -> None:
        self.added_cookies.extend(cookies)

    def add_init_script(self, script: str) -> None:
        # Real Playwright's add_init_script attaches a JS snippet that
        # runs in every frame before any page script. The fake just
        # records what was attached so tests can inspect it.
        self.init_scripts.append(script)

    def storage_state(self) -> dict:
        return {"cookies": [{"name": "session", "value": "abc"}], "origins": []}


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

    def launch(
        self,
        *,
        headless: bool,
        channel: str | None = None,
        slow_mo: int | None = None,
        args: list[str] | None = None,
    ) -> FakeBrowser:
        self.launch_calls.append(
            {
                "headless": headless,
                "channel": channel,
                "slow_mo": slow_mo,
                "args": list(args) if args else [],
            }
        )
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


def test_wait_for_any_selector_returns_first_match_after_poll(monkeypatch):
    import megaton_lib.validation.playwright_pages as mod

    page = FakePage()
    timeline = iter([0.0, 0.2, 0.6, 0.6])

    def _monotonic() -> float:
        return next(timeline)

    def _wait_for_timeout(wait_ms: int) -> None:
        page.wait_calls.append(wait_ms)
        page.selector_map["#loaded"] = object()

    monkeypatch.setattr(mod.time, "monotonic", _monotonic)
    monkeypatch.setattr(page, "wait_for_timeout", _wait_for_timeout)

    result = wait_for_any_selector(
        page,
        ["#missing", "#loaded"],
        timeout_ms=1000,
        poll_ms=500,
        settle_ms=300,
    )

    assert result == "#loaded"
    assert page.wait_calls == [500, 300]


def test_wait_for_any_selector_returns_none_on_timeout(monkeypatch):
    import megaton_lib.validation.playwright_pages as mod

    page = FakePage()
    timeline = iter([0.0, 0.2, 0.6, 1.1])

    monkeypatch.setattr(mod.time, "monotonic", lambda: next(timeline))

    result = wait_for_any_selector(
        page,
        ["#missing"],
        timeout_ms=1000,
        poll_ms=400,
    )

    assert result is None
    assert page.wait_calls == [400, 400]


def test_click_selector_if_visible_clicks_visible_element():
    page = FakePage()
    page.selector_map["#banner"] = FakeElement(visible=True)

    result = click_selector_if_visible(page, "#banner", settle_ms=200)

    assert result is True
    assert page.click_calls == [("#banner", False)]
    assert page.wait_calls == [200]


def test_click_selector_if_visible_skips_hidden_element():
    page = FakePage()
    page.selector_map["#banner"] = FakeElement(visible=False)

    result = click_selector_if_visible(page, "#banner")

    assert result is False
    assert page.click_calls == []


def test_scroll_selector_region_to_end_scrolls_existing_region():
    page = FakePage()
    page.selector_map["#region"] = object()

    result = scroll_selector_region_to_end(page, "#region", settle_ms=150)

    assert result is True
    assert page.region_scrolls == ["#region"]
    assert page.wait_calls == [150]


def test_enable_selector_marks_selector_enabled():
    page = FakePage()
    page.selector_map["#submit"] = object()

    result = enable_selector(page, "#submit", settle_ms=120)

    assert result is True
    assert page.enabled_selectors == ["#submit"]
    assert page.wait_calls == [120]


def test_set_checkbox_checked_enables_and_clicks():
    page = FakePage()
    page.selector_map["#agree"] = object()

    result = set_checkbox_checked(page, "#agree", settle_ms=180)

    assert result is True
    assert page.enabled_selectors == ["#agree"]
    assert page.click_calls == [("#agree", True)]
    assert page.wait_calls == [180]


def test_scroll_selector_into_view_returns_true_for_existing_element():
    page = FakePage()
    page.selector_map["#hero"] = object()

    result = scroll_selector_into_view(page, "#hero", block="center", settle_ms=250)

    assert result is True
    assert page.scroll_calls == [{"selector": "#hero", "block": "center"}]
    assert page.wait_calls == [250]


def test_scroll_selector_into_view_returns_false_when_missing():
    page = FakePage()

    result = scroll_selector_into_view(page, "#hero")

    assert result is False
    assert page.scroll_calls == []
    assert page.wait_calls == []


def test_capture_satellite_info_uses_page_evaluation():
    class SatellitePage:
        def evaluate(self, script: str) -> dict:
            assert "hasSatellite" in script
            return {"hasSatellite": True, "buildDate": "2026-04-10T00:00:00Z"}

    result = capture_satellite_info(SatellitePage())

    assert result == {
        "hasSatellite": True,
        "buildDate": "2026-04-10T00:00:00Z",
    }


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
    assert chromium.launch_calls == [
        {
            "headless": True,
            "channel": None,
            "slow_mo": None,
            # stealth=True default adds the AutomationControlled blink-feature
            # disable arg; tests for the legacy headless fingerprint should
            # opt out via stealth=False (see other tests below).
            "args": ["--disable-blink-features=AutomationControlled"],
        }
    ]
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


def test_build_gtm_preview_override_parses_tagassistant_url():
    override = build_gtm_preview_override(
        {
            "previewUrl": (
                "https://tagassistant.google.com/#/?id=GTM-TJKK7S5"
                "&url=https%3A%2F%2Fcorp.shiseido.com%2Fjp%2Frd%2Fsafety%2F"
                "&source=TAG_MANAGER"
                "&canonical_id=6020437"
                "&gtm_auth=token123"
                "&gtm_preview=env-361"
            )
        },
        require=True,
    )

    assert override == GtmPreviewOverride(
        container_id="GTM-TJKK7S5",
        auth_token="token123",
        preview_id="env-361",
        cookies_win="x",
    )


def test_configure_gtm_preview_override_rewrites_gtm_request():
    page = FakePage()
    override = GtmPreviewOverride(
        container_id="GTM-TJKK7S5",
        auth_token="token123",
        preview_id="env-361",
    )

    configure_gtm_preview_override(page, override)

    assert len(page.route_calls) == 2
    handler = page.route_calls[0][1]
    route = FakeRoute()
    request = FakeRequest(
        "https://www.googletagmanager.com/gtm.js?id=GTM-TJKK7S5&l=dataLayer",
    )
    handler(route, request)
    assert route.actions == [
        (
            "continue",
            {
                "url": (
                    "https://www.googletagmanager.com/gtm.js"
                    "?id=GTM-TJKK7S5&l=dataLayer&gtm_auth=token123"
                    "&gtm_preview=env-361&gtm_cookies_win=x"
                )
            },
        )
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
    assert chromium.launch_calls == [
        {
            "headless": True,
            "channel": None,
            "slow_mo": None,
            # stealth=True default adds the AutomationControlled blink-feature
            # disable arg; tests for the legacy headless fingerprint should
            # opt out via stealth=False (see other tests below).
            "args": ["--disable-blink-features=AutomationControlled"],
        }
    ]
    assert [pattern for pattern, _handler in page.route_calls] == [
        "**/launch-*staging*.js",
        "**/launch-*development*.js",
    ]


def test_run_page_passes_storage_state_and_viewport(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    result = run_page(
        "https://example.test/page",
        storage_state={"cookies": []},
        viewport={"width": 1440, "height": 900},
        callback=lambda opened_page: opened_page.url,
    )

    assert result == "https://example.test/page"
    assert browser.context.options["storage_state"] == {"cookies": []}
    assert browser.context.options["viewport"] == {"width": 1440, "height": 900}


def test_capture_storage_state_returns_context_state(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    state = capture_storage_state(
        ignore_https_errors=True,
        viewport={"width": 1280, "height": 720},
        setup=lambda opened_page: opened_page.wait_for_timeout(5),
        callback=lambda opened_page: opened_page.goto("https://example.test/login"),
    )

    assert state == {"cookies": [{"name": "session", "value": "abc"}], "origins": []}
    assert browser.context.options["ignore_https_errors"] is True
    assert browser.context.options["viewport"] == {"width": 1280, "height": 720}


def test_run_page_session_supports_cookies_and_launch_options(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    seen = []

    result = run_page_session(
        headless=False,
        channel="chrome",
        slow_mo=200,
        cookies=[{"name": "sid", "value": "123"}],
        context_setup=lambda context: seen.append(("context", context.options.copy())),
        setup=lambda opened_page: seen.append(("setup", opened_page.url)),
        callback=lambda opened_page: ("ok", opened_page.context.added_cookies),
    )

    assert result == ("ok", [{"name": "sid", "value": "123"}])
    assert chromium.launch_calls == [
        {
            "headless": False,
            "channel": "chrome",
            "slow_mo": 200,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
    ]
    assert seen == [
        ("context", {"user_agent": DEFAULT_STEALTH_USER_AGENT}),
        ("setup", "https://example.test/page"),
    ]


def test_run_page_session_stealth_defaults_and_opt_out(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    run_page_session(callback=lambda opened_page: opened_page.url)

    assert chromium.launch_calls[0]["args"] == [
        "--disable-blink-features=AutomationControlled"
    ]
    assert browser.context.options["user_agent"] == DEFAULT_STEALTH_USER_AGENT
    assert browser.context.init_scripts

    page2 = FakePage()
    browser2 = FakeBrowser(page2)
    chromium2 = FakeChromium(browser2)
    monkeypatch.setattr(
        mod,
        "sync_playwright",
        lambda: FakePlaywrightManager(FakePlaywright(chromium2)),
    )

    run_page_session(stealth=False, callback=lambda opened_page: opened_page.url)

    assert chromium2.launch_calls[0]["args"] == []
    assert "user_agent" not in browser2.context.options
    assert browser2.context.init_scripts == []


def test_run_page_wrapper_accepts_user_agent_and_stealth_opt_out(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    run_page(
        "https://example.test/page",
        user_agent="Custom UA",
        stealth=False,
        callback=lambda opened_page: opened_page.url,
    )

    assert chromium.launch_calls[0]["args"] == []
    assert browser.context.options["user_agent"] == "Custom UA"
    assert browser.context.init_scripts == []


def test_run_page_with_bootstrapped_state_reuses_captured_state(monkeypatch):
    import megaton_lib.validation.playwright_pages as mod

    seen = []

    monkeypatch.setattr(
        mod,
        "capture_storage_state",
        lambda **kwargs: (
            seen.append(("bootstrap", kwargs["ignore_https_errors"], kwargs["viewport"])),
            {"cookies": [{"name": "session", "value": "boot"}]},
        )[1],
    )
    monkeypatch.setattr(
        mod,
        "run_page",
        lambda url, **kwargs: (
            seen.append(("run", url, kwargs["storage_state"], kwargs["tags_override"])),
            "ok",
        )[1],
    )

    result = run_page_with_bootstrapped_state(
        "https://example.test/page",
        ignore_https_errors=True,
        viewport={"width": 1200, "height": 800},
        tags_override=TagsLaunchOverride(
            launch_url="https://assets.adobedtm.com/company/property/launch-dev.js",
            mode="launch_env",
        ),
        bootstrap=lambda page: None,
        callback=lambda page: None,
    )

    assert result == "ok"
    assert seen == [
        ("bootstrap", True, {"width": 1200, "height": 800}),
        (
            "run",
            "https://example.test/page",
            {"cookies": [{"name": "session", "value": "boot"}]},
            TagsLaunchOverride(
                launch_url="https://assets.adobedtm.com/company/property/launch-dev.js",
                mode="launch_env",
            ),
        ),
    ]


def test_run_page_applies_gtm_preview_before_callback(monkeypatch):
    page = FakePage()
    browser = FakeBrowser(page)
    chromium = FakeChromium(browser)
    manager = FakePlaywrightManager(FakePlaywright(chromium))

    import megaton_lib.validation.playwright_pages as mod

    monkeypatch.setattr(mod, "sync_playwright", lambda: manager)

    result = run_page(
        "https://example.test/page",
        gtm_preview=GtmPreviewOverride(
            container_id="GTM-TJKK7S5",
            auth_token="token123",
            preview_id="env-361",
        ),
        callback=lambda opened_page: len(opened_page.route_calls),
    )

    assert result == 2


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


def test_run_page_session_requires_route_url_for_tags_override():
    try:
        run_page_session(
            tags_override=TagsLaunchOverride(
                launch_url="https://assets.adobedtm.com/company/property/launch-dev.js",
            ),
            callback=lambda page: None,
        )
    except ValueError as exc:
        assert str(exc) == "route_url is required when tags_override is provided"
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError")
