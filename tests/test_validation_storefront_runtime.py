from __future__ import annotations

from datetime import datetime
from pathlib import Path

from megaton_lib.validation.storefront_runtime import (
    JST,
    CapturedBeacons,
    DEFAULT_CAPTCHA_SELECTORS,
    StorefrontCheckoutState,
    analytics_satisfies_requirements,
    analytics_value_matches,
    append_pending_verification_task,
    append_unique_checkpoint,
    attempt_cart_checkout_entry,
    build_storefront_checkpoint,
    capture_storefront_checkpoint,
    get_analytics_path,
    is_login_form_page,
    load_storefront_session_cookies,
    next_aa_reflection_time,
    parse_analytics_path,
    perform_storefront_login,
    record_checkout_stage,
    run_storefront_validation_session,
    save_storefront_session_cookies,
    summarize_failed_analytics_requirements,
    wait_until_login_completed,
    write_progress_json,
)


def test_append_pending_verification_task_deduplicates(tmp_path: Path) -> None:
    pending_file = tmp_path / "pending.json"
    task = {
        "id": "dev-ec-baseline-202603281200",
        "verification_file": "out.json",
        "verification_type": "ec-baseline",
        "status": "pending",
    }

    added, existing = append_pending_verification_task(
        pending_file,
        task,
        duplicate_keys=("verification_file", "verification_type"),
    )
    assert added is True
    assert existing is None

    added, existing = append_pending_verification_task(
        pending_file,
        dict(task, id="dev-ec-baseline-202603281201"),
        duplicate_keys=("verification_file", "verification_type"),
    )
    assert added is False
    assert existing is not None
    assert existing["id"] == "dev-ec-baseline-202603281200"


def test_append_pending_verification_task_appends_without_duplicate_keys(tmp_path: Path) -> None:
    pending_file = tmp_path / "pending.json"

    append_pending_verification_task(pending_file, {"id": "one"})
    append_pending_verification_task(pending_file, {"id": "two"})

    payload = pending_file.read_text(encoding="utf-8")
    assert '"id": "one"' in payload
    assert '"id": "two"' in payload


def test_next_aa_reflection_time_rounds_to_next_batch() -> None:
    now = datetime(2026, 3, 28, 10, 12, 34, tzinfo=JST)
    due_at = next_aa_reflection_time(now)
    assert due_at.isoformat() == "2026-03-28T11:00:00+09:00"


def test_parse_and_get_analytics_path_supports_list_indexes() -> None:
    data = {"productListItems": [{"SKU": "sku-1"}], "merchandisingEVars": {"item": [{"eVar8": "M"}]}}

    assert parse_analytics_path("productListItems[0].SKU") == ["productListItems", 0, "SKU"]
    assert get_analytics_path(data, "productListItems[0].SKU") == "sku-1"
    assert get_analytics_path(data, "merchandisingEVars.item[0].eVar8") == "M"
    assert get_analytics_path(data, "productListItems[1].SKU") is None


def test_analytics_value_matching_and_requirement_diagnostics() -> None:
    analytics = {"pageName": "cart", "events": {"event11": {"value": 1}}, "items": ["a", "b"]}
    requirements = [
        {"key": "pageName", "op": "matches", "value": "cart"},
        {"key": "items", "op": "contains", "value": "b"},
        {"key": "events.event12.value", "op": "exists"},
    ]

    assert analytics_value_matches("abc", "contains", "b") is True
    assert analytics_value_matches(None, "empty") is True
    assert analytics_satisfies_requirements(analytics, requirements[:2]) is True
    assert analytics_satisfies_requirements(analytics, requirements) is False
    assert summarize_failed_analytics_requirements(analytics, requirements) == [
        {"key": "events.event12.value", "op": "exists", "expected": None, "actual": None}
    ]


def test_captured_beacons_find_matching_picks_richest_matching_event() -> None:
    beacons = CapturedBeacons()
    beacons.add(
        "https://edge.adobedc.net/ee/collect",
        {
            "events": [
                {
                    "xdm": {
                        "eventType": "commerce.productViews",
                        "web": {"webPageDetails": {"name": "product/detail"}},
                        "productListItems": [
                            {
                                "SKU": "sku-1",
                                "quantity": 1,
                                "_experience": {
                                    "analytics": {
                                        "customDimensions": {"eVars": {"eVar8": "color-red"}}
                                    }
                                },
                            }
                        ],
                        "_experience": {"analytics": {"customDimensions": {"eVars": {"eVar1": "page"}}}},
                    },
                },
                {
                    "xdm": {
                        "eventType": "commerce.productViews",
                        "web": {"webPageDetails": {"name": "product/detail"}},
                    },
                },
            ]
        },
    )

    rejected: list[dict] = []
    match = beacons.find_matching(
        page_name_pattern="product",
        event_type="commerce.productViews",
        requirements=[{"key": "productListItems[0].SKU", "op": "equals", "value": "sku-1"}],
        rejected_candidates=rejected,
    )

    assert match is not None
    assert match["productListItems"][0]["SKU"] == "sku-1"
    assert get_analytics_path(match, "merchandisingEVars.item[0].eVar8") == "color-red"
    assert rejected


class _FakeLocator:
    def __init__(self, visible: bool) -> None:
        self._visible = visible
        self.first = self
        self.clicked = False

    def count(self) -> int:
        return 1 if self._visible else 0

    def is_visible(self) -> bool:
        return self._visible

    def get_attribute(self, name: str):
        if name == "value":
            return getattr(self, "value", "")
        return None

    def click(self, timeout: int = 0) -> None:
        _ = timeout
        self.clicked = True


class _FakePage:
    def __init__(self, url: str, *, selectors: dict[str, bool] | None = None) -> None:
        self.url = url
        self._selectors = selectors or {}
        self.wait_calls: list[int] = []
        self.context = self
        self.locators: dict[str, _FakeLocator] = {}
        self.evaluate_calls: list[tuple[str, str | None]] = []

    def cookies(self):
        return [{"name": "CART_COUNT", "value": "2"}]

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self._selectors.get(selector, False))

    def wait_for_url(self, predicate, *, wait_until: str, timeout: int) -> None:
        _ = wait_until
        self.wait_calls.append(timeout)
        if not predicate(self.url):
            raise TimeoutError("still on login page")

    def query_selector(self, selector: str):
        if self._selectors.get(selector, False):
            locator = self.locators.get(selector)
            if locator is None:
                locator = _FakeLocator(True)
                locator.value = selector
                self.locators[selector] = locator
            return locator
        return None

    def wait_for_load_state(self, state: str, timeout: int = 0) -> None:
        _ = state
        self.wait_calls.append(timeout)

    def wait_for_timeout(self, timeout: int) -> None:
        self.wait_calls.append(timeout)

    def evaluate(self, script: str, selector: str | None = None):
        self.evaluate_calls.append((script, selector))
        return True


def test_is_login_form_page_by_url() -> None:
    page = _FakePage("https://example.com/disp/CSfLoginForm?scope=cart")
    assert is_login_form_page(page) is True


def test_is_login_form_page_by_visible_selectors() -> None:
    page = _FakePage(
        "https://example.com/order/cart",
        selectors={"input[autocomplete='username']": True},
    )
    assert is_login_form_page(page) is True


def test_wait_until_login_completed_skips_when_not_on_login_page() -> None:
    page = _FakePage("https://example.com/order/shippingStart")
    assert wait_until_login_completed(page, timeout_ms=30000, timeout_exc_type=TimeoutError) is True
    assert page.wait_calls == []


def test_wait_until_login_completed_uses_wait_helper() -> None:
    page = _FakePage("https://example.com/order/shippingStart")
    page._selectors = {"input[type='email']": True}
    page.url = "https://example.com/order/shippingStart"
    assert wait_until_login_completed(page, timeout_ms=30000, timeout_exc_type=TimeoutError) is True
    assert page.wait_calls == [5000]


def test_append_unique_checkpoint_skips_duplicates() -> None:
    checkpoints: list[dict[str, object]] = []
    assert append_unique_checkpoint(checkpoints, {"label": "cart", "url": "https://example.com/cart"}) is True
    assert append_unique_checkpoint(checkpoints, {"label": "cart", "url": "https://example.com/cart"}) is False
    assert len(checkpoints) == 1


def test_build_storefront_checkpoint_extracts_basic_fields() -> None:
    page = _FakePage("https://example.com/cart")
    snapshot = build_storefront_checkpoint(
        page,
        label="cart-start",
        digital_data={
            "page": {"pageInfo": {"pageName": "cart"}},
            "cart": {"item": [{"productInfo": {"productID": "HTY020"}}]},
        },
    )
    assert snapshot["pageName"] == "cart"
    assert snapshot["cart_count_cookie"] == "2"
    assert snapshot["cart_product_ids"] == ["HTY020"]


def test_capture_storefront_checkpoint_uses_dump_digital_data(monkeypatch) -> None:
    page = _FakePage("https://example.com/cart")
    checkpoints: list[dict[str, object]] = []
    monkeypatch.setattr(
        "megaton_lib.validation.storefront_runtime.dump_digital_data",
        lambda _page: {"page": {"pageInfo": {"pageName": "cart"}}},
    )
    assert capture_storefront_checkpoint(page, checkpoints, label="cart-start") is True
    assert checkpoints[0]["label"] == "cart-start"


def test_record_checkout_stage_maps_url_to_stage_and_checkpoint() -> None:
    seen_stages: list[str] = []
    seen_checkpoints: list[str] = []
    record_checkout_stage(
        "https://example.com/order/paymentBillingSubmit",
        log_stage=seen_stages.append,
        capture_checkpoint=seen_checkpoints.append,
    )
    assert seen_stages == ["payment"]
    assert seen_checkpoints == ["url:paymentBillingSubmit"]


def test_write_progress_json_writes_atomically(tmp_path: Path) -> None:
    progress_path = tmp_path / "progress.json"
    progress_tmp_path = tmp_path / "progress.tmp"
    write_progress_json(progress_path, progress_tmp_path, {"phase": "running"})
    assert progress_path.exists()
    assert not progress_tmp_path.exists()
    assert '"phase": "running"' in progress_path.read_text(encoding="utf-8")


def test_load_storefront_session_cookies_reads_cookie_list(tmp_path: Path) -> None:
    session_file = tmp_path / "session.json"
    session_file.write_text('[{"name":"sid","value":"123"}]', encoding="utf-8")

    cookies = load_storefront_session_cookies(session_file)

    assert cookies == [{"name": "sid", "value": "123"}]


def test_save_storefront_session_cookies_persists_context_cookies(tmp_path: Path) -> None:
    session_file = tmp_path / "nested" / "session.json"
    page = _FakePage("https://example.com/cart")

    save_storefront_session_cookies(page, session_file)

    assert '"name": "CART_COUNT"' in session_file.read_text(encoding="utf-8")


def test_run_storefront_validation_session_uses_cookie_preload_and_shared_setup(monkeypatch, tmp_path: Path) -> None:
    import megaton_lib.validation.storefront_runtime as mod

    session_file = tmp_path / "session.json"
    session_file.write_text('[{"name":"sid","value":"123"}]', encoding="utf-8")
    seen: list[object] = []

    class DummyPage:
        def __init__(self) -> None:
            self.routed = []

        def route(self, pattern, _handler) -> None:
            self.routed.append(pattern)

        def on(self, event, _handler) -> None:
            seen.append(("on", event))

    def fake_run_page_session(**kwargs):
        seen.append(kwargs["cookies"])
        page = DummyPage()
        kwargs["setup"](page)
        return kwargs["callback"](page)

    monkeypatch.setattr(mod, "run_page_session", fake_run_page_session)

    result = run_storefront_validation_session(
        headless=False,
        channel="chrome",
        slow_mo=200,
        session_file=session_file,
        domain="dev-store.example.test",
        basic_auth={"username": "u", "password": "p"},
        beacons=None,
        setup=lambda page: seen.append(("custom_setup", isinstance(page, DummyPage))),
        callback=lambda page: ("ok", page.routed),
    )

    assert result == ("ok", ["**://dev-store.example.test/**"])
    assert seen == [
        [{"name": "sid", "value": "123"}],
        ("custom_setup", True),
    ]


def test_storefront_checkout_state_as_result() -> None:
    state = StorefrontCheckoutState(checkout_clicked=True, login_submitted=True)
    page = _FakePage("https://example.com/order/shippingStart")
    result = state.as_result(page=page, checkpoints=[{"label": "cart-start"}])
    assert result["checkout_clicked"] is True
    assert result["login_submitted"] is True
    assert result["final_url"] == "https://example.com/order/shippingStart"


def test_perform_storefront_login_runs_callbacks() -> None:
    page = _FakePage("https://example.com/order/shippingStart")
    called: list[str] = []

    def fill_credentials(_page, login):
        assert login["email"] == "user@example.com"
        called.append("fill")

    def click_submit(_page):
        called.append("submit")
        return True

    submitted = perform_storefront_login(
        page,
        login={"email": "user@example.com", "password": "secret"},
        fill_credentials=fill_credentials,
        click_submit=click_submit,
        timeout_ms=30000,
        timeout_exc_type=TimeoutError,
        wait_label="checkout login to complete",
        before_submit=lambda: called.append("before"),
        on_stage=called.append,
        capture_checkpoint=lambda label: called.append(f"checkpoint:{label}"),
        on_success=lambda: called.append("success"),
    )
    assert submitted is True
    assert called == [
        "fill",
        "before",
        "login-submit",
        "submit",
        "success",
        "post-login",
        "checkpoint:post-login",
    ]


def test_perform_storefront_login_raises_on_captcha() -> None:
    selectors = {DEFAULT_CAPTCHA_SELECTORS[0]: True}
    page = _FakePage("https://example.com/Login", selectors=selectors)
    try:
        perform_storefront_login(
            page,
            login={"email": "user@example.com", "password": "secret"},
            fill_credentials=lambda *_args, **_kwargs: None,
            click_submit=lambda *_args, **_kwargs: True,
            timeout_ms=30000,
            timeout_exc_type=TimeoutError,
            wait_label="checkout login to complete",
        )
    except RuntimeError as exc:
        assert "CAPTCHA" in str(exc)
    else:
        raise AssertionError("CAPTCHA should raise RuntimeError")


def test_perform_storefront_login_uses_captcha_solver() -> None:
    selectors = {DEFAULT_CAPTCHA_SELECTORS[0]: True}
    page = _FakePage("https://example.com/order/shippingStart", selectors=selectors)
    called: list[str] = []

    def solver(_page):
        called.append("solver")
        page._selectors[DEFAULT_CAPTCHA_SELECTORS[0]] = False
        return True

    submitted = perform_storefront_login(
        page,
        login={"email": "user@example.com", "password": "secret"},
        fill_credentials=lambda *_args, **_kwargs: called.append("fill"),
        click_submit=lambda *_args, **_kwargs: called.append("submit") or True,
        timeout_ms=30000,
        timeout_exc_type=TimeoutError,
        wait_label="checkout login to complete",
        captcha_solver=solver,
    )

    assert submitted is True
    assert called == ["fill", "solver", "submit"]


def test_perform_storefront_login_raises_when_captcha_solver_returns_false() -> None:
    selectors = {DEFAULT_CAPTCHA_SELECTORS[0]: True}
    page = _FakePage("https://example.com/Login", selectors=selectors)

    try:
        perform_storefront_login(
            page,
            login={"email": "user@example.com", "password": "secret"},
            fill_credentials=lambda *_args, **_kwargs: None,
            click_submit=lambda *_args, **_kwargs: True,
            timeout_ms=30000,
            timeout_exc_type=TimeoutError,
            wait_label="checkout login to complete",
            captcha_solver=lambda _page: False,
        )
    except RuntimeError as exc:
        assert "did not solve" in str(exc)
    else:
        raise AssertionError("falsy CAPTCHA solver result should raise RuntimeError")


def test_perform_storefront_login_rechecks_captcha_after_solver() -> None:
    selectors = {DEFAULT_CAPTCHA_SELECTORS[0]: True}
    page = _FakePage("https://example.com/Login", selectors=selectors)

    try:
        perform_storefront_login(
            page,
            login={"email": "user@example.com", "password": "secret"},
            fill_credentials=lambda *_args, **_kwargs: None,
            click_submit=lambda *_args, **_kwargs: True,
            timeout_ms=30000,
            timeout_exc_type=TimeoutError,
            wait_label="checkout login to complete",
            captcha_solver=lambda _page: True,
        )
    except RuntimeError as exc:
        assert "still visible" in str(exc)
    else:
        raise AssertionError("visible CAPTCHA after solver should raise RuntimeError")


def test_perform_storefront_login_accepts_answered_capy_after_solver() -> None:
    selectors = {".capy-captcha": True, "input[name='capy_answer']": True}
    page = _FakePage("https://example.com/order/shippingStart", selectors=selectors)
    answer = _FakeLocator(True)
    answer.value = "null"
    page.locators["input[name='capy_answer']"] = answer
    called: list[str] = []

    def solver(_page):
        called.append("solver")
        answer.value = "answered-token"
        return True

    submitted = perform_storefront_login(
        page,
        login={"email": "user@example.com", "password": "secret"},
        fill_credentials=lambda *_args, **_kwargs: called.append("fill"),
        click_submit=lambda *_args, **_kwargs: called.append("submit") or True,
        timeout_ms=30000,
        timeout_exc_type=TimeoutError,
        wait_label="checkout login to complete",
        captcha_solver=solver,
    )

    assert submitted is True
    assert called == ["fill", "solver", "submit"]


def test_attempt_cart_checkout_entry_clicks_visible_button() -> None:
    page = _FakePage(
        "https://example.com/disp/viewCartLink",
        selectors={"input[value='レジへ進む']": True},
    )
    page.locators["input[value='レジへ進む']"] = _FakeLocator(True)
    page.locators["input[value='レジへ進む']"].value = "レジへ進む"

    clicked, label = attempt_cart_checkout_entry(
        page,
        selectors=["input[value='レジへ進む']"],
        skip_label_substrings=("新規会員登録",),
        before_click=lambda: page.wait_calls.append(-1),
    )
    assert clicked is True
    assert label == "レジへ進む"
    assert page.wait_calls[:2] == [-1, 30000]


def test_attempt_cart_checkout_entry_falls_back_to_form_submit() -> None:
    page = _FakePage(
        "https://example.com/disp/viewCartLink",
        selectors={"form.cart-action-checkout": True},
    )
    clicked, label = attempt_cart_checkout_entry(
        page,
        selectors=["input[value='レジへ進む']"],
    )
    assert clicked is True
    assert label == "form.cart-action-checkout.submit()"
    assert page.evaluate_calls
