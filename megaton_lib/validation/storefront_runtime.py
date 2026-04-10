"""Reusable storefront validation helpers for auth, beacon capture, and timing."""

from __future__ import annotations

import base64
import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from playwright.sync_api import Page
else:
    Page = Any

from .adobe_analytics import dump_digital_data
from .followups import JST, append_pending_verification_task, next_aa_reflection_time
from .playwright_pages import run_page_session
ADOBE_BEACON_HOSTS = ("edge.adobedc.net", "s-adobe.wacoal.jp")
DEFAULT_CAPTCHA_SELECTORS = (
    ".g-recaptcha",
    "iframe[src*='recaptcha']",
    ".h-captcha",
    "iframe[src*='hcaptcha']",
    "#captcha",
    ".captcha",
)


@dataclass
class CapturedBeacons:
    """Container for captured Adobe beacon requests plus derived ECID."""

    beacons: list[dict[str, Any]] = field(default_factory=list)
    ecid: str = ""

    def add(self, url: str, body: dict[str, Any] | None) -> None:
        self.beacons.append({"url": url, "body": body})

    def find_by_pagename(self, pattern: str) -> dict[str, Any] | None:
        """Find the first captured beacon whose pageName matches ``pattern``."""
        for beacon in self.beacons:
            body = beacon.get("body")
            if not isinstance(body, dict):
                continue
            for event in body.get("events", []):
                xdm = event.get("xdm", {})
                page_name = xdm.get("web", {}).get("webPageDetails", {}).get("name", "")
                if page_name and re.search(pattern, page_name):
                    return self._extract_analytics(event)
        return None

    def _extract_analytics(self, event: dict[str, Any]) -> dict[str, Any]:
        """Extract page-level analytics details from one edge event."""
        xdm = event.get("xdm", {})
        data_payload = event.get("data", {}) or xdm.get("data", {})
        analytics = xdm.get("_experience", {}).get("analytics", {})
        result: dict[str, Any] = {
            "pageName": xdm.get("web", {}).get("webPageDetails", {}).get("name", ""),
            "eventType": xdm.get("eventType", ""),
        }

        if analytics.get("productString"):
            result["products"] = analytics["productString"]

        for key in ("event1to100", "event101to200", "event201to300"):
            ev_data = analytics.get(key)
            if ev_data and isinstance(ev_data, dict):
                result["events"] = ev_data
                break

        custom_dims = analytics.get("customDimensions", {})
        if custom_dims.get("eVars"):
            result["eVars"] = custom_dims["eVars"]
        if custom_dims.get("props"):
            result["props"] = custom_dims["props"]
        if data_payload:
            result["data"] = data_payload
            if data_payload.get("currentTime"):
                result["currentTime"] = data_payload["currentTime"]
            if data_payload.get("currentDate"):
                result["currentDate"] = data_payload["currentDate"]

        if xdm.get("commerce"):
            result["commerce"] = xdm["commerce"]

        if xdm.get("productListItems"):
            items = xdm["productListItems"]
            result["productListItems"] = items
            merch: dict[str, dict[str, Any]] = {}
            for idx, item in enumerate(items):
                item_evars = (
                    item.get("_experience", {})
                    .get("analytics", {})
                    .get("customDimensions", {})
                    .get("eVars", {})
                )
                if item_evars:
                    merch[f"item[{idx}]"] = item_evars
            if merch:
                result["merchandisingEVars"] = merch

        identity_map = xdm.get("identityMap", {})
        renkeiid_list = identity_map.get("renkeiid", [])
        if renkeiid_list and renkeiid_list[0].get("id"):
            result["renkeiid"] = renkeiid_list[0]["id"]

        return result


@dataclass
class StorefrontCheckoutState:
    """Mutable state for a storefront checkout attempt."""

    checkout_clicked: bool = False
    checkout_label: str = ""
    login_page_seen: bool = False
    login_submitted: bool = False
    reached_login_form: bool = False
    reached_confirmation: bool = False

    def as_result(self, *, page: Page, checkpoints: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        return {
            "checkout_clicked": self.checkout_clicked,
            "checkout_label": self.checkout_label,
            "login_page_seen": self.login_page_seen,
            "login_submitted": self.login_submitted,
            "reached_login_form": self.reached_login_form,
            "reached_confirmation": self.reached_confirmation,
            "final_url": page.url,
            "checkpoints": list(checkpoints),
        }


def load_json_credentials(path: Path) -> dict[str, Any]:
    """Load a local credentials JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Credentials file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def is_login_form_page(page: Page, current_url: str = "") -> bool:
    """Return whether the current page appears to be a storefront login form."""
    url = current_url or str(getattr(page, "url", "") or "")
    if "Login" in url or "CSfLoginForm" in url or "/common/CSfLogin" in url:
        return True

    for sel in [
        "input[name='dwfrm_login_username']",
        "input[name='dwfrm_login_password']",
        "input[autocomplete='username']",
        "input[autocomplete='current-password']",
        "input[type='email']",
        "input[type='password']",
    ]:
        try:
            locator = page.locator(sel).first
            if locator.count() and locator.is_visible():
                return True
        except Exception:
            continue
    return False


def wait_until_login_completed(
    page: Page,
    *,
    timeout_ms: int,
    timeout_exc_type: type[Exception],
    label: str = "login to complete",
    heartbeat_ms: int = 5000,
) -> bool:
    """Wait until the page leaves the storefront login flow."""
    if not is_login_form_page(page):
        return True

    return wait_for_condition_with_heartbeat(
        lambda slice_ms: page.wait_for_url(
            lambda url: "Login" not in url and "CSfLoginForm" not in url and "/common/CSfLogin" not in url,
            wait_until="domcontentloaded",
            timeout=slice_ms,
        ),
        timeout_ms=timeout_ms,
        label=label,
        timeout_exc_type=timeout_exc_type,
        heartbeat_ms=heartbeat_ms,
    )


def perform_storefront_login(
    page: Page,
    *,
    login: Mapping[str, Any],
    fill_credentials,
    click_submit,
    timeout_ms: int,
    timeout_exc_type: type[Exception],
    wait_label: str,
    captcha_selectors: Sequence[str] = DEFAULT_CAPTCHA_SELECTORS,
    before_submit=None,
    on_stage=None,
    capture_checkpoint=None,
    submit_stage: str = "login-submit",
    success_stage: str = "post-login",
    success_checkpoint: str | None = None,
    on_success=None,
) -> bool:
    """Fill, submit, and wait for storefront login completion."""
    fill_credentials(page, login)

    for sel in captcha_selectors:
        try:
            elem = page.query_selector(sel)
            if elem and elem.is_visible():
                raise RuntimeError("CAPTCHA detected during storefront login")
        except RuntimeError:
            raise
        except Exception:
            continue

    if before_submit:
        before_submit()
    if on_stage:
        on_stage(submit_stage)

    submitted = bool(click_submit(page))

    wait_succeeded = wait_until_login_completed(
        page,
        timeout_ms=timeout_ms,
        timeout_exc_type=timeout_exc_type,
        label=wait_label,
    )
    if not wait_succeeded:
        raise timeout_exc_type(f"Timed out waiting for {wait_label}")

    if on_success:
        on_success()
    if on_stage:
        on_stage(success_stage)
    if capture_checkpoint:
        capture_checkpoint(success_checkpoint or success_stage)
    return submitted


def attempt_cart_checkout_entry(
    page: Page,
    *,
    selectors: Sequence[str],
    skip_label_substrings: Sequence[str] = (),
    fallback_form_selector: str = "form.cart-action-checkout",
    before_click=None,
    load_timeout_ms: int = 30000,
    post_wait_ms: int = 0,
) -> tuple[bool, str]:
    """Attempt one cart -> checkout transition via button click or form submit fallback."""
    current_url = page.url
    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if not btn or not btn.is_visible():
                continue
            value = btn.get_attribute("value") or ""
            if any(text in value for text in skip_label_substrings):
                continue
            if before_click:
                before_click()
            btn.click(timeout=5000)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=load_timeout_ms)
            except Exception:
                pass
            if post_wait_ms > 0:
                page.wait_for_timeout(post_wait_ms)
            return True, value
        except Exception:
            continue

    if page.url == current_url:
        form = page.query_selector(fallback_form_selector)
        if form:
            if before_click:
                before_click()
            page.evaluate(
                """
                (selector) => {
                  const form = document.querySelector(selector);
                  if (form) {
                    form.submit();
                    return true;
                  }
                  return false;
                }
                """,
                fallback_form_selector,
            )
            try:
                page.wait_for_load_state("domcontentloaded", timeout=load_timeout_ms)
            except Exception:
                pass
            if post_wait_ms > 0:
                page.wait_for_timeout(post_wait_ms)
            return True, f"{fallback_form_selector}.submit()"

    return False, ""


def wait_for_condition_with_heartbeat(
    wait_fn,
    *,
    timeout_ms: int,
    label: str,
    timeout_exc_type: type[Exception],
    heartbeat_ms: int = 5000,
) -> bool:
    """Wait in smaller slices so long Playwright waits stay visible in CLI output."""
    if timeout_ms <= 0:
        wait_fn(0)
        return True

    started_at = time.monotonic()
    total_seconds = max(timeout_ms / 1000, 0)
    while True:
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        remaining_ms = timeout_ms - elapsed_ms
        if remaining_ms <= 0:
            return False

        slice_ms = min(heartbeat_ms, remaining_ms)
        try:
            wait_fn(slice_ms)
            return True
        except timeout_exc_type:
            waited_seconds = min(timeout_ms, int((time.monotonic() - started_at) * 1000)) / 1000
            print(f"     ... waiting for {label} ({waited_seconds:.0f}/{total_seconds:.0f}s)", flush=True)


def append_unique_checkpoint(
    checkpoints: list[dict[str, Any]],
    snapshot: Mapping[str, Any],
) -> bool:
    """Append a checkpoint only when it differs from the last snapshot."""
    normalized = dict(snapshot)
    if checkpoints and checkpoints[-1] == normalized:
        return False
    checkpoints.append(normalized)
    return True


def build_storefront_checkpoint(
    page: Page,
    *,
    label: str,
    digital_data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized storefront checkpoint snapshot."""
    payload = dict(digital_data or {})
    cart_items = ((payload.get("cart") or {}).get("item") or []) if isinstance(payload, dict) else []
    product_ids: list[str] = []
    for item in cart_items if isinstance(cart_items, list) else []:
        product_id = (((item or {}).get("productInfo") or {}).get("productID")) or ""
        if product_id:
            product_ids.append(str(product_id))
    try:
        cookie_map = {cookie["name"]: cookie["value"] for cookie in page.context.cookies()}
    except Exception:
        cookie_map = {}
    return {
        "label": label,
        "url": page.url,
        "pageName": _get_path(payload, "page.pageInfo.pageName"),
        "cart_count_cookie": cookie_map.get("CART_COUNT"),
        "cart_product_ids": product_ids,
    }


def capture_storefront_checkpoint(
    page: Page,
    checkpoints: list[dict[str, Any]],
    *,
    label: str,
) -> bool:
    """Capture and append a storefront checkpoint snapshot."""
    try:
        digital_data = dump_digital_data(page) or {}
    except Exception:
        digital_data = {}
    snapshot = build_storefront_checkpoint(page, label=label, digital_data=digital_data)
    return append_unique_checkpoint(checkpoints, snapshot)


def record_checkout_stage(
    current_url: str,
    *,
    log_stage,
    capture_checkpoint,
) -> None:
    """Record generic checkout stage and URL checkpoint from the current URL."""
    if "shippingStart" in current_url:
        log_stage("shipping")
    elif "paymentInfo" in current_url or "paymentBillingSubmit" in current_url:
        log_stage("payment")
    elif "orderSubmit" in current_url:
        log_stage("confirmation")
    elif "viewCartLink" in current_url:
        log_stage("cart")
    elif "Login" in current_url or "CSfLogin" in current_url:
        log_stage("login")

    tokens = ("shippingStart", "paymentInfo", "paymentBillingSubmit", "orderSubmit", "viewCartLink", "Login", "CSfLogin")
    if any(token in current_url for token in tokens):
        capture_checkpoint(f"url:{current_url.split('?', 1)[0].rsplit('/', 1)[-1]}")


def write_progress_json(
    progress_path: Path,
    progress_tmp_path: Path,
    payload: Mapping[str, Any],
) -> None:
    """Write a progress payload atomically via a temp file."""
    progress_tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(progress_tmp_path, "w", encoding="utf-8") as pf:
        json.dump(dict(payload), pf, indent=2, ensure_ascii=False)
    progress_tmp_path.replace(progress_path)


def _get_path(data: Mapping[str, Any] | Any, path: str, default: Any = None) -> Any:
    current = data
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return default
        if part not in current:
            return default
        current = current[part]
    return current


def _matches_any_host(url: str, hosts: Sequence[str]) -> bool:
    return any(host in url for host in hosts)


def _parse_request_json(request) -> dict[str, Any] | None:
    body = request.post_data
    if not body:
        return None
    if isinstance(body, str):
        return json.loads(body)
    return json.loads(body.decode())


def _extract_ecid_from_interact_response(
    response,
    *,
    beacon_hosts: Sequence[str],
) -> str:
    url = response.url
    if "/interact" not in url or not _matches_any_host(url, beacon_hosts):
        return ""
    try:
        body = response.json()
    except Exception:
        return ""

    for handle in body.get("handle", []):
        for payload in handle.get("payload", []):
            namespace = payload.get("namespace", {})
            if namespace.get("code") == "ECID" and payload.get("id"):
                return str(payload["id"])
    return ""


def setup_storefront_validation_page(
    page: Page,
    *,
    domain: str | None = None,
    basic_auth: Mapping[str, str] | None = None,
    embed_override: str | None = None,
    beacons: CapturedBeacons | None = None,
    beacon_hosts: Sequence[str] = ADOBE_BEACON_HOSTS,
) -> None:
    """Attach common storefront validation routes and Adobe beacon capture."""
    if domain and basic_auth:
        auth_header = base64.b64encode(
            f"{basic_auth['username']}:{basic_auth['password']}".encode(),
        ).decode()

        def handle_auth(route):  # type: ignore[no-untyped-def]
            headers = route.request.headers.copy()
            headers["Authorization"] = f"Basic {auth_header}"
            route.continue_(headers=headers)

        page.route(f"**://{domain}/**", handle_auth)

    if embed_override:
        override_js: bytes | None = None

        def handle_embed(route):  # type: ignore[no-untyped-def]
            nonlocal override_js
            if "launch-" not in route.request.url:
                route.continue_()
                return

            if override_js is None:
                import urllib.request

                with urllib.request.urlopen(embed_override, timeout=30) as resp:
                    override_js = resp.read()
            route.fulfill(body=override_js, content_type="application/javascript")

        page.route("**/launch-*.js", handle_embed)

    if beacons is None:
        return

    def on_request(request):  # type: ignore[no-untyped-def]
        url = request.url
        if not _matches_any_host(url, beacon_hosts):
            return
        try:
            parsed = _parse_request_json(request)
            if parsed is not None:
                beacons.add(url, parsed)
        except Exception:
            beacons.add(url, None)

    def on_response(response):  # type: ignore[no-untyped-def]
        ecid = _extract_ecid_from_interact_response(response, beacon_hosts=beacon_hosts)
        if ecid:
            beacons.ecid = ecid

    page.on("request", on_request)
    page.on("response", on_response)


def load_storefront_session_cookies(
    session_file: str | Path | None,
) -> list[dict[str, Any]]:
    """Load persisted storefront session cookies from disk when present."""
    if session_file is None:
        return []
    path = Path(session_file)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError(f"session cookie file must contain a list: {path}")
    return [dict(item) for item in payload if isinstance(item, dict)]


def save_storefront_session_cookies(
    page: Page,
    session_file: str | Path,
) -> None:
    """Persist current context cookies for later storefront validation runs."""
    path = Path(session_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(page.context.cookies(), f, indent=2, ensure_ascii=False)


def run_storefront_validation_session(
    *,
    headless: bool = True,
    channel: str | None = None,
    slow_mo: int = 0,
    session_file: str | Path | None = None,
    domain: str | None = None,
    basic_auth: Mapping[str, str] | None = None,
    embed_override: str | None = None,
    beacons: CapturedBeacons | None = None,
    beacon_hosts: Sequence[str] = ADOBE_BEACON_HOSTS,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
) -> Any:
    """Run a storefront validation session with cookie preload and shared setup."""
    session_cookies = load_storefront_session_cookies(session_file)

    def _setup(page: Page) -> None:
        if setup is not None:
            setup(page)
        setup_storefront_validation_page(
            page,
            domain=domain,
            basic_auth=basic_auth,
            embed_override=embed_override,
            beacons=beacons,
            beacon_hosts=beacon_hosts,
        )

    return run_page_session(
        headless=headless,
        channel=channel,
        slow_mo=slow_mo,
        cookies=session_cookies,
        setup=_setup,
        callback=callback,
    )


__all__ = [
    "ADOBE_BEACON_HOSTS",
    "CapturedBeacons",
    "DEFAULT_CAPTCHA_SELECTORS",
    "StorefrontCheckoutState",
    "JST",
    "append_pending_verification_task",
    "append_unique_checkpoint",
    "attempt_cart_checkout_entry",
    "build_storefront_checkpoint",
    "capture_storefront_checkpoint",
    "dump_digital_data",
    "is_login_form_page",
    "load_json_credentials",
    "load_storefront_session_cookies",
    "next_aa_reflection_time",
    "perform_storefront_login",
    "record_checkout_stage",
    "run_storefront_validation_session",
    "save_storefront_session_cookies",
    "setup_storefront_validation_page",
    "wait_until_login_completed",
    "wait_for_condition_with_heartbeat",
    "write_progress_json",
]
