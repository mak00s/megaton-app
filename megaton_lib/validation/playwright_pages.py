"""Shared Playwright helpers for page checks and Adobe Tags overrides."""

from __future__ import annotations

from dataclasses import dataclass
import re
from collections.abc import Callable
from typing import Any, Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from playwright.sync_api import Page, sync_playwright


_SATELLITE_LIB_PATTERN = re.compile(
    r'https://assets\.adobedtm\.com/[0-9a-f]+/satelliteLib-[0-9a-f]+\.js'
)


@dataclass(frozen=True)
class TagsLaunchOverride:
    """Configuration for overriding Adobe Tags launch assets in Playwright."""

    launch_url: str
    mode: Literal["auto", "legacy_satellite", "launch_env"] = "auto"
    env_patterns: tuple[str, ...] = ("staging", "development")
    abort_old_property_assets: bool = False


def _property_base_prefix(launch_url: str) -> str | None:
    match = re.match(
        r"(https://assets\.adobedtm\.com/[0-9a-f]+/[0-9a-f]+/)",
        launch_url,
    )
    return match.group(1) if match else None


def _page_origin(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _matches_launch_env_request(url: str, env_patterns: tuple[str, ...]) -> bool:
    return "/launch-" in url and ".js" in url and any(env in url for env in env_patterns)


def _is_launch_asset_request(url: str) -> bool:
    return "/launch-" in url and ".js" in url


def _fetch_launch_asset(launch_url: str) -> bytes:
    req = Request(launch_url, headers={"Cache-Control": "no-cache"})
    with urlopen(req, timeout=30) as resp:
        return resp.read()


def _response_headers(response) -> dict[str, str]:
    headers = dict(response.headers)
    headers.pop("content-length", None)
    return headers


def configure_tags_launch_override(
    page: Page,
    url: str,
    override: TagsLaunchOverride,
) -> None:
    """Configure Playwright routes to replace Adobe Tags launch assets."""
    base_prefix = _property_base_prefix(override.launch_url)
    launch_asset_cache: bytes | None = None

    def _get_launch_asset() -> bytes:
        nonlocal launch_asset_cache
        if launch_asset_cache is None:
            launch_asset_cache = _fetch_launch_asset(override.launch_url)
        return launch_asset_cache

    if override.mode in ("auto", "legacy_satellite"):
        page_origin = _page_origin(url)

        def _patch_html(route, request):  # type: ignore[no-untyped-def]
            if getattr(request, "resource_type", "") != "document":
                route.continue_()
                return

            response = route.fetch()
            content_type = response.headers.get("content-type", "")
            if "html" not in content_type:
                route.fulfill(
                    status=response.status,
                    headers=_response_headers(response),
                    body=response.body(),
                )
                return

            body = response.body().decode("utf-8", errors="replace")
            new_body = _SATELLITE_LIB_PATTERN.sub(override.launch_url, body)
            route.fulfill(
                status=response.status,
                headers=_response_headers(response),
                body=new_body.encode("utf-8"),
            )

        page.route(f"{page_origin}/**", _patch_html)

    if override.mode in ("auto", "launch_env"):
        for env in override.env_patterns:
            pattern = f"**/launch-*{env}*.js"

            def _handle_launch_env(route, _request):  # type: ignore[no-untyped-def]
                route.fulfill(
                    body=_get_launch_asset(),
                    content_type="application/javascript",
                )

            page.route(pattern, _handle_launch_env)

    if base_prefix and override.abort_old_property_assets:
        def _handle_property_asset(route, request):  # type: ignore[no-untyped-def]
            if request.url == override.launch_url:
                route.continue_()
                return

            if (
                override.mode in ("auto", "launch_env")
                and _matches_launch_env_request(request.url, override.env_patterns)
            ):
                route.fulfill(
                    body=_get_launch_asset(),
                    content_type="application/javascript",
                )
                return

            if _is_launch_asset_request(request.url):
                route.abort()
                return

            route.continue_()

        page.route(f"{base_prefix}**", _handle_property_asset)


def run_page(
    url: str,
    *,
    wait_ms: int = 0,
    headless: bool = True,
    ignore_https_errors: bool = False,
    basic_auth: dict[str, str] | None = None,
    tags_override: TagsLaunchOverride | None = None,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
) -> Any:
    """Open a Playwright page, apply optional overrides, and run a callback."""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context_options: dict[str, Any] = {}
        if ignore_https_errors:
            context_options["ignore_https_errors"] = True
        if basic_auth:
            context_options["http_credentials"] = basic_auth

        context = browser.new_context(**context_options)
        page = context.new_page()
        try:
            if tags_override is not None:
                configure_tags_launch_override(page, url, tags_override)
            if setup is not None:
                setup(page)
            result = callback(page)
            if wait_ms > 0:
                page.wait_for_timeout(wait_ms)
            return result
        finally:
            context.close()
            browser.close()


def run_with_basic_auth_page(
    url: str,
    username: str,
    password: str,
    *,
    wait_ms: int = 0,
    headless: bool = True,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
) -> Any:
    """Open a page with BASIC auth and run a callback against the page."""
    def _callback(page: Page) -> Any:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if wait_ms > 0:
            page.wait_for_timeout(wait_ms)
        return callback(page)

    return run_page(
        url,
        headless=headless,
        basic_auth={"username": username, "password": password},
        setup=setup,
        callback=_callback,
    )


def run_with_launch_override(
    url: str,
    launch_url: str,
    *,
    ignore_https_errors: bool = False,
    wait_ms: int = 0,
    headless: bool = True,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
) -> Any:
    """Open a page with the Adobe Tags library replaced at the network layer.

    Useful for verifying a new dev library build against a site that still
    embeds an older version (e.g. a site using a legacy DTM-style
    ``satelliteLib-*.js`` embed).

    Strategy:
    Intercepts every HTML response for the target *origin* and replaces
    any ``assets.adobedtm.com/.../satelliteLib-*.js`` ``<script src>``
    with ``launch_url``.  This ensures the new library is loaded by the
    page instead of the old bootstrap.  Because the old ``satelliteLib``
    script is removed from the HTML entirely, its sub-files (EX / RC chunks)
    are never requested, so no additional aborting is needed.

    ``launch_url`` must be an ``assets.adobedtm.com`` URL of the form:
    ``https://assets.adobedtm.com/{company}/{property}/launch-{id}-{env}.min.js``

    Note: do NOT set ``abort_old_property_assets=True`` for this mode.
    The dev library dynamically loads its own RC/EX sub-files from the same
    property base prefix; aborting all prefix requests would prevent those
    sub-files (which contain the actual action code) from loading.
    """
    def _callback(page: Page) -> Any:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if wait_ms > 0:
            page.wait_for_timeout(wait_ms)
        return callback(page)

    return run_page(
        url,
        headless=headless,
        ignore_https_errors=ignore_https_errors,
        tags_override=TagsLaunchOverride(
            launch_url=launch_url,
            mode="legacy_satellite",
            abort_old_property_assets=False,  # dev library sub-files must load
        ),
        setup=setup,
        callback=_callback,
    )


def capture_selector_state(page: Page, selectors: list[str]) -> dict[str, Any]:
    """Return page metadata plus existence/child counts for selectors."""
    checks = page.evaluate(
        """(selectors) => {
          const out = {};
          selectors.forEach((sel) => {
            const el = document.querySelector(sel);
            out[sel] = {
              exists: !!el,
              opacity: el ? getComputedStyle(el).opacity : null,
              childCount: el ? el.children.length : 0
            };
          });
          return out;
        }""",
        selectors,
    )
    return {
        "url": page.url,
        "title": page.title(),
        "checks": checks,
    }
