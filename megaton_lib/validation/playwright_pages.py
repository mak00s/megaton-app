"""Shared Playwright helpers for page checks, GTM preview, and Adobe Tags overrides."""

from __future__ import annotations

from dataclasses import dataclass
import re
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit, urlunsplit
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from playwright.sync_api import Page
else:
    Page = Any

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - exercised in CI environments without Playwright
    class PlaywrightTimeoutError(Exception):
        """Fallback timeout error used when Playwright is unavailable."""

    def sync_playwright() -> Any:
        raise ModuleNotFoundError(
            "playwright is required to use megaton_lib.validation.playwright_pages. "
            "Install the 'playwright' package to run browser-based validation.",
        )


_SATELLITE_LIB_PATTERN = re.compile(
    r'https://assets\.adobedtm\.com/[0-9a-f]+/satelliteLib-[0-9a-f]+\.js'
)


@dataclass(frozen=True)
class TagsLaunchOverride:
    """Configuration for overriding Adobe Tags launch assets in Playwright."""

    launch_url: str
    mode: Literal["auto", "legacy_satellite", "launch_env"] = "auto"
    env_patterns: tuple[str, ...] = ("staging", "development")
    exact_match_urls: tuple[str, ...] = ()
    abort_old_property_assets: bool = False


@dataclass(frozen=True)
class GtmPreviewOverride:
    """Configuration for routing GTM container requests to a preview workspace."""

    container_id: str
    auth_token: str
    preview_id: str
    cookies_win: str = "x"


def build_tags_launch_override(
    config: Mapping[str, Any] | None,
    *,
    require: bool = False,
    label: str = "tagsOverride",
) -> TagsLaunchOverride | None:
    """Build ``TagsLaunchOverride`` from config mapping.

    The config schema matches ``adobe_analytics.tagsOverride``:
    ``launchUrl``, ``mode``, ``envPatterns``, ``exactMatchUrls``, and
    ``abortOldPropertyAssets``.
    """
    if not config:
        if require:
            raise ValueError(f"{label} is required")
        return None

    launch_url = str(config.get("launchUrl", "")).strip()
    if not launch_url:
        raise ValueError(f"{label}.launchUrl is required")

    mode = str(config.get("mode", "auto")).strip() or "auto"
    if mode not in {"auto", "legacy_satellite", "launch_env"}:
        raise ValueError(
            f"{label}.mode must be one of: auto, legacy_satellite, launch_env",
        )

    raw_env_patterns = config.get("envPatterns", ("staging", "development"))
    if isinstance(raw_env_patterns, str):
        env_patterns = tuple(
            part.strip() for part in raw_env_patterns.split(",") if part.strip()
        )
    else:
        env_patterns = tuple(
            str(part).strip() for part in raw_env_patterns if str(part).strip()
        )
    if not env_patterns:
        env_patterns = ("staging", "development")

    raw_exact_urls = config.get("exactMatchUrls") or config.get("exactMatchUrl") or ()
    if isinstance(raw_exact_urls, str):
        exact_match_urls = tuple(
            part.strip() for part in raw_exact_urls.split(",") if part.strip()
        )
    else:
        exact_match_urls = tuple(
            str(part).strip() for part in raw_exact_urls if str(part).strip()
        )

    return TagsLaunchOverride(
        launch_url=launch_url,
        mode=mode,
        env_patterns=env_patterns,
        exact_match_urls=exact_match_urls,
        abort_old_property_assets=bool(config.get("abortOldPropertyAssets", False)),
    )


def _parse_tagassistant_preview_url(preview_url: str) -> dict[str, str]:
    """Parse a Tag Assistant preview URL into GTM preview parameters."""
    stripped = preview_url.strip()
    if not stripped:
        return {}

    parsed = urlsplit(stripped)
    query = parse_qs(parsed.query, keep_blank_values=True)
    if not query and parsed.fragment:
        fragment = parsed.fragment
        if fragment.startswith("/?"):
            fragment = fragment[2:]
        elif fragment.startswith("?"):
            fragment = fragment[1:]
        query = parse_qs(fragment, keep_blank_values=True)

    out: dict[str, str] = {}
    for src_key, dst_key in (
        ("id", "containerId"),
        ("gtm_auth", "authToken"),
        ("gtm_preview", "previewId"),
        ("gtm_cookies_win", "cookiesWin"),
    ):
        value = (query.get(src_key) or [""])[-1].strip()
        if value:
            out[dst_key] = value
    return out


def build_gtm_preview_override(
    config: Mapping[str, Any] | None,
    *,
    require: bool = False,
    label: str = "gtmPreview",
) -> GtmPreviewOverride | None:
    """Build ``GtmPreviewOverride`` from config mapping.

    Supported input keys:
    - ``previewUrl``: Tag Assistant preview URL
    - ``containerId`` or ``id``
    - ``authToken`` or ``gtm_auth``
    - ``previewId`` or ``gtm_preview``
    - ``cookiesWin`` or ``gtm_cookies_win`` (defaults to ``x``)
    """
    if not config:
        if require:
            raise ValueError(f"{label} is required")
        return None

    merged = dict(config)
    preview_url = str(merged.get("previewUrl", "")).strip()
    if preview_url:
        merged = {**_parse_tagassistant_preview_url(preview_url), **merged}

    container_id = str(merged.get("containerId") or merged.get("id") or "").strip()
    auth_token = str(merged.get("authToken") or merged.get("gtm_auth") or "").strip()
    preview_id = str(merged.get("previewId") or merged.get("gtm_preview") or "").strip()
    cookies_win = str(merged.get("cookiesWin") or merged.get("gtm_cookies_win") or "x").strip() or "x"

    missing: list[str] = []
    if not container_id:
        missing.append("containerId")
    if not auth_token:
        missing.append("authToken")
    if not preview_id:
        missing.append("previewId")
    if missing:
        raise ValueError(f"{label} is missing required field(s): {', '.join(missing)}")

    return GtmPreviewOverride(
        container_id=container_id,
        auth_token=auth_token,
        preview_id=preview_id,
        cookies_win=cookies_win,
    )


def describe_tags_launch_override(
    override: TagsLaunchOverride | None,
) -> dict[str, Any] | None:
    """Return stable metadata for saved validation results."""
    if override is None:
        return None
    return {
        "launchUrl": override.launch_url,
        "mode": override.mode,
        "envPatterns": list(override.env_patterns),
        "exactMatchUrls": list(override.exact_match_urls),
        "abortOldPropertyAssets": override.abort_old_property_assets,
    }


def describe_gtm_preview_override(
    override: GtmPreviewOverride | None,
) -> dict[str, Any] | None:
    """Return stable metadata for saved GTM preview runs."""
    if override is None:
        return None
    return {
        "containerId": override.container_id,
        "previewId": override.preview_id,
        "cookiesWin": override.cookies_win,
        "authTokenPresent": bool(override.auth_token),
    }


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


def _matches_exact_launch_request(url: str, exact_match_urls: tuple[str, ...]) -> bool:
    if not exact_match_urls:
        return False
    request_base = url.split("?", 1)[0]
    return request_base in exact_match_urls


def _fetch_launch_asset(launch_url: str) -> bytes:
    req = Request(launch_url, headers={"Cache-Control": "no-cache"})
    with urlopen(req, timeout=30) as resp:
        return resp.read()


def _response_headers(response) -> dict[str, str]:
    headers = dict(response.headers)
    headers.pop("content-length", None)
    return headers


def _inject_gtm_preview_params(
    request_url: str,
    override: GtmPreviewOverride,
) -> str:
    """Return GTM request URL with preview parameters appended/replaced."""
    parsed = urlsplit(request_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["id"] = [override.container_id]
    query["gtm_auth"] = [override.auth_token]
    query["gtm_preview"] = [override.preview_id]
    query["gtm_cookies_win"] = [override.cookies_win]
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, doseq=True),
            parsed.fragment,
        )
    )


def configure_gtm_preview_override(
    page: Page,
    override: GtmPreviewOverride,
) -> None:
    """Configure Playwright routes to load a GTM container in preview mode."""
    for path in ("gtm.js", "ns.html"):
        pattern = re.compile(
            rf"^https://www\.googletagmanager\.com/{re.escape(path)}\?.*([?&])id={re.escape(override.container_id)}(?:[&#].*)?$"
        )

        def _handle_gtm_preview(route, request, *, _override=override):  # type: ignore[no-untyped-def]
            route.continue_(url=_inject_gtm_preview_params(request.url, _override))

        page.route(pattern, _handle_gtm_preview)


def configure_tags_launch_override(
    page: Page,
    url: str,
    override: TagsLaunchOverride,
) -> None:
    """Configure Playwright routes to replace Adobe Tags launch assets.

    Supports three complementary strategies:
    - HTML patch for legacy ``satelliteLib-*.js`` embeds
    - env-pattern replacement for ``launch-...development|staging...js``
    - exact URL replacement for production launch embeds that should be
      swapped to a dev/staging build during validation
    """
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

    for match_url in override.exact_match_urls:
        normalized = match_url.strip()
        if not normalized:
            continue

        exact_pattern = re.compile(rf"^{re.escape(normalized)}(?:\?.*)?$")

        def _handle_exact_launch(route, _request):  # type: ignore[no-untyped-def]
            route.fulfill(
                body=_get_launch_asset(),
                content_type="application/javascript",
            )

        page.route(exact_pattern, _handle_exact_launch)

    if base_prefix and override.abort_old_property_assets:
        def _handle_property_asset(route, request):  # type: ignore[no-untyped-def]
            if request.url == override.launch_url:
                route.continue_()
                return

            if _matches_exact_launch_request(request.url, override.exact_match_urls):
                route.fulfill(
                    body=_get_launch_asset(),
                    content_type="application/javascript",
                )
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
    channel: str | None = None,
    slow_mo: int = 0,
    ignore_https_errors: bool = False,
    basic_auth: dict[str, str] | None = None,
    storage_state: Any = None,
    cookies: list[Mapping[str, Any]] | None = None,
    viewport: Mapping[str, int] | None = None,
    gtm_preview: GtmPreviewOverride | None = None,
    tags_override: TagsLaunchOverride | None = None,
    context_setup: Callable[[Any], None] | None = None,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
) -> Any:
    """Open a Playwright page, apply optional overrides, and run a callback."""
    def _callback(page: Page) -> Any:
        result = callback(page)
        if wait_ms > 0:
            page.wait_for_timeout(wait_ms)
        return result

    return run_page_session(
        headless=headless,
        channel=channel,
        slow_mo=slow_mo,
        ignore_https_errors=ignore_https_errors,
        basic_auth=basic_auth,
        storage_state=storage_state,
        cookies=cookies,
        viewport=viewport,
        gtm_preview=gtm_preview,
        tags_override=tags_override,
        route_url=url,
        context_setup=context_setup,
        setup=setup,
        callback=_callback,
        user_agent=user_agent,
        stealth=stealth,
    )


# Real Chrome UA used as a default when ``stealth=True`` (the default).
# Adobe Analytics Bot Rules / IAB known-bot list will drop hits whose
# user-agent contains "HeadlessChrome", "PhantomJS", "Selenium", etc.
# Validation traffic that is meant to land in AA reports MUST therefore
# pose as a real browser. Verified against chugaihcdev on 2026-04-25:
# Playwright default UA (HeadlessChrome) → 0 hits in AA; real Chrome UA
# + navigator.webdriver hiding → hits land normally.
DEFAULT_STEALTH_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)


def run_page_session(
    *,
    headless: bool = True,
    channel: str | None = None,
    slow_mo: int = 0,
    ignore_https_errors: bool = False,
    basic_auth: dict[str, str] | None = None,
    storage_state: Any = None,
    cookies: list[Mapping[str, Any]] | None = None,
    viewport: Mapping[str, int] | None = None,
    gtm_preview: GtmPreviewOverride | None = None,
    tags_override: TagsLaunchOverride | None = None,
    route_url: str | None = None,
    context_setup: Callable[[Any], None] | None = None,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
) -> Any:
    """Open a browser/context/page session, run a callback, and handle cleanup.

    Stealth defaults (``stealth=True``):
      - Override Chrome user-agent to ``DEFAULT_STEALTH_USER_AGENT`` (real
        Chrome on macOS) unless ``user_agent`` is set explicitly. Avoids
        AA Bot Rules / IAB-list rejection of hits whose UA contains
        ``HeadlessChrome``.
      - Launch with ``--disable-blink-features=AutomationControlled`` so
        ``navigator.webdriver`` is not auto-set by Chromium.
      - Inject an init script that hides ``navigator.webdriver`` even if
        the launch flag wasn't honoured.

    Pass ``stealth=False`` for tests that intentionally want to surface
    automation fingerprints (e.g. validating that bot rules DO catch
    headless traffic).
    """
    if tags_override is not None and not route_url:
        raise ValueError("route_url is required when tags_override is provided")

    with sync_playwright() as playwright:
        launch_options: dict[str, Any] = {"headless": headless}
        if channel:
            launch_options["channel"] = channel
        if slow_mo > 0:
            launch_options["slow_mo"] = slow_mo
        if stealth:
            launch_options.setdefault("args", []).append(
                "--disable-blink-features=AutomationControlled"
            )
        browser = playwright.chromium.launch(**launch_options)

        context_options: dict[str, Any] = {}
        if ignore_https_errors:
            context_options["ignore_https_errors"] = True
        if basic_auth:
            context_options["http_credentials"] = basic_auth
        if storage_state is not None:
            context_options["storage_state"] = storage_state
        if viewport is not None:
            context_options["viewport"] = dict(viewport)
        # Resolve effective user-agent: explicit override > stealth default >
        # Playwright's HeadlessChrome default. The Playwright default leaks
        # 'HeadlessChrome/X.Y.Z' which AA Bot Rules drop on sight.
        effective_ua = user_agent
        if effective_ua is None and stealth:
            effective_ua = DEFAULT_STEALTH_USER_AGENT
        if effective_ua:
            context_options["user_agent"] = effective_ua

        context = browser.new_context(**context_options)
        try:
            if stealth:
                # navigator.webdriver = true is the canonical bot-detection
                # signal. Chromium tries to suppress it via the launch flag
                # above, but a redundant init-script ensures it's hidden
                # everywhere (including any frames spawned later).
                context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', "
                    "{get: () => undefined})"
                )
            if cookies:
                context.add_cookies([dict(cookie) for cookie in cookies])
            if context_setup is not None:
                context_setup(context)

            page = context.new_page()
            if gtm_preview is not None:
                configure_gtm_preview_override(page, gtm_preview)
            if tags_override is not None:
                configure_tags_launch_override(page, route_url or "", tags_override)
            if setup is not None:
                setup(page)
            return callback(page)
        finally:
            context.close()
            browser.close()


def capture_storage_state(
    *,
    headless: bool = True,
    channel: str | None = None,
    slow_mo: int = 0,
    ignore_https_errors: bool = False,
    basic_auth: dict[str, str] | None = None,
    cookies: list[Mapping[str, Any]] | None = None,
    viewport: Mapping[str, int] | None = None,
    context_setup: Callable[[Any], None] | None = None,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
) -> dict[str, Any]:
    """Open a temporary context, run a callback, and return Playwright storage state."""
    state: dict[str, Any] | None = None

    def _callback(page: Page) -> None:
        nonlocal state
        callback(page)
        state = page.context.storage_state()

    run_page_session(
        headless=headless,
        channel=channel,
        slow_mo=slow_mo,
        ignore_https_errors=ignore_https_errors,
        basic_auth=basic_auth,
        cookies=cookies,
        viewport=viewport,
        context_setup=context_setup,
        setup=setup,
        callback=_callback,
        user_agent=user_agent,
        stealth=stealth,
    )
    return state or {"cookies": [], "origins": []}


def run_page_with_bootstrapped_state(
    url: str,
    *,
    bootstrap: Callable[[Page], Any],
    wait_ms: int = 0,
    headless: bool = True,
    channel: str | None = None,
    slow_mo: int = 0,
    ignore_https_errors: bool = False,
    basic_auth: dict[str, str] | None = None,
    viewport: Mapping[str, int] | None = None,
    gtm_preview: GtmPreviewOverride | None = None,
    tags_override: TagsLaunchOverride | None = None,
    context_setup: Callable[[Any], None] | None = None,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
) -> Any:
    """Create storage state via ``bootstrap`` and reuse it for a second page run."""
    storage_state = capture_storage_state(
        headless=headless,
        channel=channel,
        slow_mo=slow_mo,
        ignore_https_errors=ignore_https_errors,
        basic_auth=basic_auth,
        viewport=viewport,
        context_setup=context_setup,
        callback=bootstrap,
        user_agent=user_agent,
        stealth=stealth,
    )
    return run_page(
        url,
        wait_ms=wait_ms,
        headless=headless,
        channel=channel,
        slow_mo=slow_mo,
        ignore_https_errors=ignore_https_errors,
        basic_auth=basic_auth,
        storage_state=storage_state,
        viewport=viewport,
        gtm_preview=gtm_preview,
        tags_override=tags_override,
        context_setup=context_setup,
        setup=setup,
        callback=callback,
        user_agent=user_agent,
        stealth=stealth,
    )


def run_with_basic_auth_page(
    url: str,
    username: str,
    password: str,
    *,
    wait_ms: int = 0,
    headless: bool = True,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
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
        user_agent=user_agent,
        stealth=stealth,
    )


def run_with_launch_override(
    url: str,
    launch_url: str,
    *,
    exact_match_urls: tuple[str, ...] = (),
    ignore_https_errors: bool = False,
    wait_ms: int = 0,
    headless: bool = True,
    setup: Callable[[Page], None] | None = None,
    callback: Callable[[Page], Any],
    user_agent: str | None = None,
    stealth: bool = True,
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

    ``exact_match_urls`` can be used when the page embeds a production
    ``launch-*.js`` URL that should be replaced verbatim with ``launch_url``.

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
            exact_match_urls=exact_match_urls,
            abort_old_property_assets=False,  # dev library sub-files must load
        ),
        setup=setup,
        callback=_callback,
        user_agent=user_agent,
        stealth=stealth,
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


def wait_for_any_selector(
    page: Page,
    selectors: list[str],
    *,
    timeout_ms: int,
    poll_ms: int = 500,
    settle_ms: int = 0,
) -> str | None:
    """Poll until any selector matches and optionally wait for a settle period."""
    if not selectors:
        raise ValueError("selectors must not be empty")
    if timeout_ms < 0:
        raise ValueError("timeout_ms must be >= 0")
    if poll_ms <= 0:
        raise ValueError("poll_ms must be > 0")
    if settle_ms < 0:
        raise ValueError("settle_ms must be >= 0")

    deadline = time.monotonic() + (timeout_ms / 1000)
    while True:
        for selector in selectors:
            if page.query_selector(selector) is not None:
                if settle_ms > 0:
                    page.wait_for_timeout(settle_ms)
                return selector
        if time.monotonic() >= deadline:
            return None
        page.wait_for_timeout(poll_ms)


def click_selector_if_visible(
    page: Page,
    selector: str,
    *,
    force: bool = False,
    settle_ms: int = 0,
) -> bool:
    """Click a selector only when it exists and is visible."""
    element = page.query_selector(selector)
    if element is None:
        return False
    is_visible = getattr(element, "is_visible", None)
    if callable(is_visible) and not is_visible():
        return False
    page.click(selector, force=force)
    if settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    return True


def scroll_selector_region_to_end(
    page: Page,
    selector: str,
    *,
    settle_ms: int = 0,
) -> bool:
    """Scroll a container element to its end and report whether it existed."""
    found = bool(
        page.evaluate(
            """(selector) => {
              const el = document.querySelector(selector);
              if (!el) return false;
              el.scrollTop = el.scrollHeight;
              return true;
            }""",
            selector,
        )
    )
    if found and settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    return found


def enable_selector(
    page: Page,
    selector: str,
    *,
    settle_ms: int = 0,
) -> bool:
    """Clear disabled state from a selector and report whether it existed."""
    found = bool(
        page.evaluate(
            """(selector) => {
              const el = document.querySelector(selector);
              if (!el) return false;
              el.disabled = false;
              el.removeAttribute('disabled');
              el.removeAttribute('aria-disabled');
              return true;
            }""",
            selector,
        )
    )
    if found and settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    return found


def set_checkbox_checked(
    page: Page,
    selector: str,
    *,
    force: bool = True,
    settle_ms: int = 0,
) -> bool:
    """Enable and check a checkbox-like selector when it exists."""
    if not enable_selector(page, selector):
        return False
    page.click(selector, force=force)
    if settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    return True


def scroll_selector_into_view(
    page: Page,
    selector: str,
    *,
    block: Literal["start", "center", "end", "nearest"] = "center",
    settle_ms: int = 0,
) -> bool:
    """Scroll a selector into view and return whether the element existed."""
    found = bool(
        page.evaluate(
            """({ selector, block }) => {
              const el = document.querySelector(selector);
              if (!el) return false;
              el.scrollIntoView({ block });
              return true;
            }""",
            {"selector": selector, "block": block},
        )
    )
    if found and settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    return found


def capture_satellite_info(page: Page) -> dict[str, Any]:
    """Return stable `_satellite` presence/build metadata from the current page."""
    return page.evaluate(
        "() => { try { return {"
        "  hasSatellite: typeof _satellite !== 'undefined',"
        "  buildDate: typeof _satellite !== 'undefined' && _satellite.buildInfo"
        "    ? _satellite.buildInfo.buildDate : null"
        "}; } catch(e) { return {hasSatellite: false}; } }"
    )
