"""Shared Playwright helpers for authenticated page checks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from playwright.sync_api import Page, sync_playwright


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
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(
            http_credentials={"username": username, "password": password}
        )
        page = context.new_page()
        try:
            if setup is not None:
                setup(page)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if wait_ms > 0:
                page.wait_for_timeout(wait_ms)
            return callback(page)
        finally:
            context.close()
            browser.close()


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
