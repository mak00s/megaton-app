"""Reusable Playwright capture helpers for console and Target delivery traffic."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from playwright.sync_api import Page
else:
    Page = Any


def capture_console_args(msg: Any) -> list[Any]:
    """Safely serialize Playwright console arguments."""
    args_data: list[Any] = []
    for arg in getattr(msg, "args", []):
        try:
            args_data.append(arg.json_value())
        except Exception:
            args_data.append(str(arg))
    return args_data


def select_headers(headers: dict[str, Any]) -> dict[str, Any]:
    """Keep only stable debugging headers."""
    keys = ("content-type", "x-request-id", "x-trace-id", "date")
    return {k: headers[k] for k in keys if k in headers}


def extract_mbox_names(payload: Any) -> list[str]:
    """Extract execute/prefetch mbox names from a delivery payload."""
    if not isinstance(payload, dict):
        return []

    names: list[str] = []
    for section in ("execute", "prefetch"):
        block = payload.get(section)
        if not isinstance(block, dict):
            continue
        mboxes = block.get("mboxes")
        if not isinstance(mboxes, list):
            continue
        for item in mboxes:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if isinstance(name, str) and name:
                names.append(name)

    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


class PageEventCapture:
    """Collect console logs, page errors, failed requests, and delivery calls."""

    def __init__(
        self,
        *,
        delivery_path_fragment: str | None = None,
        on_console_entry: Callable[[str, list[Any]], None] | None = None,
        console_tail_limit: int = 20,
    ) -> None:
        self.delivery_path_fragment = delivery_path_fragment
        self.on_console_entry = on_console_entry
        self.console_tail_limit = console_tail_limit

        self.console_entries: list[dict[str, Any]] = []
        self.console_tail: list[str] = []
        self.page_errors: list[str] = []
        self.failed_requests: list[dict[str, Any]] = []
        self.delivery_calls: list[dict[str, Any]] = []
        self._pending: dict[int, dict[str, Any]] = {}

    def attach(self, page: Page) -> None:
        page.on("console", self._on_console)
        page.on("pageerror", self._on_pageerror)
        page.on("request", self._on_request)
        page.on("response", self._on_response)
        page.on("requestfailed", self._on_request_failed)

    def _on_console(self, msg: Any) -> None:
        text = getattr(msg, "text", "")
        args_data = capture_console_args(msg)
        self.console_entries.append({"text": text, "args": args_data})
        self.console_tail.append(text)
        if len(self.console_tail) > self.console_tail_limit:
            self.console_tail.pop(0)
        if self.on_console_entry is not None:
            self.on_console_entry(text, args_data)

    def _on_pageerror(self, err: Any) -> None:
        self.page_errors.append(str(err))

    def _on_request(self, request: Any) -> None:
        if not self._is_delivery(request.url):
            return

        post_json = None
        post_text = getattr(request, "post_data", None) or ""
        if post_text:
            try:
                post_json = json.loads(post_text)
            except json.JSONDecodeError:
                post_json = None

        call = {
            "request": {
                "url": request.url,
                "method": request.method,
                "headers": select_headers(request.headers),
                "postDataJson": post_json,
                "postDataText": post_text if post_json is None else "",
            },
            "mboxes": extract_mbox_names(post_json),
            "response": None,
        }
        self.delivery_calls.append(call)
        self._pending[id(request)] = call

    def _on_response(self, response: Any) -> None:
        if not self._is_delivery(response.url):
            return

        call = self._pending.get(id(response.request))
        if call is None:
            call = {
                "request": {
                    "url": response.url,
                    "method": response.request.method,
                    "headers": {},
                    "postDataJson": None,
                    "postDataText": "",
                },
                "mboxes": [],
                "response": None,
            }
            self.delivery_calls.append(call)

        body: Any = None
        try:
            body = response.json()
        except Exception:
            try:
                body = response.text()
            except Exception:
                body = "<unavailable>"

        call["response"] = {
            "status": response.status,
            "ok": response.ok,
            "headers": select_headers(response.headers),
            "body": body,
        }

    def _on_request_failed(self, request: Any) -> None:
        failure = request.failure
        if isinstance(failure, dict):
            failure_text = failure.get("errorText")
        else:
            failure_text = str(failure)
        self.failed_requests.append({"url": request.url, "failure": failure_text})

    def _is_delivery(self, url: str) -> bool:
        return bool(self.delivery_path_fragment) and self.delivery_path_fragment in url
