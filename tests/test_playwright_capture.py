from __future__ import annotations

from megaton_lib.validation.playwright_capture import (
    PageEventCapture,
    capture_console_args,
    extract_mbox_names,
    select_headers,
)


class FakeArg:
    def __init__(self, value):
        self.value = value

    def json_value(self):
        return self.value


class BrokenArg:
    def json_value(self):
        raise RuntimeError("boom")

    def __str__(self) -> str:
        return "<broken>"


class FakeConsoleMessage:
    def __init__(self, text: str, args: list) -> None:
        self.text = text
        self.args = args


class FakeRequest:
    def __init__(self, url: str, *, post_data=None, headers=None, method="POST", failure=None) -> None:
        self.url = url
        self.post_data = post_data
        self.headers = headers or {}
        self.method = method
        self.failure = failure


class FakeResponse:
    def __init__(self, request, *, url: str, status=200, ok=True, headers=None, body=None) -> None:
        self.request = request
        self.url = url
        self.status = status
        self.ok = ok
        self.headers = headers or {}
        self._body = body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body

    def text(self):
        return str(self._body)


def test_capture_console_args_handles_json_and_fallback():
    msg = FakeConsoleMessage("A=", [FakeArg({"a": 1}), BrokenArg()])
    assert capture_console_args(msg) == [{"a": 1}, "<broken>"]


def test_extract_mbox_names_keeps_order_and_uniqueness():
    payload = {
        "execute": {"mboxes": [{"name": "CSK-A"}, {"name": "CSK-B"}]},
        "prefetch": {"mboxes": [{"name": "CSK-A"}, {"name": "CSK-C"}]},
    }
    assert extract_mbox_names(payload) == ["CSK-A", "CSK-B", "CSK-C"]


def test_select_headers_filters_to_stable_keys():
    headers = {"content-type": "application/json", "date": "now", "server": "edge"}
    assert select_headers(headers) == {"content-type": "application/json", "date": "now"}


def test_page_event_capture_collects_delivery_and_console():
    seen = []
    capture = PageEventCapture(
        delivery_path_fragment="/rest/v1/delivery",
        on_console_entry=lambda text, args: seen.append((text, args)),
        console_tail_limit=2,
    )

    capture._on_console(FakeConsoleMessage("A=", [FakeArg("A="), FakeArg({"x": 1})]))
    capture._on_console(FakeConsoleMessage("B=", [FakeArg("B=")]))
    capture._on_console(FakeConsoleMessage("C=", [FakeArg("C=")]))
    assert capture.console_tail == ["B=", "C="]
    assert seen[0][0] == "A="

    request = FakeRequest(
        "https://example.test/rest/v1/delivery",
        post_data='{"execute":{"mboxes":[{"name":"CSK-A"}]}}',
        headers={"content-type": "application/json", "x-request-id": "req-1"},
    )
    capture._on_request(request)
    response = FakeResponse(
        request,
        url=request.url,
        status=200,
        ok=True,
        headers={"content-type": "application/json", "date": "today"},
        body={"execute": {"mboxes": [{"name": "CSK-A", "options": []}]}},
    )
    capture._on_response(response)
    capture._on_pageerror(RuntimeError("oops"))
    capture._on_request_failed(FakeRequest("https://example.test/x.js", failure={"errorText": "net::ERR"}))

    assert capture.delivery_calls[0]["mboxes"] == ["CSK-A"]
    assert capture.delivery_calls[0]["response"]["status"] == 200
    assert capture.page_errors == ["oops"]
    assert capture.failed_requests == [{"url": "https://example.test/x.js", "failure": "net::ERR"}]
