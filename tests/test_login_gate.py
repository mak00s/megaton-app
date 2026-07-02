from __future__ import annotations

import logging

import pytest

from megaton_lib import login_gate
from megaton_lib.login_gate import (
    LoginGateError,
    add_runtime_notice,
    autologin,
    collect_runtime_notices,
    domain_guard_js,
    page_content_settled,
    wait_for_login,
)

pytestmark = pytest.mark.unit


class _FakePage:
    def __init__(self, *, content: str = "", evals: list | None = None, url: str = "about:blank"):
        self._content = content
        self._evals = list(evals or [])
        self.url = url
        self.waits = 0
        self.gotos: list[str] = []
        self.eval_calls: list[str] = []
        self.load_state_errors = 0
        self.content_errors = 0

    def wait_for_load_state(self, *_a, **_k):
        if self.load_state_errors:
            self.load_state_errors -= 1
            raise RuntimeError("load state failed")

    def wait_for_timeout(self, *_a, **_k):
        self.waits += 1

    def content(self):
        if self.content_errors:
            self.content_errors -= 1
            raise RuntimeError("content failed")
        return self._content

    def evaluate(self, expr: str, *_args):
        self.eval_calls.append(expr)
        if self._evals:
            value = self._evals.pop(0)
            if isinstance(value, Exception):
                raise value
            return value
        return False

    def goto(self, url: str, **_kw):
        self.gotos.append(url)
        self.url = url

    def wait_for_selector(self, *_a, **_k):
        return None

    def bring_to_front(self):
        return None

    def query_selector(self, *_a, **_k):
        return None


def test_runtime_notices_are_context_local():
    add_runtime_notice("warning", "outside", "ignored")
    with collect_runtime_notices() as first:
        add_runtime_notice("info", "inside", "first")
        with collect_runtime_notices() as second:
            add_runtime_notice("info", "inside", "second")
        add_runtime_notice("info", "after", "first")

    assert [n["message"] for n in first] == ["first", "first"]
    assert [n["message"] for n in second] == ["second"]


def test_page_content_settled_tolerates_load_and_first_content_error():
    page = _FakePage(content="ok")
    page.load_state_errors = 1
    page.content_errors = 1

    assert page_content_settled(page) == "ok"
    assert page.waits == 1


def test_autologin_returns_no_fill_result_without_logging_submit(caplog):
    page = _FakePage(evals=[False, True, "wrong-domain"])
    creds = type("Creds", (), {"user": "u", "password": "p"})()
    logger = logging.getLogger("test.autologin")

    with caplog.at_level(logging.INFO):
        result = autologin(
            page,
            creds=creds,
            login_url="https://bank.example/login",
            logged_in_predicate="logged",
            label="bank",
            logger=logger,
        )

    assert result == "wrong-domain"
    assert "ログイン自動記入" not in caplog.text


def test_domain_guard_js_escapes_hosts_and_returns_wrong_domain():
    guard = domain_guard_js("jibunbank.co.jp")

    assert r"jibunbank\.co\.jp" in guard
    assert "(^|\\.)" in guard
    assert "wrong-domain" in guard


def test_wait_for_login_uses_custom_error_class():
    class CustomLoginError(RuntimeError):
        pass

    page = _FakePage(evals=[False] * 10, url="https://example.test/login")

    with pytest.raises(CustomLoginError):
        wait_for_login(page, "bank", ready_js="ready", timeout_s=0, error_cls=CustomLoginError)


def test_default_wait_for_login_error_is_login_gate_error():
    page = _FakePage(evals=[False] * 10, url="https://example.test/login")

    with pytest.raises(LoginGateError):
        wait_for_login(page, "bank", ready_js="ready", timeout_s=0)


def test_js_literals_match_expected_migration_surface():
    assert "getOwnPropertyDescriptor" in login_gate.JS_LOGIN_HELPERS
    assert "input[type=\"password\"]" in login_gate.LOGIN_FILL_JS
    assert "session_error" in login_gate.GENERIC_LOGIN_ERROR_JS
