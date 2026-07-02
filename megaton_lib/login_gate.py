"""Shared browser login gates and form-fill helpers.

Promoted from minkabu's broker/bank login infrastructure. Keep behaviour
stable: consumer repos depend on these exact waits, errors, and JS snippets.
"""

from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar


class LoginGateError(RuntimeError):
    """Login/authentication gate could not complete."""


_RUNTIME_NOTICES: ContextVar[list[dict] | None] = ContextVar("minkabu_runtime_notices", default=None)


def add_runtime_notice(level: str, stage: str, message: str, **details) -> None:
    """Attach an operator-visible diagnostic to the current CLI result.

    Logging alone is easy to miss in agent-driven runs. These notices are
    collected by orchestration code and printed in the command's JSON output.
    """
    bucket = _RUNTIME_NOTICES.get()
    if bucket is None:
        return
    notice = {"level": level, "stage": stage, "message": message}
    for key, value in details.items():
        if value not in (None, ""):
            notice[key] = value
    bucket.append(notice)


@contextmanager
def collect_runtime_notices():
    """Collect diagnostics emitted by shared login/fetch helpers."""
    notices: list[dict] = []
    token = _RUNTIME_NOTICES.set(notices)
    try:
        yield notices
    finally:
        _RUNTIME_NOTICES.reset(token)


GENERIC_LOGIN_ERROR_JS = (
    "() => {"
    " const t=document.body ? document.body.innerText : '';"
    " return /システムエラー|お取り扱いできません|エラーが発生|ただいま.*利用できません|"
    "セッション.*無効|セッション.*切れ|タイムアウト|再度.?ログイン|ログイン画面へ/.test(t)"
    " || /session_error/i.test(location.href);"
    "}"
)


def page_content_settled(page) -> str:
    """Return page HTML, tolerating an in-flight navigation (CDP-attach tabs
    can be mid-navigation when an adapter starts, e.g. after auto-opening a
    login page). Returns "" if it never settles."""
    import contextlib

    with contextlib.suppress(Exception):
        page.wait_for_load_state("domcontentloaded", timeout=8000)
    try:
        return page.content()
    except Exception:  # noqa: BLE001 - retry once after a beat
        with contextlib.suppress(Exception):
            page.wait_for_timeout(1500)
            return page.content()
    return ""


def _bring_to_front(page) -> None:
    # Human auth (passkey/SMS/app approval) is the point where visibility
    # matters most. Keep this best-effort so headless/fake pages still work.
    with contextlib.suppress(Exception):
        page.bring_to_front()
    with contextlib.suppress(Exception):
        import os
        import subprocess
        import sys

        if sys.platform == "darwin":
            app_name = os.environ.get("CDP_CHROME_APP", "Google Chrome")
            subprocess.run(
                ["osascript", "-e", f'tell application "{app_name}" to activate'],
                check=False,
                timeout=2,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    with contextlib.suppress(Exception):
        page.bring_to_front()

def wait_for_login(
    page,
    broker: str,
    *,
    marker: str = "ログアウト",
    ready_js: str | None = None,
    passkey_js: str | None = None,
    passkey_click: str | None = None,
    system_error_js: str | None = None,
    recover: Callable[[], None] | None = None,
    max_restarts: int = 3,
    timeout_s: int = 300,
    error_cls: type[Exception] = LoginGateError,
    gate: str | None = None,
    manual_hint: str = "passkey/SMS など",
) -> None:
    """Block until the page is logged in AND parse-ready.

    Lets the user complete login (passkey / SMS) in the attached Chrome while
    the adapter waits, instead of erroring on the first not-logged-in check.

    ``marker`` (default "ログアウト") is a substring whose presence in the page
    HTML proves login. This is a weak gate: logged-in is not the same as
    parse-ready (e.g. Rakuten lands on info_top.do which has "ログアウト" but
    not the holdings link; Connect's Web3App home loads scripts a beat after
    login, and a stale sub-page has no nav forms at all).

    ``ready_js`` is the strict gate: a JS predicate that returns truthy when
    the DOM is actually ready for the adapter's first action. When set, it
    REPLACES the marker check — the wait returns only when ``ready_js`` is
    truthy. Adapters should pass this whenever "logged-in" is broader than
    "ready to fetch" (which is most of them once you look hard).

    ``passkey_js`` is evaluated once on the login page to surface the OS
    passkey prompt (truthy = trigger fired). ``passkey_click`` is a CSS
    selector clicked via Playwright instead: a real input event grants the
    WebAuthn *user activation* a synthetic ``element.click()`` from
    ``evaluate`` does NOT — some passkey buttons (e.g. Auth0 universal login)
    only surface Touch ID on a real click.

    ``system_error_js`` + ``recover`` add stale/error page recovery. When an
    expired-session/system-error page is detected, ``recover`` should reopen the
    login page (and optionally auto-fill credentials). The passkey trigger is
    retried after recovery.

    ``gate`` and ``manual_hint`` are diagnostic/user-facing overrides used by
    bank adapters that delegate to this shared loop.

    Polls the live tab (it follows the post-login redirect). Raises
    ``BrokerError`` on timeout. Does NOT navigate (a goto can break in-URL
    session tokens like Rakuten's BV_SessionID).
    """
    import contextlib
    import re
    import sys
    import time

    def _ready() -> bool:
        # ready_js, when given, is the sole gate (it implies login). Otherwise
        # fall back to the substring marker on the page HTML.
        if ready_js is not None:
            with contextlib.suppress(Exception):
                return bool(page.evaluate(ready_js))
            return False
        return marker in page_content_settled(page)

    def _system_error() -> bool:
        if system_error_js is None:
            return False
        with contextlib.suppress(Exception):
            return bool(page.evaluate(system_error_js))
        return False

    def _selector_text(selector: str) -> str | None:
        m = re.search(r":has-text\(([\"'])(.*?)\1\)", selector)
        if m:
            return m.group(2)
        if selector.startswith("text="):
            return selector.removeprefix("text=").strip()
        return None

    def _click_text_target(text: str) -> bool:
        """Fallback for auth buttons whose DOM/class changes but visible label
        stays stable. Playwright's text/role locators are more forgiving than a
        strict CSS selector and still issue a real user click."""
        for click in (
            lambda: page.get_by_role("button", name=re.compile(re.escape(text))).first.click(timeout=500),
            lambda: page.get_by_text(text, exact=False).first.click(timeout=500),
        ):
            with contextlib.suppress(Exception):
                click()
                return True
        return False

    def _click_selector(selector: str) -> bool:
        with contextlib.suppress(Exception):
            page.locator(selector).first.click(timeout=1000)
            return True
        with contextlib.suppress(Exception):
            page.click(selector, timeout=500, force=True)
            return True
        text = _selector_text(selector)
        if text and _click_text_target(text):
            return True
        with contextlib.suppress(Exception):
            el = page.query_selector(selector)
            if el:
                el.click()  # real input -> user activation -> passkey prompt
                return True
        return False

    def _trigger_passkey() -> bool:
        if passkey_click:
            selectors = [s.strip() for s in passkey_click.split(",") if s.strip()]
            for _ in range(20):  # auth page may redirect/load slowly (~30s budget)
                _bring_to_front(page)
                clicked = False
                for selector in selectors:
                    clicked = _click_selector(selector)
                    if clicked:
                        add_runtime_notice(
                            "info",
                            "passkey_triggered",
                            "passkey/auth button clicked",
                            broker=broker,
                            selector=selector,
                        )
                        break
                if clicked:
                    return True
                page.wait_for_timeout(1500)
            add_runtime_notice(
                "warning",
                "passkey_not_found",
                "passkey/auth button was not found before manual wait",
                broker=broker,
            )
            # Prefer a real click for WebAuthn user activation, but do not
            # suppress an adapter-provided JS trigger when the site's DOM
            # changed and the selector no longer matches.
            if not passkey_js:
                return False
        if passkey_js:
            for _ in range(3):  # the login page may still be loading
                _bring_to_front(page)
                fired = False
                with contextlib.suppress(Exception):
                    fired = bool(page.evaluate(passkey_js))
                if fired:
                    add_runtime_notice(
                        "info",
                        "passkey_triggered",
                        "passkey/auth script fired",
                        broker=broker,
                    )
                    return True
                page.wait_for_timeout(1500)
            add_runtime_notice(
                "warning",
                "passkey_not_fired",
                "passkey/auth script did not fire before manual wait",
                broker=broker,
            )
            return False
        return False

    def _recover_once() -> None:
        with contextlib.suppress(Exception):
            recover()

    restarts = 0
    if _ready():
        add_runtime_notice(
            "info",
            "auth_ready",
            "auth/ready gate already passed",
            broker=broker,
            gate=gate or ("ready_js" if ready_js is not None else f"marker {marker!r}"),
            url=page.url,
        )
        return
    # Recover stale/error pages before prompting the operator. Auto-recoverable
    # cases should not print a manual-login request.
    if recover is not None and _system_error():
        restarts += 1
        add_runtime_notice(
            "warning",
            "system_error_detected",
            "error/session page detected before manual prompt; reopening login page",
            broker=broker,
            url=page.url,
            restart=restarts,
        )
        _recover_once()
        if _ready():
            add_runtime_notice(
                "info",
                "auth_recovered",
                "auth/ready gate passed after automatic recovery",
                broker=broker,
                url=page.url,
                restart=restarts,
            )
            return
    _trigger_passkey()
    _bring_to_front(page)
    add_runtime_notice(
        "info",
        "manual_auth_wait",
        "waiting for human authentication",
        broker=broker,
        gate=gate or ("ready_js" if ready_js is not None else f"marker {marker!r}"),
        url=page.url,
    )
    sys.stderr.write(
        f"\n🔐 [{broker}] その Chrome でログインしてください "
        f"({manual_hint})。完了を自動検知します… 最大 {timeout_s}秒\n"
    )
    sys.stderr.flush()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        page.wait_for_timeout(2000)
        if _ready():
            sys.stderr.write(f"✅ [{broker}] ログイン検知。続行します。\n")
            sys.stderr.flush()
            return
        if recover is not None and _system_error():
            restarts += 1
            if restarts > max_restarts:
                add_runtime_notice(
                    "error",
                    "recovery_exceeded",
                    f"error page recovery exceeded {max_restarts}",
                    broker=broker,
                    url=page.url,
                    restart=restarts,
                )
                raise error_cls(f"[{broker}] error page recovery exceeded {max_restarts}")
            add_runtime_notice(
                "warning",
                "system_error_detected",
                "error/session page detected during manual wait; reopening login page",
                broker=broker,
                url=page.url,
                restart=restarts,
            )
            _recover_once()
            if _ready():
                add_runtime_notice(
                    "info",
                    "auth_recovered",
                    "auth/ready gate passed after automatic recovery",
                    broker=broker,
                    url=page.url,
                    restart=restarts,
                )
                sys.stderr.write(f"✅ [{broker}] ログイン検知。続行します。\n")
                sys.stderr.flush()
                return
            _trigger_passkey()
            _bring_to_front(page)
    gate = gate or ("ready_js" if ready_js is not None else f"marker {marker!r}")
    add_runtime_notice(
        "error",
        "auth_timeout",
        f"login/ready not detected within {timeout_s}s",
        broker=broker,
        gate=gate,
        url=page.url,
    )
    raise error_cls(
        f"[{broker}] login/ready not detected within {timeout_s}s "
        f"(gate={gate}, last url={page.url!r})"
    )


def ensure_authenticated(
    page,
    broker: str,
    *,
    auth_probe_url: str | None,
    auth_ready_js: str,
    login_url: str | None = None,
    auth_probe_reload: bool = False,
    passkey_js: str | None = None,
    passkey_click: str | None = None,
    system_error_js: str | None = GENERIC_LOGIN_ERROR_JS,
    recover: Callable[[], None] | None = None,
    timeout_s: int = 300,
    error_cls: type[Exception] = LoginGateError,
) -> None:
    """Prove the browser session is live before any scraping.

    A restored Chrome tab can show yesterday's holdings table even after the
    server-side session has expired. This helper deliberately refuses to trust
    the existing tab DOM: it re-fetches from the server first, then requires
    ``auth_ready_js`` to pass. Only if that probe proves the session is live may
    an adapter navigate to and parse its data page.

    The probe is either ``page.goto(auth_probe_url)`` or, when
    ``auth_probe_reload`` is set, ``page.reload()``. Reload is for sites whose
    URLs carry an in-URL session token (e.g. Rakuten's BV_SessionID): reloading
    keeps the token so a live session re-fetches fresh data, whereas a bare goto
    to such a URL strips the token and yields a spurious session_error. Either
    way the probe is a real server round-trip, so a passing ``auth_ready_js``
    reflects fresh server state, not the restored DOM.

    If the probe lands on a login/session-error page, ``wait_for_login`` handles
    the normal passkey/manual flow and re-checks the same authenticated gate.
    """
    if recover is None and login_url:
        def _default_recover() -> None:
            page.goto(login_url, wait_until="domcontentloaded", timeout=30000)

        recover = _default_recover

    if auth_probe_reload or auth_probe_url:
        def _probe() -> None:
            if auth_probe_reload:
                add_runtime_notice(
                    "info",
                    "auth_probe",
                    "reloading current page to prove session freshness",
                    broker=broker,
                    url=page.url,
                )
                page.reload(wait_until="domcontentloaded", timeout=30000)
            else:
                add_runtime_notice(
                    "info",
                    "auth_probe",
                    "opening auth probe URL to prove session freshness",
                    broker=broker,
                    url=auth_probe_url,
                )
                page.goto(auth_probe_url, wait_until="domcontentloaded", timeout=30000)

        try:
            _probe()
        except Exception as exc:  # noqa: BLE001 - any nav failure is unsafe here
            # A failed probe leaves the (untrusted) restored tab in place. Falling
            # through to ``auth_ready_js`` could then pass against a stale holdings
            # DOM and parse yesterday's positions — the exact failure this probe
            # exists to prevent. Recover to a known login page so the gate is
            # re-checked there; if even that can't be reached, fail the broker
            # rather than risk stale data.
            if recover is None:
                add_runtime_notice(
                    "error",
                    "auth_probe_failed",
                    "auth probe navigation failed and no recovery is available",
                    broker=broker,
                    error=str(exc),
                )
                raise error_cls(
                    f"[{broker}] auth probe navigation failed, no recovery available: {exc}"
                ) from exc
            try:
                add_runtime_notice(
                    "warning",
                    "auth_probe_failed",
                    "auth probe navigation failed; reopening login page",
                    broker=broker,
                    error=str(exc),
                )
                recover()
            except Exception as exc2:  # noqa: BLE001 - recovery nav also failed
                add_runtime_notice(
                    "error",
                    "auth_recovery_failed",
                    "auth probe recovery navigation failed",
                    broker=broker,
                    error=str(exc2),
                )
                raise error_cls(
                    f"[{broker}] auth probe + recovery navigation failed: {exc2}"
                ) from exc2

    wait_for_login(
        page,
        broker,
        ready_js=auth_ready_js,
        passkey_js=passkey_js,
        passkey_click=passkey_click,
        system_error_js=system_error_js,
        recover=recover,
        timeout_s=timeout_s,
        gate="auth_probe",
        error_cls=error_cls,
    )


# Shared login-form JS helpers, reused across bank/source fill scripts so the
# React-safe value setter and the domain guard live in ONE place.
#
# ``set`` writes through the element's OWN native value setter (input → React's
# patched HTMLInputElement setter, textarea → HTMLTextAreaElement, …) so
# framework-controlled inputs (MoneyForward / 楽天 / 一部の銀行) actually register
# the typed value — a plain ``el.value=v`` is ignored by React/Vue. Using the
# element's own ``constructor.prototype`` (not a hard-coded HTMLInputElement)
# avoids an "Illegal invocation" on non-input elements; a try/catch falls back to
# a plain assignment. ``vis`` is a cheap visibility check (rendered + not disabled).
JS_LOGIN_HELPERS = (
    " const vis=e=>!!e && e.offsetParent!==null && !e.disabled;"
    " const set=(el,v)=>{ if(!el) return false; el.focus && el.focus();"
    "   try { const p=el.constructor && el.constructor.prototype;"
    "     const d=p && Object.getOwnPropertyDescriptor(p,'value');"
    "     if(d && d.set){ d.set.call(el,v); } else { el.value=v; } }"
    "   catch(_e){ try { el.value=v; } catch(__e){ return false; } }"
    "   el.dispatchEvent(new Event('input',{bubbles:true}));"
    "   el.dispatchEvent(new Event('change',{bubbles:true})); return true; };"
)


# Fill-script result strings that mean "nothing was filled" — the caller should
# keep waiting / retrying rather than treat them as a successful submit. Kept in
# ONE place so a new fill result (e.g. a future guard sentinel) is handled
# consistently by every autologin path.
NO_FILL_RESULTS = frozenset({"no-form", "wrong-domain"})


def domain_guard_js(*hosts: str) -> str:
    """JS guard snippet: ``return 'wrong-domain'`` unless ``location.hostname``
    equals one of ``hosts`` or a subdomain of it.

    Mirrors rimawarikun's not-rakuten / MoneyForward's not-moneyforward guard so
    credentials are never typed into an unexpected domain (e.g. after a redirect
    to an external SSO/phishing page).
    """
    alt = "|".join(re.escape(h) for h in hosts)
    return f" if(!/(^|\\.)({alt})$/.test(location.hostname)) return 'wrong-domain';"


# ログインフォーム自動記入 JS。``creds`` = {user, password}。戻り値:
#   'no-form'          パスワード欄が無い (ログイン画面ではない)
#   'submitted'        ユーザー名+パスワードを記入しログインボタンを押した
#   'submitted-no-user' ユーザー名欄が見つからずパスワードのみ記入して送信
#   'form-submit'      ボタンが無く form.submit() した
#   'filled-no-submit' 記入のみ (送信手段なし)
LOGIN_FILL_JS = (
    "(creds) => {"
    + JS_LOGIN_HELPERS +
    # フォームに不可視のダミー password 欄がある銀行 (住信SBI: passid=hidden /
    # loginPwd=visible) があるので、必ず「可視のパスワード欄」を選ぶ。
    " const pws = [...document.querySelectorAll('input[type=\"password\"]')];"
    " const pw = pws.find(vis) || pws[0];"
    " if (!pw) return 'no-form';"
    " const scope = pw.form || document;"
    " const user = [...scope.querySelectorAll('input')].find(i =>"
    "   i !== pw && /^(text|tel|)$/.test(i.type || '') && vis(i)"
    "   && !/search|query/i.test((i.id || '') + (i.name || '')));"
    " if (user) set(user, creds.user);"
    " set(pw, creds.password);"
    " const btn = [...document.querySelectorAll('input[type=submit],button,a,[onclick]')]"
    "   .find(b => /ログイン|log\\s*in/i.test((b.value || b.innerText || '').trim())"
    "             && vis(b));"
    " if (btn) { btn.click(); return user ? 'submitted' : 'submitted-no-user'; }"
    " if (pw.form) { pw.form.submit(); return 'form-submit'; }"
    " return 'filled-no-submit'; }"
)

_HAS_PW_JS = "() => !!document.querySelector('input[type=\"password\"]')"


def logged_in_js(host_re: str, ok_re: str, *, exclude_re: str | None = None) -> str:
    """Build a JS predicate: ``host`` matches ``host_re`` AND body text matches
    ``ok_re`` AND (if given) does NOT match ``exclude_re`` (e.g. ログアウト完了)。"""
    excl = " && !/" + exclude_re + "/.test(t)" if exclude_re else ""
    return (
        "(() => { const t = document.body ? document.body.innerText : '';"
        " return /" + host_re + "/.test(location.host)" + excl
        + " && /" + ok_re + "/.test(t); })()"
    )


def autologin(
    page,
    *,
    creds,
    login_url: str,
    logged_in_predicate: str,
    label: str,
    logger: logging.Logger,
) -> str | None:
    """ログイン画面へ env の ID/PW を自動記入して送信する。

    既にログイン済み (``logged_in_predicate`` が真) なら何もしない。パスワード欄が
    無い画面 (ログアウト完了 等) なら ``login_url`` へ移動してフォームを出してから
    記入する。戻り値は ``LOGIN_FILL_JS`` の結果文字列 (記入しなければ None)。
    """
    # 既にログイン済みなら記入不要。
    with contextlib.suppress(Exception):
        if page.evaluate(logged_in_predicate):
            return None
    # パスワード欄が無ければログイン URL へ移動して出す。
    with contextlib.suppress(Exception):
        if not page.evaluate(_HAS_PW_JS):
            page.goto(login_url, wait_until="domcontentloaded")
            with contextlib.suppress(Exception):
                page.wait_for_selector("input[type=password]", timeout=15000)
    result = None
    with contextlib.suppress(Exception):
        result = page.evaluate(LOGIN_FILL_JS, {"user": creds.user, "password": creds.password})
        if result and result not in NO_FILL_RESULTS:
            logger.info("[%s] ログイン自動記入: %s", label, result)
            with contextlib.suppress(Exception):
                page.wait_for_load_state("domcontentloaded", timeout=15000)
    return result


def recovering_wait_for_login(
    page,
    *,
    label: str,
    ready_js: str,
    system_error_js: str,
    recover,
    logger: logging.Logger,
    timeout_s: int = 300,
    max_restarts: int = 3,
    error_cls: type[Exception] = LoginGateError,
    gate: str = "login_ready",
    manual_hint: str = "passkey/SMS など",
) -> None:
    """Wait for an authenticated page, reopening login when recoverable errors appear.

    ``recover`` should reopen the login page and, when credentials are available,
    submit the login form again. Additional human authentication is still handled
    by the shared broker wait loop. This function intentionally owns no polling
    state machine; it is a bank-named adapter around ``wait_for_login`` so
    broker, bank, MoneyForward, and 利回りくん recovery semantics stay identical.
    """
    restarts = 0

    def logged_recover() -> None:
        nonlocal restarts
        restarts += 1
        logger.warning("[%s] エラー画面を検知したためログインページを開き直します (%d)",
                       label, restarts)
        recover()

    wait_for_login(
        page,
        label,
        ready_js=ready_js,
        system_error_js=system_error_js,
        recover=logged_recover,
        max_restarts=max_restarts,
        timeout_s=timeout_s,
        gate=gate,
        manual_hint=manual_hint,
        error_cls=error_cls,
    )
