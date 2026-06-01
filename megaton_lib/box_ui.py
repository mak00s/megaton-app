from __future__ import annotations

import asyncio
import json
import re
import threading
import time
import tempfile
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from megaton_lib.playwright_browser import async_browser_page

try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except ImportError:  # pragma: no cover - exercised when optional extra is absent
    class PlaywrightTimeoutError(TimeoutError):
        pass


_DEFAULT_BOX_LAUNCH_ARGS = [
    "--disable-popup-blocking",
    "--disable-blink-features=AutomationControlled",
]

BOX_SHARED_LINK_ACCESS_ALIASES = {
    "all": "open",
    "anyone": "open",
    "anyone_with_link": "open",
    "open": "open",
    "public": "open",
    "company": "company",
    "account": "company",
    "account_holders": "company",
    "organization": "company",
    "org": "company",
    "enterprise": "company",
}

BOX_SHARED_LINK_ACCESS_PATTERNS = {
    "open": [
        r"People with the link",
        r"Anyone with the link",
        r"Anyone who has the link",
        r"リンクを知っている全員",
        r"リンクを持つ全員",
        r"全員",
    ],
    "company": [
        r"People in your company",
        r"People in this company",
        r"Company",
        r"Organization",
        r"Enterprise",
        r"会社",
        r"組織",
        r"アカウント保持者",
        r"アカウント所有者",
    ],
}


def normalize_box_shared_link_access(value: str) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    access = BOX_SHARED_LINK_ACCESS_ALIASES.get(normalized)
    if not access:
        raise ValueError(f"Unsupported Box shared link access: {value}")
    return access


def _build_box_login_url(url: str) -> str:
    return "https://account.box.com/login?" + urlencode({"redirect_url": url})


async def download_from_box(
    *,
    url: str,
    login: str,
    password: str,
    download_dir: Path,
    login_link_selector: str = "div.login-link-non-sso a",
    login_input_selector: str = "input[name='login']",
    password_input_selector: str = "input[name='password']",
    submit_selector: str = "button[type='submit']",
    headless: bool = False,
    emulate_mobile: bool = True,
    mobile_device_name: str = "iPhone 13 Mini",
    locale: str = "ja-JP",
    browser_channel: str = "chrome",
    launch_args: list[str] | None = None,
    timeout_ms: int = 120_000,
    post_login_wait_ms: int = 2_000,
    mobile_menu_button_selector: str = "div.ItemListActions>button",
    mobile_download_item_selector: str = "li.DownloadMenuItem",
    desktop_more_button_pattern: str = r"Show More Options|More Options",
    desktop_download_item_pattern: str = r"Download|ダウンロード",
    folder_file_href_pattern: str = r"^/file/\d+$",
    print_permission_dialog_hint: bool = True,
) -> Path:
    """Download the currently-open Box item via login flow.

    Selectors and role-name patterns are configurable so callers can adapt this
    flow to different Box layouts without changing notebook code.
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    async with _open_box_session(
        url=url,
        login=login,
        password=password,
        login_link_selector=login_link_selector,
        login_input_selector=login_input_selector,
        password_input_selector=password_input_selector,
        submit_selector=submit_selector,
        headless=headless,
        emulate_mobile=emulate_mobile,
        mobile_device_name=mobile_device_name,
        locale=locale,
        browser_channel=browser_channel,
        launch_args=launch_args,
        timeout_ms=timeout_ms,
        post_login_wait_ms=post_login_wait_ms,
        print_permission_dialog_hint=print_permission_dialog_hint,
    ) as (context, page):

        if "/folder/" in page.url:
            await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)
            file_links = await _collect_box_folder_file_links(
                page=page,
                folder_file_href_pattern=folder_file_href_pattern,
            )
            if not file_links:
                raise RuntimeError("No downloadable file links found in Box folder view")

            downloaded_paths: list[Path] = []
            for item in file_links:
                file_page = await context.new_page()
                try:
                    href = str(item["href"])
                    if href.startswith("http://") or href.startswith("https://"):
                        target = href
                    else:
                        target = f"https://app.box.com{href}"
                    await file_page.goto(target)
                    out_path = await _download_current_box_item(
                        page=file_page,
                        download_dir=download_dir,
                        mobile_menu_button_selector=mobile_menu_button_selector,
                        mobile_download_item_selector=mobile_download_item_selector,
                        desktop_more_button_pattern=desktop_more_button_pattern,
                        desktop_download_item_pattern=desktop_download_item_pattern,
                        timeout_ms=timeout_ms,
                    )
                    downloaded_paths.append(out_path)
                finally:
                    await file_page.close()

            if len(downloaded_paths) == 1:
                out_path = downloaded_paths[0]
            else:
                archive_name = f"box-folder-{time.strftime('%Y%m%d-%H%M%S')}.zip"
                out_path = download_dir / archive_name
                _archive_box_downloads(output_path=out_path, downloaded_paths=downloaded_paths)
        else:
            out_path = await _download_current_box_item(
                page=page,
                download_dir=download_dir,
                mobile_menu_button_selector=mobile_menu_button_selector,
                mobile_download_item_selector=mobile_download_item_selector,
                desktop_more_button_pattern=desktop_more_button_pattern,
                desktop_download_item_pattern=desktop_download_item_pattern,
                timeout_ms=timeout_ms,
            )
        return out_path


async def upload_file_to_box_folder_via_ui(
    *,
    parent_folder_url: str,
    target_subfolder_name: str | list[str] | tuple[str, ...] = "",
    file_path: str | Path,
    login: str,
    password: str,
    login_link_selector: str = "div.login-link-non-sso a",
    login_input_selector: str = "input[name='login']",
    password_input_selector: str = "input[name='password']",
    submit_selector: str = "button[type='submit']",
    headless: bool = False,
    emulate_mobile: bool = False,
    mobile_device_name: str = "iPhone 13 Mini",
    locale: str = "ja-JP",
    browser_channel: str = "chrome",
    launch_args: list[str] | None = None,
    timeout_ms: int = 120_000,
    post_login_wait_ms: int = 2_000,
    print_permission_dialog_hint: bool = True,
    new_item_button_pattern: str = r"^(New|新規|Create|作成)$",
    new_folder_item_pattern: str = r"^(Create a new )?Folder$|^フォルダ$|^新しいフォルダ$",
    create_button_pattern: str = r"Create|作成",
    upload_button_pattern: str = r"^(New|新規|Create|作成|Upload|アップロード)$",
    upload_file_item_pattern: str = r"^File Upload$|^Upload File$|^ファイルアップロード$",
    upload_post_wait_ms: int = 3_000,
    create_shared_link: bool = False,
    shared_link_access: str = "company",
) -> dict:
    """Upload a file to a Box folder via the web UI.

    The flow logs in, optionally opens or creates ``target_subfolder_name``
    under ``parent_folder_url``, then uploads ``file_path`` there.
    """
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(path)

    download_dir = path.parent
    async with _open_box_session(
        url=parent_folder_url,
        login=login,
        password=password,
        login_link_selector=login_link_selector,
        login_input_selector=login_input_selector,
        password_input_selector=password_input_selector,
        submit_selector=submit_selector,
        headless=headless,
        emulate_mobile=emulate_mobile,
        mobile_device_name=mobile_device_name,
        locale=locale,
        browser_channel=browser_channel,
        launch_args=launch_args,
        timeout_ms=timeout_ms,
        post_login_wait_ms=post_login_wait_ms,
        print_permission_dialog_hint=print_permission_dialog_hint,
    ) as (_context, page):
        await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)

        if isinstance(target_subfolder_name, (list, tuple)):
            subfolder_candidates = [
                str(name).strip() for name in target_subfolder_name if str(name).strip()
            ]
        else:
            subfolder_candidates = [str(target_subfolder_name).strip()] if str(target_subfolder_name).strip() else []

        resolved_subfolder_name = subfolder_candidates[0] if subfolder_candidates else ""
        folder_created = False
        if subfolder_candidates:
            folder_created, resolved_subfolder_name = await _open_or_create_box_subfolder(
                page=page,
                folder_names=subfolder_candidates,
                timeout_ms=timeout_ms,
                new_item_button_pattern=new_item_button_pattern,
                new_folder_item_pattern=new_folder_item_pattern,
                create_button_pattern=create_button_pattern,
            )

        await _upload_file_to_current_box_folder(
            page=page,
            file_path=path,
            timeout_ms=timeout_ms,
            upload_button_pattern=upload_button_pattern,
            upload_file_item_pattern=upload_file_item_pattern,
            upload_post_wait_ms=upload_post_wait_ms,
        )
        shared_url = ""
        resolved_shared_link_access = ""
        shared_link_status = "skipped"
        shared_link_error = ""
        if create_shared_link:
            resolved_shared_link_access = normalize_box_shared_link_access(shared_link_access)
            try:
                shared_url = await _create_or_get_box_shared_link(
                    page=page,
                    item_name=path.name,
                    access=resolved_shared_link_access,
                    timeout_ms=timeout_ms,
                )
                shared_link_status = "created" if shared_url else "failed"
            except Exception as exc:
                shared_link_error = str(exc)
                shared_link_status = "failed"
                print(f"[warn] Could not create Box shared link for uploaded file: {path.name}; upload succeeded: {exc}")
        return {
            "uploaded_file_name": path.name,
            "target_subfolder_name": resolved_subfolder_name,
            "folder_created": folder_created,
            "upload_mode": "playwright-ui",
            "output_dir": str(download_dir),
            "shared_url": shared_url,
            "shared_link_access": resolved_shared_link_access,
            "shared_link_status": shared_link_status,
            "shared_link_error": shared_link_error,
        }


async def upload_files_to_box_folder_via_ui(
    *,
    parent_folder_url: str,
    target_subfolder_name: str | list[str] | tuple[str, ...] = "",
    nested_subfolder_name: str | list[str] | tuple[str, ...] = "",
    file_paths: list[str | Path],
    login: str,
    password: str,
    login_link_selector: str = "div.login-link-non-sso a",
    login_input_selector: str = "input[name='login']",
    password_input_selector: str = "input[name='password']",
    submit_selector: str = "button[type='submit']",
    headless: bool = False,
    emulate_mobile: bool = False,
    mobile_device_name: str = "iPhone 13 Mini",
    locale: str = "ja-JP",
    browser_channel: str = "chrome",
    launch_args: list[str] | None = None,
    timeout_ms: int = 120_000,
    post_login_wait_ms: int = 2_000,
    print_permission_dialog_hint: bool = True,
    new_item_button_pattern: str = r"^(New|新規|Create|作成)$",
    new_folder_item_pattern: str = r"^(Create a new )?Folder$|^フォルダ$|^新しいフォルダ$",
    create_button_pattern: str = r"Create|作成",
    upload_button_pattern: str = r"^(New|新規|Create|作成|Upload|アップロード)$",
    upload_file_item_pattern: str = r"^File Upload$|^Upload File$|^ファイルアップロード$",
    upload_post_wait_ms: int = 3_000,
    create_shared_link: bool = False,
    shared_link_access: str = "company",
    shared_link_target: str = "file",
) -> list[dict]:
    """Upload multiple files to a (optionally nested) Box folder in one session.

    Navigates ``parent_folder_url`` → ``target_subfolder_name`` →
    ``nested_subfolder_name`` (each accepts a string or list of candidates;
    pass empty to skip a level). Then uploads every entry of ``file_paths``
    in turn without reopening the browser.
    """
    if not file_paths:
        return []
    paths: list[Path] = []
    for raw in file_paths:
        path = Path(raw).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        paths.append(path)

    def _candidates(value: str | list[str] | tuple[str, ...]) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(name).strip() for name in value if str(name).strip()]
        text = str(value).strip()
        return [text] if text else []

    first_candidates = _candidates(target_subfolder_name)
    second_candidates = _candidates(nested_subfolder_name)
    normalized_shared_link_target = str(shared_link_target or "file").strip().lower()
    if normalized_shared_link_target not in {"file", "folder"}:
        raise ValueError(f"Unsupported Box shared link target: {shared_link_target}")

    async with _open_box_session(
        url=parent_folder_url,
        login=login,
        password=password,
        login_link_selector=login_link_selector,
        login_input_selector=login_input_selector,
        password_input_selector=password_input_selector,
        submit_selector=submit_selector,
        headless=headless,
        emulate_mobile=emulate_mobile,
        mobile_device_name=mobile_device_name,
        locale=locale,
        browser_channel=browser_channel,
        launch_args=launch_args,
        timeout_ms=timeout_ms,
        post_login_wait_ms=post_login_wait_ms,
        print_permission_dialog_hint=print_permission_dialog_hint,
    ) as (_context, page):
        await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)

        navigated: list[tuple[str, bool]] = []
        for level_candidates in (first_candidates, second_candidates):
            if not level_candidates:
                continue
            created, resolved = await _open_or_create_box_subfolder(
                page=page,
                folder_names=level_candidates,
                timeout_ms=timeout_ms,
                new_item_button_pattern=new_item_button_pattern,
                new_folder_item_pattern=new_folder_item_pattern,
                create_button_pattern=create_button_pattern,
            )
            navigated.append((resolved, created))

        target_subfolder = navigated[0][0] if navigated else ""
        nested_subfolder = navigated[1][0] if len(navigated) > 1 else ""
        target_created = navigated[0][1] if navigated else False
        nested_created = navigated[1][1] if len(navigated) > 1 else False

        results: list[dict] = []
        for path in paths:
            await _upload_file_to_current_box_folder(
                page=page,
                file_path=path,
                timeout_ms=timeout_ms,
                upload_button_pattern=upload_button_pattern,
                upload_file_item_pattern=upload_file_item_pattern,
                upload_post_wait_ms=upload_post_wait_ms,
            )
            result = {
                "uploaded_file_name": path.name,
                "target_subfolder_name": target_subfolder,
                "nested_subfolder_name": nested_subfolder,
                "folder_created": target_created,
                "nested_folder_created": nested_created,
                "upload_mode": "playwright-ui",
                "output_dir": str(path.parent),
            }
            if create_shared_link and normalized_shared_link_target == "file":
                resolved_access = normalize_box_shared_link_access(shared_link_access)
                shared_url = ""
                shared_link_status = "failed"
                shared_link_error = ""
                try:
                    shared_url = await _create_or_get_box_shared_link(
                        page=page,
                        item_name=path.name,
                        access=resolved_access,
                        timeout_ms=timeout_ms,
                    )
                    shared_link_status = "created" if shared_url else "failed"
                except Exception as exc:
                    shared_link_error = str(exc)
                    print(f"[warn] Could not create Box shared link for uploaded file: {path.name}; upload succeeded: {exc}")
                result.update(
                    {
                        "shared_url": shared_url,
                        "shared_link_access": resolved_access,
                        "shared_link_status": shared_link_status,
                        "shared_link_error": shared_link_error,
                    }
                )
            results.append(result)

        shared_url = ""
        resolved_shared_link_access = ""
        shared_link_status = "skipped"
        shared_link_error = ""
        if create_shared_link and normalized_shared_link_target == "folder":
            resolved_shared_link_access = normalize_box_shared_link_access(shared_link_access)
            try:
                shared_url = await _create_or_get_current_box_folder_shared_link(
                    page=page,
                    access=resolved_shared_link_access,
                    timeout_ms=timeout_ms,
                )
                shared_link_status = "created" if shared_url else "failed"
            except Exception as exc:
                shared_link_error = str(exc)
                shared_link_status = "failed"
                folder_name = nested_subfolder or target_subfolder or "current folder"
                print(f"[warn] Could not create Box shared link for {folder_name}; upload succeeded: {exc}")

        if create_shared_link:
            for result in results:
                result.update(
                    {
                        "folder_shared_url": shared_url,
                        "folder_shared_link_access": resolved_shared_link_access,
                        "folder_shared_link_status": shared_link_status,
                        "folder_shared_link_error": shared_link_error,
                    }
                )

    return results


def upload_files_to_box_folder_via_ui_sync(**kwargs) -> list[dict]:
    """Run the async multi-file Box UI upload helper from sync code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(upload_files_to_box_folder_via_ui(**kwargs))

    result: dict = {}
    error_holder: dict = {}

    def _runner():
        try:
            result["value"] = asyncio.run(upload_files_to_box_folder_via_ui(**kwargs))
        except Exception as exc:  # pragma: no cover - thread handoff
            error_holder["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in error_holder:
        raise error_holder["error"]
    return result["value"]


def upload_file_to_box_folder_via_ui_sync(**kwargs) -> dict:
    """Run the async Box UI upload helper from sync code, including notebooks."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(upload_file_to_box_folder_via_ui(**kwargs))

    result: dict = {}
    error_holder: dict = {}

    def _runner():
        try:
            result["value"] = asyncio.run(upload_file_to_box_folder_via_ui(**kwargs))
        except Exception as exc:  # pragma: no cover - thread handoff
            error_holder["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "error" in error_holder:
        raise error_holder["error"]
    return result["value"]


@asynccontextmanager
async def _open_box_session(
    *,
    url: str,
    login: str,
    password: str,
    login_link_selector: str,
    login_input_selector: str,
    password_input_selector: str,
    submit_selector: str,
    headless: bool,
    emulate_mobile: bool,
    mobile_device_name: str,
    locale: str,
    browser_channel: str,
    launch_args: list[str] | None,
    timeout_ms: int,
    post_login_wait_ms: int,
    print_permission_dialog_hint: bool,
):
    args = launch_args if launch_args is not None else list(_DEFAULT_BOX_LAUNCH_ARGS)

    async with async_browser_page(
        headless=headless,
        locale=locale,
        browser_channel=browser_channel,
        launch_args=args,
        accept_downloads=True,
        device_name=mobile_device_name if emulate_mobile else None,
    ) as page:
        await _login_to_box(
            page=page,
            url=url,
            login=login,
            password=password,
            login_link_selector=login_link_selector,
            login_input_selector=login_input_selector,
            password_input_selector=password_input_selector,
            submit_selector=submit_selector,
            timeout_ms=timeout_ms,
            post_login_wait_ms=post_login_wait_ms,
            print_permission_dialog_hint=print_permission_dialog_hint,
        )
        yield page.context, page


async def _collect_box_folder_file_links(*, page, folder_file_href_pattern: str) -> list[dict[str, str]]:
    return await page.evaluate(
        """(pattern) => {
            const re = new RegExp(pattern);
            return Array.from(document.querySelectorAll("a.item-link[href], a[href*='/file/']"))
                .map((el) => ({
                    href: el.getAttribute('href'),
                    name: (el.textContent || '').trim(),
                }))
                .filter((item) => item.href && re.test(item.href) && item.name);
        }""",
        folder_file_href_pattern,
    )


def _archive_box_downloads(*, output_path: Path, downloaded_paths: list[Path]) -> None:
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item_path in downloaded_paths:
            zf.write(item_path, arcname=item_path.name)


async def _download_current_box_item(
    *,
    page,
    download_dir: Path,
    mobile_menu_button_selector: str,
    mobile_download_item_selector: str,
    desktop_more_button_pattern: str,
    desktop_download_item_pattern: str,
    timeout_ms: int,
) -> Path:
    try:
        await page.wait_for_selector(mobile_menu_button_selector, timeout=10_000)
        await page.click(mobile_menu_button_selector)
        await page.wait_for_selector(mobile_download_item_selector, timeout=10_000)
        async with page.expect_download(timeout=timeout_ms) as download_info:
            await page.click(mobile_download_item_selector)
    except PlaywrightTimeoutError:
        more_btn = page.get_by_role(
            "button", name=re.compile(desktop_more_button_pattern, re.I)
        ).first
        await more_btn.click(timeout=timeout_ms)
        download_item = page.get_by_role(
            "menuitem", name=re.compile(desktop_download_item_pattern, re.I)
        ).first
        async with page.expect_download(timeout=timeout_ms) as download_info:
            await download_item.click(timeout=timeout_ms)

    download = await download_info.value
    out_path = download_dir / download.suggested_filename
    await download.save_as(str(out_path))
    return out_path


async def _login_to_box(
    *,
    page,
    url: str,
    login: str,
    password: str,
    login_link_selector: str,
    login_input_selector: str,
    password_input_selector: str,
    submit_selector: str,
    timeout_ms: int,
    post_login_wait_ms: int,
    print_permission_dialog_hint: bool,
) -> None:
    login_url = _build_box_login_url(url)
    await page.goto(login_url)
    await page.wait_for_load_state("domcontentloaded")

    resolved_login_selector = ",".join(
        [
            login_input_selector,
            "input[type='email']",
            "input[name='email']",
            "input[autocomplete='username']",
        ]
    )
    resolved_password_selector = ",".join(
        [
            password_input_selector,
            "input[type='password']",
            "input[autocomplete='current-password']",
        ]
    )
    resolved_submit_selector = ",".join(
        [
            submit_selector,
            "button[data-testid='login-submit']",
            "button[data-resin-target='button']",
            "input[type='submit']",
        ]
    )

    if await page.locator(resolved_login_selector).count() == 0:
        if await page.locator(login_link_selector).count() > 0:
            await page.click(login_link_selector)
        elif await page.locator("a[href*='login']").count() > 0:
            await page.locator("a[href*='login']").first.click()
        else:
            # Shared folder pages can render without a visible login ribbon.
            await page.goto(login_url)
            await page.wait_for_load_state("domcontentloaded")

    await page.wait_for_selector(resolved_login_selector, timeout=timeout_ms)
    await page.fill(resolved_login_selector, login)
    await _click_box_submit(page=page, submit_selector=resolved_submit_selector, timeout_ms=timeout_ms)

    try:
        await page.wait_for_selector(resolved_password_selector, timeout=timeout_ms)
    except PlaywrightTimeoutError as exc:
        debug_dir = await _dump_box_login_debug(page=page, reason="password-step-timeout")
        raise RuntimeError(
            "Box login did not reach the password step. "
            f"url={page.url} debug_dir={debug_dir}"
        ) from exc

    await page.fill(resolved_password_selector, password)
    await _click_box_submit(page=page, submit_selector=resolved_submit_selector, timeout_ms=timeout_ms)

    if print_permission_dialog_hint:
        print("[info] If a Chrome permission dialog appears, close it manually to continue.")
    await page.wait_for_timeout(post_login_wait_ms)
    await page.goto(url)
    await page.wait_for_load_state("domcontentloaded")


async def _click_box_submit(*, page, submit_selector: str, timeout_ms: int) -> None:
    locator = page.locator(submit_selector).first
    try:
        await locator.wait_for(timeout=5_000)
        await locator.click(timeout=timeout_ms)
        return
    except PlaywrightTimeoutError:
        pass

    for role_name in ("Continue", "Next", "Sign In", "Log In", "続行", "次へ", "ログイン", "サインイン"):
        button = page.get_by_role("button", name=re.compile(f"^{re.escape(role_name)}$", re.I)).first
        if await button.count() > 0:
            await button.click(timeout=timeout_ms)
            return

    await page.keyboard.press("Enter")


async def _dump_box_login_debug(*, page, reason: str) -> str:
    debug_root = Path(tempfile.gettempdir()) / "box-login-debug"
    debug_root.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    debug_dir = debug_root / f"{reason}-{stamp}"
    debug_dir.mkdir(parents=True, exist_ok=True)

    html_path = debug_dir / "page.html"
    json_path = debug_dir / "page.json"
    screenshot_path = debug_dir / "page.png"

    html_path.write_text(await page.content(), encoding="utf-8")
    await page.screenshot(path=str(screenshot_path), full_page=True)
    json_path.write_text(
        json.dumps(
            {
                "url": page.url,
                "title": await page.title(),
                "inputs": await page.locator("input").evaluate_all(
                    """els => els.map(el => ({
                        type: el.getAttribute('type') || '',
                        name: el.getAttribute('name') || '',
                        placeholder: el.getAttribute('placeholder') || '',
                        autocomplete: el.getAttribute('autocomplete') || '',
                        ariaLabel: el.getAttribute('aria-label') || '',
                    }))"""
                ),
                "buttons": await page.locator("button,input[type='submit']").evaluate_all(
                    """els => els.map(el => ({
                        text: (el.textContent || '').trim(),
                        type: el.getAttribute('type') || '',
                        ariaLabel: el.getAttribute('aria-label') || '',
                        value: el.getAttribute('value') || '',
                    }))"""
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[warn] Box login debug captured: {debug_dir}")
    return str(debug_dir)


async def _ensure_box_folder_page_ready(*, page, timeout_ms: int) -> None:
    await page.wait_for_load_state("domcontentloaded")
    # Box folder pages render their file list lazily. Wait for actual folder content,
    # not just the shell container.
    await page.wait_for_function(
        """() => {
            return Boolean(
                document.querySelector('a.item-link[href]') ||
                Array.from(document.querySelectorAll("a[href*='/folder/']")).some((el) => {
                    const text = (el.textContent || '').trim();
                    return text && text !== 'Files';
                }) ||
                Array.from(document.querySelectorAll('button')).some((el) => {
                    const text = ((el.getAttribute('aria-label') || '') + ' ' + (el.textContent || '')).trim();
                    return /new|新規|upload|アップロード|share|共有/i.test(text);
                })
            );
        }""",
        timeout=timeout_ms,
    )


async def _open_or_create_box_subfolder(
    *,
    page,
    folder_names: list[str],
    timeout_ms: int,
    new_item_button_pattern: str,
    new_folder_item_pattern: str,
    create_button_pattern: str,
) -> tuple[bool, str]:
    await page.wait_for_timeout(5_000)
    new_btn = await _find_box_action_button(
        page=page,
        primary_pattern=new_item_button_pattern,
        fallback_patterns=[r"^(New Item|Create New)$", r"^(新規作成)$"],
    )
    await new_btn.wait_for(timeout=timeout_ms)

    for folder_name in folder_names:
        existing = await _find_box_item_link(page=page, item_name=folder_name)
        if existing is not None:
            await existing.click(timeout=timeout_ms)
            await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)
            await page.wait_for_timeout(3_000)
            return False, folder_name

    folder_name = folder_names[0]

    await new_btn.click(timeout=timeout_ms)
    folder_item = page.get_by_role("menuitem", name=re.compile(new_folder_item_pattern, re.I)).first
    await folder_item.wait_for(timeout=timeout_ms)
    await folder_item.click(timeout=timeout_ms)

    folder_input = page.locator(
        "input[name='folder-name'],"
        "input[placeholder='My New Folder'],"
        "input[name='name'],"
        "input[aria-label*='Folder'],"
        "input[aria-label*='フォルダ'],"
        "input[placeholder*='Folder'],"
        "input[placeholder*='フォルダ']"
    ).first
    await folder_input.wait_for(timeout=timeout_ms)
    await folder_input.fill(folder_name)

    create_btn = page.get_by_role("button", name=re.compile(create_button_pattern, re.I)).first
    await create_btn.click(timeout=timeout_ms)

    created = await _wait_for_box_item_link(page=page, item_name=folder_name, timeout_ms=timeout_ms)
    await created.click(timeout=timeout_ms)
    await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)
    return True, folder_name


async def _upload_file_to_current_box_folder(
    *,
    page,
    file_path: Path,
    timeout_ms: int,
    upload_button_pattern: str,
    upload_file_item_pattern: str,
    upload_post_wait_ms: int,
) -> None:
    await _ensure_box_folder_page_ready(page=page, timeout_ms=timeout_ms)
    await page.wait_for_timeout(5_000)
    file_already_present = await _find_box_item_link(page=page, item_name=file_path.name) is not None
    upload_via_menu = False
    upload_btn = await _find_box_action_button(
        page=page,
        primary_pattern=upload_button_pattern,
        fallback_patterns=[r"^(New Item|Create New|Upload)$", r"^(新規作成|アップロード)$"],
    )
    try:
        await upload_btn.wait_for(timeout=10_000)
    except PlaywrightTimeoutError:
        upload_btn = page.locator("__missing__")

    if await upload_btn.count() > 0:
        upload_via_menu = True
        await upload_btn.click(timeout=timeout_ms)
        upload_file_item = page.get_by_role("menuitem", name=re.compile(upload_file_item_pattern, re.I)).first
        await upload_file_item.wait_for(timeout=timeout_ms)
        async with page.expect_file_chooser(timeout=timeout_ms) as chooser_info:
            await upload_file_item.click(timeout=timeout_ms)
        chooser = await chooser_info.value
        await chooser.set_files(str(file_path))

    if not upload_via_menu:
        file_input = page.locator("input[type='file']").first
        if await file_input.count() > 0:
            await file_input.set_input_files(str(file_path), timeout=timeout_ms)
        else:
            raise RuntimeError("Box upload UI not found: neither New/File Upload nor input[type=file] is available")

    await page.wait_for_timeout(upload_post_wait_ms)
    try:
        await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 30_000))
    except PlaywrightTimeoutError:
        pass

    try:
        await page.wait_for_function(
            """() => {
                const text = (document.body?.innerText || '');
                return !/(Uploading\\b|アップロード中|処理中|Preparing upload|Versioning)/i.test(text);
            }""",
            timeout=min(timeout_ms, 30_000),
        )
    except PlaywrightTimeoutError:
        pass

    await _handle_box_refresh_notification(page=page, timeout_ms=min(timeout_ms, 10_000))

    if file_already_present:
        # A same-named file already visible in the folder would satisfy the old
        # success check immediately. For version updates, wait a bit longer so
        # Box can finish replacing the existing item.
        await page.wait_for_timeout(max(upload_post_wait_ms, 8_000))
        return

    await _wait_for_box_upload_completion(
        page=page,
        item_name=file_path.name,
        timeout_ms=timeout_ms,
    )


async def _handle_box_refresh_notification(*, page, timeout_ms: int) -> bool:
    refresh_patterns = [
        r"refresh|reload|update|最新表示|更新|再読み込み|再表示",
        r"^(OK|確認|更新する|再読み込み)$",
    ]
    deadline = time.time() + (timeout_ms / 1000)
    click_count = 0
    while time.time() < deadline:
        clicked = False
        for pattern in refresh_patterns:
            button = page.get_by_role("button", name=re.compile(pattern, re.I)).first
            try:
                if await button.count() > 0 and await button.is_visible():
                    await button.click(timeout=2_000)
                    clicked = True
                    click_count += 1
                    break
            except PlaywrightTimeoutError:
                continue
        if not clicked:
            return click_count > 0
        if click_count >= 3:
            return True
        await page.wait_for_timeout(1_000)
    return click_count > 0


async def _wait_for_box_upload_completion(*, page, item_name: str, timeout_ms: int) -> None:
    deadline = time.time() + (timeout_ms / 1000)
    reload_attempts = 0
    idle_checks = 0
    while time.time() < deadline:
        locator = await _find_box_item_link(page=page, item_name=item_name)
        if locator is not None:
            return

        clicked_refresh = await _handle_box_refresh_notification(page=page, timeout_ms=2_000)
        locator = await _find_box_item_link(page=page, item_name=item_name)
        if locator is not None:
            return

        if reload_attempts < 3:
            try:
                await page.reload(wait_until="domcontentloaded", timeout=15_000)
                await _ensure_box_folder_page_ready(page=page, timeout_ms=15_000)
            except PlaywrightTimeoutError:
                pass
            reload_attempts += 1
            idle_checks = 0
        else:
            idle_checks += 0 if clicked_refresh else 1
            if idle_checks >= 3:
                break

        await page.wait_for_timeout(2_000)

    raise RuntimeError(f"Timed out waiting for uploaded Box file to appear: {item_name}")


async def _create_or_get_box_shared_link(
    *,
    page,
    item_name: str,
    access: str,
    timeout_ms: int,
) -> str:
    await page.context.grant_permissions(
        ["clipboard-read", "clipboard-write"],
        origin="https://app.box.com",
    )
    if not await _open_box_item_share_dialog_from_folder(
        page=page,
        item_name=item_name,
        timeout_ms=timeout_ms,
    ):
        await _open_box_item_page(page=page, item_name=item_name, timeout_ms=timeout_ms)
        await _click_box_share_button(page=page, timeout_ms=timeout_ms)

    dialog = await _box_active_dialog(page=page)
    await _ensure_box_shared_link_enabled(page=page, dialog=dialog, timeout_ms=timeout_ms)
    await _set_box_shared_link_access(page=page, dialog=dialog, access=access, timeout_ms=timeout_ms)

    shared_url = await _read_box_shared_link_from_dialog(page=page, dialog=dialog)
    if shared_url:
        return shared_url

    copy_button = dialog.get_by_role("button", name=re.compile(r"Copy Link|Copy shared link|リンクをコピー|コピー", re.I)).first
    await copy_button.click(timeout=timeout_ms)
    try:
        shared_url = str(await page.evaluate("navigator.clipboard.readText()")).strip()
    except Exception:
        shared_url = ""
    if not shared_url:
        shared_url = await _read_box_shared_link_from_dialog(page=page, dialog=dialog)
    if not shared_url:
        print(f"[warn] Could not read Box shared link for uploaded file: {item_name}; upload succeeded")
        return ""
    return shared_url


async def _create_or_get_current_box_folder_shared_link(
    *,
    page,
    access: str,
    timeout_ms: int,
) -> str:
    await page.context.grant_permissions(
        ["clipboard-read", "clipboard-write"],
        origin="https://app.box.com",
    )
    await _click_box_share_button(page=page, timeout_ms=timeout_ms)
    dialog = await _box_active_dialog(page=page)
    await _ensure_box_shared_link_enabled(page=page, dialog=dialog, timeout_ms=timeout_ms)
    await _set_box_shared_link_access(page=page, dialog=dialog, access=access, timeout_ms=timeout_ms)

    shared_url = await _read_box_shared_link_from_dialog(page=page, dialog=dialog)
    if shared_url:
        return shared_url

    copy_button = dialog.get_by_role("button", name=re.compile(r"Copy Link|Copy shared link|リンクをコピー|コピー", re.I)).first
    await copy_button.click(timeout=timeout_ms)
    try:
        shared_url = str(await page.evaluate("navigator.clipboard.readText()")).strip()
    except Exception:
        shared_url = ""
    if not shared_url:
        shared_url = await _read_box_shared_link_from_dialog(page=page, dialog=dialog)
    if not shared_url:
        print("[warn] Could not read Box shared link for current folder; upload succeeded")
        return ""
    return shared_url


async def _open_box_item_share_dialog_from_folder(*, page, item_name: str, timeout_ms: int) -> bool:
    item_link = await _wait_for_box_item_link(page=page, item_name=item_name, timeout_ms=timeout_ms)
    await item_link.hover(timeout=timeout_ms)
    row = page.get_by_role("row", name=re.compile(re.escape(item_name), re.I)).first
    if await row.count() > 0:
        share_button = row.get_by_role("button", name=re.compile(r"Share|共有", re.I)).first
        if await share_button.count() > 0:
            await share_button.click(timeout=timeout_ms)
            return True
    return False


async def _open_box_item_page(*, page, item_name: str, timeout_ms: int) -> None:
    item_link = await _wait_for_box_item_link(page=page, item_name=item_name, timeout_ms=timeout_ms)
    href = str(await item_link.get_attribute("href") or "").strip()
    if href:
        target = href if href.startswith(("http://", "https://")) else f"https://app.box.com{href}"
        await page.goto(target, wait_until="domcontentloaded", timeout=timeout_ms)
    else:
        await item_link.click(timeout=timeout_ms)
        await page.wait_for_load_state("domcontentloaded")


async def _click_box_share_button(*, page, timeout_ms: int) -> None:
    share_button = page.get_by_role("button", name=re.compile(r"Share|共有", re.I)).first
    await share_button.wait_for(timeout=timeout_ms)
    await share_button.click(timeout=timeout_ms)


async def _box_active_dialog(*, page):
    dialog = page.get_by_role("dialog").last
    try:
        await dialog.wait_for(timeout=10_000)
        return dialog
    except PlaywrightTimeoutError:
        return page.locator("body")


async def _ensure_box_shared_link_enabled(*, page, dialog, timeout_ms: int) -> None:
    create_patterns = [
        r"Create Link",
        r"Create shared link",
        r"Enable shared link",
        r"共有リンクを作成",
        r"共有リンクを有効",
        r"リンクを作成",
    ]
    for pattern in create_patterns:
        button = dialog.get_by_role("button", name=re.compile(pattern, re.I)).first
        if await button.count() > 0:
            try:
                await button.click(timeout=timeout_ms)
                await page.wait_for_timeout(1_000)
                return
            except PlaywrightTimeoutError:
                continue


async def _set_box_shared_link_access(*, page, dialog, access: str, timeout_ms: int) -> None:
    patterns = BOX_SHARED_LINK_ACCESS_PATTERNS[access]
    if await _click_box_access_option(page=page, scope=dialog, patterns=patterns, timeout_ms=2_000):
        return

    menu_openers = dialog.get_by_role("button").filter(
        has_text=re.compile(
            r"Invited|People|Anyone|Company|Organization|Link|招待|全員|会社|組織|リンク|アクセス",
            re.I,
        )
    )
    count = await menu_openers.count()
    for idx in range(count):
        opener = menu_openers.nth(idx)
        try:
            if not await opener.is_visible():
                continue
            await opener.click(timeout=5_000)
            if await _click_box_access_option(page=page, scope=page.locator("body"), patterns=patterns, timeout_ms=5_000):
                return
        except PlaywrightTimeoutError:
            continue

    combo = dialog.get_by_role("combobox").first
    if await combo.count() > 0:
        await combo.click(timeout=timeout_ms)
        if await _click_box_access_option(page=page, scope=page.locator("body"), patterns=patterns, timeout_ms=5_000):
            return

    raise RuntimeError(f"Could not set Box shared link access: {access}")


async def _click_box_access_option(*, page, scope, patterns: list[str], timeout_ms: int) -> bool:
    for pattern in patterns:
        option_pattern = re.compile(pattern, re.I)
        for role in ("option", "menuitem", "button"):
            target = scope.get_by_role(role, name=option_pattern).first
            if await target.count() > 0:
                try:
                    await target.click(timeout=timeout_ms)
                    await page.wait_for_timeout(1_000)
                    return True
                except PlaywrightTimeoutError:
                    continue
        text_target = scope.get_by_text(option_pattern).first
        if await text_target.count() > 0:
            try:
                await text_target.click(timeout=timeout_ms)
                await page.wait_for_timeout(1_000)
                return True
            except PlaywrightTimeoutError:
                continue
    return False


async def _read_box_shared_link_from_dialog(*, page, dialog) -> str:
    link = await dialog.locator("input, textarea").evaluate_all(
        """els => {
            const found = els
                .map((el) => el.value || el.getAttribute('value') || '')
                .find((value) => /^https:\\/\\/[^\\s]+/.test(value));
            return found || '';
        }"""
    )
    if str(link).strip():
        return str(link).strip()
    text_link = await dialog.evaluate(
        """el => {
            const text = el.innerText || '';
            const match = text.match(/https:\\/\\/[^\\s]+/);
            return match ? match[0] : '';
        }"""
    )
    if str(text_link).strip():
        return str(text_link).strip()
    try:
        clip = str(await page.evaluate("navigator.clipboard.readText()")).strip()
        if clip.startswith("https://"):
            return clip
    except Exception:
        pass
    return ""


async def _find_box_action_button(*, page, primary_pattern: str, fallback_patterns: list[str]):
    button = page.get_by_role("button", name=re.compile(primary_pattern, re.I)).first
    if await button.count() > 0:
        return button

    for pattern in fallback_patterns:
        candidate = page.get_by_role("button", name=re.compile(pattern, re.I)).first
        if await candidate.count() > 0:
            return candidate

    return page.locator("__missing__")


async def _find_box_item_link(*, page, item_name: str):
    locator = page.get_by_role("link", name=re.compile(f"^{re.escape(item_name)}$", re.I)).first
    if await locator.count() > 0:
        return locator

    dom_locator = page.locator(f"a.item-link:has-text('{item_name}')").first
    if await dom_locator.count() > 0:
        return dom_locator

    generic_folder_locator = page.locator(f"a[href*='/folder/']:has-text('{item_name}')").first
    if await generic_folder_locator.count() > 0:
        return generic_folder_locator
    return None


async def _wait_for_box_item_link(*, page, item_name: str, timeout_ms: int):
    end_at = time.time() + (timeout_ms / 1000)
    while time.time() < end_at:
        locator = await _find_box_item_link(page=page, item_name=item_name)
        if locator is not None:
            return locator
        await page.wait_for_timeout(500)
    raise RuntimeError(f"Timed out waiting for Box item link: {item_name}")
