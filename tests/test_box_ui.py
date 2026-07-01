from __future__ import annotations

import builtins
import importlib
import asyncio
import inspect
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from megaton_lib import box_ui


def test_box_ui_imports_without_playwright(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("playwright"):
            raise ImportError("No module named 'playwright'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    sys.modules.pop("megaton_lib.box_ui", None)
    try:
        mod = importlib.import_module("megaton_lib.box_ui")
    finally:
        monkeypatch.setattr(builtins, "__import__", real_import)
        sys.modules.pop("megaton_lib.box_ui", None)
        importlib.import_module("megaton_lib.box_ui")

    assert hasattr(mod, "download_from_box")
    assert hasattr(mod, "upload_file_to_box_folder_via_ui")


def test_build_box_login_url_preserves_redirect_query():
    target = "https://app.box.com/folder/123?sortColumn=name&sortDirection=ASC"

    login_url = box_ui._build_box_login_url(target)

    parsed = urlparse(login_url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "account.box.com"
    assert parsed.path == "/login"
    assert parse_qs(parsed.query) == {"redirect_url": [target]}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("anyone", "open"),
        ("anyone-with-link", "open"),
        ("public", "open"),
        ("company", "company"),
        ("account holders", "company"),
        ("org", "company"),
        ("invited people only", "invited"),
        ("people with access", "invited"),
    ],
)
def test_normalize_box_shared_link_access_aliases(raw, expected):
    assert box_ui.normalize_box_shared_link_access(raw) == expected


def test_normalize_box_shared_link_access_rejects_unknown():
    with pytest.raises(ValueError, match="Unsupported Box shared link access"):
        box_ui.normalize_box_shared_link_access("private")


def test_box_upload_shared_link_access_defaults_to_invited():
    assert inspect.signature(box_ui.upload_file_to_box_folder_via_ui).parameters[
        "shared_link_access"
    ].default == "invited"
    assert inspect.signature(box_ui.upload_files_to_box_folder_via_ui).parameters[
        "shared_link_access"
    ].default == "invited"


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("/file/123", "https://app.box.com/file/123"),
        ("file/123", "https://app.box.com/file/123"),
        ("https://app.box.com/file/123", "https://app.box.com/file/123"),
        ("", ""),
    ],
)
def test_normalize_box_item_web_url(href, expected):
    assert box_ui._normalize_box_item_web_url(href) == expected


def test_company_access_patterns_match_enterprise_specific_label():
    patterns = box_ui.BOX_SHARED_LINK_ACCESS_PATTERNS["company"]

    assert any(
        re.search(pattern, "People in Shiseido with the link", re.I)
        for pattern in patterns
    )
    assert any(
        re.search(pattern, "Boxアカウント保持者", re.I)
        for pattern in patterns
    )
    assert any(
        re.search(pattern, "People at Shiseido with the link", re.I)
        for pattern in patterns
    )
    assert any(
        re.search(pattern, "リンクを知っている会社のユーザー", re.I)
        for pattern in patterns
    )


def test_invited_access_patterns_match_box_labels():
    patterns = box_ui.BOX_SHARED_LINK_ACCESS_PATTERNS["invited"]

    assert any(
        re.search(pattern, "Invited people only", re.I)
        for pattern in patterns
    )
    assert any(
        re.search(pattern, "People with access", re.I)
        for pattern in patterns
    )


def test_sanitize_box_debug_text_redacts_emails_and_urls():
    text = box_ui._sanitize_box_debug_text(
        "Share with user@example.com https://app.box.com/s/token"
    )

    assert text == "Share with [email] [url]"


def test_box_text_implies_invited_shared_link_from_invite_only_controls():
    assert box_ui._box_text_implies_invited_shared_link(
        "",
        ["Add names or email addresses", "Shared link / shared-link", "Close"],
    )


def test_box_text_implies_invited_shared_link_rejects_broader_access():
    assert not box_ui._box_text_implies_invited_shared_link(
        "People with the link",
        ["Add names or email addresses", "Shared link / shared-link"],
    )
    assert not box_ui._box_text_implies_invited_shared_link(
        "People in Shiseido with the link",
        ["Add names or email addresses", "Shared link / shared-link"],
    )


class _FakeComboLocator:
    """Minimal locator stub: get_by_role(...).first.count() -> 0."""

    def __init__(self, count: int = 0):
        self._count = count

    def get_by_role(self, *args, **kwargs):
        return self

    @property
    def first(self):
        return self

    async def count(self):
        return self._count


def test_set_box_shared_link_access_returns_false_without_raising(monkeypatch):
    # Simulate a Box dialog where no access control matches (UI wording drift):
    # the function must warn and return False, NOT raise, so the caller can
    # still read the already-enabled link URL.
    async def _false(*args, **kwargs):
        return False

    async def _empty(*args, **kwargs):
        return []

    monkeypatch.setattr(box_ui, "_box_access_text_visible", _false)
    monkeypatch.setattr(box_ui, "_click_box_access_option", _false)
    monkeypatch.setattr(box_ui, "_box_dialog_implies_invited_shared_link", _false)
    monkeypatch.setattr(box_ui, "_box_shared_link_access_menu_openers", _empty)
    monkeypatch.setattr(box_ui, "_box_dialog_control_labels", _empty)

    result = asyncio.run(
        box_ui._set_box_shared_link_access(
            page=object(), dialog=_FakeComboLocator(count=0), access="invited", timeout_ms=1_000
        )
    )
    assert result is False


def test_set_box_shared_link_access_returns_true_when_confirmed(monkeypatch):
    async def _true(*args, **kwargs):
        return True

    monkeypatch.setattr(box_ui, "_box_access_text_visible", _true)

    result = asyncio.run(
        box_ui._set_box_shared_link_access(
            page=object(), dialog=object(), access="invited", timeout_ms=1_000
        )
    )
    assert result is True


class _FakeGrantContext:
    async def grant_permissions(self, *args, **kwargs):
        return None


class _FakeBoxPage:
    def __init__(self):
        self.context = _FakeGrantContext()
        self.url = ""

    def locator(self, *args, **kwargs):
        return _FakeComboLocator()


def _patch_folder_share_helpers(monkeypatch, *, access_confirmed, shared_url=""):
    async def _noop(*args, **kwargs):
        return None

    async def _dialog(*args, **kwargs):
        return object()

    async def _set_access(*args, **kwargs):
        return access_confirmed

    async def _read(*args, **kwargs):
        return shared_url

    monkeypatch.setattr(box_ui, "_click_box_share_button", _noop)
    monkeypatch.setattr(box_ui, "_box_active_dialog", _dialog)
    monkeypatch.setattr(box_ui, "_ensure_box_shared_link_enabled", _noop)
    monkeypatch.setattr(box_ui, "_set_box_shared_link_access", _set_access)
    monkeypatch.setattr(box_ui, "_read_box_shared_link_from_dialog", _read)


def test_folder_shared_link_raises_when_broad_access_unconfirmed(monkeypatch):
    # company/open must NOT be returned as a success when the access could not be
    # confirmed: the link may still be the "invited" default and unusable by the
    # recipient. Fail loudly instead.
    for access in ("company", "open"):
        _patch_folder_share_helpers(
            monkeypatch, access_confirmed=False, shared_url="https://app.box.com/s/should-not-return"
        )
        with pytest.raises(RuntimeError, match="mis-scoped"):
            asyncio.run(
                box_ui._create_or_get_current_box_folder_shared_link(
                    page=_FakeBoxPage(), access=access, timeout_ms=1_000
                )
            )


def test_folder_shared_link_continues_for_invited_when_unconfirmed(monkeypatch):
    # invited is Box's default; failing to positively re-confirm it must still
    # return the (already enabled) link URL.
    _patch_folder_share_helpers(
        monkeypatch, access_confirmed=False, shared_url="https://app.box.com/s/inv123"
    )
    url = asyncio.run(
        box_ui._create_or_get_current_box_folder_shared_link(
            page=_FakeBoxPage(), access="invited", timeout_ms=1_000
        )
    )
    assert url == "https://app.box.com/s/inv123"


def test_upload_file_to_box_folder_via_ui_sync_forwards_kwargs(monkeypatch, tmp_path):
    captured = {}

    async def fake_upload(**kwargs):
        captured.update(kwargs)
        return {"uploaded_file_name": Path(kwargs["file_path"]).name}

    monkeypatch.setattr(box_ui, "upload_file_to_box_folder_via_ui", fake_upload)
    file_path = tmp_path / "report.pdf"
    file_path.write_text("x", encoding="utf-8")

    result = box_ui.upload_file_to_box_folder_via_ui_sync(
        parent_folder_url="https://app.box.com/folder/123",
        file_path=file_path,
        login="user@example.test",
        password="secret",
        create_shared_link=True,
    )

    assert result == {"uploaded_file_name": "report.pdf"}
    assert captured == {
        "parent_folder_url": "https://app.box.com/folder/123",
        "file_path": file_path,
        "login": "user@example.test",
        "password": "secret",
        "create_shared_link": True,
    }


def test_upload_files_to_box_folder_via_ui_sync_forwards_kwargs(monkeypatch, tmp_path):
    captured = {}

    async def fake_upload(**kwargs):
        captured.update(kwargs)
        return [{"uploaded_file_name": Path(kwargs["file_paths"][0]).name}]

    monkeypatch.setattr(box_ui, "upload_files_to_box_folder_via_ui", fake_upload)
    file_path = tmp_path / "report.pdf"
    file_path.write_text("x", encoding="utf-8")

    result = box_ui.upload_files_to_box_folder_via_ui_sync(
        parent_folder_url="https://app.box.com/folder/123",
        target_subfolder_name="202605",
        file_paths=[file_path],
        login="user@example.test",
        password="secret",
        create_shared_link=True,
    )

    assert result == [{"uploaded_file_name": "report.pdf"}]
    assert captured == {
        "parent_folder_url": "https://app.box.com/folder/123",
        "target_subfolder_name": "202605",
        "file_paths": [file_path],
        "login": "user@example.test",
        "password": "secret",
        "create_shared_link": True,
    }


def test_download_from_box_downloads_current_file(monkeypatch, tmp_path):
    calls = []

    class FakePage:
        url = "https://app.box.com/file/123"

    class FakeContext:
        pass

    @asynccontextmanager
    async def fake_open_box_session(**kwargs):
        calls.append(("session", kwargs))
        yield FakeContext(), FakePage()

    async def fake_download_current_box_item(**kwargs):
        calls.append(("download", kwargs))
        output = tmp_path / "downloaded.pdf"
        output.write_text("x", encoding="utf-8")
        return output

    monkeypatch.setattr(box_ui, "_open_box_session", fake_open_box_session)
    monkeypatch.setattr(box_ui, "_download_current_box_item", fake_download_current_box_item)

    result = asyncio.run(
        box_ui.download_from_box(
            url="https://app.box.com/file/123",
            login="user@example.test",
            password="secret",
            download_dir=tmp_path,
            headless=True,
        )
    )

    assert result == tmp_path / "downloaded.pdf"
    assert calls[0][0] == "session"
    assert calls[0][1]["url"] == "https://app.box.com/file/123"
    assert calls[0][1]["headless"] is True
    assert calls[1][0] == "download"
    assert calls[1][1]["download_dir"] == tmp_path


def test_upload_file_to_box_folder_via_ui_returns_result_shape(monkeypatch, tmp_path):
    calls = []

    class FakePage:
        pass

    class FakeContext:
        pass

    @asynccontextmanager
    async def fake_open_box_session(**kwargs):
        calls.append(("session", kwargs))
        yield FakeContext(), FakePage()

    async def fake_ensure_box_folder_page_ready(**kwargs):
        calls.append(("ready", kwargs))

    async def fake_upload_file_to_current_box_folder(**kwargs):
        calls.append(("upload", kwargs))

    monkeypatch.setattr(box_ui, "_open_box_session", fake_open_box_session)
    monkeypatch.setattr(box_ui, "_ensure_box_folder_page_ready", fake_ensure_box_folder_page_ready)
    monkeypatch.setattr(box_ui, "_upload_file_to_current_box_folder", fake_upload_file_to_current_box_folder)
    file_path = tmp_path / "report.pdf"
    file_path.write_text("x", encoding="utf-8")

    result = asyncio.run(
        box_ui.upload_file_to_box_folder_via_ui(
            parent_folder_url="https://app.box.com/folder/123",
            file_path=file_path,
            login="user@example.test",
            password="secret",
            headless=True,
        )
    )

    assert result == {
        "uploaded_file_name": "report.pdf",
        "target_subfolder_name": "",
        "folder_created": False,
        "upload_mode": "playwright-ui",
        "output_dir": str(tmp_path),
        "shared_url": "",
        "shared_link_access": "",
        "shared_link_status": "skipped",
        "shared_link_error": "",
    }
    assert [call[0] for call in calls] == ["session", "ready", "upload"]
    assert calls[0][1]["url"] == "https://app.box.com/folder/123"
    assert calls[0][1]["headless"] is True
    assert calls[2][1]["file_path"] == file_path.resolve()


def test_upload_files_to_box_folder_via_ui_adds_folder_shared_link(monkeypatch, tmp_path):
    calls = []

    class FakePage:
        pass

    class FakeContext:
        pass

    @asynccontextmanager
    async def fake_open_box_session(**kwargs):
        calls.append(("session", kwargs))
        yield FakeContext(), FakePage()

    async def fake_ensure_box_folder_page_ready(**kwargs):
        calls.append(("ready", kwargs))

    async def fake_open_or_create_box_subfolder(**kwargs):
        calls.append(("subfolder", kwargs))
        return False, "202605"

    async def fake_upload_file_to_current_box_folder(**kwargs):
        calls.append(("upload", kwargs))

    async def fake_create_or_get_current_box_folder_shared_link(**kwargs):
        calls.append(("share-folder", kwargs))
        return "https://app.box.com/s/folder"

    monkeypatch.setattr(box_ui, "_open_box_session", fake_open_box_session)
    monkeypatch.setattr(box_ui, "_ensure_box_folder_page_ready", fake_ensure_box_folder_page_ready)
    monkeypatch.setattr(box_ui, "_open_or_create_box_subfolder", fake_open_or_create_box_subfolder)
    monkeypatch.setattr(box_ui, "_upload_file_to_current_box_folder", fake_upload_file_to_current_box_folder)
    monkeypatch.setattr(
        box_ui,
        "_create_or_get_current_box_folder_shared_link",
        fake_create_or_get_current_box_folder_shared_link,
    )
    file_path = tmp_path / "report.pdf"
    file_path.write_text("x", encoding="utf-8")

    result = asyncio.run(
        box_ui.upload_files_to_box_folder_via_ui(
            parent_folder_url="https://app.box.com/folder/123",
            target_subfolder_name="202605",
            file_paths=[file_path],
            login="user@example.test",
            password="secret",
            headless=True,
            create_shared_link=True,
            shared_link_access="company",
            shared_link_target="folder",
        )
    )

    assert result == [
        {
            "uploaded_file_name": "report.pdf",
            "target_subfolder_name": "202605",
            "nested_subfolder_name": "",
            "folder_created": False,
            "nested_folder_created": False,
            "upload_mode": "playwright-ui",
            "output_dir": str(tmp_path),
            "web_url": "",
            "folder_shared_url": "https://app.box.com/s/folder",
            "folder_shared_link_access": "company",
            "folder_shared_link_status": "created",
            "folder_shared_link_error": "",
        }
    ]
    assert [call[0] for call in calls] == ["session", "ready", "subfolder", "upload", "share-folder"]
