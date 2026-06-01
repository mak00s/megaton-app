from __future__ import annotations

import builtins
import importlib
import asyncio
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
    ],
)
def test_normalize_box_shared_link_access_aliases(raw, expected):
    assert box_ui.normalize_box_shared_link_access(raw) == expected


def test_normalize_box_shared_link_access_rejects_unknown():
    with pytest.raises(ValueError, match="Unsupported Box shared link access"):
        box_ui.normalize_box_shared_link_access("private")


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
            "folder_shared_url": "https://app.box.com/s/folder",
            "folder_shared_link_access": "company",
            "folder_shared_link_status": "created",
            "folder_shared_link_error": "",
        }
    ]
    assert [call[0] for call in calls] == ["session", "ready", "subfolder", "upload", "share-folder"]
