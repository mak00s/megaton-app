"""Tests for GTM sync helpers (_write_json_if_changed, _sync_resource_dir)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from megaton_lib.audit.providers.tag_config.gtm import (
    _resource_id,
    _slugify,
    _sync_resource_dir,
    _write_json_if_changed,
)


# ---------------------------------------------------------------------------
# _write_json_if_changed
# ---------------------------------------------------------------------------


class TestWriteJsonIfChanged:
    def test_added_when_file_missing(self, tmp_path: Path):
        path = tmp_path / "new.json"
        status = _write_json_if_changed(path, {"key": "value"})
        assert status == "added"
        assert path.exists()
        assert json.loads(path.read_text("utf-8")) == {"key": "value"}

    def test_unchanged_when_content_same(self, tmp_path: Path):
        path = tmp_path / "same.json"
        data = {"key": "value"}
        _write_json_if_changed(path, data)
        status = _write_json_if_changed(path, data)
        assert status == "unchanged"

    def test_updated_when_content_differs(self, tmp_path: Path):
        path = tmp_path / "diff.json"
        _write_json_if_changed(path, {"v": 1})
        status = _write_json_if_changed(path, {"v": 2})
        assert status == "updated"
        assert json.loads(path.read_text("utf-8")) == {"v": 2}

    def test_creates_parent_dirs(self, tmp_path: Path):
        path = tmp_path / "sub" / "deep" / "file.json"
        status = _write_json_if_changed(path, {"ok": True})
        assert status == "added"
        assert path.exists()


# ---------------------------------------------------------------------------
# _sync_resource_dir
# ---------------------------------------------------------------------------


def _make_item(rid: str, name: str, **extra) -> dict:
    """Build a minimal GTM-style resource item."""
    item = {"path": f"accounts/1/containers/2/workspaces/3/tags/{rid}", "name": name}
    item.update(extra)
    return item


class TestSyncResourceDir:
    def test_fresh_dir_all_added(self, tmp_path: Path):
        items = [_make_item("10", "Tag A"), _make_item("20", "Tag B")]
        stats = _sync_resource_dir(items, tmp_path / "tags")

        assert stats == {"added": 2, "updated": 0, "deleted": 0, "unchanged": 0}
        assert (tmp_path / "tags" / "index.json").exists()
        assert (tmp_path / "tags" / "10_tag-a.json").exists()
        assert (tmp_path / "tags" / "20_tag-b.json").exists()

    def test_second_run_all_unchanged(self, tmp_path: Path):
        items = [_make_item("10", "Tag A")]
        d = tmp_path / "tags"
        _sync_resource_dir(items, d)
        stats = _sync_resource_dir(items, d)

        assert stats == {"added": 0, "updated": 0, "deleted": 0, "unchanged": 1}

    def test_updated_item(self, tmp_path: Path):
        d = tmp_path / "tags"
        _sync_resource_dir([_make_item("10", "Tag A", type="html")], d)
        stats = _sync_resource_dir([_make_item("10", "Tag A", type="custom_image")], d)

        assert stats["updated"] == 1
        assert stats["unchanged"] == 0
        updated = json.loads((d / "10_tag-a.json").read_text("utf-8"))
        assert updated["type"] == "custom_image"

    def test_deleted_item(self, tmp_path: Path):
        d = tmp_path / "tags"
        _sync_resource_dir([_make_item("10", "A"), _make_item("20", "B")], d)
        stats = _sync_resource_dir([_make_item("10", "A")], d)

        assert stats == {"added": 0, "updated": 0, "deleted": 1, "unchanged": 1}
        assert (d / "10_a.json").exists()
        assert not (d / "20_b.json").exists()

    def test_added_and_deleted_together(self, tmp_path: Path):
        d = tmp_path / "tags"
        _sync_resource_dir([_make_item("10", "Old")], d)
        stats = _sync_resource_dir([_make_item("20", "New")], d)

        assert stats == {"added": 1, "updated": 0, "deleted": 1, "unchanged": 0}
        assert not (d / "10_old.json").exists()
        assert (d / "20_new.json").exists()

    def test_index_only_no_individual_files(self, tmp_path: Path):
        d = tmp_path / "built_in"
        items = [_make_item("10", "Click"), _make_item("20", "URL")]
        stats = _sync_resource_dir(items, d, index_only=True)

        assert stats == {"added": 0, "updated": 0, "deleted": 0, "unchanged": 0}
        assert (d / "index.json").exists()
        index = json.loads((d / "index.json").read_text("utf-8"))
        assert len(index) == 2
        # No individual files
        assert not (d / "10_click.json").exists()

    def test_index_json_content(self, tmp_path: Path):
        d = tmp_path / "tags"
        items = [_make_item("10", "My Tag", type="html")]
        _sync_resource_dir(items, d)

        index = json.loads((d / "index.json").read_text("utf-8"))
        assert index == [{"id": "10", "name": "My Tag", "type": "html"}]

    def test_stale_file_not_in_api_deleted(self, tmp_path: Path):
        """A manually created file should be cleaned up."""
        d = tmp_path / "tags"
        d.mkdir(parents=True)
        (d / "999_stale.json").write_text("{}", encoding="utf-8")

        stats = _sync_resource_dir([_make_item("10", "Real")], d)

        assert stats["added"] == 1
        assert stats["deleted"] == 1
        assert not (d / "999_stale.json").exists()

    def test_empty_items_deletes_all(self, tmp_path: Path):
        d = tmp_path / "tags"
        _sync_resource_dir([_make_item("10", "A"), _make_item("20", "B")], d)
        stats = _sync_resource_dir([], d)

        assert stats == {"added": 0, "updated": 0, "deleted": 2, "unchanged": 0}
        # Only index.json should remain
        remaining = list(d.glob("*.json"))
        assert [f.name for f in remaining] == ["index.json"]


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestSlugify:
    def test_basic(self):
        assert _slugify("My Tag Name") == "my-tag-name"

    def test_special_chars(self):
        assert _slugify("Tag (v2) — updated!") == "tag-v2-updated"

    def test_empty(self):
        assert _slugify("") == "unnamed"

    def test_truncation(self):
        long_name = "a" * 100
        assert len(_slugify(long_name)) == 80


class TestResourceId:
    def test_from_path(self):
        assert _resource_id({"path": "accounts/1/containers/2/workspaces/3/tags/42"}) == "42"

    def test_from_tag_id(self):
        assert _resource_id({"tagId": "99"}) == "99"

    def test_fallback(self):
        assert _resource_id({"path": "simple"}) == "simple"
