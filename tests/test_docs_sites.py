from __future__ import annotations

import pytest

from megaton_lib.docs_sites import (
    format_md_value,
    render_markdown_table,
    replace_generated_section,
    update_generated_markdown,
)


def test_format_md_value_scalars():
    assert format_md_value(None) == "—"
    assert format_md_value(True) == "Yes"
    assert format_md_value(False) == "No"
    assert format_md_value(42) == "42"
    assert format_md_value("  spaced  ") == "spaced"
    assert format_md_value("") == "—"


def test_format_md_value_multiline_becomes_br():
    assert format_md_value("line1\nline2") == "line1<br>line2"


def test_format_md_value_list_joins_and_drops_empties():
    assert format_md_value(["a", "", None, "b"]) == "a<br>b"
    assert format_md_value([]) == "—"
    assert format_md_value([None, ""]) == "—"


def test_format_md_value_dict_renders_key_values():
    assert format_md_value({"k": "v", "n": 3}) == "`k`=v<br>`n`=3"
    assert format_md_value({}) == "—"


def test_render_markdown_table_shape():
    table = render_markdown_table(["A", "B"], [["1", "2"], [None, True]])
    lines = table.split("\n")
    assert lines[0] == "| A | B |"
    assert lines[1] == "| --- | --- |"
    assert lines[2] == "| 1 | 2 |"
    assert lines[3] == "| — | Yes |"


def test_render_markdown_table_no_rows():
    table = render_markdown_table(["A"], [])
    assert table == "| A |\n| --- |"


def test_replace_generated_section_replaces_only_inner_block():
    text = (
        "intro\n"
        "<!-- BEGIN GENERATED: sites -->\n"
        "old content\n"
        "<!-- END GENERATED: sites -->\n"
        "outro\n"
    )
    result = replace_generated_section(text, "sites", "new content")
    assert "old content" not in result
    assert "new content" in result
    assert result.startswith("intro\n")
    assert result.endswith("outro\n")
    # markers preserved
    assert "<!-- BEGIN GENERATED: sites -->" in result
    assert "<!-- END GENERATED: sites -->" in result


def test_replace_generated_section_missing_markers_raises():
    with pytest.raises(ValueError, match="Missing generated markers"):
        replace_generated_section("no markers here", "sites", "x")


def test_update_generated_markdown_writes_all_sections(tmp_path):
    path = tmp_path / "doc.md"
    path.write_text(
        "# Doc\n"
        "<!-- BEGIN GENERATED: one -->\n"
        "old1\n"
        "<!-- END GENERATED: one -->\n"
        "mid\n"
        "<!-- BEGIN GENERATED: two -->\n"
        "old2\n"
        "<!-- END GENERATED: two -->\n",
        encoding="utf-8",
    )
    update_generated_markdown(path, {"one": "new1", "two": "new2"})
    out = path.read_text(encoding="utf-8")
    assert "new1" in out and "new2" in out
    assert "old1" not in out and "old2" not in out


def test_update_generated_markdown_raises_on_missing_section(tmp_path):
    path = tmp_path / "doc.md"
    path.write_text(
        "<!-- BEGIN GENERATED: one -->\nx\n<!-- END GENERATED: one -->\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        update_generated_markdown(path, {"missing": "y"})
