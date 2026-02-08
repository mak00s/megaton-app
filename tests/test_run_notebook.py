"""tests for scripts/run_notebook.py"""

from __future__ import annotations

import os
import textwrap
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.run_notebook import (
    extract_cells,
    inject_params,
    _format_value,
    _parse_param_pairs,
    parse_args,
    run,
)


# ===== extract_cells =====

SIMPLE_NB = textwrap.dedent("""\
    # ---
    # jupyter:
    #   jupytext:
    #     text_representation:
    #       format_name: percent
    # ---

    # %% [markdown]
    # # Title

    # %% tags=["parameters"]
    X = 1
    Y = "hello"

    # %%
    print(X, Y)
""")


class TestExtractCells:
    def test_skips_yaml_header(self):
        cells = extract_cells(SIMPLE_NB)
        # ヘッダーはセルに含まれない
        for c in cells:
            assert "jupytext" not in c["source"]

    def test_cell_count(self):
        cells = extract_cells(SIMPLE_NB)
        # ヘッダー後の空行セル + markdown + params + code
        code_cells = [c for c in cells if c["marker"] and "markdown" not in c["marker"]]
        assert len(code_cells) == 2  # params + code

    def test_params_tag_detected(self):
        cells = extract_cells(SIMPLE_NB)
        params_cells = [c for c in cells if c["is_params"]]
        assert len(params_cells) == 1
        assert "X = 1" in params_cells[0]["source"]

    def test_markdown_cell(self):
        cells = extract_cells(SIMPLE_NB)
        md_cells = [c for c in cells if "markdown" in c["marker"]]
        assert len(md_cells) == 1

    def test_no_header(self):
        """ヘッダーなしのノートブックも処理できる。"""
        src = "# %% tags=[\"parameters\"]\nA = 1\n\n# %%\nprint(A)\n"
        cells = extract_cells(src)
        assert len(cells) == 2
        assert cells[0]["is_params"] is True


# ===== inject_params =====

class TestInjectParams:
    def _make_params_cell(self, source: str) -> list[dict]:
        return [
            {"marker": '# %% tags=["parameters"]', "source": source, "is_params": True},
            {"marker": "# %%", "source": "print(X)\n", "is_params": False},
        ]

    def test_string_override(self):
        cells = self._make_params_cell('X = "old"\nY = 42\n')
        result = inject_params(cells, {"X": "new"})
        assert "X = 'new'" in result[0]["source"]
        assert "Y = 42" in result[0]["source"]  # 未変更

    def test_int_override(self):
        cells = self._make_params_cell("COUNT = 10\n")
        result = inject_params(cells, {"COUNT": "99"})
        assert "COUNT = 99" in result[0]["source"]

    def test_float_override(self):
        cells = self._make_params_cell("RATE = 0.5\n")
        result = inject_params(cells, {"RATE": "1.5"})
        assert "RATE = 1.5" in result[0]["source"]

    def test_date_template_resolved(self):
        cells = self._make_params_cell('START = "2025-01-01"\n')
        with patch("megaton_lib.date_template.resolve_date", return_value="2025-12-25"):
            result = inject_params(cells, {"START": "today"})
        assert "START = '2025-12-25'" in result[0]["source"]

    def test_unknown_key_ignored(self):
        cells = self._make_params_cell('X = "a"\n')
        result = inject_params(cells, {"NONEXISTENT": "value"})
        assert result[0]["source"] == 'X = "a"\n'

    def test_empty_overrides(self):
        cells = self._make_params_cell('X = "a"\n')
        result = inject_params(cells, {})
        assert result[0]["source"] == 'X = "a"\n'

    def test_non_params_cell_untouched(self):
        cells = self._make_params_cell('X = "a"\n')
        result = inject_params(cells, {"X": "b"})
        assert result[1]["source"] == "print(X)\n"  # code cell unchanged

    def test_comment_lines_preserved(self):
        src = '# === 設定 ===\nX = "old"\n# コメント\nY = 1\n'
        cells = self._make_params_cell(src)
        result = inject_params(cells, {"X": "new"})
        assert "# === 設定 ===" in result[0]["source"]
        assert "# コメント" in result[0]["source"]


# ===== _format_value =====

class TestFormatValue:
    def test_int(self):
        assert _format_value("42") == "42"

    def test_float(self):
        assert _format_value("3.14") == "3.14"

    def test_string(self):
        assert _format_value("hello") == "'hello'"

    def test_date_string(self):
        assert _format_value("2025-01-01") == "'2025-01-01'"

    def test_url(self):
        assert _format_value("https://example.com/") == "'https://example.com/'"

    def test_string_escaped_safely(self):
        assert _format_value('a"b\\c') == '\'a"b\\\\c\''


# ===== _parse_param_pairs =====

class TestParseParamPairs:
    def test_basic(self):
        assert _parse_param_pairs(["K=V"]) == {"K": "V"}

    def test_multiple(self):
        result = _parse_param_pairs(["A=1", "B=hello"])
        assert result == {"A": "1", "B": "hello"}

    def test_value_with_equals(self):
        """値に = を含む場合（URL等）。"""
        result = _parse_param_pairs(["URL=https://x.com?a=1"])
        assert result == {"URL": "https://x.com?a=1"}

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="KEY=VALUE"):
            _parse_param_pairs(["NOEQ"])


# ===== parse_args =====

class TestParseArgs:
    def test_notebook_only(self):
        args = parse_args(["nb.py"])
        assert args.notebook == "nb.py"
        assert args.param == []

    def test_with_params(self):
        args = parse_args(["nb.py", "-p", "A=1", "-p", "B=x"])
        assert args.param == ["A=1", "B=x"]


# ===== run (E2E) =====

class TestRun:
    def test_e2e_simple(self, tmp_path):
        """簡易ノートブックを作成して実行、出力ファイルを検証。"""
        nb = tmp_path / "test_nb.py"
        out = tmp_path / "out.txt"
        nb.write_text(textwrap.dedent(f"""\
            # ---
            # jupyter:
            #   jupytext:
            #     text_representation:
            #       format_name: percent
            # ---

            # %% tags=["parameters"]
            MSG = "default"
            COUNT = 3

            # %%
            from pathlib import Path
            Path("{out}").write_text(f"{{MSG}} x{{COUNT}}")
        """))
        run(str(nb), {})
        assert out.read_text() == "default x3"

    def test_e2e_with_overrides(self, tmp_path):
        nb = tmp_path / "test_nb.py"
        out = tmp_path / "out.txt"
        nb.write_text(textwrap.dedent(f"""\
            # %% tags=["parameters"]
            MSG = "default"
            COUNT = 3

            # %%
            from pathlib import Path
            Path("{out}").write_text(f"{{MSG}} x{{COUNT}}")
        """))
        run(str(nb), {"MSG": "overridden", "COUNT": "7"})
        assert out.read_text() == "overridden x7"

    def test_e2e_markdown_skipped(self, tmp_path):
        """markdown セルのコードは実行されない。"""
        nb = tmp_path / "test_nb.py"
        out = tmp_path / "out.txt"
        nb.write_text(textwrap.dedent(f"""\
            # %% [markdown]
            # raise RuntimeError("this should not execute")

            # %% tags=["parameters"]
            X = 1

            # %%
            from pathlib import Path
            Path("{out}").write_text(str(X))
        """))
        run(str(nb), {})
        assert out.read_text() == "1"

    def test_cwd_restored(self, tmp_path):
        """実行後に CWD が元に戻る。"""
        nb = tmp_path / "sub" / "test_nb.py"
        nb.parent.mkdir()
        nb.write_text('# %% tags=["parameters"]\nX = 1\n\n# %%\npass\n')
        original = os.getcwd()
        run(str(nb), {})
        assert os.getcwd() == original

    def test_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="Notebook not found"):
            run("/nonexistent/nb.py", {})

    def test_file_available_in_exec_context(self, tmp_path):
        nb = tmp_path / "test_nb.py"
        out = tmp_path / "out.txt"
        nb.write_text(textwrap.dedent(f"""\
            # %% tags=["parameters"]
            X = 1

            # %%
            from pathlib import Path
            Path("{out}").write_text(Path(__file__).name)
        """))
        run(str(nb), {})
        assert out.read_text() == "test_nb.py"
