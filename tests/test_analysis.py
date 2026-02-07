"""Tests for lib/analysis.py — show(), properties(), sites()."""

import os
import tempfile

import pandas as pd
import pytest

import lib.analysis as analysis
from lib.analysis import show


@pytest.fixture
def sample_df():
    """30行のサンプルDataFrame."""
    return pd.DataFrame({
        "month": [f"2025-{i:02d}" for i in range(1, 31)],
        "sessions": list(range(100, 130)),
    })


@pytest.fixture
def small_df():
    """5行のサンプルDataFrame."""
    return pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        "clicks": [10, 20, 30, 40, 50],
    })


class TestShow:
    """show() のテスト."""

    def test_limits_rows(self, sample_df, capsys):
        """デフォルトn=20で30行のDFは20行+省略表示."""
        show(sample_df)
        captured = capsys.readouterr().out
        assert "... (10 more rows)" in captured
        assert "[30 rows x 2 cols]" in captured

    def test_custom_n(self, sample_df, capsys):
        """n=5で30行のDFは5行+省略表示."""
        show(sample_df, n=5)
        captured = capsys.readouterr().out
        assert "... (25 more rows)" in captured
        assert "[30 rows x 2 cols]" in captured

    def test_full_when_small(self, small_df, capsys):
        """行数がn以下なら全行表示、省略なし."""
        show(small_df)
        captured = capsys.readouterr().out
        assert "more rows" not in captured
        assert "[5 rows x 2 cols]" in captured
        assert "2025-01-05" in captured

    def test_exact_n(self, capsys):
        """行数がちょうどnなら全行表示."""
        df = pd.DataFrame({"a": range(20)})
        show(df, n=20)
        captured = capsys.readouterr().out
        assert "more rows" not in captured
        assert "[20 rows x 1 cols]" in captured

    def test_saves_csv(self, sample_df, capsys):
        """save指定時にCSVファイルが作成される."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            show(sample_df, save=path)
            captured = capsys.readouterr().out
            assert f"saved: {path}" in captured

            saved = pd.read_csv(path)
            assert len(saved) == 30
            assert list(saved.columns) == ["month", "sessions"]
        finally:
            os.unlink(path)

    def test_saves_csv_all_rows(self, sample_df):
        """save時はn制限に関係なく全行保存."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            show(sample_df, n=5, save=path)
            saved = pd.read_csv(path)
            assert len(saved) == 30  # 表示は5行だが保存は全行
        finally:
            os.unlink(path)

    def test_empty_df(self, capsys):
        """空のDataFrameでもエラーにならない."""
        df = pd.DataFrame(columns=["a", "b"])
        show(df)
        captured = capsys.readouterr().out
        assert "[0 rows x 2 cols]" in captured

    def test_invalid_n_raises(self, sample_df):
        """n<=0 は ValueError."""
        with pytest.raises(ValueError):
            show(sample_df, n=0)

    def test_save_creates_parent_dir(self, sample_df):
        """save先の親ディレクトリがなければ自動作成する."""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "result.csv")
            show(sample_df, save=path)
            assert os.path.exists(path)


class TestListHelpers:
    """properties()/sites() のテスト."""

    def test_properties_prints_list(self, capsys, monkeypatch):
        monkeypatch.setattr(
            analysis,
            "get_ga4_properties",
            lambda: [
                {"id": "123", "name": "Prop A"},
                {"id": "456", "name": "Prop B"},
            ],
        )
        analysis.properties()
        captured = capsys.readouterr().out
        assert "123" in captured
        assert "Prop A" in captured
        assert "[2 properties]" in captured

    def test_sites_prints_list(self, capsys, monkeypatch):
        monkeypatch.setattr(
            analysis,
            "get_gsc_sites",
            lambda: ["sc-domain:example.com", "https://example.org/"],
        )
        analysis.sites()
        captured = capsys.readouterr().out
        assert "sc-domain:example.com" in captured
        assert "[2 sites]" in captured
