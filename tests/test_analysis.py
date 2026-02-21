"""Tests for lib/analysis.py — show(), properties(), sites()."""

import os
import tempfile

import pandas as pd
import pytest

import megaton_lib.analysis as analysis
from megaton_lib.analysis import show


@pytest.fixture
def sample_df():
    """Sample DataFrame with 30 rows."""
    return pd.DataFrame({
        "month": [f"2025-{i:02d}" for i in range(1, 31)],
        "sessions": list(range(100, 130)),
    })


@pytest.fixture
def small_df():
    """Sample DataFrame with 5 rows."""
    return pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        "clicks": [10, 20, 30, 40, 50],
    })


class TestShow:
    """Tests for show()."""

    def test_limits_rows(self, sample_df, capsys):
        """With default n=20, a 30-row DF shows 20 rows + truncation note."""
        show(sample_df)
        captured = capsys.readouterr().out
        assert "... (10 more rows)" in captured
        assert "[30 rows x 2 cols]" in captured

    def test_custom_n(self, sample_df, capsys):
        """With n=5, a 30-row DF shows 5 rows + truncation note."""
        show(sample_df, n=5)
        captured = capsys.readouterr().out
        assert "... (25 more rows)" in captured
        assert "[30 rows x 2 cols]" in captured

    def test_full_when_small(self, small_df, capsys):
        """If row count <= n, all rows are shown with no truncation."""
        show(small_df)
        captured = capsys.readouterr().out
        assert "more rows" not in captured
        assert "[5 rows x 2 cols]" in captured
        assert "2025-01-05" in captured

    def test_exact_n(self, capsys):
        """If row count equals n, all rows are shown."""
        df = pd.DataFrame({"a": range(20)})
        show(df, n=20)
        captured = capsys.readouterr().out
        assert "more rows" not in captured
        assert "[20 rows x 1 cols]" in captured

    def test_saves_csv(self, sample_df, capsys):
        """CSV file is created when save is specified."""
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
        """Saving persists all rows regardless of n."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            show(sample_df, n=5, save=path)
            saved = pd.read_csv(path)
            assert len(saved) == 30  # display is 5 rows, saved data is full
        finally:
            os.unlink(path)

    def test_empty_df(self, capsys):
        """Empty DataFrame does not raise."""
        df = pd.DataFrame(columns=["a", "b"])
        show(df)
        captured = capsys.readouterr().out
        assert "[0 rows x 2 cols]" in captured

    def test_invalid_n_raises(self, sample_df):
        """n<=0 raises ValueError."""
        with pytest.raises(ValueError):
            show(sample_df, n=0)

    def test_save_creates_parent_dir(self, sample_df):
        """Parent directory is created automatically when missing."""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "result.csv")
            show(sample_df, save=path)
            assert os.path.exists(path)


class TestListHelpers:
    """Tests for properties()/sites()."""

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
