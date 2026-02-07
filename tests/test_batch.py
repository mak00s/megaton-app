"""Tests for lib/batch_runner.py and --batch mode."""

import json
import tempfile
from pathlib import Path

import pytest

from megaton_lib.batch_runner import collect_configs, run_batch


class TestCollectConfigs:
    """collect_configs() のテスト。"""

    def test_directory_with_json_files(self, tmp_path):
        (tmp_path / "02_second.json").write_text("{}")
        (tmp_path / "01_first.json").write_text("{}")
        (tmp_path / "03_third.json").write_text("{}")
        (tmp_path / "readme.txt").write_text("ignore")

        configs = collect_configs(str(tmp_path))
        names = [c.name for c in configs]
        assert names == ["01_first.json", "02_second.json", "03_third.json"]

    def test_single_json_file(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text("{}")
        configs = collect_configs(str(f))
        assert len(configs) == 1
        assert configs[0].name == "config.json"

    def test_nonexistent_path(self):
        with pytest.raises(FileNotFoundError):
            collect_configs("/nonexistent/path")

    def test_empty_directory(self, tmp_path):
        with pytest.raises(ValueError, match="No JSON files"):
            collect_configs(str(tmp_path))

    def test_non_json_file(self, tmp_path):
        f = tmp_path / "config.yaml"
        f.write_text("key: value")
        with pytest.raises(ValueError, match="Not a JSON file"):
            collect_configs(str(f))


class TestRunBatch:
    """run_batch() のテスト。"""

    def _make_config(self, tmp_path, name, data):
        f = tmp_path / name
        f.write_text(json.dumps(data), encoding="utf-8")
        return f

    def _valid_gsc_params(self, **overrides):
        base = {
            "schema_version": "1.0",
            "source": "gsc",
            "site_url": "sc-domain:example.com",
            "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
            "dimensions": ["query"],
        }
        base.update(overrides)
        return base

    def test_all_succeed(self, tmp_path):
        self._make_config(tmp_path, "01.json", self._valid_gsc_params())
        self._make_config(tmp_path, "02.json", self._valid_gsc_params())

        def mock_execute(params, config_path):
            return {"status": "ok", "row_count": 10}

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert result["total"] == 2
        assert result["succeeded"] == 2
        assert result["failed"] == 0
        assert result["skipped"] == 0

    def test_one_fails(self, tmp_path):
        self._make_config(tmp_path, "01.json", self._valid_gsc_params())
        self._make_config(tmp_path, "02.json", self._valid_gsc_params())

        call_count = {"n": 0}

        def mock_execute(params, config_path):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"status": "ok", "row_count": 10}
            raise Exception("API error")

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert result["total"] == 2
        assert result["succeeded"] == 1
        assert result["failed"] == 1
        assert result["skipped"] == 0

    def test_invalid_json_skipped(self, tmp_path):
        (tmp_path / "01_bad.json").write_text("not json")
        self._make_config(tmp_path, "02_good.json", self._valid_gsc_params())

        def mock_execute(params, config_path):
            return {"status": "ok", "row_count": 5}

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert result["total"] == 2
        assert result["succeeded"] == 1
        assert result["skipped"] == 1

    def test_invalid_params_skipped(self, tmp_path):
        # schema_version がない → validation failure
        self._make_config(tmp_path, "01.json", {"source": "gsc"})
        self._make_config(tmp_path, "02.json", self._valid_gsc_params())

        def mock_execute(params, config_path):
            return {"status": "ok", "row_count": 5}

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert result["total"] == 2
        assert result["succeeded"] == 1
        assert result["skipped"] == 1

    def test_progress_callback(self, tmp_path):
        self._make_config(tmp_path, "01.json", self._valid_gsc_params())
        self._make_config(tmp_path, "02.json", self._valid_gsc_params())

        progress_log = []

        def on_progress(config_name, index, total, result):
            progress_log.append((config_name, index, total, result["status"]))

        def mock_execute(params, config_path):
            return {"status": "ok", "row_count": 10}

        run_batch(str(tmp_path), execute_fn=mock_execute, on_progress=on_progress)
        assert len(progress_log) == 2
        assert progress_log[0] == ("01.json", 1, 2, "ok")
        assert progress_log[1] == ("02.json", 2, 2, "ok")

    def test_elapsed_sec_present(self, tmp_path):
        self._make_config(tmp_path, "01.json", self._valid_gsc_params())

        def mock_execute(params, config_path):
            return {"status": "ok"}

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert "elapsed_sec" in result
        assert isinstance(result["elapsed_sec"], float)

    def test_date_template_resolved(self, tmp_path):
        """バッチ内のconfigで日付テンプレートが解決される。"""
        config = self._valid_gsc_params()
        config["date_range"] = {"start": "today-30d", "end": "today-3d"}
        self._make_config(tmp_path, "01.json", config)

        received_params = {}

        def mock_execute(params, config_path):
            received_params.update(params)
            return {"status": "ok"}

        result = run_batch(str(tmp_path), execute_fn=mock_execute)
        assert result["succeeded"] == 1
        # テンプレートが解決されている
        assert received_params["date_range"]["start"] != "today-30d"
        assert len(received_params["date_range"]["start"]) == 10  # YYYY-MM-DD

    def test_execution_order(self, tmp_path):
        """ファイル名のアルファベット順に実行される。"""
        self._make_config(tmp_path, "03_c.json", self._valid_gsc_params())
        self._make_config(tmp_path, "01_a.json", self._valid_gsc_params())
        self._make_config(tmp_path, "02_b.json", self._valid_gsc_params())

        order = []

        def mock_execute(params, config_path):
            order.append(config_path.name)
            return {"status": "ok"}

        run_batch(str(tmp_path), execute_fn=mock_execute)
        assert order == ["01_a.json", "02_b.json", "03_c.json"]
