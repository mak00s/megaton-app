import io
import json
import signal
import sys
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import scripts.query as query_cli
from megaton_lib.job_manager import JobStore


def _args(**kwargs):
    base = {
        "json": True,
        "output": None,
        "transform": None,
        "where": None,
        "sort": None,
        "columns": None,
        "group_by": None,
        "aggregate": None,
        "head": None,
        "summary": False,
        "params": "input/params.json",
        "inline": None,
        "submit": False,
        "status": None,
        "cancel": None,
        "result": None,
        "list_jobs": False,
        "job_limit": 20,
        "run_job": None,
        "list_ga4_properties": False,
        "list_gsc_sites": False,
        "list_bq_datasets": False,
        "project": None,
        "batch": None,
    }
    base.update(kwargs)
    return Namespace(**base)


class TestQueryCoverageGateMeaningful(unittest.TestCase):
    def setUp(self):
        self._sites_cache = query_cli._sites_cache

    def tearDown(self):
        query_cli._sites_cache = self._sites_cache

    def test_collect_messages_and_emit_warnings_non_json(self):
        messages = query_cli._collect_messages("warn\n\nwarn\nx\n", "x\n")
        self.assertEqual(messages, ["warn", "x"])

        err = io.StringIO()
        with redirect_stderr(err):
            query_cli.emit_warnings(_args(json=False), ["a", "b"])
        self.assertIn("[warn] a", err.getvalue())
        self.assertIn("[warn] b", err.getvalue())

    def test_load_sites_and_alias_resolution_paths(self):
        query_cli._sites_cache = None
        query_cli._site_aliases.clear_cache()
        loaded = query_cli._load_sites()
        self.assertIsInstance(loaded, dict)

        query_cli._sites_cache = None
        query_cli._site_aliases.clear_cache()
        with patch("scripts.query._site_aliases.load_sites", return_value={}):
            self.assertEqual(query_cli._load_sites(), {})

        query_cli._sites_cache = {"corp": {"gsc_site_url": "https://corp.example/", "ga4_property_id": "123"}}
        with patch("scripts.query._site_aliases.load_sites", return_value=query_cli._sites_cache):
            got_gsc = query_cli.resolve_site_alias({"source": "gsc", "site": "corp", "schema_version": "1.0"})
            self.assertEqual(got_gsc["site_url"], "https://corp.example/")
            self.assertNotIn("site", got_gsc)

        with patch("scripts.query._site_aliases.load_sites", return_value=query_cli._sites_cache):
            got_ga4 = query_cli.resolve_site_alias({"source": "ga4", "site": "corp", "schema_version": "1.0"})
            self.assertEqual(got_ga4["property_id"], "123")

        with patch("scripts.query._site_aliases.load_sites", return_value=query_cli._sites_cache):
            with self.assertRaises(ValueError):
                query_cli.resolve_site_alias({"source": "ga4", "site": "unknown"})

        query_cli._sites_cache = {}
        params, err = query_cli._validate_raw({"schema_version": "1.0", "source": "ga4", "site": "unknown"})
        self.assertIsNone(params)
        self.assertEqual(err["error_code"], "INVALID_SITE_ALIAS")

        params, err = query_cli.load_params_from_json("{bad json")
        self.assertIsNone(params)
        self.assertEqual(err["error_code"], "INVALID_JSON")

    def test_load_params_from_args_inline(self):
        with patch.object(query_cli, "load_params_from_json", return_value=({"ok": 1}, None)) as mock_inline:
            params, err = query_cli._load_params_from_args(_args(inline='{"schema_version":"1.0"}', params="x.json"))
        self.assertEqual(params, {"ok": 1})
        self.assertIsNone(err)
        mock_inline.assert_called_once()

    def test_output_result_includes_warnings_in_json_payload(self):
        df = pd.DataFrame([{"a": 1}])
        with tempfile.TemporaryDirectory() as tmp:
            out_path = str(Path(tmp) / "saved.csv")
            out = io.StringIO()
            with redirect_stdout(out):
                query_cli.output_result(df, _args(json=True, output=out_path), warnings=["captured warning"])
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["warnings"], ["captured warning"])

        out = io.StringIO()
        with redirect_stdout(out):
            query_cli.output_result(df, _args(json=True, output=None), warnings=["captured warning"])
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["data"]["warnings"], ["captured warning"])

    def test_cancel_job_missing_and_process_lookup_break(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.cancel_job("missing_job", _args(json=True), store)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "JOB_NOT_FOUND")

            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="running", runner_pid=9876)
            with patch("scripts.query.os.killpg") as killpg, patch(
                "scripts.query.os.kill", side_effect=ProcessLookupError
            ), patch("scripts.query.time.time", side_effect=[0.0, 0.5]):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.cancel_job(job["job_id"], _args(json=True), store)
                self.assertEqual(code, 0)
                payload = json.loads(out.getvalue())
                self.assertEqual(payload["data"]["terminate_status"], "terminated")
                killpg.assert_called_once_with(9876, signal.SIGTERM)

    def test_show_job_result_non_json_empty_branches(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            artifact = store.artifact_path(job["job_id"])
            pd.DataFrame([{"page": "/a", "clicks": 1}]).to_csv(artifact, index=False, encoding="utf-8-sig")
            store.update_job(job["job_id"], status="succeeded", artifact_path=str(artifact), row_count=1)

            with tempfile.TemporaryDirectory() as out_tmp:
                output_path = str(Path(out_tmp) / "pipeline.csv")
                out = io.StringIO()
                with patch("scripts.query.apply_pipeline", return_value=pd.DataFrame()):
                    with redirect_stdout(out):
                        code = query_cli.show_job_result(
                            job["job_id"],
                            _args(json=False, output=output_path, sort="clicks DESC"),
                            store,
                        )
                self.assertEqual(code, 0)
                text = out.getvalue()
                self.assertIn("(no rows)", text)
                self.assertIn(f"saved_to: {output_path}", text)

            job2 = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            artifact2 = store.artifact_path(job2["job_id"])
            pd.DataFrame(columns=["a"]).to_csv(artifact2, index=False, encoding="utf-8-sig")
            store.update_job(job2["job_id"], status="succeeded", artifact_path=str(artifact2), row_count=0)

            with tempfile.TemporaryDirectory() as out_tmp:
                copy_to = str(Path(out_tmp) / "copy.csv")
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(
                        job2["job_id"],
                        _args(json=False, output=copy_to, head=1),
                        store,
                    )
                self.assertEqual(code, 0)
                text = out.getvalue()
                self.assertIn(f"copied_to: {copy_to}", text)
                self.assertIn("(no rows)", text)

    def test_run_list_mode_warning_and_error_paths(self):
        cee = query_cli.CapturedExecutionError(RuntimeError("boom"), ["w"])

        with patch("scripts.query.capture_stdio", side_effect=cee):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(_args(json=True, list_ga4_properties=True))
            self.assertTrue(handled)
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "LIST_OPERATION_FAILED")
            self.assertEqual(payload["details"]["warnings"], ["w"])

        with patch("scripts.query.capture_stdio", side_effect=RuntimeError("x")):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(_args(json=True, list_ga4_properties=True))
            self.assertTrue(handled)
            self.assertEqual(code, 1)

        with patch("scripts.query.capture_stdio", return_value=([{"display": "p1"}], ["warn1"])):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(_args(json=True, list_ga4_properties=True))
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["warnings"], ["warn1"])

        with patch("scripts.query.capture_stdio", side_effect=RuntimeError("x")):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(_args(json=True, list_gsc_sites=True))
            self.assertTrue(handled)
            self.assertEqual(code, 1)

        with patch("scripts.query.capture_stdio", return_value=(["sc-domain:example.com"], ["warn2"])):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(_args(json=True, list_gsc_sites=True))
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["warnings"], ["warn2"])

        with patch("scripts.query.capture_stdio", side_effect=RuntimeError("x")):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(
                    _args(json=True, list_bq_datasets=True, project="project-x")
                )
            self.assertTrue(handled)
            self.assertEqual(code, 1)

        with patch("scripts.query.capture_stdio", return_value=(["ds1"], ["warn3"])):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(
                    _args(json=True, list_bq_datasets=True, project="project-x")
                )
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["warnings"], ["warn3"])

    def test_execute_single_config_batch_paths(self):
        params = {"source": "ga4"}
        config_path = Path("dummy.json")

        with patch(
            "scripts.query.capture_stdio",
            side_effect=query_cli.CapturedExecutionError(RuntimeError("query fail"), ["warn-x"]),
        ):
            result = query_cli._execute_single_config(params, config_path)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "QUERY_EXECUTION_FAILED")
        self.assertEqual(result["warnings"], ["warn-x"])

        with patch("scripts.query.capture_stdio", side_effect=RuntimeError("query fail")):
            result = query_cli._execute_single_config(params, config_path)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "QUERY_EXECUTION_FAILED")

        with patch("scripts.query.capture_stdio", return_value=((pd.DataFrame(), []), ["warn-empty"])):
            result = query_cli._execute_single_config(params, config_path)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "NO_DATA_RETURNED")
        self.assertEqual(result["warnings"], ["warn-empty"])

        df = pd.DataFrame([{"a": 1}])
        with patch("scripts.query.capture_stdio", return_value=((df, []), [])), patch(
            "scripts.query.apply_pipeline", side_effect=ValueError("Invalid sort: bad")
        ):
            result = query_cli._execute_single_config({"source": "ga4", "pipeline": {"sort": "bad"}}, config_path)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "INVALID_SORT")

        with patch("scripts.query.capture_stdio", return_value=((df, []), [])), patch(
            "scripts.query.apply_pipeline", side_effect=RuntimeError("pipeline fail")
        ):
            result = query_cli._execute_single_config({"source": "ga4", "pipeline": {"sort": "a DESC"}}, config_path)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "PIPELINE_FAILED")

        with patch("scripts.query.capture_stdio", return_value=((df, []), [])), patch(
            "scripts.query.execute_save", side_effect=RuntimeError("save fail")
        ):
            result = query_cli._execute_single_config(
                {"source": "ga4", "save": {"to": "csv", "path": "out.csv"}}, config_path
            )
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_code"], "SAVE_FAILED")

        with patch("scripts.query.capture_stdio", return_value=((df, []), ["warn-ok"])), patch(
            "scripts.query.execute_save", return_value={"saved_to": "out.csv"}
        ):
            result = query_cli._execute_single_config(
                {"source": "ga4", "save": {"to": "csv", "path": "out.csv"}}, config_path
            )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(result["warnings"], ["warn-ok"])

    def test_run_batch_mode_json_nonjson_and_error(self):
        with patch("scripts.query.run_batch", side_effect=FileNotFoundError("missing")):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.run_batch_mode(_args(json=True, batch="configs/missing"))
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "BATCH_FAILED")

        def _fake_run_batch(_batch, execute_fn, on_progress):
            on_progress("01.json", 1, 2, {"status": "ok"})
            on_progress("02.json", 2, 2, {"status": "error"})
            return {"total": 2, "succeeded": 1, "failed": 1, "skipped": 0, "elapsed_sec": 1.23}

        with patch("scripts.query.run_batch", side_effect=_fake_run_batch):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.run_batch_mode(_args(json=False, batch="configs/x"))
            self.assertEqual(code, 1)
            text = out.getvalue()
            self.assertIn("[1/2] 01.json: ok", text)
            self.assertIn("[2/2] 02.json: error", text)
            self.assertIn("Batch complete:", text)

        with patch(
            "scripts.query.run_batch",
            return_value={"total": 1, "succeeded": 1, "failed": 0, "skipped": 0, "elapsed_sec": 0.5},
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.run_batch_mode(_args(json=True, batch="configs/x"))
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["succeeded"], 1)

    def test_main_batch_route_and_fallback_exception_handlers(self):
        with patch.object(query_cli, "run_batch_mode", return_value=7), patch.object(
            sys, "argv", ["query.py", "--batch", "configs/x"]
        ):
            self.assertEqual(query_cli.main(), 7)

        params = {"schema_version": "1.0", "source": "ga4"}
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "_load_params_from_args", return_value=(params, None)
        ), patch.object(query_cli, "capture_stdio", side_effect=ValueError("fallback value error")), patch.object(
            sys, "argv", ["query.py", "--json"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "INVALID_QUERY")

        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "_load_params_from_args", return_value=(params, None)
        ), patch.object(query_cli, "capture_stdio", side_effect=RuntimeError("fallback runtime error")), patch.object(
            sys, "argv", ["query.py", "--json"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "QUERY_EXECUTION_FAILED")


if __name__ == "__main__":
    unittest.main()
