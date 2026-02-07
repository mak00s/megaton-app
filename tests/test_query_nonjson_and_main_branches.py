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
from lib.job_manager import JobStore


def _args(**kwargs):
    base = {
        "json": False,
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
    }
    base.update(kwargs)
    return Namespace(**base)


class _DummyProc:
    def __init__(self, pid=123):
        self.pid = pid


class TestFunctionNonJsonBranches(unittest.TestCase):
    def test_emit_success_non_json_no_output(self):
        out = io.StringIO()
        with redirect_stdout(out):
            query_cli.emit_success(_args(json=False), {"x": 1}, mode="m")
        self.assertEqual(out.getvalue(), "")

    def test_map_pipeline_error_variants(self):
        self.assertEqual(query_cli.map_pipeline_error("Invalid transform: x")[0], "INVALID_TRANSFORM")
        self.assertEqual(query_cli.map_pipeline_error("Invalid where expression: x")[0], "INVALID_WHERE")
        self.assertEqual(query_cli.map_pipeline_error("Invalid columns: x")[0], "INVALID_COLUMNS")
        self.assertEqual(query_cli.map_pipeline_error("Invalid aggregate: x")[0], "INVALID_AGGREGATE")
        self.assertEqual(query_cli.map_pipeline_error("Invalid head: x")[0], "INVALID_ARGUMENT")
        self.assertEqual(query_cli.map_pipeline_error("something else")[0], "INVALID_ARGUMENT")

    def test_output_result_branches(self):
        df = pd.DataFrame([{"a": 1}, {"a": 2}])
        with tempfile.TemporaryDirectory() as tmp:
            out_path = str(Path(tmp) / "out.csv")

            # args.output + json
            out = io.StringIO()
            with redirect_stdout(out):
                query_cli.output_result(df, _args(json=True, output=out_path), pipeline={"head": 1}, save={"to": "csv"})
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["saved_to"], out_path)

            # args.output + non-json
            out = io.StringIO()
            with redirect_stdout(out):
                query_cli.output_result(df, _args(json=False, output=out_path))
            self.assertIn("保存しました", out.getvalue())

            # non-json print table
            out = io.StringIO()
            with redirect_stdout(out):
                query_cli.output_result(df, _args(json=False, output=None))
            self.assertIn("合計: 2行", out.getvalue())

    def test_submit_job_load_error_and_nonjson_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")

            # load_params error path
            with patch.object(query_cli, "load_params", return_value=(None, {"error_code": "E", "message": "m"})):
                out = io.StringIO()
                with redirect_stderr(out):
                    code = query_cli.submit_job(_args(json=False, params="x.json"), store)
                self.assertEqual(code, 1)

            # non-json success message
            params = {"schema_version": "1.0", "source": "ga4"}
            with patch.object(query_cli, "load_params", return_value=(params, None)), patch(
                "scripts.query.subprocess.Popen", return_value=_DummyProc(555)
            ):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.submit_job(_args(json=False, params="x.json"), store)
                self.assertEqual(code, 0)
                self.assertIn("ジョブを投入しました", out.getvalue())

    def test_cancel_job_nonjson_and_terminated_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="canceled")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.cancel_job(job["job_id"], _args(json=False), store)
            self.assertEqual(code, 0)
            self.assertIn("既にキャンセル済み", out.getvalue())

            job2 = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job2["job_id"], status="running", runner_pid=999)

            # terminate path -> SIGTERM then timeout then SIGKILL
            times = [0.0, 0.5, 1.5, 2.5, 3.1]
            with patch("scripts.query.os.killpg") as killpg, patch("scripts.query.os.kill", return_value=None), patch(
                "scripts.query.time.sleep", return_value=None
            ), patch("scripts.query.time.time", side_effect=times):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.cancel_job(job2["job_id"], _args(json=False), store)
                self.assertEqual(code, 0)
                self.assertIn("terminate_status: terminated", out.getvalue())
                killpg.assert_any_call(999, signal.SIGTERM)
                killpg.assert_any_call(999, signal.SIGKILL)

    def test_run_job_remaining_branches(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")

            # canceled before start
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="canceled")
            self.assertEqual(query_cli.run_job(job["job_id"], store), 0)

            # df is None
            job2 = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            with patch.object(query_cli, "execute_query_from_params", return_value=(None, [])):
                self.assertEqual(query_cli.run_job(job2["job_id"], store), 1)
            self.assertEqual(store.load_job(job2["job_id"])["status"], "failed")

            # canceled during run (latest canceled)
            job3 = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            df = pd.DataFrame([{"a": 1}])

            def _exec(_):
                store.update_job(job3["job_id"], status="canceled")
                return df, []

            with patch.object(query_cli, "execute_query_from_params", side_effect=_exec):
                self.assertEqual(query_cli.run_job(job3["job_id"], store), 1)

            # generic exception in query
            job4 = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            with patch.object(query_cli, "execute_query_from_params", side_effect=RuntimeError("boom")):
                self.assertEqual(query_cli.run_job(job4["job_id"], store), 1)
            self.assertEqual(store.load_job(job4["job_id"])["status"], "failed")

    def test_show_job_result_output_and_nonjson_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            artifact = store.artifact_path(job["job_id"])
            pd.DataFrame([{"page": "/a", "clicks": 3}, {"page": "/a", "clicks": 2}]).to_csv(
                artifact, index=False, encoding="utf-8-sig"
            )
            store.update_job(job["job_id"], status="succeeded", artifact_path=str(artifact), row_count=2)

            with tempfile.TemporaryDirectory() as tmp2:
                out_path = str(Path(tmp2) / "p.csv")
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(
                        job["job_id"],
                        _args(json=True, output=out_path, group_by="page", aggregate="sum:clicks", sort="sum_clicks DESC"),
                        store,
                    )
                self.assertEqual(code, 0)
                payload = json.loads(out.getvalue())
                self.assertEqual(payload["data"]["saved_to"], out_path)
                self.assertTrue(Path(out_path).exists())

            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_result(
                    job["job_id"],
                    _args(json=False, group_by="page", aggregate="sum:clicks", sort="sum_clicks DESC"),
                    store,
                )
            self.assertEqual(code, 0)
            self.assertIn("pipeline:", out.getvalue())

            with tempfile.TemporaryDirectory() as tmp3:
                copy_to = str(Path(tmp3) / "copy.csv")
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(
                        job["job_id"],
                        _args(json=True, output=copy_to, head=1, summary=True),
                        store,
                    )
                self.assertEqual(code, 0)
                payload = json.loads(out.getvalue())
                self.assertEqual(payload["data"]["copied_to"], copy_to)
                self.assertTrue(Path(copy_to).exists())

            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_result(job["job_id"], _args(json=False, head=1, summary=True), store)
            self.assertEqual(code, 0)
            txt = out.getvalue()
            self.assertIn("head: first 1 rows", txt)
            self.assertIn("summary:", txt)

    def test_show_jobs_json_and_run_list_nonjson(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            store.create_job(params={"source": "ga4"}, params_path="input/params.json")

            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_jobs(_args(json=True, job_limit=10), store)
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(out.getvalue())["mode"], "list_jobs")

        # run_list_mode non-json branches
        args = _args(json=False, list_ga4_properties=True)
        with patch("scripts.query.get_ga4_properties", return_value=[{"display": "P1"}]):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            self.assertIn("GA4プロパティ一覧", out.getvalue())

        args = _args(json=False, list_gsc_sites=True)
        with patch("scripts.query.get_gsc_sites", return_value=["sc-domain:example.com"]):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            self.assertIn("GSCサイト一覧", out.getvalue())

        args = _args(json=False, list_bq_datasets=True, project=None)
        err = io.StringIO()
        with redirect_stderr(err):
            handled, code = query_cli.run_list_mode(args)
        self.assertTrue(handled)
        self.assertEqual(code, 1)

        args = _args(json=False, list_bq_datasets=True, project="p")
        with patch("scripts.query.get_bq_datasets", side_effect=RuntimeError("x")):
            err = io.StringIO()
            with redirect_stderr(err):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 1)

        args = _args(json=False, list_bq_datasets=True, project="p")
        with patch("scripts.query.get_bq_datasets", return_value=["d1"]):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            self.assertIn("データセット一覧 (p)", out.getvalue())


class TestMainBranches(unittest.TestCase):
    def test_main_return_from_handled_and_action_routes(self):
        with patch.object(query_cli, "run_list_mode", return_value=(True, 9)), patch.object(
            sys, "argv", ["query.py", "--json"]
        ):
            self.assertEqual(query_cli.main(), 9)

        with patch.object(query_cli, "run_job", return_value=0), patch.object(
            sys, "argv", ["query.py", "--run-job", "job_x"]
        ):
            self.assertEqual(query_cli.main(), 0)

        with patch.object(query_cli, "cancel_job", return_value=0), patch.object(
            sys, "argv", ["query.py", "--cancel", "job_x"]
        ):
            self.assertEqual(query_cli.main(), 0)

        with patch.object(query_cli, "show_jobs", return_value=0), patch.object(
            sys, "argv", ["query.py", "--list-jobs"]
        ):
            self.assertEqual(query_cli.main(), 0)

    def test_main_argument_error_branches(self):
        # sync query + pipeline opts
        with patch.object(sys, "argv", ["query.py", "--json", "--where", "x > 1"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        # pipeline opts with action but not result
        with patch.object(sys, "argv", ["query.py", "--json", "--status", "job_x", "--where", "x > 1"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        with patch.object(sys, "argv", ["query.py", "--json", "--result", "job_x", "--aggregate", "sum:clicks"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        with patch.object(
            sys, "argv", ["query.py", "--json", "--result", "job_x", "--summary", "--where", "x > 1"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        with patch.object(sys, "argv", ["query.py", "--json", "--summary"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        with patch.object(sys, "argv", ["query.py", "--json", "--submit", "--head", "3"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

    def test_main_params_and_pipeline_save_errors_and_nonjson_headers(self):
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(None, {"error_code": "E", "message": "m"})
        ), patch.object(sys, "argv", ["query.py", "--json"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "E")

        params = {
            "schema_version": "1.0",
            "source": "ga4",
            "pipeline": {"sort": "bad"},
        }
        df = pd.DataFrame([{"x": 1}])
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", return_value=(df, ["h"])), patch.object(
            query_cli, "apply_pipeline", side_effect=ValueError("Invalid sort: bad")
        ), patch.object(sys, "argv", ["query.py", "--json"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_SORT")

        params = {"schema_version": "1.0", "source": "ga4", "save": {"to": "csv", "path": "x.csv"}}
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", return_value=(df, ["h1", "h2"])), patch.object(
            query_cli, "execute_save", side_effect=RuntimeError("save fail")
        ), patch.object(sys, "argv", ["query.py", "--json"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "SAVE_FAILED")

        # non-json header print path
        params = {"schema_version": "1.0", "source": "ga4"}
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", return_value=(df, ["h1", "h2"])), patch.object(
            query_cli, "output_result", return_value=None
        ), patch.object(sys, "argv", ["query.py"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(query_cli.main(), 0)
            txt = out.getvalue()
            self.assertIn("h1", txt)
            self.assertIn("h2", txt)


if __name__ == "__main__":
    unittest.main()
