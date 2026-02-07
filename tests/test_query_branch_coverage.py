import io
import json
import os
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
        "job_limit": 20,
    }
    base.update(kwargs)
    return Namespace(**base)


class TestCancelBranches(unittest.TestCase):
    def test_cancel_already_canceled(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="canceled")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.cancel_job(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["already_canceled"], True)

    def test_cancel_not_cancellable(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="succeeded")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.cancel_job(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "JOB_NOT_CANCELLABLE")

    def test_cancel_pid_not_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="running", runner_pid=43210)
            with patch("scripts.query.os.killpg", side_effect=ProcessLookupError):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.cancel_job(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["data"]["terminate_status"], "not_found")

    def test_cancel_pid_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="running", runner_pid=999)
            with patch("scripts.query.os.killpg", side_effect=RuntimeError("boom")):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.cancel_job(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "JOB_CANCEL_FAILED")


class TestListAndShowBranches(unittest.TestCase):
    def test_show_jobs_empty_and_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_jobs(_args(json=False, job_limit=20), store)
            self.assertEqual(code, 0)
            self.assertIn("ジョブはありません", out.getvalue())

            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="succeeded", row_count=12)
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_jobs(_args(json=False, job_limit=20), store)
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("job_id", text)
            self.assertIn("succeeded", text)

    def test_run_list_mode_success_and_error(self):
        args = _args(
            json=True,
            list_ga4_properties=True,
            list_gsc_sites=False,
            list_bq_datasets=False,
            project=None,
        )
        with patch("scripts.query.get_ga4_properties", return_value=[{"display": "prop"}]):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["mode"], "list_ga4_properties")

        args = _args(
            json=True,
            list_ga4_properties=False,
            list_gsc_sites=True,
            list_bq_datasets=False,
            project=None,
        )
        with patch("scripts.query.get_gsc_sites", side_effect=RuntimeError("x")):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 1)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["error_code"], "LIST_OPERATION_FAILED")

        args = _args(
            json=True,
            list_ga4_properties=False,
            list_gsc_sites=False,
            list_bq_datasets=True,
            project="p",
        )
        with patch("scripts.query.get_bq_datasets", return_value=["d1"]):
            out = io.StringIO()
            with redirect_stdout(out):
                handled, code = query_cli.run_list_mode(args)
            self.assertTrue(handled)
            self.assertEqual(code, 0)
            payload = json.loads(out.getvalue())
            self.assertEqual(payload["mode"], "list_bq_datasets")

    def test_show_job_status_not_found_and_plain(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_status("missing", _args(json=True), store)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "JOB_NOT_FOUND")

            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            store.update_job(job["job_id"], status="failed", error={"type": "E", "message": "m"})
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_status(job["job_id"], _args(json=False), store)
            self.assertEqual(code, 0)
            text = out.getvalue()
            self.assertIn("job_id:", text)
            self.assertIn("error: E - m", text)


class TestErrorBranches(unittest.TestCase):
    def test_show_job_result_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")

            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_result("missing", _args(json=True), store)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "JOB_NOT_FOUND")

            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_result(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "JOB_NOT_READY")

            store.update_job(job["job_id"], status="succeeded", artifact_path=None)
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.show_job_result(job["job_id"], _args(json=True), store)
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "ARTIFACT_NOT_FOUND")

    def test_show_job_result_pipeline_and_summary_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(params={"source": "ga4"}, params_path="input/params.json")
            artifact = store.artifact_path(job["job_id"])
            pd.DataFrame([{"a": 1}]).to_csv(artifact, index=False, encoding="utf-8-sig")
            store.update_job(job["job_id"], status="succeeded", artifact_path=str(artifact))

            with patch("scripts.query.apply_pipeline", side_effect=ValueError("Invalid sort: x")):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(job["job_id"], _args(json=True, sort="a DESC"), store)
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_SORT")

            with patch("scripts.query.apply_pipeline", side_effect=RuntimeError("broken")):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(job["job_id"], _args(json=True, sort="a DESC"), store)
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(out.getvalue())["error_code"], "RESULT_READ_FAILED")

            with patch("scripts.query.read_head", side_effect=RuntimeError("bad head")):
                out = io.StringIO()
                with redirect_stdout(out):
                    code = query_cli.show_job_result(job["job_id"], _args(json=True, head=1), store)
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(out.getvalue())["error_code"], "RESULT_READ_FAILED")

    def test_main_error_branches(self):
        # --head <= 0
        with patch.object(sys, "argv", ["query.py", "--json", "--head", "0"]):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_ARGUMENT")

        params = {"schema_version": "1.0", "source": "ga4"}

        # no data
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", return_value=(pd.DataFrame(), [])), patch.object(
            sys, "argv", ["query.py", "--json", "--params", "x.json"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "NO_DATA")

        # invalid query
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", side_effect=ValueError("bad")), patch.object(
            sys, "argv", ["query.py", "--json", "--params", "x.json"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "INVALID_QUERY")

        # query execution failed
        with patch.object(query_cli, "run_list_mode", return_value=(False, 0)), patch.object(
            query_cli, "load_params", return_value=(params, None)
        ), patch.object(query_cli, "execute_query_from_params", side_effect=RuntimeError("oops")), patch.object(
            sys, "argv", ["query.py", "--json", "--params", "x.json"]
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = query_cli.main()
            self.assertEqual(code, 1)
            self.assertEqual(json.loads(out.getvalue())["error_code"], "QUERY_EXECUTION_FAILED")

    def test_emit_error_non_json_and_run_job_stderr(self):
        err = io.StringIO()
        with redirect_stderr(err):
            code = query_cli.emit_error(
                _args(json=False),
                "X",
                "msg",
                "hint",
                {"errors": [{"error_code": "E", "path": "$", "message": "m", "hint": "h"}]},
            )
        self.assertEqual(code, 1)
        self.assertIn("msg", err.getvalue())
        self.assertIn("hint: hint", err.getvalue())
        self.assertIn("[E]", err.getvalue())

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            err = io.StringIO()
            with redirect_stderr(err):
                code = query_cli.run_job("job_missing", store)
            self.assertEqual(code, 1)
            self.assertIn("jobが見つかりません", err.getvalue())


if __name__ == "__main__":
    unittest.main()
