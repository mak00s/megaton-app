import io
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import scripts.query as query_cli
from lib.job_manager import JobStore


class _DummyProc:
    def __init__(self, pid=12345):
        self.pid = pid


class TestQuerySuccessFlows(unittest.TestCase):
    def test_sync_query_success_with_pipeline_and_csv_save(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_csv = Path(tmp) / "saved.csv"
            params = {
                "schema_version": "1.0",
                "source": "ga4",
                "property_id": "123",
                "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
                "dimensions": ["date"],
                "metrics": ["sessions"],
                "pipeline": {"sort": "clicks DESC", "head": 1},
                "save": {"to": "csv", "path": str(out_csv), "mode": "overwrite"},
            }
            df = pd.DataFrame(
                [
                    {"date": "2026-01-01", "clicks": 10},
                    {"date": "2026-01-02", "clicks": 20},
                ]
            )

            with patch.object(query_cli, "load_params", return_value=(params, None)), patch.object(
                query_cli,
                "execute_query_from_params",
                return_value=(df, ["header"]),
            ), patch.object(sys, "argv", ["query.py", "--json", "--params", "dummy.json"]):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    code = query_cli.main()

            self.assertEqual(code, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["mode"], "query")
            self.assertEqual(payload["data"]["row_count"], 1)
            self.assertEqual(payload["data"]["save"]["saved_to"], str(out_csv))
            self.assertTrue(out_csv.exists())
            saved = pd.read_csv(out_csv)
            self.assertEqual(len(saved), 1)
            self.assertEqual(int(saved.iloc[0]["clicks"]), 20)

    def test_submit_status_result_chain_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_dir = Path(tmp) / "jobs"
            env = {"QUERY_JOB_DIR": str(job_dir)}
            params = {
                "schema_version": "1.0",
                "source": "ga4",
                "property_id": "123",
                "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
                "dimensions": ["date"],
                "metrics": ["sessions"],
            }

            # submit
            with patch.dict(os.environ, env, clear=False), patch.object(
                query_cli, "load_params", return_value=(params, None)
            ), patch("scripts.query.subprocess.Popen", return_value=_DummyProc(99999)), patch.object(
                sys, "argv", ["query.py", "--json", "--submit", "--params", "dummy.json"]
            ):
                submit_out = io.StringIO()
                with redirect_stdout(submit_out):
                    submit_code = query_cli.main()
            self.assertEqual(submit_code, 0)
            submit_payload = json.loads(submit_out.getvalue())
            job_id = submit_payload["data"]["job_id"]

            # status
            with patch.dict(os.environ, env, clear=False), patch.object(
                sys, "argv", ["query.py", "--json", "--status", job_id]
            ):
                status_out = io.StringIO()
                with redirect_stdout(status_out):
                    status_code = query_cli.main()
            self.assertEqual(status_code, 0)
            status_payload = json.loads(status_out.getvalue())
            self.assertEqual(status_payload["data"]["status"], "queued")

            # make artifact + mark job as succeeded, then read result
            store = JobStore(job_dir)
            artifact = store.artifact_path(job_id)
            pd.DataFrame([{"date": "2026-01-01", "clicks": 7}]).to_csv(
                artifact, index=False, encoding="utf-8-sig"
            )
            store.update_job(
                job_id,
                status="succeeded",
                row_count=1,
                artifact_path=str(artifact),
                finished_at="2026-02-07T00:00:00+00:00",
            )

            with patch.dict(os.environ, env, clear=False), patch.object(
                sys, "argv", ["query.py", "--json", "--result", job_id, "--head", "1", "--summary"]
            ):
                result_out = io.StringIO()
                with redirect_stdout(result_out):
                    result_code = query_cli.main()
            self.assertEqual(result_code, 0)
            result_payload = json.loads(result_out.getvalue())
            self.assertEqual(result_payload["mode"], "job_result")
            self.assertEqual(result_payload["data"]["row_count"], 1)
            self.assertEqual(result_payload["data"]["head_rows"], 1)
            self.assertIn("summary", result_payload["data"])

    def test_run_job_success_updates_status_and_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(
                params={
                    "schema_version": "1.0",
                    "source": "ga4",
                    "property_id": "123",
                    "date_range": {"start": "2026-01-01", "end": "2026-01-02"},
                    "dimensions": ["date"],
                    "metrics": ["sessions"],
                },
                params_path="input/params.json",
            )

            df = pd.DataFrame([{"date": "2026-01-01", "sessions": 11}])
            with patch.object(query_cli, "execute_query_from_params", return_value=(df, ["header"])):
                code = query_cli.run_job(job["job_id"], store)

            self.assertEqual(code, 0)
            updated = store.load_job(job["job_id"])
            self.assertEqual(updated["status"], "succeeded")
            self.assertEqual(updated["row_count"], 1)
            self.assertTrue(Path(updated["artifact_path"]).exists())

    def test_show_job_result_pipeline_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(
                params={"schema_version": "1.0", "source": "ga4"},
                params_path="input/params.json",
            )
            artifact = store.artifact_path(job["job_id"])
            pd.DataFrame(
                [{"page": "/a", "clicks": 3}, {"page": "/a", "clicks": 2}, {"page": "/b", "clicks": 5}]
            ).to_csv(artifact, index=False, encoding="utf-8-sig")
            store.update_job(job["job_id"], status="succeeded", artifact_path=str(artifact), row_count=3)

            args = Namespace(
                json=True,
                output=None,
                transform=None,
                where=None,
                sort="sum_clicks DESC",
                columns=None,
                group_by="page",
                aggregate="sum:clicks",
                head=1,
                summary=False,
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                code = query_cli.show_job_result(job["job_id"], args, store)

            self.assertEqual(code, 0)
            payload = json.loads(buf.getvalue())
            self.assertEqual(payload["mode"], "job_result")
            self.assertEqual(payload["data"]["row_count"], 1)
            self.assertEqual(payload["data"]["rows"][0]["page"], "/a")


if __name__ == "__main__":
    unittest.main()
