import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from megaton_lib.job_manager import JobStore


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "query.py"


class TestQueryJsonErrors(unittest.TestCase):
    def run_cli(self, args, env=None):
        env_vars = os.environ.copy()
        if env:
            env_vars.update(env)
        return subprocess.run(
            ["python", str(SCRIPT), *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            env=env_vars,
        )

    def test_summary_requires_result(self):
        proc = self.run_cli(["--json", "--summary"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")

    def test_cli_pipeline_args_rejected_with_params(self):
        """--params + --where should fail (pipeline must be in params.json)."""
        proc = self.run_cli(["--json", "--params", "input/params.json", "--where", "clicks > 10"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")
        self.assertIn("pipeline", payload["message"].lower())

    def test_where_invalid_with_submit_action(self):
        proc = self.run_cli(["--json", "--submit", "--where", "clicks > 10"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")

    def test_head_invalid_with_submit_action_hint(self):
        proc = self.run_cli(["--json", "--submit", "--head", "10"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")
        self.assertIn("pipeline.head", payload.get("hint", ""))

    def test_group_by_requires_aggregate(self):
        proc = self.run_cli(["--json", "--result", "job_dummy", "--group-by", "page"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")

    def test_summary_exclusive_with_pipeline(self):
        proc = self.run_cli(["--json", "--result", "job_dummy", "--summary", "--where", "clicks > 10"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")

    def test_missing_params_file(self):
        proc = self.run_cli(["--json", "--params", "input/not_found.json"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "PARAMS_FILE_NOT_FOUND")

    def test_params_validation_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_params = Path(tmp) / "bad.json"
            bad_params.write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "source": "ga4",
                        "property_id": "x",
                        "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                        "dimensions": ["date"],
                        "metrics": ["sessions"],
                        "unexpected_field": "oops",
                    }
                ),
                encoding="utf-8",
            )
            proc = self.run_cli(["--json", "--params", str(bad_params)])
            self.assertNotEqual(proc.returncode, 0)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_code"], "PARAMS_VALIDATION_FAILED")
            self.assertIn("details", payload)

    def test_transform_invalid_with_submit_action(self):
        proc = self.run_cli(["--json", "--submit", "--transform", "date:date_format"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "INVALID_ARGUMENT")

    def test_list_bq_requires_project(self):
        proc = self.run_cli(["--json", "--list-bq-datasets"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "MISSING_REQUIRED_ARG")

    def test_cancel_missing_job(self):
        proc = self.run_cli(["--json", "--cancel", "job_not_found"])
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_code"], "JOB_NOT_FOUND")

    def test_cancel_queued_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp) / "jobs")
            job = store.create_job(
                params={
                    "schema_version": "1.0",
                    "source": "ga4",
                    "property_id": "123",
                    "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                    "dimensions": ["date"],
                    "metrics": ["sessions"],
                },
                params_path="input/params.json",
            )
            env = {"QUERY_JOB_DIR": str(Path(tmp) / "jobs")}
            proc = self.run_cli(["--json", "--cancel", job["job_id"]], env=env)
            self.assertEqual(proc.returncode, 0)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["mode"], "cancel")
            self.assertEqual(payload["data"]["job_status"], "canceled")

            updated = store.load_job(job["job_id"])
            self.assertEqual(updated["status"], "canceled")

    def test_save_csv_missing_path_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_params = Path(tmp) / "bad_save.json"
            bad_params.write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "source": "ga4",
                        "property_id": "x",
                        "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                        "dimensions": ["date"],
                        "metrics": ["sessions"],
                        "save": {"to": "csv"},
                    }
                ),
                encoding="utf-8",
            )
            proc = self.run_cli(["--json", "--params", str(bad_params)])
            self.assertNotEqual(proc.returncode, 0)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_code"], "PARAMS_VALIDATION_FAILED")

    def test_save_bq_upsert_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_params = Path(tmp) / "bad_save_bq.json"
            bad_params.write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "source": "ga4",
                        "property_id": "x",
                        "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                        "dimensions": ["date"],
                        "metrics": ["sessions"],
                        "save": {
                            "to": "bigquery",
                            "project_id": "p",
                            "dataset": "d",
                            "table": "t",
                            "mode": "upsert",
                        },
                    }
                ),
                encoding="utf-8",
            )
            proc = self.run_cli(["--json", "--params", str(bad_params)])
            self.assertNotEqual(proc.returncode, 0)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_code"], "PARAMS_VALIDATION_FAILED")

    def test_save_invalid_target_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_params = Path(tmp) / "bad_save_target.json"
            bad_params.write_text(
                json.dumps(
                    {
                        "schema_version": "1.0",
                        "source": "ga4",
                        "property_id": "x",
                        "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                        "dimensions": ["date"],
                        "metrics": ["sessions"],
                        "save": {"to": "s3"},
                    }
                ),
                encoding="utf-8",
            )
            proc = self.run_cli(["--json", "--params", str(bad_params)])
            self.assertNotEqual(proc.returncode, 0)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_code"], "PARAMS_VALIDATION_FAILED")


if __name__ == "__main__":
    unittest.main()
