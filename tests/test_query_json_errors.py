import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from lib.job_manager import JobStore


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


if __name__ == "__main__":
    unittest.main()
