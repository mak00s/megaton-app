import tempfile
import unittest

from lib.job_manager import JobStore


class TestJobStore(unittest.TestCase):
    def test_create_and_load_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(tmp)
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
            loaded = store.load_job(job["job_id"])
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded["status"], "queued")
            self.assertEqual(loaded["source"], "ga4")

    def test_update_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(tmp)
            job = store.create_job(
                params={
                    "schema_version": "1.0",
                    "source": "gsc",
                    "site_url": "https://example.com",
                    "date_range": {"start": "2026-02-01", "end": "2026-02-03"},
                    "dimensions": ["query"],
                },
                params_path="input/params.json",
            )
            updated = store.update_job(job["job_id"], status="running", runner_pid=999)
            self.assertEqual(updated["status"], "running")
            self.assertEqual(updated["runner_pid"], 999)

    def test_list_jobs_latest_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(tmp)
            first = store.create_job(
                params={
                    "schema_version": "1.0",
                    "source": "bigquery",
                    "project_id": "p",
                    "sql": "SELECT 1",
                },
                params_path="input/params.json",
            )
            second = store.create_job(
                params={
                    "schema_version": "1.0",
                    "source": "bigquery",
                    "project_id": "p",
                    "sql": "SELECT 2",
                },
                params_path="input/params.json",
            )
            jobs = store.list_jobs(limit=2)
            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0]["job_id"], second["job_id"])
            self.assertEqual(jobs[1]["job_id"], first["job_id"])


if __name__ == "__main__":
    unittest.main()
