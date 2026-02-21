"""File-based job management."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStore:
    """Manage job state under ``output/jobs``."""

    def __init__(self, base_dir: str | Path = "output/jobs"):
        self.base_dir = Path(base_dir)
        self.records_dir = self.base_dir / "records"
        self.artifacts_dir = self.base_dir / "artifacts"
        self.logs_dir = self.base_dir / "logs"
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def _job_path(self, job_id: str) -> Path:
        return self.records_dir / f"{job_id}.json"

    def artifact_path(self, job_id: str) -> Path:
        return self.artifacts_dir / f"{job_id}.csv"

    def log_path(self, job_id: str) -> Path:
        return self.logs_dir / f"{job_id}.log"

    def create_job(self, params: dict[str, Any], params_path: str) -> dict[str, Any]:
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        job = {
            "job_id": job_id,
            "status": "queued",
            "source": params.get("source"),
            "params_path": params_path,
            "params": params,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "started_at": None,
            "finished_at": None,
            "runner_pid": None,
            "row_count": None,
            "artifact_path": None,
            "log_path": str(self.log_path(job_id)),
            "error": None,
        }
        self.save_job(job)
        return job

    def save_job(self, job: dict[str, Any]) -> None:
        job["updated_at"] = now_iso()
        path = self._job_path(job["job_id"])
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(job, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(path)

    def load_job(self, job_id: str) -> dict[str, Any] | None:
        path = self._job_path(job_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def update_job(self, job_id: str, **fields: Any) -> dict[str, Any] | None:
        job = self.load_job(job_id)
        if not job:
            return None
        job.update(fields)
        self.save_job(job)
        return job

    def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        files = sorted(
            self.records_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        jobs = []
        for path in files[: max(1, limit)]:
            try:
                jobs.append(json.loads(path.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                # Skip corrupted records.
                continue
        return jobs
