"""Adobe Analytics Classifications API operations.

Pure API client wrapping AdobeOAuthClient for classification
dataset discovery, export, import, and job polling.

All methods are parameterised (company_id, rsid, dimension) so the same
client instance can operate on multiple report suites and dimensions.

Usage::

    from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient
    from megaton_lib.audit.providers.analytics.classifications import (
        ClassificationsClient,
    )

    auth = AdobeOAuthClient(org_id="...", token_cache_file="...")
    client = ClassificationsClient(auth=auth, company_id="wacoal1")

    dataset_id = client.find_dataset_id(rsid="wacoal-all", dimension="evar29")
    columns = client.get_classification_columns(dataset_id)
"""

from __future__ import annotations

import csv
import io
import time
from pathlib import Path
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient

AA_API_BASE = "https://analytics.adobe.io/api"

# Polling defaults
POLL_INTERVAL_SEC = 10
POLL_MAX_ATTEMPTS = 60

# Terminal job states
_TERMINAL_STATES = frozenset({"completed", "failed", "error", "cancelled"})


class ClassificationsClient:
    """Stateless client for AA Classifications API operations.

    Parameters
    ----------
    auth : AdobeOAuthClient
        Authenticated Adobe OAuth client (from megaton_lib).
    company_id : str
        Adobe Analytics global company ID (e.g. ``"wacoal1"``).
    poll_interval : int
        Seconds between job status polls (default 10).
    poll_max_attempts : int
        Maximum poll attempts before timeout (default 60).
    """

    def __init__(
        self,
        auth: AdobeOAuthClient,
        company_id: str,
        *,
        poll_interval: int = POLL_INTERVAL_SEC,
        poll_max_attempts: int = POLL_MAX_ATTEMPTS,
    ) -> None:
        self._auth = auth
        self.company_id = company_id
        self.poll_interval = poll_interval
        self.poll_max_attempts = poll_max_attempts

    # ------------------------------------------------------------------
    # Headers
    # ------------------------------------------------------------------

    def _headers(self, *, json_content: bool = True) -> dict[str, str]:
        extra: dict[str, str] = {
            "Accept": "application/json",
            "x-proxy-global-company-id": self.company_id,
        }
        if json_content:
            extra["Content-Type"] = "application/json"
        return self._auth.get_headers(extra=extra)

    # ------------------------------------------------------------------
    # Dataset discovery
    # ------------------------------------------------------------------

    def find_dataset_id(self, rsid: str, dimension: str) -> str:
        """Look up the classification dataset ID for a given dimension.

        Parameters
        ----------
        rsid : str
            Report suite ID (e.g. ``"wacoal-all"``).
        dimension : str
            AA dimension name (e.g. ``"evar29"``, ``"prop10"``).

        Returns
        -------
        str
            The dataset ID.

        Raises
        ------
        RuntimeError
            If no dataset is found for the dimension.
        """
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/datasets/compatibilityMetrics/{rsid}"
        )
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()

        for metric in resp.json().get("metrics", []):
            ids = metric.get("id", [])
            # ids may be a list ["variables/evar29"] or a bare string.
            # Match dimension exactly or as "variables/{dimension}" to
            # avoid prefix collisions (e.g. evar2 vs evar29).
            if isinstance(ids, str):
                ids = [ids]
            if any(
                entry == dimension or entry == f"variables/{dimension}"
                for entry in ids
            ):
                datasets = metric.get("datasets", [])
                if datasets:
                    return datasets[0]

        available = [str(m.get("id")) for m in resp.json().get("metrics", [])]
        raise RuntimeError(
            f"Classification dataset not found for {dimension!r} in {rsid!r}. "
            f"Available: {available}"
        )

    # ------------------------------------------------------------------
    # Job polling (shared by export & import)
    # ------------------------------------------------------------------

    def poll_job(self, job_id: str, *, verbose: bool = True) -> str:
        """Poll a job until it reaches a terminal state.

        Returns
        -------
        str
            Final state (``"completed"``, ``"failed"``, etc.).

        Raises
        ------
        RuntimeError
            If the job does not complete within the timeout or ends in
            a non-completed terminal state.
        """
        url = f"{AA_API_BASE}/{self.company_id}/classifications/job/{job_id}"

        for attempt in range(self.poll_max_attempts):
            resp = requests.get(url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()

            state = str(
                data.get("state", data.get("status", "unknown"))
            ).lower()

            if verbose:
                total_lines = data.get("totalLines", "?")
                print(f"  [{attempt + 1}] state={state}, totalLines={total_lines}")

            if state == "completed":
                return state
            if state in _TERMINAL_STATES:
                raise RuntimeError(f"Job {job_id} failed: state={state}")

            time.sleep(self.poll_interval)

        raise RuntimeError(
            f"Job {job_id} timed out after "
            f"{self.poll_max_attempts * self.poll_interval}s"
        )

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def create_export_job(
        self,
        dataset_id: str,
        *,
        job_name: str = "",
        row_limit: int = 0,
    ) -> str:
        """Create a classification export job.

        Parameters
        ----------
        dataset_id : str
            Classification dataset ID.
        job_name : str
            Optional job name (auto-generated if empty).
        row_limit : int
            Maximum rows to export. 0 = unlimited.

        Returns
        -------
        str
            The export job ID.
        """
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/job/export/{dataset_id}"
        )
        body: dict = {
            "dataFormat": "tsv",
            "encoding": "UTF8",
            "jobName": job_name or f"export {dataset_id}",
            "rowLimit": row_limit,
        }
        resp = requests.post(url, headers=self._headers(), json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        job_id = (
            data.get("export_job_id")
            or data.get("id")
            or data.get("job_id")
            or data.get("jobId")
        )
        if not job_id:
            raise RuntimeError(f"No export job_id in response: {data}")
        return job_id

    def download_export_file(self, job_id: str) -> str:
        """Download the exported TSV content for a completed export job.

        Returns
        -------
        str
            Raw TSV text.
        """
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/job/export/file/{job_id}"
        )
        resp = requests.get(
            url, headers=self._headers(json_content=False), timeout=120
        )
        resp.raise_for_status()
        return resp.text

    def export_classification(
        self,
        dataset_id: str,
        *,
        job_name: str = "",
        row_limit: int = 0,
        verbose: bool = True,
    ) -> str:
        """Create export job, poll until done, and return TSV text.

        Convenience method combining :meth:`create_export_job`,
        :meth:`poll_job`, and :meth:`download_export_file`.
        """
        job_id = self.create_export_job(
            dataset_id, job_name=job_name, row_limit=row_limit
        )
        if verbose:
            print(f"  export job: {job_id}")
        self.poll_job(job_id, verbose=verbose)
        return self.download_export_file(job_id)

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def create_import_job(self, dataset_id: str, *, job_name: str = "") -> str:
        """Create a classification import job.

        Returns
        -------
        str
            The import ``api_job_id``.
        """
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/job/import/createApiJob/{dataset_id}"
        )
        body = {
            "dataFormat": "tsv",
            "encoding": "UTF8",
            "jobName": job_name or f"import {dataset_id}",
            "listDelimiter": ",",
            "source": "Direct API Upload",
        }
        resp = requests.post(url, headers=self._headers(), json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        job_id = data.get("api_job_id")
        if not job_id:
            raise RuntimeError(f"No api_job_id in import response: {data}")
        return job_id

    def upload_file(
        self,
        job_id: str,
        content: str | bytes | Path,
        *,
        filename: str = "classification.tsv",
    ) -> None:
        """Upload a TSV file (or content) to an import job.

        Parameters
        ----------
        content : str | bytes | Path
            TSV content as string, bytes, or a file path.
        filename : str
            Filename sent in the multipart upload.
        """
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/job/import/uploadFile/{job_id}"
        )

        if isinstance(content, Path):
            filename = content.name
            file_bytes = content.read_bytes()
        elif isinstance(content, str):
            file_bytes = content.encode("utf-8")
        else:
            file_bytes = content

        files = {
            "file": (filename, file_bytes, "text/tab-separated-values"),
        }
        resp = requests.post(
            url,
            headers=self._headers(json_content=False),
            files=files,
            timeout=120,
        )
        resp.raise_for_status()

    def commit_job(self, job_id: str) -> None:
        """Commit an import job to start processing."""
        url = (
            f"{AA_API_BASE}/{self.company_id}"
            f"/classifications/job/import/commitApiJob/{job_id}"
        )
        resp = requests.post(
            url,
            headers=self._headers(),
            json={"api_job_id": job_id},
            timeout=30,
        )
        resp.raise_for_status()

    def import_classification(
        self,
        dataset_id: str,
        content: str | bytes | Path,
        *,
        job_name: str = "",
        filename: str = "classification.tsv",
        verbose: bool = True,
    ) -> str:
        """Create import job, upload content, commit, and poll.

        Convenience method combining :meth:`create_import_job`,
        :meth:`upload_file`, :meth:`commit_job`, and :meth:`poll_job`.

        Returns
        -------
        str
            Final job state (``"completed"``).
        """
        job_id = self.create_import_job(dataset_id, job_name=job_name)
        if verbose:
            print(f"  import job: {job_id}")

        self.upload_file(job_id, content, filename=filename)
        if verbose:
            size = len(content) if isinstance(content, (str, bytes)) else content.stat().st_size
            print(f"  uploaded: {size:,} bytes")

        self.commit_job(job_id)
        if verbose:
            print("  committed, polling...")

        return self.poll_job(job_id, verbose=verbose)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def get_classification_columns(
        self,
        dataset_id: str,
        *,
        verbose: bool = False,
    ) -> list[str]:
        """Export 1 row to discover current classification column names.

        Returns
        -------
        list[str]
            Column names excluding ``"Key"``.
        """
        tsv_text = self.export_classification(
            dataset_id,
            job_name="column discovery",
            row_limit=1,
            verbose=verbose,
        )
        header_line = tsv_text.split("\n", 1)[0]
        columns = header_line.strip().split("\t")
        # columns[0] is "Key"
        return columns[1:]

    def export_column_as_dict(
        self,
        dataset_id: str,
        column: str,
        *,
        job_name: str = "",
        verbose: bool = True,
    ) -> dict[str, str]:
        """Export and parse a single classification column as ``{Key: value}``.

        Parameters
        ----------
        column : str
            Classification column name (e.g. ``"関係者"``).

        Returns
        -------
        dict[str, str]
            Mapping of Key to the column's value.
        """
        tsv_text = self.export_classification(
            dataset_id,
            job_name=job_name or f"export {column}",
            verbose=verbose,
        )
        result: dict[str, str] = {}
        reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")
        if reader.fieldnames and column in reader.fieldnames:
            for row in reader:
                key = (row.get("Key") or "").strip()
                val = (row.get(column) or "").strip()
                if key:
                    result[key] = val
        return result

    def verify_column(
        self,
        rsid: str,
        dimension: str,
        column: str,
        expected: dict[str, str],
        *,
        verbose: bool = True,
    ) -> dict[str, dict]:
        """Verify that expected classification values are reflected in AA.

        Intended to run hours after an upload to confirm reflection.

        Parameters
        ----------
        expected : dict[str, str]
            Mapping of ``{Key: expected_value}``.

        Returns
        -------
        dict[str, dict]
            Per-key result with ``"expected"``, ``"actual"``, ``"match"`` keys.
        """
        dataset_id = self.find_dataset_id(rsid=rsid, dimension=dimension)
        if verbose:
            print(f"[info] Dataset: {dataset_id} ({rsid} / {dimension})")

        columns = self.get_classification_columns(dataset_id, verbose=False)
        if column not in columns:
            raise RuntimeError(
                f"Column {column!r} not found in {dimension} classification. "
                f"Available: {columns}"
            )

        if verbose:
            print(f"[info] Exporting current {column!r} values...")
        current = self.export_column_as_dict(
            dataset_id, column, job_name=f"verify {column}", verbose=verbose
        )
        if verbose:
            print(f"[info] Exported {len(current):,} keys")

        results: dict[str, dict] = {}
        for key, exp_val in expected.items():
            actual = current.get(key, "")
            results[key] = {
                "expected": exp_val,
                "actual": actual,
                "match": actual == exp_val,
            }
        return results


def print_verify_results(results: dict[str, dict]) -> None:
    """Print verification results in a readable table."""
    total = len(results)
    matched = sum(1 for r in results.values() if r["match"])
    mismatched = total - matched

    print(f"\n{'Key':<25} {'Expected':<15} {'Actual':<15} {'Result'}")
    print("-" * 65)
    for key, r in results.items():
        status = "OK" if r["match"] else "NG"
        print(f"{key:<25} {r['expected']:<15} {r['actual']:<15} {status}")
    print("-" * 65)
    print(f"Total: {total}, OK: {matched}, NG: {mismatched}")

    if mismatched > 0:
        print(
            "\n[warn] 未反映のキーがあります。"
            "反映には数時間かかる場合があります。時間をおいて再実行してください。"
        )


# ---------------------------------------------------------------------------
# CLI entrypoint (python -m ...classifications)
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    """CLI for verifying classification upload reflection."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="AA 分類データの反映を確認する",
    )
    parser.add_argument("--company-id", required=True, help="Adobe Analytics company ID")
    parser.add_argument("--rsid", required=True, help="Report suite ID")
    parser.add_argument("--dimension", required=True, help="AA dimension (e.g. evar29, prop10)")
    parser.add_argument("--column", required=True, help="Classification column name")
    parser.add_argument("--org-id", default="", help="Adobe org ID (default: ADOBE_ORG_ID env)")
    parser.add_argument("--token-cache", default="", help="Path to token cache file")
    parser.add_argument(
        "--creds-file",
        default="",
        help="JSON file with client_id, client_secret, org_id",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--keys",
        help="Comma-separated key=value pairs (e.g. A100012345=社員,A100067890=業者)",
    )
    group.add_argument("--diff-tsv", type=Path, help="TSV file with Key and column value")
    parser.add_argument("--sample", type=int, default=0, help="Sample N keys from diff-tsv (0=all)")

    args = parser.parse_args()

    # Parse expected values
    expected: dict[str, str] = {}
    if args.keys:
        for pair in args.keys.split(","):
            pair = pair.strip()
            if "=" not in pair:
                print(f"[error] Invalid key=value pair: {pair!r}", file=sys.stderr)
                sys.exit(1)
            k, v = pair.split("=", 1)
            expected[k.strip()] = v.strip()
    elif args.diff_tsv:
        if not args.diff_tsv.exists():
            print(f"[error] File not found: {args.diff_tsv}", file=sys.stderr)
            sys.exit(1)
        with open(args.diff_tsv, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                key = (row.get("Key") or "").strip()
                val = (row.get(args.column) or "").strip()
                if key and val:
                    expected[key] = val

    if not expected:
        print("[error] No keys to verify", file=sys.stderr)
        sys.exit(1)

    # Sample if requested
    if args.sample > 0 and len(expected) > args.sample:
        import random

        keys = random.sample(list(expected), args.sample)
        expected = {k: expected[k] for k in keys}

    print(f"[info] Verifying {len(expected):,} keys...")

    from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient

    kwargs: dict = {}
    if args.creds_file:
        import json as _json

        creds = _json.loads(Path(args.creds_file).read_text(encoding="utf-8"))
        for k in ("client_id", "client_secret", "org_id"):
            if creds.get(k):
                kwargs[k] = creds[k]
    if args.org_id:
        kwargs["org_id"] = args.org_id
    if args.token_cache:
        kwargs["token_cache_file"] = args.token_cache
    auth = AdobeOAuthClient(**kwargs)
    client = ClassificationsClient(auth=auth, company_id=args.company_id)
    results = client.verify_column(
        rsid=args.rsid,
        dimension=args.dimension,
        column=args.column,
        expected=expected,
    )
    print_verify_results(results)

    mismatched = sum(1 for r in results.values() if not r["match"])
    sys.exit(1 if mismatched else 0)


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _Path

    # Enable direct execution (python classifications.py ...)
    _megaton_app = str(_Path(__file__).resolve().parents[4])
    if _megaton_app not in _sys.path:
        _sys.path.insert(0, _megaton_app)

    _cli_main()
