#!/usr/bin/env python
"""統合クエリ実行CLI（GA4 / GSC / BigQuery + Job管理）

使用例:
    # params.json から同期実行
    python scripts/query.py --params input/params.json

    # ジョブ投入（非同期）
    python scripts/query.py --submit --params input/params.json

    # ジョブ状態確認
    python scripts/query.py --status <job_id>

    # ジョブをキャンセル
    python scripts/query.py --cancel <job_id>

    # ジョブ結果確認
    python scripts/query.py --result <job_id>

    # ジョブ結果の先頭N行だけ取得
    python scripts/query.py --result <job_id> --head 20

    # ジョブ結果の要約統計だけ取得
    python scripts/query.py --result <job_id> --summary
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

# libをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.megaton_client import (
    get_ga4_properties,
    get_gsc_sites,
    get_bq_datasets,
    query_ga4,
    query_gsc,
    query_bq,
)
from lib.job_manager import JobStore, now_iso
from lib.params_validator import validate_params
from lib.result_inspector import read_head, build_summary, apply_pipeline


def emit_success(args, data, **meta) -> None:
    """--json時は構造化JSON、通常時は何もしない"""
    if not args.json:
        return
    payload = {"status": "ok", "data": data}
    if meta:
        payload.update(meta)
    print(json.dumps(payload, ensure_ascii=False))


def emit_error(args, error_code: str, message: str, hint: str | None = None, details=None) -> int:
    """--json時は構造化JSONエラー、通常時はstderr出力"""
    if args.json:
        payload = {
            "status": "error",
            "error_code": error_code,
            "message": message,
        }
        if hint:
            payload["hint"] = hint
        if details is not None:
            payload["details"] = details
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(message, file=sys.stderr)
        if hint:
            print(f"hint: {hint}", file=sys.stderr)
        if details and isinstance(details, dict) and "errors" in details:
            for err in details["errors"]:
                print(
                    f"- [{err.get('error_code')}] {err.get('path')}: {err.get('message')} ({err.get('hint')})",
                    file=sys.stderr,
                )
    return 1


def has_pipeline_opts(args) -> bool:
    """結果パイプライン系オプションが指定されているか"""
    return any([args.transform, args.where, args.sort, args.columns, args.group_by, args.aggregate])


def map_pipeline_error(message: str) -> tuple[str, str]:
    """apply_pipelineのValueErrorメッセージをerror_code/hintに変換"""
    if message.startswith("Invalid transform"):
        return "INVALID_TRANSFORM", "Use format: 'column:func'. Supported: date_format, url_decode, path_only, strip_qs."
    if message.startswith("Invalid where expression"):
        return "INVALID_WHERE", "Use pandas query syntax. Example: 'clicks > 10 and ctr < 0.05'"
    if message.startswith("Invalid sort"):
        return "INVALID_SORT", "Use sort format: 'column DESC,column2 ASC'."
    if message.startswith("Invalid columns"):
        return "INVALID_COLUMNS", "Use existing column names in comma-separated format."
    if message.startswith("Invalid aggregate"):
        return "INVALID_AGGREGATE", "Use --group-by with --aggregate like 'sum:clicks,mean:ctr'."
    if message.startswith("Invalid head"):
        return "INVALID_ARGUMENT", "Use --head 1 or greater."
    return "INVALID_ARGUMENT", "Check pipeline options."


def parse_gsc_filter(filter_str: str) -> list | None:
    """GSCフィルタ文字列をパース"""
    if not filter_str:
        return None

    filters = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)
        if len(parts) != 3:
            raise ValueError(
                f"Invalid filter format: {part}. Expected dimension:operator:expression"
            )
        filters.append(
            {
                "dimension": parts[0],
                "operator": parts[1],
                "expression": parts[2],
            }
        )
    return filters


def load_params(params_path: str) -> tuple[dict | None, dict | None]:
    """params.jsonを読み込み・検証"""
    try:
        with open(params_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return None, {
            "error_code": "PARAMS_FILE_NOT_FOUND",
            "message": f"params file not found: {params_path}",
            "hint": "Create the file or pass --params with a valid path.",
        }
    except json.JSONDecodeError as e:
        return None, {
            "error_code": "INVALID_JSON",
            "message": f"Invalid JSON in params file: {e}",
            "hint": "Fix JSON syntax in params file.",
        }

    params, errors = validate_params(raw)
    if errors:
        return None, {
            "error_code": "PARAMS_VALIDATION_FAILED",
            "message": "Params validation failed.",
            "hint": "Fix params based on details[].",
            "details": {"errors": errors},
        }

    return params, None


def execute_query_from_params(params: dict) -> tuple[object, list[str]]:
    """sourceに応じてクエリを実行し、DataFrameとヘッダ行を返す"""
    source = params["source"]
    if source == "ga4":
        start_date = params["date_range"]["start"]
        end_date = params["date_range"]["end"]
        df = query_ga4(
            property_id=params["property_id"],
            start_date=start_date,
            end_date=end_date,
            dimensions=params["dimensions"],
            metrics=params["metrics"],
            filter_d=params.get("filter_d"),
            limit=params.get("limit", 1000),
        )
        header_lines = [
            f"期間: {start_date} 〜 {end_date}",
            f"プロパティ: {params['property_id']}",
        ]
        return df, header_lines

    if source == "gsc":
        start_date = params["date_range"]["start"]
        end_date = params["date_range"]["end"]
        dimension_filter = parse_gsc_filter(params.get("filter", "")) if params.get("filter") else None
        df = query_gsc(
            site_url=params["site_url"],
            start_date=start_date,
            end_date=end_date,
            dimensions=params["dimensions"],
            limit=params.get("limit", 1000),
            dimension_filter=dimension_filter,
        )
        if df is not None and "clicks" in df.columns:
            df = df.sort_values("clicks", ascending=False)
        header_lines = [
            f"期間: {start_date} 〜 {end_date}",
            f"サイト: {params['site_url']}",
        ]
        if params.get("filter"):
            header_lines.append(f"フィルタ: {params['filter']}")
        return df, header_lines

    if source == "bigquery":
        df = query_bq(params["project_id"], params["sql"])
        header_lines = [f"プロジェクト: {params['project_id']}"]
        return df, header_lines

    raise ValueError(f"不明なsourceです: {source}")


def output_result(df, args, pipeline: dict | None = None):
    """結果出力"""
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        if args.json:
            payload = {
                "saved_to": args.output,
                "row_count": int(len(df)),
            }
            if pipeline is not None:
                payload["pipeline"] = pipeline
            emit_success(
                args,
                payload,
                mode="query",
            )
        else:
            print(f"保存しました: {args.output} ({len(df)}行)")
    elif args.json:
        rows = json.loads(df.to_json(orient="records", force_ascii=False))
        payload = {
            "rows": rows,
            "row_count": int(len(rows)),
        }
        if pipeline is not None:
            payload["pipeline"] = pipeline
        emit_success(
            args,
            payload,
            mode="query",
        )
    else:
        print(df.to_string(index=False))
        print(f"\n合計: {len(df)}行")


def submit_job(args, store: JobStore) -> int:
    params, err = load_params(args.params)
    if err:
        return emit_error(args, **err)

    job = store.create_job(params=params, params_path=args.params)
    job_id = job["job_id"]
    log_path = store.log_path(job_id)

    cmd = [sys.executable, str(Path(__file__).resolve()), "--run-job", job_id]
    with open(log_path, "a", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(Path(__file__).resolve().parent.parent),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    store.update_job(job_id, runner_pid=proc.pid)

    payload = {
        "job_id": job_id,
        "job_status": "queued",
        "log_path": str(log_path),
    }
    if args.json:
        emit_success(args, payload, mode="submit")
    else:
        print(f"ジョブを投入しました: {job_id}")
        print(f"ログ: {log_path}")
    return 0


def cancel_job(job_id: str, args, store: JobStore) -> int:
    job = store.load_job(job_id)
    if not job:
        return emit_error(
            args,
            "JOB_NOT_FOUND",
            f"job not found: {job_id}",
            "Check job_id or run --list-jobs.",
        )

    status = job.get("status")
    if status == "canceled":
        payload = {
            "job_id": job_id,
            "job_status": "canceled",
            "already_canceled": True,
        }
        if args.json:
            emit_success(args, payload, mode="cancel")
        else:
            print(f"jobは既にキャンセル済みです: {job_id}")
        return 0

    if status in {"succeeded", "failed"}:
        return emit_error(
            args,
            "JOB_NOT_CANCELLABLE",
            f"job cannot be canceled in status={status}",
            "Only queued/running jobs can be canceled.",
            {"job_status": status},
        )

    pid = job.get("runner_pid")
    terminate_status = "not_required"
    if pid:
        try:
            # submit時にstart_new_session=Trueのため、プロセスグループごと停止
            os.killpg(pid, signal.SIGTERM)
            deadline = time.time() + 3.0
            while time.time() < deadline:
                try:
                    os.kill(pid, 0)
                    time.sleep(0.1)
                except ProcessLookupError:
                    break
            else:
                # SIGTERMで止まらない場合は強制終了
                os.killpg(pid, signal.SIGKILL)
            terminate_status = "terminated"
        except ProcessLookupError:
            terminate_status = "not_found"
        except Exception as e:
            return emit_error(
                args,
                "JOB_CANCEL_FAILED",
                f"failed to terminate process for job {job_id}: {e}",
                "Check process permissions and job status.",
            )

    store.update_job(
        job_id,
        status="canceled",
        finished_at=now_iso(),
        error={"type": "Canceled", "message": "Canceled by user"},
    )

    payload = {
        "job_id": job_id,
        "job_status": "canceled",
        "previous_status": status,
        "terminate_status": terminate_status,
    }
    if args.json:
        emit_success(args, payload, mode="cancel")
    else:
        print(f"jobをキャンセルしました: {job_id}")
        print(f"previous_status: {status}")
        print(f"terminate_status: {terminate_status}")
    return 0


def run_job(job_id: str, store: JobStore) -> int:
    job = store.load_job(job_id)
    if not job:
        print(f"jobが見つかりません: {job_id}", file=sys.stderr)
        return 1

    if job.get("status") == "canceled":
        # 実行開始前にキャンセル済みなら何もしない
        return 0

    store.update_job(
        job_id,
        status="running",
        started_at=now_iso(),
        error=None,
    )

    try:
        params = job["params"]
        df, header_lines = execute_query_from_params(params)
        if df is None:
            raise RuntimeError("クエリ結果が取得できませんでした")

        latest = store.load_job(job_id)
        if latest and latest.get("status") == "canceled":
            # 実行中にキャンセルされた場合、成功で上書きしない
            return 1

        artifact_path = store.artifact_path(job_id)
        df.to_csv(artifact_path, index=False, encoding="utf-8-sig")
        store.update_job(
            job_id,
            status="succeeded",
            finished_at=now_iso(),
            row_count=len(df),
            artifact_path=str(artifact_path),
            header_lines=header_lines,
        )
        return 0
    except Exception as e:
        store.update_job(
            job_id,
            status="failed",
            finished_at=now_iso(),
            error={"type": type(e).__name__, "message": str(e)},
        )
        return 1


def show_job_status(job_id: str, args, store: JobStore) -> int:
    job = store.load_job(job_id)
    if not job:
        return emit_error(
            args,
            "JOB_NOT_FOUND",
            f"job not found: {job_id}",
            "Check job_id or run --list-jobs.",
        )

    if args.json:
        emit_success(args, job, mode="job_status")
    else:
        print(f"job_id: {job['job_id']}")
        print(f"status: {job['status']}")
        print(f"source: {job.get('source')}")
        print(f"created_at: {job.get('created_at')}")
        print(f"started_at: {job.get('started_at')}")
        print(f"finished_at: {job.get('finished_at')}")
        print(f"row_count: {job.get('row_count')}")
        print(f"artifact_path: {job.get('artifact_path')}")
        if job.get("error"):
            print(f"error: {job['error']['type']} - {job['error']['message']}")
    return 0


def show_job_result(job_id: str, args, store: JobStore) -> int:
    job = store.load_job(job_id)
    if not job:
        return emit_error(
            args,
            "JOB_NOT_FOUND",
            f"job not found: {job_id}",
            "Check job_id or run --list-jobs.",
        )

    if job["status"] != "succeeded":
        return emit_error(
            args,
            "JOB_NOT_READY",
            f"job is not completed yet: status={job['status']}",
            "Wait and retry with --status.",
            {"job_status": job["status"]},
        )

    artifact_path = job.get("artifact_path")
    if not artifact_path:
        return emit_error(
            args,
            "ARTIFACT_NOT_FOUND",
            "artifact_path is missing in job record",
            "Re-run the job.",
        )

    # パイプライン系オプションあり: DataFrame全体を読み込み -> apply_pipeline
    if has_pipeline_opts(args):
        try:
            df = pd.read_csv(artifact_path)
            input_rows = int(len(df))
            out_df = apply_pipeline(
                df,
                transform=args.transform,
                where=args.where,
                group_by=args.group_by,
                aggregate=args.aggregate,
                sort=args.sort,
                columns=args.columns,
                head=args.head,
            )
        except ValueError as e:
            code, hint = map_pipeline_error(str(e))
            return emit_error(args, code, str(e), hint)
        except Exception as e:
            return emit_error(
                args,
                "RESULT_READ_FAILED",
                f"failed to read result artifact: {e}",
                "Check artifact file and pipeline options.",
            )

        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            out_df.to_csv(args.output, index=False, encoding="utf-8-sig")

        rows = json.loads(out_df.to_json(orient="records", force_ascii=False))
        payload = {
            "job_id": job["job_id"],
            "pipeline": {
                "transform": args.transform,
                "where": args.where,
                "sort": args.sort,
                "columns": args.columns,
                "group_by": args.group_by,
                "aggregate": args.aggregate,
                "head": args.head,
                "input_rows": input_rows,
                "output_rows": int(len(out_df)),
            },
            "rows": rows,
            "row_count": int(len(rows)),
        }
        if args.output:
            payload["saved_to"] = args.output

        if args.json:
            emit_success(args, payload, mode="job_result")
        else:
            print(f"job_id: {job['job_id']}")
            print("\npipeline:")
            print(json.dumps(payload["pipeline"], ensure_ascii=False, indent=2))
            if len(out_df):
                print()
                print(out_df.to_string(index=False))
            else:
                print("\n(no rows)")
            print(f"\n合計: {len(out_df)}行")
            if args.output:
                print(f"saved_to: {args.output}")
        return 0

    # 既存動作: head/summary/metadata
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(artifact_path, args.output)

    payload = {
        "job_id": job["job_id"],
        "status": job["status"],
        "row_count": job.get("row_count"),
        "artifact_path": artifact_path,
        "log_path": job.get("log_path"),
    }
    if args.output:
        payload["copied_to"] = args.output

    try:
        if args.head is not None:
            head_df = read_head(artifact_path, args.head)
            head_records = json.loads(head_df.to_json(orient="records", force_ascii=False))
            payload["head_rows"] = len(head_records)
            payload["head"] = head_records

        if args.summary:
            payload["summary"] = build_summary(artifact_path)
    except Exception as e:
        return emit_error(
            args,
            "RESULT_READ_FAILED",
            f"failed to read result artifact: {e}",
            "Check artifact file and options.",
        )

    if args.json:
        emit_success(args, payload, mode="job_result")
    else:
        print(f"job_id: {job['job_id']}")
        print(f"status: {job['status']}")
        print(f"row_count: {job.get('row_count')}")
        print(f"artifact_path: {artifact_path}")
        print(f"log_path: {job.get('log_path')}")
        if args.output:
            print(f"copied_to: {args.output}")
        if args.head is not None:
            print(f"\nhead: first {args.head} rows")
            if payload["head"]:
                # to_string表示用にDataFrameとして再読込
                preview_df = read_head(artifact_path, args.head)
                print(preview_df.to_string(index=False))
            else:
                print("(no rows)")
        if args.summary:
            print("\nsummary:")
            print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


def show_jobs(args, store: JobStore) -> int:
    jobs = store.list_jobs(limit=args.job_limit)
    if args.json:
        emit_success(args, {"jobs": jobs, "count": len(jobs)}, mode="list_jobs")
        return 0

    if not jobs:
        print("ジョブはありません")
        return 0

    print("job_id                           status     source     rows  created_at")
    for job in jobs:
        job_id = job["job_id"]
        status = job.get("status", "-")
        source = job.get("source", "-")
        rows = job.get("row_count")
        created_at = job.get("created_at", "-")
        rows_text = "-" if rows is None else str(rows)
        print(f"{job_id:32} {status:10} {source:10} {rows_text:5} {created_at}")
    return 0


def run_list_mode(args) -> tuple[bool, int]:
    """一覧表示モード"""
    if args.list_ga4_properties:
        try:
            props = get_ga4_properties()
        except Exception as e:
            return True, emit_error(
                args,
                "LIST_OPERATION_FAILED",
                f"failed to list GA4 properties: {e}",
                "Check credentials and GA4 permissions.",
            )
        if args.json:
            emit_success(args, {"properties": props, "count": len(props)}, mode="list_ga4_properties")
        else:
            print("GA4プロパティ一覧:")
            for p in props:
                print(f"  - {p['display']}")
        return True, 0

    if args.list_gsc_sites:
        try:
            sites = get_gsc_sites()
        except Exception as e:
            return True, emit_error(
                args,
                "LIST_OPERATION_FAILED",
                f"failed to list GSC sites: {e}",
                "Check credentials and Search Console permissions.",
            )
        if args.json:
            emit_success(args, {"sites": sites, "count": len(sites)}, mode="list_gsc_sites")
        else:
            print("GSCサイト一覧:")
            for s in sites:
                print(f"  - {s}")
        return True, 0

    if args.list_bq_datasets:
        if not args.project:
            return True, emit_error(
                args,
                "MISSING_REQUIRED_ARG",
                "--project is required for --list-bq-datasets",
                "Use --list-bq-datasets --project <gcp_project_id>.",
            )
        try:
            datasets = get_bq_datasets(args.project)
        except Exception as e:
            return True, emit_error(
                args,
                "LIST_OPERATION_FAILED",
                f"failed to list BigQuery datasets: {e}",
                "Check project_id and BigQuery permissions.",
            )
        if args.json:
            emit_success(
                args,
                {"project_id": args.project, "datasets": datasets, "count": len(datasets)},
                mode="list_bq_datasets",
            )
        else:
            print(f"データセット一覧 ({args.project}):")
            for ds in datasets:
                print(f"  - {ds}")
        return True, 0

    return False, 0


def main():
    parser = argparse.ArgumentParser(description="Unified Query CLI")
    parser.add_argument("--params", default="input/params.json", help="Strict params JSON path (schema_version=1.0)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--output", help="CSV output file path")
    parser.add_argument("--submit", action="store_true", help="Submit job asynchronously")
    parser.add_argument("--status", help="Show job status by job_id")
    parser.add_argument("--cancel", help="Cancel job by job_id")
    parser.add_argument("--result", help="Show job result by job_id")
    parser.add_argument("--head", type=int, help="Show first N rows with --result")
    parser.add_argument("--summary", action="store_true", help="Show summary stats with --result")
    parser.add_argument("--where", help="Filter rows (pandas query expression)")
    parser.add_argument("--sort", help="Sort rows (e.g. 'clicks DESC,ctr ASC')")
    parser.add_argument("--columns", help="Select columns (comma-separated)")
    parser.add_argument("--group-by", help="Group by columns (comma-separated)")
    parser.add_argument("--aggregate", help="Aggregate functions (e.g. 'sum:clicks,mean:ctr')")
    parser.add_argument("--transform", help="Transform columns (e.g. 'date:date_format,page:url_decode')")
    parser.add_argument("--list-jobs", action="store_true", help="List recent jobs")
    parser.add_argument("--job-limit", type=int, default=20, help="Max items for --list-jobs")
    parser.add_argument("--run-job", help=argparse.SUPPRESS)

    # List options
    parser.add_argument("--list-ga4-properties", action="store_true", help="List GA4 properties")
    parser.add_argument("--list-gsc-sites", action="store_true", help="List GSC sites")
    parser.add_argument("--list-bq-datasets", action="store_true", help="List BigQuery datasets")
    parser.add_argument("--project", help="GCP project ID for --list-bq-datasets")
    args = parser.parse_args()
    store = JobStore(os.environ.get("QUERY_JOB_DIR", "output/jobs"))

    handled, code = run_list_mode(args)
    if handled:
        return code

    action_mode_used = any(
        [
            args.run_job,
            args.submit,
            args.status,
            args.cancel,
            args.result,
            args.list_jobs,
        ]
    )

    if args.head is not None and args.head <= 0:
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--head must be a positive integer",
            "Use --head 1 or greater.",
        )

    pipeline_opts_used = has_pipeline_opts(args)
    # --params 同期実行時（action_modeなし）はCLIパイプライン引数を禁止（params.json の pipeline を使う）
    is_sync_query = not action_mode_used and not args.submit
    if is_sync_query and (pipeline_opts_used or args.head is not None):
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "Pipeline options (--where/--sort/--head/etc.) cannot be used with --params. Use pipeline field in params.json instead.",
            'Move pipeline options to params.json: {"pipeline": {"where": "...", ...}}',
        )
    if pipeline_opts_used and action_mode_used and not args.result:
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--where/--sort/--columns/--group-by/--aggregate cannot be used with this action",
            "Use pipeline options with --result <job_id>.",
        )
    if (args.group_by and not args.aggregate) or (args.aggregate and not args.group_by):
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--group-by and --aggregate must be used together",
            "Specify both --group-by and --aggregate.",
        )
    if args.summary and pipeline_opts_used:
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--summary cannot be combined with pipeline options",
            "Use either --summary or pipeline options.",
        )
    if args.summary and not args.result:
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--summary must be used with --result",
            "Use --result <job_id> with --summary.",
        )
    if args.head is not None and action_mode_used and not args.result:
        return emit_error(
            args,
            "INVALID_ARGUMENT",
            "--head cannot be used with this action",
            "Use --head with --result <job_id>, or set pipeline.head in params.json for direct query.",
        )

    if args.run_job:
        return run_job(args.run_job, store)

    if args.submit:
        return submit_job(args, store)

    if args.status:
        return show_job_status(args.status, args, store)

    if args.cancel:
        return cancel_job(args.cancel, args, store)

    if args.result:
        return show_job_result(args.result, args, store)

    if args.list_jobs:
        return show_jobs(args, store)

    params, err = load_params(args.params)
    if err:
        return emit_error(args, **err)

    try:
        df, header_lines = execute_query_from_params(params)
        if df is None or df.empty:
            return emit_error(
                args,
                "NO_DATA",
                "No data returned from query.",
                "Check date range, filters, and property/site settings.",
            )

        pipeline_info = None
        # params.json の pipeline フィールドからパイプライン適用
        pipeline_conf = params.get("pipeline") or {}
        if pipeline_conf:
            try:
                input_rows = int(len(df))
                df = apply_pipeline(
                    df,
                    transform=pipeline_conf.get("transform"),
                    where=pipeline_conf.get("where"),
                    group_by=pipeline_conf.get("group_by"),
                    aggregate=pipeline_conf.get("aggregate"),
                    sort=pipeline_conf.get("sort"),
                    columns=pipeline_conf.get("columns"),
                    head=pipeline_conf.get("head"),
                )
                pipeline_info = {
                    "transform": pipeline_conf.get("transform"),
                    "where": pipeline_conf.get("where"),
                    "sort": pipeline_conf.get("sort"),
                    "columns": pipeline_conf.get("columns"),
                    "group_by": pipeline_conf.get("group_by"),
                    "aggregate": pipeline_conf.get("aggregate"),
                    "head": pipeline_conf.get("head"),
                    "input_rows": input_rows,
                    "output_rows": int(len(df)),
                }
            except ValueError as e:
                code, hint = map_pipeline_error(str(e))
                return emit_error(args, code, str(e), hint)

        if not args.json and not args.output:
            for line in header_lines:
                print(line)
            print()

        output_result(df, args, pipeline=pipeline_info)
        return 0
    except ValueError as e:
        return emit_error(
            args,
            "INVALID_QUERY",
            str(e),
            "Check params and query fields.",
        )
    except Exception as e:
        return emit_error(
            args,
            "QUERY_EXECUTION_FAILED",
            str(e),
            "Check source credentials and query parameters.",
        )


if __name__ == "__main__":
    sys.exit(main())
