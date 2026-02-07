#!/usr/bin/env python
"""統合クエリ実行CLI（GA4 / GSC / BigQuery + Job管理）

使用例:
    # params.json から同期実行
    python scripts/query.py --params input/params.json

    # ジョブ投入（非同期）
    python scripts/query.py --submit --params input/params.json

    # ジョブ状態確認
    python scripts/query.py --status <job_id>

    # ジョブ結果確認
    python scripts/query.py --result <job_id>
"""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

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


def load_params(params_path: str) -> dict | None:
    """params.jsonを読み込み・検証"""
    try:
        with open(params_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        print(f"paramsファイルが見つかりません: {params_path}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"JSONパースエラー: {e}", file=sys.stderr)
        return None

    params, errors = validate_params(raw)
    if errors:
        print("params検証エラー:", file=sys.stderr)
        for err in errors:
            print(
                f"- [{err['error_code']}] {err['path']}: {err['message']} ({err['hint']})",
                file=sys.stderr,
            )
        return None

    return params


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


def output_result(df, args):
    """結果出力"""
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"保存しました: {args.output} ({len(df)}行)")
    elif args.json:
        print(df.to_json(orient="records", force_ascii=False))
    else:
        print(df.to_string(index=False))
        print(f"\n合計: {len(df)}行")


def submit_job(args, store: JobStore) -> int:
    params = load_params(args.params)
    if not params:
        return 1

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
        "status": "queued",
        "log_path": str(log_path),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"ジョブを投入しました: {job_id}")
        print(f"ログ: {log_path}")
    return 0


def run_job(job_id: str, store: JobStore) -> int:
    job = store.load_job(job_id)
    if not job:
        print(f"jobが見つかりません: {job_id}", file=sys.stderr)
        return 1

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
        print(f"jobが見つかりません: {job_id}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(job, ensure_ascii=False))
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
        print(f"jobが見つかりません: {job_id}", file=sys.stderr)
        return 1

    if job["status"] != "succeeded":
        print(f"jobは未完了です: status={job['status']}", file=sys.stderr)
        return 1

    artifact_path = job.get("artifact_path")
    if not artifact_path:
        print("artifact_path がありません", file=sys.stderr)
        return 1

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

    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(f"job_id: {job['job_id']}")
        print(f"status: {job['status']}")
        print(f"row_count: {job.get('row_count')}")
        print(f"artifact_path: {artifact_path}")
        print(f"log_path: {job.get('log_path')}")
        if args.output:
            print(f"copied_to: {args.output}")
    return 0


def show_jobs(args, store: JobStore) -> int:
    jobs = store.list_jobs(limit=args.job_limit)
    if args.json:
        print(json.dumps(jobs, ensure_ascii=False))
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


def run_list_mode(args) -> tuple[bool, bool]:
    """一覧表示モード"""
    if args.list_ga4_properties:
        props = get_ga4_properties()
        if args.json:
            print(json.dumps(props, ensure_ascii=False))
        else:
            print("GA4プロパティ一覧:")
            for p in props:
                print(f"  - {p['display']}")
        return True, True

    if args.list_gsc_sites:
        sites = get_gsc_sites()
        if args.json:
            print(json.dumps(sites, ensure_ascii=False))
        else:
            print("GSCサイト一覧:")
            for s in sites:
                print(f"  - {s}")
        return True, True

    if args.list_bq_datasets:
        if not args.project:
            print("--list-bq-datasets には --project が必要です", file=sys.stderr)
            return True, False
        datasets = get_bq_datasets(args.project)
        if args.json:
            print(json.dumps(datasets, ensure_ascii=False))
        else:
            print(f"データセット一覧 ({args.project}):")
            for ds in datasets:
                print(f"  - {ds}")
        return True, True

    return False, True


def main():
    parser = argparse.ArgumentParser(description="Unified Query CLI")
    parser.add_argument("--params", default="input/params.json", help="Strict params JSON path (schema_version=1.0)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--output", help="CSV output file path")
    parser.add_argument("--submit", action="store_true", help="Submit job asynchronously")
    parser.add_argument("--status", help="Show job status by job_id")
    parser.add_argument("--result", help="Show job result by job_id")
    parser.add_argument("--list-jobs", action="store_true", help="List recent jobs")
    parser.add_argument("--job-limit", type=int, default=20, help="Max items for --list-jobs")
    parser.add_argument("--run-job", help=argparse.SUPPRESS)

    # List options
    parser.add_argument("--list-ga4-properties", action="store_true", help="List GA4 properties")
    parser.add_argument("--list-gsc-sites", action="store_true", help="List GSC sites")
    parser.add_argument("--list-bq-datasets", action="store_true", help="List BigQuery datasets")
    parser.add_argument("--project", help="GCP project ID for --list-bq-datasets")
    args = parser.parse_args()
    store = JobStore()

    handled, ok = run_list_mode(args)
    if handled:
        return 0 if ok else 1

    if args.run_job:
        return run_job(args.run_job, store)

    if args.submit:
        return submit_job(args, store)

    if args.status:
        return show_job_status(args.status, args, store)

    if args.result:
        return show_job_result(args.result, args, store)

    if args.list_jobs:
        return show_jobs(args, store)

    params = load_params(args.params)
    if not params:
        return 1

    try:
        df, header_lines = execute_query_from_params(params)
        if df is None or df.empty:
            print("データが取得できませんでした", file=sys.stderr)
            return 1

        if not args.json and not args.output:
            for line in header_lines:
                print(line)
            print()

        output_result(df, args)
        return 0
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
