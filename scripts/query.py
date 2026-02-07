#!/usr/bin/env python
"""統合クエリ実行CLI（GA4 / GSC / BigQuery）

使用例:
    # params.json から実行（sourceで自動分岐）
    python scripts/query.py --params input/params.json

    # JSON出力
    python scripts/query.py --params input/params.json --json

    # CSV保存
    python scripts/query.py --params input/params.json --output output/result.csv

    # リソース一覧
    python scripts/query.py --list-ga4-properties
    python scripts/query.py --list-gsc-sites
    python scripts/query.py --list-bq-datasets --project my-project
"""
import argparse
import json
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

    # List options
    parser.add_argument("--list-ga4-properties", action="store_true", help="List GA4 properties")
    parser.add_argument("--list-gsc-sites", action="store_true", help="List GSC sites")
    parser.add_argument("--list-bq-datasets", action="store_true", help="List BigQuery datasets")
    parser.add_argument("--project", help="GCP project ID for --list-bq-datasets")
    args = parser.parse_args()

    handled, ok = run_list_mode(args)
    if handled:
        return 0 if ok else 1

    params = load_params(args.params)
    if not params:
        return 1

    source = params["source"]
    try:
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
            if df is None or df.empty:
                print("データが取得できませんでした", file=sys.stderr)
                return 1
            if not args.json and not args.output:
                print(f"期間: {start_date} 〜 {end_date}")
                print(f"プロパティ: {params['property_id']}\n")
            output_result(df, args)
            return 0

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
            if df is None or df.empty:
                print("データが取得できませんでした", file=sys.stderr)
                return 1
            if "clicks" in df.columns:
                df = df.sort_values("clicks", ascending=False)
            if not args.json and not args.output:
                print(f"期間: {start_date} 〜 {end_date}")
                print(f"サイト: {params['site_url']}")
                if params.get("filter"):
                    print(f"フィルタ: {params['filter']}")
                print()
            output_result(df, args)
            return 0

        if source == "bigquery":
            df = query_bq(params["project_id"], params["sql"])
            if df is None or df.empty:
                print("データが取得できませんでした", file=sys.stderr)
                return 1
            if not args.json and not args.output:
                print(f"プロジェクト: {params['project_id']}\n")
            output_result(df, args)
            return 0

        print(f"不明なsourceです: {source}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
