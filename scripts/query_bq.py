#!/usr/bin/env python
"""BigQueryクエリ実行CLI

使用例:
    # 基本
    python scripts/query_bq.py --project my-project --sql "SELECT * FROM dataset.table LIMIT 10"
    
    # SQLファイルから読み込み
    python scripts/query_bq.py --project my-project --file query.sql
    
    # JSON出力
    python scripts/query_bq.py --project my-project --sql "..." --json
    
    # CSVとして保存
    python scripts/query_bq.py --project my-project --sql "..." --output result.csv
    
    # データセット一覧
    python scripts/query_bq.py --project my-project --list-datasets
"""
import argparse
import json
import sys
from pathlib import Path

# libをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.megaton_client import query_bq, get_bq_datasets


def main():
    parser = argparse.ArgumentParser(description="BigQuery Query CLI")
    parser.add_argument("--project", required=True, help="GCPプロジェクトID")
    parser.add_argument("--sql", help="実行するSQL")
    parser.add_argument("--file", help="SQLファイルパス")
    parser.add_argument("--list-datasets", action="store_true", help="データセット一覧を表示")
    parser.add_argument("--json", action="store_true", help="JSON形式で出力")
    parser.add_argument("--output", help="CSV出力ファイルパス")
    args = parser.parse_args()
    
    # データセット一覧
    if args.list_datasets:
        datasets = get_bq_datasets(args.project)
        if args.json:
            print(json.dumps(datasets, ensure_ascii=False))
        else:
            print(f"データセット一覧 ({args.project}):")
            for ds in datasets:
                print(f"  - {ds}")
        return
    
    # SQLを取得
    sql = args.sql
    if args.file:
        sql = Path(args.file).read_text(encoding="utf-8")
    
    if not sql:
        parser.error("--sql または --file が必要です")
    
    # クエリ実行
    df = query_bq(args.project, sql)
    
    if df is None or df.empty:
        print("データが取得できませんでした", file=sys.stderr)
        return
    
    # 出力
    if args.output:
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"保存しました: {args.output} ({len(df)}行)")
    elif args.json:
        print(df.to_json(orient="records", force_ascii=False))
    else:
        print(df.to_string())
        print(f"\n合計: {len(df)}行")


if __name__ == "__main__":
    main()
