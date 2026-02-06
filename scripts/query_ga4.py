#!/usr/bin/env python
"""GA4クエリ実行CLI

使用例:
    # 基本
    python scripts/query_ga4.py --days 7
    
    # フィルタ付き
    python scripts/query_ga4.py --filter "sessionDefaultChannelGroup==Organic Search"
    
    # JSON出力
    python scripts/query_ga4.py --json
    
    # CSVとして保存
    python scripts/query_ga4.py --output result.csv
"""
import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# libをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.megaton_client import query_ga4, get_ga4_properties

DEFAULT_PROPERTY = "254470346"


def main():
    parser = argparse.ArgumentParser(description="GA4 Query CLI")
    parser.add_argument("--property", default=DEFAULT_PROPERTY, help="Property ID")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Days to look back")
    parser.add_argument("--dimensions", default="date", help="Dimensions (comma-separated)")
    parser.add_argument("--metrics", default="sessions", help="Metrics (comma-separated)")
    parser.add_argument("--filter", help="Filter as 'field==value'")
    parser.add_argument("--limit", type=int, default=10000, help="Result limit")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--output", help="CSV output file path")
    parser.add_argument("--list-properties", action="store_true", help="List GA4 properties")
    args = parser.parse_args()
    
    # プロパティ一覧
    if args.list_properties:
        props = get_ga4_properties()
        if args.json:
            print(json.dumps(props, ensure_ascii=False))
        else:
            print("GA4プロパティ一覧:")
            for p in props:
                print(f"  - {p['display']}")
        return
    
    # 日付設定
    if args.end:
        end_date = args.end
    else:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if args.start:
        start_date = args.start
    else:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    # クエリ実行
    dimensions = args.dimensions.split(",")
    metrics = args.metrics.split(",")
    
    df = query_ga4(
        property_id=args.property,
        start_date=start_date,
        end_date=end_date,
        dimensions=dimensions,
        metrics=metrics,
        filter_d=args.filter,
        limit=args.limit
    )
    
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
        print(f"期間: {start_date} 〜 {end_date}")
        print(f"プロパティ: {args.property}")
        print()
        print(df.to_string(index=False))
        print(f"\n合計: {len(df)}行")


if __name__ == "__main__":
    main()
