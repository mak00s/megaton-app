#!/usr/bin/env python
"""GA4クエリ実行CLI"""
import argparse
import json
from datetime import datetime, timedelta
from megaton import start

CREDS_PATH = "credentials/sa-shibuya-kyousei.json"
DEFAULT_ACCOUNT = "141366107"
DEFAULT_PROPERTY = "254470346"

def main():
    parser = argparse.ArgumentParser(description="GA4 Query CLI")
    parser.add_argument("--account", default=DEFAULT_ACCOUNT, help="Account ID")
    parser.add_argument("--property", default=DEFAULT_PROPERTY, help="Property ID")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Days to look back")
    parser.add_argument("--dimensions", default="date", help="Dimensions (comma-separated)")
    parser.add_argument("--metrics", default="sessions", help="Metrics (comma-separated)")
    parser.add_argument("--filter", help="Filter as 'field==value'")
    parser.add_argument("--limit", type=int, default=100, help="Result limit")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()
    
    # 日付設定
    if args.end:
        end_date = args.end
    else:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if args.start:
        start_date = args.start
    else:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    # megaton初期化
    mg = start.Megaton(CREDS_PATH, headless=True)
    mg.ga["4"].account.select(args.account)
    mg.ga["4"].property.select(args.property)
    mg.report.set.dates(start_date, end_date)
    
    # フィルタ設定
    filters = None
    if args.filter:
        field, value = args.filter.split("==")
        filters = [(field, "==", value)]
    
    # クエリ実行
    dimensions = args.dimensions.split(",")
    metrics = args.metrics.split(",")
    
    mg.report.run(d=dimensions, m=metrics, filters=filters, limit=args.limit, show=False)
    
    df = mg.report.data
    if df is None or df.empty:
        print("No data")
        return
    
    if args.json:
        print(df.to_json(orient="records", force_ascii=False))
    else:
        print(f"期間: {start_date} 〜 {end_date}")
        print(f"プロパティ: {args.property}")
        print()
        print(df.to_string(index=False))

if __name__ == "__main__":
    main()
