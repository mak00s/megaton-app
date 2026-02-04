#!/usr/bin/env python
"""GSCクエリ実行CLI"""
import argparse
import json
from datetime import datetime, timedelta
from megaton import start

CREDS_PATH = "credentials/sa-shibuya-kyousei.json"

def main():
    parser = argparse.ArgumentParser(description="GSC Query CLI")
    parser.add_argument("--site", default="https://www.shibuyakyousei.jp/", help="Site URL")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Days to look back")
    parser.add_argument("--dimensions", default="query", help="Dimensions (comma-separated)")
    parser.add_argument("--limit", type=int, default=20, help="Result limit")
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
    mg.search.use(args.site)
    mg.search.set.dates(start_date, end_date)
    
    # クエリ実行
    dimensions = args.dimensions.split(",")
    mg.search.run(dimensions=dimensions, metrics=["clicks", "impressions", "ctr", "position"])
    
    df = mg.search.data
    if df is None or df.empty:
        print("No data")
        return
    
    df = df.sort_values("clicks", ascending=False).head(args.limit)
    
    if args.json:
        print(df.to_json(orient="records", force_ascii=False))
    else:
        print(f"期間: {start_date} 〜 {end_date}")
        print(f"サイト: {args.site}")
        print()
        print(df.to_string(index=False))

if __name__ == "__main__":
    main()
