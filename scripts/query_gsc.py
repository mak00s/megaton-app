#!/usr/bin/env python
"""GSCクエリ実行CLI

使用例:
    # 基本
    python scripts/query_gsc.py --days 14 --limit 1000
    
    # フィルタ付き（queryに「渋谷」を含む）
    python scripts/query_gsc.py --filter "query:contains:渋谷"
    
    # 複数フィルタ（セミコロン区切り）
    python scripts/query_gsc.py --filter "query:contains:渋谷;page:includingRegex:/blog/"
    
    # JSON出力
    python scripts/query_gsc.py --json
    
    # CSVとして保存
    python scripts/query_gsc.py --output result.csv
    
    # サイト一覧
    python scripts/query_gsc.py --list-sites

フィルタ書式: dimension:operator:expression
  演算子: contains, notContains, equals, notEquals, includingRegex, excludingRegex
"""
import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# libをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.megaton_client import query_gsc, get_gsc_sites

DEFAULT_SITE = "https://www.shibuyakyousei.jp/"


def parse_filter(filter_str: str) -> list:
    """フィルタ文字列をパース
    
    書式: dimension:operator:expression
    複数: dimension:operator:expression;dimension:operator:expression
    """
    if not filter_str:
        return None
    
    filters = []
    for part in filter_str.split(";"):
        parts = part.split(":", 2)  # 最大3分割（expressionに:が含まれる可能性）
        if len(parts) != 3:
            raise ValueError(f"Invalid filter format: {part}. Expected dimension:operator:expression")
        filters.append({
            "dimension": parts[0],
            "operator": parts[1],
            "expression": parts[2]
        })
    return filters


def main():
    parser = argparse.ArgumentParser(description="GSC Query CLI")
    parser.add_argument("--site", default=DEFAULT_SITE, help="Site URL")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Days to look back")
    parser.add_argument("--dimensions", default="query", help="Dimensions (comma-separated)")
    parser.add_argument("--filter", help="Filter as 'dimension:operator:expression' (semicolon for multiple)")
    parser.add_argument("--limit", type=int, default=1000, help="Result limit")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--output", help="CSV output file path")
    parser.add_argument("--list-sites", action="store_true", help="List GSC sites")
    args = parser.parse_args()
    
    # サイト一覧
    if args.list_sites:
        sites = get_gsc_sites()
        if args.json:
            print(json.dumps(sites, ensure_ascii=False))
        else:
            print("GSCサイト一覧:")
            for s in sites:
                print(f"  - {s}")
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
    
    # フィルタ解析
    dimension_filter = None
    if args.filter:
        try:
            dimension_filter = parse_filter(args.filter)
        except ValueError as e:
            print(f"エラー: {e}", file=sys.stderr)
            return
    
    # クエリ実行
    dimensions = args.dimensions.split(",")
    
    df = query_gsc(
        site_url=args.site,
        start_date=start_date,
        end_date=end_date,
        dimensions=dimensions,
        limit=args.limit,
        dimension_filter=dimension_filter
    )
    
    if df is None or df.empty:
        print("データが取得できませんでした", file=sys.stderr)
        return
    
    # クリック数でソート
    if "clicks" in df.columns:
        df = df.sort_values("clicks", ascending=False)
    
    # 出力
    if args.output:
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"保存しました: {args.output} ({len(df)}行)")
    elif args.json:
        print(df.to_json(orient="records", force_ascii=False))
    else:
        print(f"期間: {start_date} 〜 {end_date}")
        print(f"サイト: {args.site}")
        if dimension_filter:
            print(f"フィルタ: {args.filter}")
        print()
        print(df.head(args.limit if args.limit < len(df) else len(df)).to_string(index=False))
        print(f"\n合計: {len(df)}行")


if __name__ == "__main__":
    main()
