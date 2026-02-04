"""Search Console からクエリ別データを取得（megaton使用）"""
from datetime import datetime, timedelta
from megaton import start

# 設定
CREDS_PATH = "credentials/sa-shibuya-kyousei.json"
GSC_SITE_URL = "https://www.shibuyakyousei.jp/"  # 渋谷矯正歯科

# 初期化（headlessモード）
mg = start.Megaton(CREDS_PATH, headless=True)

# サイトURLが未設定の場合、一覧を表示
if not GSC_SITE_URL:
    print("=== Search Console サイト一覧 ===")
    print("（GSC_SITE_URL を設定してください）\n")
    
    sites = mg.search.get.sites()
    for site in sites:
        print(f"  - {site}")
    
    print("\n上記から GSC_SITE_URL を選んでスクリプトに設定してください")
    exit(0)

# サイトを選択
mg.search.use(GSC_SITE_URL)
print(f"サイト: {GSC_SITE_URL}")

# 期間設定（直近7日）
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
mg.search.set.dates(start_date, end_date)
print(f"期間: {start_date} 〜 {end_date}\n")

# クエリ別データを取得
mg.search.run(
    dimensions=['query'],
    metrics=['clicks', 'impressions', 'ctr', 'position'],
)

df = mg.search.data

TOP_N = 20  # 表示件数

if df is not None and not df.empty:
    # クリック数でソート
    df = df.sort_values('clicks', ascending=False)
    
    print(f"=== クエリ別パフォーマンス（上位{TOP_N}件） ===")
    print(f"{'クエリ':<40} {'クリック':>8} {'表示':>10} {'CTR':>8} {'順位':>6}")
    print("-" * 76)
    
    for _, row in df.head(TOP_N).iterrows():
        query = row['query'][:38] + '..' if len(row['query']) > 40 else row['query']
        clicks = int(row['clicks'])
        impressions = int(row['impressions'])
        ctr = float(row['ctr'])
        position = float(row['position'])
        print(f"{query:<40} {clicks:>8,} {impressions:>10,} {ctr:>7.1%} {position:>6.1f}")
    
    # 全体集計
    total_clicks = int(df['clicks'].sum())
    total_impressions = int(df['impressions'].sum())
    
    print("-" * 76)
    print(f"{'合計':<40} {total_clicks:>8,} {total_impressions:>10,}")
else:
    print("データが取得できませんでした")
