"""GA4 から Organic Search のセッション数を日別で取得（megaton使用）"""
from datetime import datetime, timedelta
from megaton import start
from megaton_lib.credentials import resolve_service_account_path

# 設定
CREDS_PATH = resolve_service_account_path()
GA4_ACCOUNT = "YOUR_GA4_ACCOUNT_ID"
GA4_PROPERTY = "YOUR_GA4_PROPERTY_ID"

# 初期化（JSONファイルを直接指定、headlessモード）
mg = start.Megaton(CREDS_PATH, headless=True)

# GA4 アカウント・プロパティを選択
mg.ga['4'].account.select(GA4_ACCOUNT)
mg.ga['4'].property.select(GA4_PROPERTY)

print(f"Account: {GA4_ACCOUNT}, Property: {GA4_PROPERTY}")

# 期間設定（直近7日）
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
mg.report.set.dates(start_date, end_date)
print(f"期間: {start_date} 〜 {end_date}\n")

# Organic Search のセッション数を日別で取得
mg.report.run(
    d=['date'],
    m=['sessions'],
    filter_d='defaultChannelGroup==Organic Search',  # 文字列で指定
    show=False  # headlessモードでは表示しない
)

df = mg.report.data

if df is not None and not df.empty:
    df = df.sort_values('date')
    
    print("=== Organic Search 日別セッション数 ===")
    print(f"{'日付':<12} {'セッション':>10}")
    print("-" * 24)
    
    total = 0
    for _, row in df.iterrows():
        date = row['date']
        sessions = int(row['sessions'])
        total += sessions
        # 日付がdatetime.dateの場合とstr(YYYYMMDD)の場合に対応
        if hasattr(date, 'strftime'):
            date_fmt = date.strftime('%Y-%m-%d')
        else:
            date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        print(f"{date_fmt:<12} {sessions:>10,}")
    
    print("-" * 24)
    print(f"{'合計':<12} {total:>10,}")
else:
    print("データが取得できませんでした")
