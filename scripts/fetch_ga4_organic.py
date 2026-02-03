"""GA4 から Organic Search のセッション数を日別で取得"""
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    FilterExpression,
    Filter,
    OrderBy,
)
from google.oauth2 import service_account

# 認証情報
CREDS_PATH = "credentials/sa-shibuya-kyousei.json"

# GA4 プロパティ ID（Notebookで確認した値を指定）
# 未設定の場合はアカウント一覧を表示して終了
PROPERTY_ID = "254470346"  # 渋谷 - GA4

# サービスアカウントで認証
credentials = service_account.Credentials.from_service_account_file(
    CREDS_PATH,
    scopes=["https://www.googleapis.com/auth/analytics.readonly"]
)

# プロパティIDが未設定の場合、アカウント一覧を表示
if not PROPERTY_ID:
    from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
    admin_client = AnalyticsAdminServiceClient(credentials=credentials)
    
    print("=== GA4 アカウント・プロパティ一覧 ===")
    print("（PROPERTY_ID を設定してください）\n")
    
    # list_account_summaries を使用
    for summary in admin_client.list_account_summaries():
        print(f"{summary.display_name} (Account: {summary.account.split('/')[-1]})")
        for prop in summary.property_summaries:
            prop_id = prop.property.split('/')[-1]
            print(f"  - {prop.display_name} (Property ID: {prop_id})")
    
    print("\n上記から PROPERTY_ID を選んでスクリプトに設定してください")
    exit(0)

# GA4 Data API クライアント
client = BetaAnalyticsDataClient(credentials=credentials)

# 期間設定（直近7日）
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"Property ID: {PROPERTY_ID}")
print(f"期間: {start_date} 〜 {end_date}\n")

# レポートリクエスト
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    dimensions=[Dimension(name="date")],
    metrics=[Metric(name="sessions")],
    dimension_filter=FilterExpression(
        filter=Filter(
            field_name="sessionDefaultChannelGroup",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value="Organic Search"
            )
        )
    ),
    order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))]
)

response = client.run_report(request)

# 結果表示
print("=== Organic Search 日別セッション数 ===")
print(f"{'日付':<12} {'セッション':>10}")
print("-" * 24)

total = 0
for row in response.rows:
    date = row.dimension_values[0].value
    sessions = int(row.metric_values[0].value)
    total += sessions
    # 日付フォーマット
    date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    print(f"{date_fmt:<12} {sessions:>10,}")

print("-" * 24)
print(f"{'合計':<12} {total:>10,}")
