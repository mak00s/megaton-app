"""Fetch daily Organic Search sessions from GA4."""
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
from megaton_lib.credentials import resolve_service_account_path

# Credentials
CREDS_PATH = resolve_service_account_path()

# GA4 property ID (use a value confirmed in your notebook)
# If unset, this script prints account/property candidates and exits.
PROPERTY_ID = ""  # e.g. "123456789"

# Authenticate with service account
credentials = service_account.Credentials.from_service_account_file(
    CREDS_PATH,
    scopes=["https://www.googleapis.com/auth/analytics.readonly"]
)

# If PROPERTY_ID is unset, show available accounts/properties.
if not PROPERTY_ID:
    from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
    admin_client = AnalyticsAdminServiceClient(credentials=credentials)
    
    print("=== GA4 Accounts and Properties ===")
    print("(Set PROPERTY_ID and re-run)\n")
    
    # Use list_account_summaries
    for summary in admin_client.list_account_summaries():
        print(f"{summary.display_name} (Account: {summary.account.split('/')[-1]})")
        for prop in summary.property_summaries:
            prop_id = prop.property.split('/')[-1]
            print(f"  - {prop.display_name} (Property ID: {prop_id})")
    
    print("\nPick a PROPERTY_ID from the list above and set it in the script.")
    exit(0)

# GA4 Data API client
client = BetaAnalyticsDataClient(credentials=credentials)

# Date range (last 7 days)
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
print(f"Property ID: {PROPERTY_ID}")
print(f"Period: {start_date} - {end_date}\n")

# Report request
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

# Print results
print("=== Organic Search Sessions by Date ===")
print(f"{'Date':<12} {'Sessions':>10}")
print("-" * 24)

total = 0
for row in response.rows:
    date = row.dimension_values[0].value
    sessions = int(row.metric_values[0].value)
    total += sessions
    # Date formatting
    date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    print(f"{date_fmt:<12} {sessions:>10,}")

print("-" * 24)
print(f"{'Total':<12} {total:>10,}")
