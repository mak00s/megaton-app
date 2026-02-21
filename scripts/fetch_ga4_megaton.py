"""Fetch daily Organic Search sessions from GA4 (via megaton)."""
from datetime import datetime, timedelta
from megaton import start
from megaton_lib.credentials import resolve_service_account_path

# Configuration
CREDS_PATH = resolve_service_account_path()
GA4_ACCOUNT = "YOUR_GA4_ACCOUNT_ID"
GA4_PROPERTY = "YOUR_GA4_PROPERTY_ID"

# Initialize (explicit JSON credentials, headless mode)
mg = start.Megaton(CREDS_PATH, headless=True)

# Select GA4 account and property
mg.ga['4'].account.select(GA4_ACCOUNT)
mg.ga['4'].property.select(GA4_PROPERTY)

print(f"Account: {GA4_ACCOUNT}, Property: {GA4_PROPERTY}")

# Date range (last 7 days)
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
mg.report.set.dates(start_date, end_date)
print(f"Period: {start_date} - {end_date}\n")

# Fetch daily Organic Search sessions
mg.report.run(
    d=['date'],
    m=['sessions'],
    filter_d='defaultChannelGroup==Organic Search',  # string expression
    show=False  # no visual output in headless mode
)

df = mg.report.data

if df is not None and not df.empty:
    df = df.sort_values('date')
    
    print("=== Organic Search Sessions by Date ===")
    print(f"{'Date':<12} {'Sessions':>10}")
    print("-" * 24)
    
    total = 0
    for _, row in df.iterrows():
        date = row['date']
        sessions = int(row['sessions'])
        total += sessions
        # Support both datetime.date and str(YYYYMMDD)
        if hasattr(date, 'strftime'):
            date_fmt = date.strftime('%Y-%m-%d')
        else:
            date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        print(f"{date_fmt:<12} {sessions:>10,}")
    
    print("-" * 24)
    print(f"{'Total':<12} {total:>10,}")
else:
    print("No data was returned.")
