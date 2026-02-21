"""Fetch query-level Search Console data (via megaton)."""
from datetime import datetime, timedelta
from megaton import start
from megaton_lib.credentials import resolve_service_account_path

# Configuration
CREDS_PATH = resolve_service_account_path()
GSC_SITE_URL = ""  # e.g. "sc-domain:example.com"

# Initialize (headless mode)
mg = start.Megaton(CREDS_PATH, headless=True)

# If GSC_SITE_URL is unset, show available sites.
if not GSC_SITE_URL:
    print("=== Search Console Sites ===")
    print("(Set GSC_SITE_URL and re-run)\n")
    
    sites = mg.search.get.sites()
    for site in sites:
        print(f"  - {site}")
    
    print("\nPick a site from the list above and set GSC_SITE_URL in the script.")
    exit(0)

# Select site
mg.search.use(GSC_SITE_URL)
print(f"Site: {GSC_SITE_URL}")

# Date range (last 7 days)
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
mg.search.set.dates(start_date, end_date)
print(f"Period: {start_date} - {end_date}\n")

# Fetch query-level data
mg.search.run(
    dimensions=['query'],
    metrics=['clicks', 'impressions', 'ctr', 'position'],
)

df = mg.search.data

TOP_N = 20  # number of rows to display

if df is not None and not df.empty:
    # Sort by clicks
    df = df.sort_values('clicks', ascending=False)

    print(f"=== Query Performance (Top {TOP_N}) ===")
    print(f"{'Query':<40} {'Clicks':>8} {'Impr.':>10} {'CTR':>8} {'Pos':>6}")
    print("-" * 76)
    
    for _, row in df.head(TOP_N).iterrows():
        query = row['query'][:38] + '..' if len(row['query']) > 40 else row['query']
        clicks = int(row['clicks'])
        impressions = int(row['impressions'])
        ctr = float(row['ctr'])
        position = float(row['position'])
        print(f"{query:<40} {clicks:>8,} {impressions:>10,} {ctr:>7.1%} {position:>6.1f}")
    
    # Totals
    total_clicks = int(df['clicks'].sum())
    total_impressions = int(df['impressions'].sum())

    print("-" * 76)
    print(f"{'Total':<40} {total_clicks:>8,} {total_impressions:>10,}")
else:
    print("No data was returned.")
