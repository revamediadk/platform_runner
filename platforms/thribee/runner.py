from datetime import datetime
from platforms.common.markets_data import MARKETS_BY_SECTOR
from platforms.common.bigquery_utils import insert_to_bigquery
from .api import fetch_cost_and_campaigns
import traceback

def run_display_and_bigquery(start_date: datetime, end_date: datetime, mode: str):
    table_id = "big-query-database-376613.thribeeDaily.thribee_daily"  # Replace as needed

    for sector in MARKETS_BY_SECTOR:
        print(f"\n--- Sector: {sector['sector']} ---")
        for market in sector['markets']:
            website = market.get('website')
            thribee_id = market.get('thribee_id')
            ga4_property_id = market.get('ga4')

            if not thribee_id:
                print(f"[!] Skipping {website}: No thribee_id")
                continue

            print(f"\n📥 Fetching Thribee data for {website}...")
            try:
                market_data, campaign_data = fetch_cost_and_campaigns(
                    thribee_id, start_date, end_date, ga4_property_id
                )

                print(f"✅ Total Cost: {market_data['cost']}")
                if market_data.get('conversions') is not None:
                    print(f"🎯 GA4 Conversions: {market_data['conversions']}")
                else:
                    print("🎯 GA4 Conversions: Not available")

                print("📊 Campaigns:")
                for campaign in campaign_data:
                    print(
                        f"  - {campaign['name']}: Desktop = {campaign['desktopCost']}, Mobile = {campaign['mobileCost']}"
                    )

                row = {
                    "date": start_date.date().isoformat(),
                    "cost": market_data["cost"],
                    "conversions": market_data.get("conversions", 0),
                    "market_name": website,
                    "market_id": thribee_id,
                    "market_sector": sector["sector"]
                }
                print(f"📤 Inserting row to BigQuery: {row}")
                insert_to_bigquery([row], table_id)

            except Exception as e:
                print(f"[ERROR] Failed to process {website}: {e}")
                traceback.print_exc()
