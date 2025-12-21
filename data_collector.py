import pandas as pd
import os
import sys
from datetime import datetime

# Add parent dir to path to import Working_Version modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Working_Version.fuel_engine import FuelEngine

# Configuration
COLLECTION_FILE = os.path.join(os.path.dirname(__file__), "brisbane_fuel_live_collection.csv")
TOKEN = "028c992c-dc6a-4509-a94b-db707308841d" # Using the known token

def collect_live_data():
    """
    Fetches live fuel data and appends it to the collection CSV.
    Returns the number of records added.
    """
    print(f"🔄 Starting Data Collection at {datetime.now()}...")
    
    try:
        engine = FuelEngine(TOKEN)
        snapshot = engine.get_market_snapshot()
        
        if snapshot is None or snapshot.empty:
            print("⚠️ No data fetched from API.")
            return 0
            
        # Align with Master File Schema:
        # site_id, price_cpl, reported_at, region, latitude, longitude
        
        # Select and order columns
        # Note: snapshot has 'reported_at' from the API transaction time.
        # We might also want 'scraped_at' for our own records, but to match 'master', we stick to these.
        # However, for a *new* collection file, keeping extra metadata is useful.
        # The user asked to store it for "training and validation". 
        # Let's keep the core columns + scraped_at to know when WE saw it.
        
        cols_to_keep = ['site_id', 'price_cpl', 'reported_at', 'region', 'latitude', 'longitude', 'scraped_at']
        
        # Ensure all columns exist
        for col in cols_to_keep:
            if col not in snapshot.columns:
                snapshot[col] = None
                
        final_df = snapshot[cols_to_keep]
        
        # Append to CSV
        write_header = not os.path.exists(COLLECTION_FILE)
        
        final_df.to_csv(COLLECTION_FILE, mode='a', header=write_header, index=False)
        
        count = len(final_df)
        print(f"✅ Successfully collected {count} records.")
        print(f"📂 Saved to: {COLLECTION_FILE}")
        
        return count
        
    except Exception as e:
        print(f"❌ Collection Failed: {e}")
        return 0

if __name__ == "__main__":
    collect_live_data()
