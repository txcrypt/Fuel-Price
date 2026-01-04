import pandas as pd
import os

HISTORY_FILE = "brisbane_fuel_live_collection.csv"

try:
    print(f"Loading {HISTORY_FILE}...")
    df = pd.read_csv(HISTORY_FILE)
    print(f"Loaded {len(df)} rows.")
    
    col = 'reported_at' if 'reported_at' in df.columns else 'scraped_at'
    print(f"Using date column: {col}")
    
    df['date'] = pd.to_datetime(df[col], errors='coerce')
    df['day'] = df['date'].dt.normalize()
    
    daily = df.groupby('day')['price_cpl'].median().reset_index().sort_values('day')
    print("Daily aggregation successful.")
    print(daily.tail())
except Exception as e:
    print(f"ERROR: {e}")
