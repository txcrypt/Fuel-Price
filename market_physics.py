import pandas as pd
import numpy as np
import os
import json

def load_daily_data(force_refresh=False, state="QLD"):
    """
    Loads daily median prices for a specific state.
    Used by the AI Engine to generate forecasts.
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    HISTORY_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
    CACHE_FILE = os.path.join(BASE_DIR, f"daily_stats_{state}.json")
    
    # 1. Try Cache
    if not force_refresh and os.path.exists(CACHE_FILE):
        try:
            mtime = os.path.getmtime(CACHE_FILE)
            if (pd.Timestamp.now().timestamp() - mtime) < 3600:
                with open(CACHE_FILE, 'r') as f:
                    data = json.load(f)
                df = pd.DataFrame(data)
                df['day'] = pd.to_datetime(df['day'])
                return df
        except Exception as e:
            print(f"⚠️ Cache load failed for {state}: {e}")

    # 2. Build from CSV
    if not os.path.exists(HISTORY_FILE): 
        return pd.DataFrame(columns=['day', 'price_cpl'])
    
    try:
        # Optimized load: Only needed columns
        df = pd.read_csv(HISTORY_FILE, usecols=['price_cpl', 'reported_at', 'scraped_at', 'state'])
        
        # Filter by state
        if 'state' in df.columns:
            df = df[df['state'] == state].copy()
        elif state != "QLD":
            return pd.DataFrame(columns=['day', 'price_cpl'])
        
        # Prefer scraped_at
        col = 'scraped_at' if 'scraped_at' in df.columns else 'reported_at'
        
        df['date'] = pd.to_datetime(df[col], errors='coerce')
        df = df.dropna(subset=['date', 'price_cpl'])
        df['day'] = df['date'].dt.normalize()
        
        # Group
        daily_df = df.groupby('day')['price_cpl'].median().reset_index().sort_values('day')
        
        # Save Cache
        daily_df['day_str'] = daily_df['day'].dt.strftime('%Y-%m-%d')
        cache_data = daily_df[['day_str', 'price_cpl']].rename(columns={'day_str': 'day'}).to_dict(orient='records')
        
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
            
        return daily_df[['day', 'price_cpl']]
        
    except Exception as e:
        print(f"❌ Daily Data Load Error: {e}")
        return pd.DataFrame()