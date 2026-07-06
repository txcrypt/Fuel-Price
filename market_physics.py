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
        available_cols = pd.read_csv(HISTORY_FILE, nrows=0).columns.tolist()
        desired_cols = ['price_cpl', 'reported_at', 'scraped_at', 'state']
        use_cols = [c for c in desired_cols if c in available_cols]
        if 'price_cpl' not in use_cols or ('scraped_at' not in use_cols and 'reported_at' not in use_cols):
            return pd.DataFrame(columns=['day', 'price_cpl'])
        col = 'scraped_at' if 'scraped_at' in use_cols else 'reported_at'
        daily_values = {}

        for chunk in pd.read_csv(HISTORY_FILE, usecols=use_cols, chunksize=50000):
            if 'state' in chunk.columns:
                chunk = chunk[chunk['state'].astype(str).str.upper() == state.upper()].copy()
            elif state != "QLD":
                return pd.DataFrame(columns=['day', 'price_cpl'])

            if chunk.empty:
                continue

            chunk['price_cpl'] = pd.to_numeric(chunk['price_cpl'], errors='coerce')
            chunk['date'] = pd.to_datetime(chunk[col], errors='coerce')
            chunk = chunk.dropna(subset=['date', 'price_cpl'])
            chunk = chunk[(chunk['price_cpl'] > 80) & (chunk['price_cpl'] < 350)]
            if chunk.empty:
                continue

            chunk['day'] = chunk['date'].dt.normalize()
            for day, prices in chunk.groupby('day')['price_cpl']:
                daily_values.setdefault(day, []).extend(prices.astype(float).tolist())

        if not daily_values:
            return pd.DataFrame(columns=['day', 'price_cpl'])

        daily_df = pd.DataFrame(
            {'day': day, 'price_cpl': float(pd.Series(values).median())}
            for day, values in daily_values.items()
        ).sort_values('day')
        
        # Save Cache
        daily_df['day_str'] = daily_df['day'].dt.strftime('%Y-%m-%d')
        cache_data = daily_df[['day_str', 'price_cpl']].rename(columns={'day_str': 'day'}).to_dict(orient='records')
        
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
            
        return daily_df[['day', 'price_cpl']]
        
    except Exception as e:
        print(f"❌ Daily Data Load Error: {e}")
        return pd.DataFrame()
