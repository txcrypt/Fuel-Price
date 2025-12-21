import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "cycle_prediction.json")
CONFIG_FILE = os.path.join(BASE_DIR, "algo_config.json")

def load_config():
    """Loads algorithm parameters from JSON or returns defaults."""
    default_config = {'hike_threshold': 8.0}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return default_config

def save_config(config):
    """Persists algorithm parameters."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def load_data():
    if not os.path.exists(MASTER_FILE):
        return None
    try:
        df = pd.read_csv(MASTER_FILE)
        rename_map = {}
        if 'TransactionDateutc' in df.columns: rename_map['TransactionDateutc'] = 'reported_at'
        if 'Price' in df.columns: rename_map['Price'] = 'price_cpl'
        if 'SiteId' in df.columns: rename_map['SiteId'] = 'site_id'
        if 'Brand' in df.columns: rename_map['Brand'] = 'brand'
        df.rename(columns=rename_map, inplace=True)
        
        # Robust date parsing
        ts_col = 'reported_at' if 'reported_at' in df.columns else 'scraped_at'
        if ts_col in df.columns:
            df['date'] = pd.to_datetime(df[ts_col], format='mixed', errors='coerce').dt.normalize()
        else:
            # If no date, assume today
            df['date'] = pd.Timestamp.now().normalize()
            
        df['price_cpl'] = pd.to_numeric(df['price_cpl'], errors='coerce')
        df.dropna(subset=['price_cpl'], inplace=True)
        if 'site_id' in df.columns: df['site_id'] = df['site_id'].astype(str)
        
        # Merge with metadata to get Brand
        meta_path = os.path.join(BASE_DIR, "station_metadata.csv")
        if os.path.exists(meta_path):
            try:
                meta_df = pd.read_csv(meta_path, dtype={'site_id': str})
                if 'brand' in meta_df.columns:
                    df = df.merge(meta_df[['site_id', 'brand']], on='site_id', how='left')
            except: pass
            
        return df
    except:
        return None

def load_daily_data():
    """Helper for dashboard to get simple daily stats"""
    df = load_data()
    if df is None: return None
    return df.groupby('date')['price_cpl'].median().reset_index().sort_values('date')

def analyze_cycles(daily_df, hike_threshold=None):
    # Load config if threshold not explicitly provided
    if hike_threshold is None:
        config = load_config()
        hike_threshold = config.get('hike_threshold', 8.0)

    daily_df['change'] = daily_df['price_cpl'].diff()
    hike_dates = daily_df[daily_df['change'] > hike_threshold]['date'].tolist()
    
    cycles = []
    for i in range(len(hike_dates) - 1):
        start = hike_dates[i]
        end = hike_dates[i+1]
        duration = (end - start).days
        
        cycle_data = daily_df[(daily_df['date'] >= start) & (daily_df['date'] < end)]
        if cycle_data.empty: continue
        min_price = cycle_data['price_cpl'].min()
        min_date = cycle_data[cycle_data['price_cpl'] == min_price]['date'].iloc[0]
        
        cycles.append({
            'total_days': duration,
            'relenting_days': (min_date - start).days
        })
        
    stats = pd.DataFrame(cycles)
    if stats.empty:
        return 30.0, 20.0, pd.Timestamp.now() # Fallbacks
        
    return stats['total_days'].mean(), stats['relenting_days'].mean(), hike_dates[-1] if hike_dates else pd.Timestamp.now()

def predict_status(avg_relenting, last_hike_date):
    today = pd.Timestamp.now().normalize()
    days_elapsed = (today - last_hike_date).days
    est_days_remaining = avg_relenting - days_elapsed
    
    prediction = {
        'last_hike': str(last_hike_date.date()),
        'days_elapsed': days_elapsed,
        'days_remaining': int(est_days_remaining) if est_days_remaining > 0 else 0
    }
    
    if days_elapsed < 3: prediction['status'] = 'HIKE'
    elif est_days_remaining > 3: prediction['status'] = 'DROPPING'
    elif est_days_remaining > 0: prediction['status'] = 'BOTTOM'
    else: prediction['status'] = 'OVERDUE'
    
    return prediction

def detect_leader_hike(live_data):
    """
    Detects if a price hike has started based on 'Price Leader' behavior.
    """
    if live_data.empty: return "UNKNOWN"
    
    # Ensure datetime
    ts_col = 'scraped_at' if 'scraped_at' in live_data.columns else 'reported_at'
    if ts_col not in live_data.columns: return "UNKNOWN"
    
    # Work on a copy to avoid SettingWithCopy warnings on the original df
    df = live_data.copy()
    df['ts'] = pd.to_datetime(df[ts_col], errors='coerce')
    
    # Check for 'name' or 'brand'
    cols_needed = [c for c in ['name', 'brand'] if c in df.columns]
    
    if not cols_needed:
        # Attempt to load metadata
        meta_path = os.path.join(BASE_DIR, "station_metadata.csv")
        if os.path.exists(meta_path):
            try:
                meta_df = pd.read_csv(meta_path, dtype={'site_id': str})
                # Ensure site_id compatibility
                if 'site_id' in df.columns and 'site_id' in meta_df.columns:
                    # Normalize IDs
                    df['site_id'] = df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
                    meta_df['site_id'] = meta_df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
                    
                    df = df.merge(meta_df[['site_id', 'name', 'brand']], on='site_id', how='left', suffixes=('', '_meta'))
                    # Coalesce
                    if 'name_meta' in df.columns: df['name'] = df['name'].fillna(df['name_meta'])
                    if 'brand_meta' in df.columns: df['brand'] = df['brand'].fillna(df['brand_meta'])
            except: pass
    
    # Check again
    if 'name' not in df.columns and 'brand' not in df.columns:
        return "UNKNOWN"
        
    # 1. Identify Daily Low (Floor)
    cutoff_24h = pd.Timestamp.now() - pd.Timedelta(hours=24)
    recent_data = df[df['ts'] > cutoff_24h]
    
    if recent_data.empty: return "UNKNOWN"
    daily_low = recent_data['price_cpl'].min()
    
    # 2. Filter for Leaders (BP, Coles Express, Ampol)
    # Priority: Brand column -> Name column
    mask = pd.Series(False, index=recent_data.index)
    
    if 'brand' in recent_data.columns:
        # Cast to string to avoid AttributeError if mixed types exist
        mask |= recent_data['brand'].astype(str).str.contains('BP|Coles|Ampol', case=False, na=False)
        
    if 'name' in recent_data.columns:
        # Cast to string to avoid AttributeError if mixed types exist
        mask |= recent_data['name'].astype(str).str.contains('BP|Coles|Ampol', case=False, na=False)
        
    leaders = recent_data[mask]
    
    if leaders.empty: return "STABLE"
    
    # 3. Calculate Recent Leader Avg (Last 4 Hours)
    cutoff_4h = pd.Timestamp.now() - pd.Timedelta(hours=4)
    current_leaders = leaders[leaders['ts'] > cutoff_4h]
    
    if current_leaders.empty: return "STABLE"
    
    leader_avg = current_leaders['price_cpl'].mean()
    
    # 4. Trigger Condition
    # If leaders are > 15c above the floor, it's a hike.
    if leader_avg > (daily_low + 15.0):
        return "HIKE_STARTED"
        
    return "STABLE"

def simple_profiling(df):
    """
    Robust profiling that works even with a single day of data.
    Classifies stations as Cheap/Expensive relative to the daily market average.
    """
    print("🧠 Running Competitor Profiling...")
    
    if df.empty: return pd.DataFrame()
    
    # Calculate Market Average per Day
    market_means = df.groupby('date')['price_cpl'].mean().reset_index()
    market_means.columns = ['date', 'market_avg']
    
    merged = df.merge(market_means, on='date', how='left')
    merged['diff'] = merged['price_cpl'] - merged['market_avg']
    
    # Group by Station to get average behavior
    station_stats = merged.groupby('site_id')['diff'].mean().reset_index()
    
    # Classify
    def classify(diff):
        if diff < -3.0: return "Discounter (Value)"
        elif diff > 3.0: return "Premium (Leader)"
        else: return "Market Follower"
        
    station_stats['strategy'] = station_stats['diff'].apply(classify)
    station_stats['discount_depth'] = station_stats['diff'] * -1 # Positive means cheaper
    
    return station_stats

def main():
    df = load_data()
    if df is None or df.empty:
        print("⚠️ No data for game theory.")
        return

    # 1. Profiling
    profiles = simple_profiling(df)
    profiles.to_csv(os.path.join(BASE_DIR, "station_profiles.csv"), index=False)
    
    # 2. Market Leaders (Simplified)
    leaders = {}
    if 'brand' in df.columns:
        brand_stats = df.groupby('brand')['price_cpl'].mean().sort_values(ascending=False).head(3)
        for brand, price in brand_stats.items():
            leaders[brand] = f"High Price Position (~{price:.1f}c)"
            
    # 3. Cycle Status
    avg_len = 30
    last_hike = pd.Timestamp.now() - timedelta(days=15) 
    
    daily = df.groupby('date')['price_cpl'].median().sort_index()
    if len(daily) > 5:
        diffs = daily.diff()
        hikes = diffs[diffs > 5.0]
        if not hikes.empty:
            last_hike = hikes.index[-1]
    
    result = {
        'avg_cycle_length': avg_len,
        'last_hike': str(last_hike.date()),
        'days_elapsed': (pd.Timestamp.now() - last_hike).days,
        'status': 'STABLE',
        'market_leaders': leaders
    }
    
    elapsed = result['days_elapsed']
    if elapsed < 5: result['status'] = 'HIKE'
    elif elapsed > 35: result['status'] = 'OVERDUE'
    elif elapsed > 20: result['status'] = 'BOTTOM'
    else: result['status'] = 'DROPPING'

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(result, f, indent=4)
        
    print("✅ Game Theory Analysis Complete.")

if __name__ == "__main__":
    main()