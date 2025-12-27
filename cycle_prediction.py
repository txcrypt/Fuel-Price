import pandas as pd
import numpy as np
import os
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# We assume this file contains historical data
HISTORY_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")

def load_data(file_path=HISTORY_FILE):
    """
    Loads fuel price data from CSV.
    """
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        
        # Standardize Columns
        rename_map = {
            'TransactionDateutc': 'reported_at',
            'Price': 'price_cpl',
            'SiteId': 'site_id',
            'Brand': 'brand'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Parse Dates
        ts_col = 'reported_at' if 'reported_at' in df.columns else 'scraped_at'
        if ts_col in df.columns:
            df['date'] = pd.to_datetime(df[ts_col], format='mixed', errors='coerce')
        
        # Numeric Price
        df['price_cpl'] = pd.to_numeric(df['price_cpl'], errors='coerce')
        df.dropna(subset=['price_cpl'], inplace=True)
        
        if 'site_id' in df.columns:
            df['site_id'] = df['site_id'].astype(str)
            
        return df
    except Exception as e:
        print(f"Data Load Error: {e}")
        return None

def load_daily_data():
    """Helper for dashboard to get simple daily stats (median price)."""
    df = load_data()
    if df is None or 'date' not in df.columns: return None
    # Group by date (normalize to day)
    df['day'] = df['date'].dt.normalize()
    return df.groupby('day')['price_cpl'].median().reset_index().sort_values('day')

def detect_market_phase(current_prices_df, historical_prices_df, tgp=160.0):
    """
    Determines the current Edgeworth Cycle phase using 'Critical Mass' detection.
    
    Logic:
    1. RESTORATION (Hike): Confirmed if >7% of stations hiked >10c in the last 24h.
    2. BOTTOM: Median price is near wholesale floor (TGP + 12c).
    3. RELENTING: Default state (prices slowly falling).
    
    Args:
        current_prices_df (pd.DataFrame): Latest snapshot (must have site_id, price_cpl).
        historical_prices_df (pd.DataFrame): Data from ~24h ago (must have site_id, price_cpl).
        tgp (float): Terminal Gate Price (Wholesale) for benchmark.
        
    Returns:
        dict: {
            'phase': str, 
            'advice': str, 
            'stats': dict (metrics used for decision)
        }
    """
    # Defaults
    phase = "RELENTING"
    advice = "Prices falling. Wait if you can."
    stats = {'hiker_ratio': 0.0, 'median_price': 0.0, 'tgp': tgp}
    
    if current_prices_df is None or current_prices_df.empty:
        return {'phase': "UNKNOWN", 'advice': "Insufficient Data", 'stats': stats}

    # 1. Calculate Current Median
    current_median = current_prices_df['price_cpl'].median()
    stats['median_price'] = current_median
    
    # 2. Check for Restoration (Hike)
    # We need historical data to compare deltas
    is_restoration = False
    
    if historical_prices_df is not None and not historical_prices_df.empty:
        # Merge on site_id
        # Suffix x=Current, y=History
        merged = current_prices_df.merge(historical_prices_df, on='site_id', suffixes=('', '_prev'))
        
        if not merged.empty:
            # Calculate Price Delta
            merged['delta'] = merged['price_cpl'] - merged['price_cpl_prev']
            
            # Count Market Size (N) - Intersection of stations reporting both days
            total_stations = len(merged)
            
            # Count Hikers (> 10c increase)
            hikers = merged[merged['delta'] > 10.0]
            num_hikers = len(hikers)
            
            # Calculate Ratio
            hiker_ratio = num_hikers / total_stations if total_stations > 0 else 0.0
            stats['hiker_ratio'] = round(hiker_ratio, 3)
            stats['hikers_count'] = num_hikers
            stats['total_analyzed'] = total_stations
            
            # DECISION: Threshold 7% (0.07)
            if hiker_ratio >= 0.07:
                is_restoration = True
    
    # 3. Apply Decision Logic
    if is_restoration:
        phase = "RESTORATION"
        advice = "⚠️ ALERT: Prices spiking! Fill up immediately."
        
    elif current_median <= (tgp + 12.0):
        phase = "BOTTOM"
        advice = "✅ BUY NOW: Prices are at the bottom."
        
    else:
        # Default fallback
        phase = "RELENTING"
        advice = "Prices falling. Wait if you can."
        
    return {
        'phase': phase,
        'advice': advice,
        'stats': stats
    }
