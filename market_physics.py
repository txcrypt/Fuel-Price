import pandas as pd
import numpy as np
import os
import json
from datetime import timedelta

def calculate_margin_pressure(retail_median, tgp):
    """
    Calculates the Gross Retail Margin.
    """
    if retail_median is None or tgp is None: return 0.0
    return retail_median - tgp

def estimate_hike_probability(margin, duration, tgp_trend_val):
    """
    Heuristic Approximation of MS-VAR Model.
    
    Inputs:
        margin (float): Current Gross Margin (cpl).
        duration (int): Days since last hike start.
        tgp_trend_val (float): Daily change in TGP (cpl).
    
    Returns:
        float: Probability of Hike (0.0 to 1.0)
    """
    prob = 0.0
    
    # 1. Margin Pressure (The Squeeze)
    if margin < 2.0:
        prob += 0.50
    if margin < 0.0:
        prob += 0.30 # Cumulative (Total 80%)
        
    # 2. Cycle Duration (Time Pressure)
    if duration > 35:
        prob += 0.10
        
    # 3. Super-Cycle Effect (Falling Costs delay hikes)
    # If TGP is dropping fast, retailers ride the margin expansion down.
    if tgp_trend_val < -0.5:
        prob -= 0.40
        
    return max(0.0, min(1.0, prob))

def analyze_cycles(daily_df):
    """
    Analyzes historical cycles to find average length and last hike date.
    Kept for backward compatibility and duration calculation.
    """
    if daily_df is None or daily_df.empty:
        return 21, 14, pd.Timestamp.now() - timedelta(days=10) # Fallbacks

    # Identify Hikes: Large jump in median price
    daily_df = daily_df.sort_values('day')
    daily_df['delta'] = daily_df['price_cpl'].diff()
    
    # Threshold for "Cycle Hike": > 10c jump in 1 day (on median)
    hikes = daily_df[daily_df['delta'] > 8.0]
    
    if hikes.empty:
        return 30, 25, daily_df['day'].min()
        
    last_hike_date = hikes['day'].iloc[-1]
    
    # Calculate average cycle length
    if len(hikes) > 1:
        cycle_lens = hikes['day'].diff().dt.days.dropna()
        avg_len = cycle_lens.mean()
        # Relenting phase length approx 80% of cycle
        avg_relent = avg_len * 0.8
        return int(avg_len), int(avg_relent), last_hike_date
    else:
        return 35, 28, last_hike_date

def detect_market_volatility(current_df, history_df):
    """
    Replaces brand-specific leader detection with market-wide volatility.
    """
    if current_df is None or history_df is None: return 0.0
    
    # Merge
    merged = current_df.merge(history_df, on='site_id', suffixes=('', '_prev'))
    if merged.empty: return 0.0
    
    # Calculate price changes
    changes = merged['price_cpl'] - merged['price_cpl_prev']
    
    # Volatility = Standard Deviation of changes
    # Stable market: most changes are ~0 or small uniform drops. StdDev is low.
    # Hike start: Some up 40c, some 0. StdDev is high.
    return changes.std()

def predict_status(current_median, tgp, duration, tgp_trend_val, volatility):
    """
    Orchestrates the physics model to determine status.
    """
    margin = calculate_margin_pressure(current_median, tgp)
    prob = estimate_hike_probability(margin, duration, tgp_trend_val)
    
    # Logic Interpretation
    status = "STABLE"
    advice = "Hold"
    
    if volatility > 5.0: # High variance = Chaos = Hike Started
        status = "HIKE_STARTED"
        advice = "FILL NOW"
    elif prob > 0.7:
        status = "HIKE_IMMINENT"
        advice = "FILL NOW"
    elif prob > 0.4:
        status = "WARNING"
        advice = "Top Up"
    elif margin > 20.0:
        status = "DROPPING"
        advice = "Wait"
    elif margin < 5.0:
        status = "BOTTOM"
        advice = "Buy"
    
    return {
        'status': status,
        'advice': advice,
        'hike_probability': round(prob * 100, 1),
        'margin': round(margin, 2),
        'volatility': round(volatility, 2)
    }

def load_daily_data(force_refresh=False, state="QLD"):
    """
    Loads daily median prices for a specific state.
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
            # If state column is missing, we can't filter other states
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
