import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

# --- Constants ---
AIP_URL = "https://www.aip.com.au/pricing/terminal-gate-prices"
VIVA_URL = "https://www.vivaenergy.com.au/quick-links/terminal-gate-pricing"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def fetch_market_data(days=90):
    """
    Fetches market indicators: Brent Crude (Oil) and AUD/USD Exchange Rate.
    Returns a merged DataFrame.
    """
    import yfinance as yf
    
    # print("📉 Fetching Market Indicators (Oil & FX)...")
    try:
        # Brent Crude Oil (BZ=F)
        oil = yf.Ticker("BZ=F").history(period=f"{days+10}d")['Close'].rename("oil_price")
        
        # AUD to USD (AUD=X)    
        fx = yf.Ticker("AUD=X").history(period=f"{days+10}d")['Close'].rename("aud_fx")
        
        if oil.empty or fx.empty:
            raise ValueError("Empty data from yfinance")

        # Merge and clean
        df = pd.concat([oil, fx], axis=1).ffill().bfill()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        
        # Trim to requested days
        return df.tail(days)
    except Exception as e:
        print(f"❌ Error fetching market data: {e}. Using synthetic fallback.")
        dates = pd.date_range(end=datetime.now(), periods=days)
        df = pd.DataFrame({
            'oil_price': [75.0] * days,
            'aud_fx': [0.65] * days
        }, index=dates)
        return df

def fetch_live_tgp():
    """
    Scrapes the current Terminal Gate Price (Brisbane) from AIP (Primary) or Viva (Secondary).
    Returns float (cents per litre).
    """
    # 1. Try AIP
    try:
        r = requests.get(AIP_URL, headers={"User-Agent": USER_AGENT}, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            # AIP Table structure: Look for Brisbane row
            # Usually in a table with 'Brisbane' and 'ULP'
            for row in soup.find_all('tr'):
                text = row.get_text().upper()
                if "BRISBANE" in text:
                    # Look for value in columns
                    cols = row.find_all('td')
                    # ULP is usually the first or second numeric column
                    for col in cols:
                        val_text = col.get_text().strip()
                        try:
                            val = float(re.sub(r'[^\d.]', '', val_text))
                            if 100 < val < 250: # Sanity check
                                # print(f"✅ Scraped TGP (AIP): {val}c")
                                return val
                        except: continue
    except Exception as e:
        print(f"⚠️ AIP Fetch Failed: {e}")

    # 2. Try Viva Energy (Fallback)
    try:
        r = requests.get(VIVA_URL, headers={"User-Agent": USER_AGENT}, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for row in soup.find_all('tr'):
                text = row.get_text().upper()
                if "BRISBANE" in text:
                    cols = row.find_all('td')
                    for col in cols:
                        raw = col.get_text().strip()
                        if not raw or "BRISBANE" in raw.upper(): continue
                        try:
                            price = float(re.sub(r'[^\d.]', '', raw))
                            if 100 < price < 250:
                                # print(f"✅ Scraped TGP (Viva): {price}c")
                                return price
                        except: continue
    except Exception as e:
        print(f"⚠️ Viva Fetch Failed: {e}")

    return None

def get_tgp_history(days=90):
    """
    Returns a pandas Series of TGP history.
    Strategy:
    1. Get current LIVE TGP.
    2. Get Market Data (Oil, FX).
    3. Calculate 'Theoretical TGP' history based on Import Parity.
    4. Bias/Shift the theoretical curve so the last point matches the LIVE TGP.
    """
    # 1. Live Anchor
    live_tgp = fetch_live_tgp()
    if live_tgp is None:
        live_tgp = 170.0 # Emergency Fallback
        
    # 2. Market Drivers
    market_df = fetch_market_data(days)
    
    # 3. Calculate Import Parity (Theoretical)
    # MOPS95 ~ Brent + Crack Spread ($12 USD/bbl approx)
    # TGP = (Oil + 12) / 159 * FX_Rate * 100 + Quality_Premium + Taxes(Excise+GST)
    # Actually simpler: TGP follows Oil/FX with a lag.
    # We construct a shape curve.
    
    # Constants
    CRACK_SPREAD = 15.0 # USD/bbl
    # Note: This is a rough proxy for the shape, not exact accounting
    
    market_df['mogas_aud'] = (market_df['oil_price'] + CRACK_SPREAD) / market_df['aud_fx']
    
    # 4. Anchor to Real Reality
    # We want the last value of the series to be `live_tgp`
    # We calculate the scaling factor or offset needed
    
    last_theoretical = market_df['mogas_aud'].iloc[-1]
    
    # Using a simple linear scaling for shape preservation
    # TGP_est = A * Mogas_AUD + B. 
    # Let's just use a ratio for simplicity as we want the *Trend* primarily.
    # ratio = live_tgp / last_theoretical ( This converts the raw 'oil/fx number' to 'cents per litre' roughly)
    
    # However, Excise is fixed (~50c), GST is 10%. 
    # Better model: TGP = (Base_Cost * 1.1) + Excise. 
    # But strictly anchoring is safer for the "Physics" model.
    
    if last_theoretical > 0:
        ratio = live_tgp / last_theoretical
        tgp_series = market_df['mogas_aud'] * ratio
    else:
        tgp_series = pd.Series([live_tgp]*len(market_df), index=market_df.index)
        
    tgp_series.name = 'tgp'
    return tgp_series

def analyze_trend():
    """
    Returns the trend analysis for the Dashboard.
    """
    history = get_tgp_history(days=30)
    current_tgp = history.iloc[-1]
    
    # Trend (Last 7 days)
    delta_7d = current_tgp - history.iloc[-8] if len(history) > 7 else 0
    trend_direction = "RISING" if delta_7d > 0.5 else "FALLING" if delta_7d < -0.5 else "STABLE"
    
    # Singapore Lag (Proxy using Oil 10 days ago vs today)
    # In a real system we'd use MOPS95 prices. We use Oil as proxy.
    market = fetch_market_data(days=20)
    if len(market) > 10:
        oil_10_ago = market['oil_price'].iloc[-11]
        oil_now = market['oil_price'].iloc[-1]
        lag_delta = (oil_now - oil_10_ago)
        lag_msg = "ROCKET (Rising Cost)" if lag_delta > 2 else "FEATHER (Dropping Cost)" if lag_delta < -2 else "NEUTRAL"
    else:
        lag_msg = "UNKNOWN"

    return {
        'current_tgp': round(current_tgp, 2),
        'trend_direction': trend_direction,
        'delta_7d': round(delta_7d, 2),
        'import_parity_lag': lag_msg,
        'history': {
            'dates': history.index.strftime('%Y-%m-%d').tolist(),
            'values': history.round(2).tolist()
        }
    }