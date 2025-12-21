import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from functools import lru_cache

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Import Local Modules ---
# Ensure local modules are findable
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness
import cycle_prediction
import tgp_forecast
import data_collector
import station_metadata

# --- Configuration ---
app = FastAPI(
    title="Brisbane Fuel AI API",
    description="Backend for Fuel Price Analysis and Forecasting",
    version="2.0.0"
)

# CORS (Allow frontend access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Data Helpers ---

def clean_nan(obj):
    """Recursively clean NaN/Infinity for JSON compliance."""
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    return obj

@lru_cache(maxsize=1)
def get_cached_metadata():
    """Cached load of station metadata."""
    file_path = station_metadata.METADATA_FILE
    if not os.path.exists(file_path):
        station_metadata.generate_metadata()
    
    if os.path.exists(file_path):
        return pd.read_csv(file_path, dtype={'site_id': str, 'postcode': str})
    return pd.DataFrame()

@lru_cache(maxsize=1)
def get_cached_ratings():
    """Cached load of station ratings."""
    ratings_file = os.path.join(os.path.dirname(__file__), "station_ratings.csv")
    if os.path.exists(ratings_file):
        return pd.read_csv(ratings_file, dtype={'site_id': str})
    return pd.DataFrame()

def load_live_data_latest():
    """Loads the latest snapshot from the collection file."""
    if os.path.exists(data_collector.COLLECTION_FILE):
        try:
            df = pd.read_csv(data_collector.COLLECTION_FILE)
            if df.empty: return pd.DataFrame()
            
            # Ensure site_id is string
            if 'site_id' in df.columns:
                df['site_id'] = df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
            
            # Filter for latest per station
            # Assuming appended data, so last occurrence is newest? 
            # Better to sort by time if available
            ts_col = 'scraped_at' if 'scraped_at' in df.columns else 'reported_at'
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
                # Filter last 48h only
                cutoff = pd.Timestamp.now() - pd.Timedelta(hours=48)
                df = df[df[ts_col] > cutoff]
                
                # Get latest entry per site
                df = df.sort_values(ts_col).drop_duplicates('site_id', keep='last')
            else:
                # Just drop dupes keep last
                df = df.drop_duplicates('site_id', keep='last')
                
            return df
        except Exception as e:
            print(f"Error loading live data: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- API Endpoints ---

@app.get("/api/market-status")
def get_market_status():
    """
    Returns current cycle phase, advice, and next estimated hike.
    """
    daily_df = cycle_prediction.load_daily_data()
    
    if daily_df is None or daily_df.empty:
        return {"status": "UNKNOWN", "advice": "No data available", "next_hike": None}
        
    avg_len, avg_relent, last_hike = cycle_prediction.analyze_cycles(daily_df)
    status_obj = cycle_prediction.predict_status(avg_relent, last_hike)
    
    # Live Leader Check
    live_df = load_live_data_latest()
    hike_status = "STABLE"
    if not live_df.empty:
        hike_status = cycle_prediction.detect_leader_hike(live_df)
    
    # Logic from dashboard
    rec_msg = "Hold"
    rec_type = "info"
    
    final_status = status_obj['status']
    
    if hike_status == "HIKE_STARTED":
        final_status = "HIKE_STARTED"
    
    if final_status in ['HIKE', 'HIKE_STARTED']:
        rec_msg = "FILL IMMEDIATELY (Prices Rising)"
        rec_type = "error"
    elif final_status == 'OVERDUE':
        rec_msg = "WARNING (Hike Imminent)"
        rec_type = "warning"
    elif final_status == 'BOTTOM':
        rec_msg = "BUY NOW (Good Price)"
        rec_type = "success"
    elif final_status == 'DROPPING':
        rec_msg = "WAIT (Prices Falling)"
        rec_type = "info"

    # Calculate next hike date
    next_hike_est = last_hike + timedelta(days=avg_len)
    
    response = {
        "status": final_status,
        "advice": rec_msg,
        "advice_type": rec_type,
        "last_hike_date": last_hike.strftime("%Y-%m-%d"),
        "next_hike_est": next_hike_est.strftime("%Y-%m-%d"),
        "days_elapsed": int(status_obj['days_elapsed']),
        "avg_cycle_length": float(avg_len)
    }
    return clean_nan(response)

@app.get("/api/stations")
def get_stations():
    """
    Returns list of stations with live prices, location, and ratings.
    """
    live_df = load_live_data_latest()
    if live_df.empty:
        return []
        
    # Get Metadata
    metadata = get_cached_metadata()
    if not metadata.empty:
        if 'site_id' in metadata.columns:
            metadata['site_id'] = metadata['site_id'].astype(str)
        live_df = live_df.merge(metadata, on='site_id', how='left', suffixes=('', '_meta'))
        # Coalesce Lat/Lon
        if 'latitude_meta' in live_df.columns: 
            live_df['latitude'] = live_df['latitude'].fillna(live_df['latitude_meta'])
        if 'longitude_meta' in live_df.columns: 
            live_df['longitude'] = live_df['longitude'].fillna(live_df['longitude_meta'])
            
    # Get Ratings
    ratings = get_cached_ratings()
    if not ratings.empty:
        ratings['site_id'] = ratings['site_id'].astype(str)
        live_df = live_df.merge(ratings[['site_id', 'fairness_score', 'rating']], on='site_id', how='left')
    
    # Format Response
    stations = []
    
    # Calculate Market Avg for "is_cheap" logic
    market_avg = live_df['price_cpl'].median() if not live_df.empty else 180.0
    
    for _, row in live_df.iterrows():
        try:
            # Skip invalid coords
            if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')):
                continue
                
            price = float(row['price_cpl'])
            
            # Determine Name/Brand
            name = row.get('name', f"Station {row['site_id']}")
            if pd.isna(name): name = str(row['site_id'])
            
            brand = row.get('brand', 'Unknown')
            if pd.isna(brand) and 'display_brand' in row: brand = row['display_brand']
            
            stations.append({
                "id": str(row['site_id']),
                "name": str(name),
                "brand": str(brand),
                "lat": float(row['latitude']),
                "lng": float(row['longitude']),
                "price": price,
                "fairness_score": float(row.get('fairness_score', 0.0)) if pd.notnull(row.get('fairness_score')) else 0.0,
                "rating": str(row.get('rating', 'Neutral')) if pd.notnull(row.get('rating')) else 'Neutral',
                "is_cheap": price < (market_avg - 2.0),
                "updated_at": str(row.get('scraped_at', row.get('reported_at')))
            })
        except Exception as e:
            continue
            
    return clean_nan(stations)

@app.get("/api/analytics")
def get_analytics():
    """
    Returns TGP trend data and forecast charts.
    """
    try:
        trend = tgp_forecast.analyze_trend()
        
        # Format chart data
        # Historical TGP
        history = trend.get('history', {})
        chart_history = []
        if history:
            dates = history.get('date', [])
            vals = history.get('tgp', [])
            for d, v in zip(dates, vals):
                chart_history.append({"date": str(pd.to_datetime(d).date()), "value": v})
        
        # Forecast
        forecast_data = []
        sx = trend.get('sarimax', {})
        if sx:
            f_dates = sx.get('forecast_dates', [])
            f_means = sx.get('forecast_mean', [])
            f_lower = sx.get('lower_ci', [])
            f_upper = sx.get('upper_ci', [])
            
            for i in range(len(f_dates)):
                forecast_data.append({
                    "date": str(pd.to_datetime(f_dates[i]).date()),
                    "mean": f_means[i],
                    "lower": f_lower[i],
                    "upper": f_upper[i]
                })

        return clean_nan({
            "current_tgp": trend.get('current_tgp'),
            "forecast_tgp": trend.get('forecast_tgp'),
            "fair_retail_price": trend.get('current_mogas'),
            "regime": trend.get('regime'),
            "history": chart_history,
            "forecast": forecast_data
        })
    except Exception as e:
        print(f"Analytics Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# --- Server Logic ---

# Mount Static Files (Frontend)
# Ensure static directory exists
if not os.path.exists("static"):
    os.makedirs("static")
    
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    # Use standard port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
