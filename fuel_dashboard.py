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
import route_optimizer
import backtester

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
            
            ts_col = 'scraped_at' if 'scraped_at' in df.columns else 'reported_at'
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
                
                # Strategy 1: Last 48h
                cutoff = pd.Timestamp.now() - pd.Timedelta(hours=48)
                recent = df[df[ts_col] > cutoff]
                
                # Strategy 2: If empty, Last 7 Days (Stale Warning)
                if recent.empty:
                    cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
                    recent = df[df[ts_col] > cutoff]
                    
                # Strategy 3: Latest Snapshot available
                if recent.empty:
                     recent = df
                
                # Get latest entry per site
                recent = recent.sort_values(ts_col).drop_duplicates('site_id', keep='last')
                return recent
            else:
                return df.drop_duplicates('site_id', keep='last')
                
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
    
    # Ticker Data
    trend = tgp_forecast.analyze_trend()
    
    response = {
        "status": final_status,
        "advice": rec_msg,
        "advice_type": rec_type,
        "last_hike_date": last_hike.strftime("%Y-%m-%d"),
        "next_hike_est": next_hike_est.strftime("%Y-%m-%d"),
        "days_elapsed": int(status_obj['days_elapsed']),
        "avg_cycle_length": float(avg_len),
        "ticker": {
            "mogas": trend.get('current_mogas'),
            "tgp": trend.get('current_tgp'),
            "oil": trend.get('current_oil'),
            "excise": 0.496
        }
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
    market_avg = live_df['price_cpl'].median() if not live_df.empty else 180.0
    
    for _, row in live_df.iterrows():
        try:
            if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')): continue
            price = float(row['price_cpl'])
            name = row.get('name', f"Station {row['site_id']}")
            brand = row.get('brand', 'Unknown')
            if pd.isna(brand) and 'display_brand' in row: brand = row['display_brand']
            
            stations.append({
                "id": str(row['site_id']),
                "name": str(name),
                "brand": str(brand),
                "suburb": str(row.get('suburb', 'Unknown')),
                "lat": float(row['latitude']),
                "lng": float(row['longitude']),
                "price": price,
                "fairness_score": float(row.get('fairness_score', 0.0)) if pd.notnull(row.get('fairness_score')) else 0.0,
                "rating": str(row.get('rating', 'Neutral')) if pd.notnull(row.get('rating')) else 'Neutral',
                "is_cheap": price < (market_avg - 2.0)
            })
        except: continue
            
    return clean_nan(stations)

@app.get("/api/sentiment")
def get_sentiment():
    """Tab 2: Global Sentiment and News Feed"""
    import market_news
    sentiment = market_news.get_market_sentiment()
    return clean_nan(sentiment)

@app.get("/api/analytics")
def get_analytics():
    """Tab 5: Econometric and Suburb Analytics"""
    try:
        trend = tgp_forecast.analyze_trend()
        live_df = load_live_data_latest()
        metadata = get_cached_metadata()
        
        # Suburb Ranking
        suburb_rank = []
        if not live_df.empty and not metadata.empty:
            merged = live_df.merge(metadata[['site_id', 'suburb']], on='site_id', how='left')
            valid = merged.dropna(subset=['suburb', 'price_cpl'])
            valid = valid[valid['suburb'] != "Unknown"]
            if not valid.empty:
                ranks = valid.groupby('suburb')['price_cpl'].mean().sort_values().head(10).reset_index()
                suburb_rank = ranks.to_dict(orient='records')

        # Price Distribution
        prices = live_df['price_cpl'].dropna().tolist() if not live_df.empty else []

        return clean_nan({
            "trend": trend,
            "suburb_ranking": suburb_rank,
            "price_distribution": prices
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/collect-status")
def get_collect_status():
    """Tab 6: Data Collector Status"""
    last_run = "Never"
    file_path = data_collector.COLLECTION_FILE
    if os.path.exists(file_path):
        mtime = os.path.getmtime(file_path)
        last_run = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
    
    return {"last_run": last_run, "file": file_path}

@app.post("/api/trigger-collect")
def trigger_collect():
    """Tab 6: Manual Collection Trigger"""
    count = data_collector.collect_live_data()
    return {"success": True, "count": count}

from pydantic import BaseModel

class RouteRequest(BaseModel):
    start: str
    end: str

class SandboxRequest(BaseModel):
    threshold: float

@app.post("/api/planner")
def plan_route(req: RouteRequest):
    """Tab 4: Route Optimizer"""
    try:
        res = route_optimizer.optimize_route(req.start, req.end)
        if not res:
            return JSONResponse(status_code=404, content={"error": "Could not resolve locations or find route."})
        
        # Clean NaN for JSON
        if 'stations' in res and not res['stations'].empty:
            res['stations'] = res['stations'].to_dict(orient='records')
        else:
            res['stations'] = []
            
        return clean_nan(res)
    except Exception as e:
        print(f"Planner Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/sandbox/backtest")
def run_sandbox_backtest(req: SandboxRequest):
    """Tab 7: Sandbox Backtest"""
    try:
        # Load uploaded data or master file
        # For simplicity, we'll run on the master file if no upload logic yet
        # Or better, this endpoint assumes the file was uploaded via another endpoint.
        # Given the scope, let's run on the CURRENT collection file as a demo
        df = data_collector.collect_live_data() # This triggers fetch, not load.
        
        # Load from disk
        if os.path.exists(data_collector.COLLECTION_FILE):
            df = pd.read_csv(data_collector.COLLECTION_FILE)
            metrics, res_df = backtester.run_backtest(df, ground_truth_threshold=req.threshold)
            
            if metrics:
                # Prepare chart data
                chart_data = []
                if res_df is not None:
                    # Limit for performance
                    res_df = res_df.tail(100)
                    chart_data = res_df.to_dict(orient='records')
                    
                return clean_nan({
                    "metrics": metrics,
                    "chart": chart_data
                })
        return JSONResponse(status_code=400, content={"error": "No data available for backtest."})
    except Exception as e:
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
