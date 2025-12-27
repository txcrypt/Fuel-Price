import sys
import os
import json
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta
from functools import lru_cache
import threading
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
import config
from fuel_engine import FuelEngine

# --- Import Local Modules ---
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness, market_physics, tgp_forecast, station_metadata, route_optimizer
from savings_calculator import SavingsCalculator

app = FastAPI(title="Brisbane Fuel AI API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Snapshot File ---
SNAPSHOT_FILE = "live_snapshot.csv"

def fetch_snapshot():
    print("📸 Fetching Live Snapshot...")
    try:
        token = os.getenv("FUEL_API_TOKEN")
        engine = FuelEngine(token=token)
        df = engine.get_market_snapshot()
        if df is not None and not df.empty:
            df.to_csv(SNAPSHOT_FILE, index=False)
            print(f"✅ Snapshot saved: {len(df)} records.")
            return True
    except Exception as e:
        print(f"❌ Snapshot failed: {e}")
    return False

@app.on_event("startup")
async def startup_event():
    print("🚀 Startup: Bootstrapping Data...")
    
    # 1. Fetch Live Data
    await run_in_threadpool(fetch_snapshot)
            
    # 2. Generate Metadata & Ratings
    print("🗺️ Generating Metadata & Ratings...")
    try: station_metadata.generate_metadata()
    except Exception as e: print(f"❌ Metadata failed: {e}")
    
    try: await run_in_threadpool(station_fairness.main)
    except Exception as e: print(f"❌ Ratings failed: {e}")
    
    print("✅ System Ready.")

def clean_nan(obj):
    if isinstance(obj, float): return None if (np.isnan(obj) or np.isinf(obj)) else obj
    if isinstance(obj, (np.integer, int)): return int(obj)
    if isinstance(obj, (np.floating, float)): return None if (np.isnan(obj) or np.isinf(obj)) else float(obj)
    if isinstance(obj, (np.bool_, bool)): return bool(obj)
    if isinstance(obj, dict): return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list): return [clean_nan(v) for v in obj]
    return obj

@lru_cache(maxsize=1)
def get_cached_metadata():
    if not os.path.exists(config.METADATA_FILE): station_metadata.generate_metadata()
    return pd.read_csv(config.METADATA_FILE, dtype={'site_id': str, 'postcode': str}) if os.path.exists(config.METADATA_FILE) else pd.DataFrame()

def load_live_data_latest():
    if not os.path.exists(SNAPSHOT_FILE): return pd.DataFrame()
    try:
        df = pd.read_csv(SNAPSHOT_FILE)
        if df.empty: return pd.DataFrame()
        # Ensure site_id is string
        if 'site_id' in df.columns: 
            df['site_id'] = df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
        return df
    except: return pd.DataFrame()

@app.get("/api/market-status")
async def get_market_status():
    # 1. Load Data
    daily_df = await run_in_threadpool(market_physics.load_daily_data)
    live_df = await run_in_threadpool(load_live_data_latest)
    trend = await run_in_threadpool(tgp_forecast.analyze_trend)
    
    current_tgp = trend.get('current_tgp', 165.0)
    current_median = live_df['price_cpl'].median() if not live_df.empty else 0.0
    
    # 2. Analyze Cycles
    avg_len, avg_relent, last_hike = await run_in_threadpool(market_physics.analyze_cycles, daily_df)
    now = pd.Timestamp.now()
    days_elapsed = (now - last_hike).days
    
    # 3. Market Physics Logic
    volatility = 0.0 
    
    status_obj = market_physics.predict_status(
        current_median, 
        current_tgp, 
        days_elapsed, 
        trend.get('delta_7d', 0), 
        volatility
    )
    
    # Refined Next Hike Estimation
    projected_date = last_hike + timedelta(days=avg_len)
    next_hike_str = projected_date.strftime("%Y-%m-%d")
    
    # Logic Overrides
    prob = status_obj.get('hike_probability', 0)
    status = status_obj.get('status', 'STABLE')
    
    if status == "HIKE_STARTED":
        next_hike_str = "HAPPENING NOW"
    elif prob >= 70.0:
        next_hike_str = "Within 24h"
    elif prob >= 50.0:
        # If projected date is far away, bring it closer
        if projected_date > (now + timedelta(days=3)):
             next_hike_str = "Within 3 Days"
    elif projected_date < now:
        # Overdue case
        next_hike_str = "Overdue (Imminent)"

    # 4. Construct Graph Data
    # History (Past 60 days)
    history = {"dates": [], "prices": []}
    if daily_df is not None and not daily_df.empty:
        # Sort and filter
        h_df = daily_df.sort_values('day').tail(60)
        history = {
            "dates": h_df['day'].dt.strftime('%Y-%m-%d').tolist(),
            "prices": h_df['price_cpl'].tolist()
        }
        
    # Forecast (Next 14 days)
    # Simple projection: If hiking, go up. If dropping, decay to TGP.
    forecast = {"dates": [], "prices": []}
    if history['dates']:
        last_date = pd.to_datetime(history['dates'][-1])
        last_price = history['prices'][-1]
        
        fc_dates = []
        fc_prices = []
        
        curr = last_price
        for i in range(1, 15):
            date = last_date + timedelta(days=i)
            fc_dates.append(date.strftime('%Y-%m-%d'))
            
            # Simple Logic based on Status
            if status_obj['status'] in ["HIKE_STARTED", "HIKE_IMMINENT"]:
                target = current_tgp + 25.0 # Hike Peak
                curr += (target - curr) * 0.3 # Fast rise
            elif status_obj['status'] == "DROPPING":
                target = current_tgp + 12.0
                curr += (target - curr) * 0.1 # Slow decay
            else: # Stable/Bottom
                target = current_tgp + 5.0
                curr += (target - curr) * 0.2
                
            fc_prices.append(round(curr, 1))
            
        forecast = {"dates": fc_dates, "prices": fc_prices}

    return clean_nan({
        "status": status_obj['status'],
        "advice": status_obj['advice'],
        "advice_type": "success" if status_obj['advice'] == "Buy" else ("error" if "FILL" in status_obj['advice'] else "info"),
        "hike_probability": status_obj['hike_probability'],
        "next_hike_est": next_hike_str,
        "days_elapsed": days_elapsed,
        "ticker": {
            "tgp": current_tgp, 
            "oil": trend.get('current_oil', 0),
            "mogas": trend.get('current_mogas', 0),
            "import_parity_lag": trend.get('import_parity_lag', 'Neutral'),
            "excise": 0.496 # Fixed approx
        },
        "history": history,
        "forecast": forecast
    })

@app.get("/api/stations")
async def get_stations():
    live_df = await run_in_threadpool(load_live_data_latest)
    if live_df.empty: return []
    meta = get_cached_metadata()
    if not meta.empty: live_df = live_df.merge(meta, on='site_id', how='left', suffixes=('', '_meta'))
    
    ratings_file = config.RATINGS_FILE
    if os.path.exists(ratings_file):
        r_df = pd.read_csv(ratings_file, dtype={'site_id': str})
        live_df = live_df.merge(r_df[['site_id', 'fairness_score', 'rating']], on='site_id', how='left')

    stations = []
    market_avg = live_df['price_cpl'].median() if not live_df.empty else 180.0
    for _, row in live_df.iterrows():
        try:
            # Explicit NaN check for coordinates
            if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')): continue
            
            stations.append({
                "id": str(row['site_id']),
                "name": str(row.get('name', row['site_id'])),
                "brand": str(row.get('display_brand', row.get('brand', 'Unknown'))),
                "suburb": str(row.get('suburb', 'Unknown')),
                "lat": float(row['latitude']),
                "lng": float(row['longitude']),
                "price": float(row['price_cpl']),
                "fairness_score": float(row.get('fairness_score', 0)),
                "rating": str(row.get('rating', 'Neutral')),
                "is_cheap": bool(float(row['price_cpl']) < (market_avg - 2.0)),
                "updated_at": str(row.get('scraped_at', row.get('reported_at')))
            })
        except: continue
    return clean_nan(stations)

@app.get("/api/analytics")
async def get_analytics():
    trend = await run_in_threadpool(tgp_forecast.analyze_trend)
    
    # Calculate Suburb Ranking & Distribution from Live Data
    live_df = await run_in_threadpool(load_live_data_latest)
    
    # Merge Metadata to get Suburbs
    meta = get_cached_metadata()
    if not live_df.empty and not meta.empty:
        live_df = live_df.merge(meta[['site_id', 'suburb']], on='site_id', how='left')

    suburb_ranking = []
    price_dist = []
    
    if not live_df.empty:
        # Suburb Ranking (Cheapest)
        if 'suburb' in live_df.columns and 'price_cpl' in live_df.columns:
            grp = live_df.groupby('suburb')['price_cpl'].mean().reset_index()
            grp = grp.sort_values('price_cpl').head(10)
            suburb_ranking = grp.to_dict(orient='records')
            
        # Price Distribution (Histogram buckets)
        if 'price_cpl' in live_df.columns:
            # Create simple buckets
            prices = live_df['price_cpl'].dropna()
            if not prices.empty:
                counts, bins = np.histogram(prices, bins=10)
                # Format for frontend
                for i in range(len(counts)):
                    price_dist.append({
                        "range": f"{int(bins[i])}-{int(bins[i+1])}",
                        "count": int(counts[i])
                    })

    return clean_nan({"trend": trend, "suburb_ranking": suburb_ranking, "price_distribution": price_dist})

@app.get("/api/sentiment")
async def get_sentiment():
    try:
        import market_news
        # Run blocking network call in threadpool
        news_data = await run_in_threadpool(market_news.get_market_news)
        return clean_nan(news_data)
    except Exception as e:
        print(f"Sentiment Error: {e}")
        return {"global": [], "domestic": []}

from pydantic import BaseModel
class SavingsRequest(BaseModel):
    tank_size: int

@app.post("/api/calculate-savings")
async def calculate_savings(req: SavingsRequest):
    # 1. Get Market Data
    live_df = await run_in_threadpool(load_live_data_latest)
    if live_df.empty: return {"error": "No market data"}
    
    current_avg = live_df['price_cpl'].median()
    best_price = live_df['price_cpl'].min()
    
    # 2. Get Phase (Simplified reuse of market-status logic)
    trend = await run_in_threadpool(tgp_forecast.analyze_trend)
    current_tgp = trend.get('current_tgp', 160.0)
    
    # We ideally need yesterday's data for full accuracy, but for savings calc
    # we can use a simpler fallback or try to load history.
    # For speed, let's use the TGP logic directly or call the detector with None history (Bottom check only)
    # Note: detect_market_phase was moved/deprecated, but kept in market_physics for legacy if needed.
    # But we should rely on predict_status now.
    
    status_obj = market_physics.predict_status(current_avg, current_tgp, 30, 0, 0)
    phase = status_obj['status']
    
    # Predicted Bottom
    pred_bottom = current_tgp + 2.0 
    
    # 3. Calculate
    calc = SavingsCalculator(
        current_avg_price=current_avg,
        best_local_price=best_price,
        cycle_phase=phase,
        predicted_bottom=pred_bottom,
        tank_size=req.tank_size
    )
    
    return clean_nan(calc.get_report())

class RouteRequest(BaseModel): start: str; end: str

@app.post("/api/planner")
async def plan_route(req: RouteRequest):
    res = await run_in_threadpool(route_optimizer.optimize_route, req.start, req.end)
    if res and 'stations' in res and not res['stations'].empty: res['stations'] = res['stations'].to_dict(orient='records')
    return clean_nan(res if res else {"error": "Route not found"})

# Helper for Distance
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers
    return c * r

class LocationRequest(BaseModel):
    latitude: float
    longitude: float

@app.post("/api/find_cheapest_nearby")
async def find_cheapest_nearby(loc: LocationRequest):
    live_df = await run_in_threadpool(load_live_data_latest)
    if live_df.empty: return []
    
    # Merge Metadata for nice names
    meta = get_cached_metadata()
    if not meta.empty:
        # Check available columns
        cols_to_merge = ['site_id', 'name', 'suburb']
        if 'display_brand' in meta.columns: cols_to_merge.append('display_brand')
        elif 'brand' in meta.columns: cols_to_merge.append('brand')
        
        live_df = live_df.merge(meta[cols_to_merge], on='site_id', how='left', suffixes=('', '_meta'))
        
        if 'name_meta' in live_df.columns: live_df['name'] = live_df['name'].fillna(live_df['name_meta'])
        # Coalesce Brand
        if 'display_brand' in live_df.columns: live_df['brand'] = live_df['display_brand']
        elif 'brand_meta' in live_df.columns: live_df['brand'] = live_df['brand'].fillna(live_df['brand_meta'])
    
    results = []
    for _, row in live_df.iterrows():
        try:
            if pd.isna(row['latitude']) or pd.isna(row['longitude']): continue
            
            slat, slon = float(row['latitude']), float(row['longitude'])
            dist = haversine(loc.longitude, loc.latitude, slon, slat)
            
            if dist <= 15.0: # Increased Radius to 15km to find better deals
                results.append({
                    "name": str(row.get('name', 'Unknown Station')),
                    "price": float(row['price_cpl']),
                    "distance": dist,
                    "brand": str(row.get('brand', 'Unknown')),
                    "suburb": str(row.get('suburb', ''))
                })
        except: continue
        
    # Sort: Primary = Price (Asc), Secondary = Distance (Asc)
    # This fulfills "Cheapest fuel near me" - strict cheapest first.
    results.sort(key=lambda x: (x['price'], x['distance']))
    
    return clean_nan(results[:10])

app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def read_root(): return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)