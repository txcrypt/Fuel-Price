import sys
import os
import json
import time
import asyncio
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
from pydantic import BaseModel
import config
from fuel_engine import FuelEngine

# --- Import Local Modules ---
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness, market_physics, tgp_forecast, station_metadata, route_optimizer, market_news
from savings_calculator import SavingsCalculator
from predictive_core import DeepCycleModel  # <--- NEW AI ENGINE

app = FastAPI(title="Brisbane Fuel AI API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- AI Model Loader ---
# Load the model once at startup to keep the API fast
ai_model = DeepCycleModel()
MODEL_LOADED = ai_model.load("brisbane")
if MODEL_LOADED:
    print("🧠 DeepCycle AI Model Loaded Successfully")
else:
    print("⚠️ DeepCycle AI Model NOT FOUND. Running in fallback mode.")

# --- Snapshot File ---
SNAPSHOT_FILE = "live_snapshot.csv"
HISTORY_FILE = "brisbane_fuel_live_collection.csv"

def fetch_snapshot():
    print("📸 Fetching Live Snapshot for all states...")
    try:
        token = os.getenv("FUEL_API_TOKEN")
        all_dfs = []
        
        for state_code in config.STATES.keys():
            # print(f"   Fetching {state_code}...")
            engine = FuelEngine(token=token, state=state_code)
            df = engine.get_market_snapshot()
            if df is not None and not df.empty:
                all_dfs.append(df)
        
        if not all_dfs:
            return False
            
        df = pd.concat(all_dfs, ignore_index=True)
        
        # 1. Save Live Snapshot
        df.to_csv(SNAPSHOT_FILE, index=False)
        
        # 2. Append to History (Rate Limited ~1h)
        should_append = False
        if not os.path.exists(HISTORY_FILE):
            should_append = True
        else:
            try:
                last_rows = pd.read_csv(HISTORY_FILE).tail(1)
                if not last_rows.empty and 'scraped_at' in last_rows.columns:
                    last_ts = pd.to_datetime(last_rows['scraped_at'].iloc[0])
                    if (datetime.now() - last_ts).total_seconds() > 3600:
                        should_append = True
                else:
                    should_append = True
            except: should_append = True

        if should_append:
            cols_to_save = ['site_id', 'price_cpl', 'reported_at', 'region', 'state', 'latitude', 'longitude', 'scraped_at']
            available_cols = [c for c in cols_to_save if c in df.columns]
            if available_cols:
                header = not os.path.exists(HISTORY_FILE)
                df[available_cols].to_csv(HISTORY_FILE, mode='a', header=header, index=False)
                print(f"📜 History appended at {datetime.now().strftime('%H:%M')}")
                
                # Trigger a model retrain/refresh logic here if needed in future
        
        print(f"✅ Snapshot saved: {len(df)} records.")
        return True
    except Exception as e:
        print(f"❌ Snapshot failed: {e}")
    return False

async def background_refresher():
    while True:
        try:
            await asyncio.sleep(1800) # 30 mins
            await run_in_threadpool(fetch_snapshot)
        except: await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    print("🚀 Startup: Bootstrapping Data...")
    await run_in_threadpool(fetch_snapshot)
    try: station_metadata.generate_metadata()
    except: pass
    asyncio.create_task(background_refresher())
    print("✅ System Ready.")

def clean_nan(obj):
    if isinstance(obj, float): return None if (np.isnan(obj) or np.isinf(obj)) else obj
    if isinstance(obj, (np.integer, int)): return int(obj)
    if isinstance(obj, list): return [clean_nan(v) for v in obj]
    if isinstance(obj, dict): return {k: clean_nan(v) for k, v in obj.items()}
    return obj

@lru_cache(maxsize=1)
def get_cached_metadata():
    if not os.path.exists(config.METADATA_FILE): station_metadata.generate_metadata()
    return pd.read_csv(config.METADATA_FILE, dtype={'site_id': str, 'postcode': str}) if os.path.exists(config.METADATA_FILE) else pd.DataFrame()

def load_live_data_latest(state="QLD"):
    # Logic to get latest data from Snapshot or History
    if os.path.exists(SNAPSHOT_FILE):
        try:
            df = pd.read_csv(SNAPSHOT_FILE)
            if not df.empty:
                if state and 'state' in df.columns: df = df[df['state'] == state].copy()
                elif state and state != "QLD": return pd.DataFrame() # Fallback
                return df
        except: pass
    
    # Fallback to history
    if os.path.exists(HISTORY_FILE):
        try:
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty:
                if state and 'state' in df.columns: df = df[df['state'] == state]
                last_scrape = df['scraped_at'].max()
                return df[df['scraped_at'] == last_scrape].copy()
        except: pass
    return pd.DataFrame()

@app.get("/api/market-status")
async def get_market_status(state: str = "QLD"):
    try:
        state = state.upper()
        # 1. Load Data
        # We need historical daily data for the AI model
        daily_df = await run_in_threadpool(market_physics.load_daily_data, state=state)
        live_df = await run_in_threadpool(load_live_data_latest, state=state)
        
        # Current Stats
        current_median = live_df['price_cpl'].median() if not live_df.empty else 0.0
        current_avg = live_df['price_cpl'].mean() if not live_df.empty else 0.0
        
        # TGP / Trend
        capital_city = config.STATES.get(state, config.STATES["QLD"])["capital"]
        trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital_city)
        current_tgp = trend.get('current_tgp', 165.0)

        # --- AI FORECASTING ---
        forecast_data = {"dates": [], "prices": []}
        hike_prob = 0.0
        status_label = "STABLE"
        advice = "Hold"
        
        if MODEL_LOADED and daily_df is not None and not daily_df.empty:
            # Prepare data for AI (Rename 'day' -> 'date')
            ai_input_df = daily_df.rename(columns={'day': 'date'}).copy()
            
            # Inject today's live price if newer than history
            today_date = pd.Timestamp.now().normalize()
            if ai_input_df['date'].max() < today_date and current_median > 0:
                new_row = pd.DataFrame({'date': [today_date], 'price_cpl': [current_median]})
                ai_input_df = pd.concat([ai_input_df, new_row], ignore_index=True)

            # Generate 14-day Forecast
            future_df = ai_model.predict_horizon(ai_input_df, days=14)
            
            if not future_df.empty:
                # Extract Advice from Tomorrow's prediction
                tomorrow = future_df.iloc[0]
                hike_prob = float(tomorrow['hike_probability']) * 100 # Convert to %
                
                # Logic: Define Status based on AI Probability
                if hike_prob > 70:
                    status_label = "HIKE_IMMINENT"
                    advice = "Buy Now"
                elif hike_prob > 50:
                    status_label = "WARNING"
                    advice = "Fill Up"
                elif current_median < (current_tgp + 10):
                    status_label = "BOTTOM"
                    advice = "Buy"
                else:
                    # Check trend direction (Feather)
                    delta_7d = future_df.iloc[6]['predicted_price'] - current_median
                    if delta_7d < -2.0:
                        status_label = "DROPPING"
                        advice = "Wait"
                    else:
                        status_label = "STABLE"
                        advice = "Check App"

                # Format for Frontend
                forecast_data = {
                    "dates": future_df['date'].astype(str).tolist(),
                    "prices": future_df['predicted_price'].tolist()
                }

        # --- Savings Calc ---
        savings_insight = "Market is stable."
        try:
            # Using AI projected bottom/peak
            future_prices = forecast_data['prices']
            if future_prices:
                min_future = min(future_prices)
                max_future = max(future_prices)
                
                if advice in ["Buy Now", "Fill Up"]:
                    save = (max_future - current_median) * 0.50 # 50L tank
                    savings_insight = f"⚡ Fill up now! Prices rising to {max_future:.0f}c soon."
                elif advice == "Wait":
                    save = (current_median - min_future) * 0.50
                    savings_insight = f"📉 Prices dropping. Wait to save ~${save:.2f}."
        except: pass

        # History Graph Data
        history_graph = {"dates": [], "prices": []}
        if daily_df is not None:
            # Last 60 days
            h_slice = daily_df.sort_values('day').tail(60)
            history_graph = {
                "dates": h_slice['day'].dt.strftime('%Y-%m-%d').tolist(),
                "prices": h_slice['price_cpl'].tolist()
            }

        return clean_nan({
            "status": status_label,
            "advice": advice,
            "advice_type": "success" if advice in ["Buy", "Buy Now", "Fill Up"] else "info",
            "hike_probability": round(hike_prob, 1),
            "next_hike_est": "AI Predicted", # Simplified
            "days_elapsed": 0, # Legacy field
            "last_updated": str(datetime.now().strftime("%H:%M")),
            "savings_insight": savings_insight,
            "current_avg": current_avg,
            "ticker": {
                "tgp": current_tgp, 
                "oil": trend.get('current_oil', 0),
                "mogas": trend.get('current_mogas', 0),
                "import_parity_lag": trend.get('import_parity_lag', 'Neutral')
            },
            "history": history_graph,
            "forecast": forecast_data
        })

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return {"status": "ERROR", "advice": "Retry", "ticker": {}}

# --- Other Endpoints (Unchanged logic, just wired up) ---
class RouteRequest(BaseModel): start: str; end: str

@app.post("/api/planner")
async def plan_route(req: RouteRequest):
    res = await run_in_threadpool(route_optimizer.optimize_route, req.start, req.end)
    if res and 'stations' in res: res['stations'] = res['stations'].to_dict(orient='records')
    return clean_nan(res if res else {"error": "Route not found"})

class LocationRequest(BaseModel): latitude: float; longitude: float

@app.post("/api/find_cheapest_nearby")
async def find_cheapest_nearby(loc: LocationRequest):
    live_df = await run_in_threadpool(load_live_data_latest, state=None)
    if live_df.empty: return []
    
    meta = get_cached_metadata()
    if not meta.empty:
        live_df = live_df.merge(meta[['site_id', 'name', 'brand', 'suburb']], on='site_id', how='left', suffixes=('', '_m'))
        for c in ['name', 'brand']: 
            if f'{c}_m' in live_df.columns: live_df[c] = live_df[c].fillna(live_df[f'{c}_m'])
            
    results = []
    for _, row in live_df.iterrows():
        try:
            dist = haversine(loc.longitude, loc.latitude, float(row['longitude']), float(row['latitude']))
            if dist <= 15.0:
                results.append({
                    "name": str(row.get('name', 'Station')),
                    "price": float(row['price_cpl']),
                    "distance": dist,
                    "brand": str(row.get('brand', 'Generic'))
                })
        except: continue
    results.sort(key=lambda x: (x['price'], x['distance']))
    return clean_nan(results[:10])

@app.get("/api/stations")
async def get_stations(state: str = "QLD"):
    # Returns map data
    live_df = await run_in_threadpool(load_live_data_latest, state=state)
    if live_df.empty: return []
    
    meta = get_cached_metadata()
    if not meta.empty:
         live_df = live_df.merge(meta[['site_id', 'name', 'brand', 'suburb']], on='site_id', how='left', suffixes=('', '_m'))
         
    live_df = live_df.rename(columns={"price_cpl": "price", "latitude": "lat", "longitude": "lng"})
    live_df = live_df.dropna(subset=['lat', 'lng'])
    
    med = live_df['price'].median()
    live_df['is_cheap'] = live_df['price'] < med
    return clean_nan(live_df.to_dict(orient='records'))

@app.get("/api/analytics")
async def get_analytics(state: str = "QLD"):
    # Uses AI for the trend line
    try:
        daily_df = await run_in_threadpool(market_physics.load_daily_data, state=state)
        forecast = {"forecast_dates": [], "forecast_mean": []}
        
        if MODEL_LOADED and daily_df is not None:
             ai_input = daily_df.rename(columns={'day': 'date'})
             future_df = ai_model.predict_horizon(ai_input, days=14)
             forecast = {
                 "forecast_dates": future_df['date'].astype(str).tolist(),
                 "forecast_mean": future_df['predicted_price'].tolist()
             }
             
        # History
        history = {}
        if daily_df is not None:
            history = {
                "dates": daily_df['day'].dt.strftime('%Y-%m-%d').tolist(),
                "values": daily_df['price_cpl'].tolist()
            }
            
        return clean_nan({"trend": {"history": history, "sarimax": forecast}, "suburb_ranking": []})
    except: return {}

# Helper
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1) * cos(lat2) * sin((lon2-lon1)/2)**2
    return 6371 * 2 * asin(sqrt(a))

app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def read_root(): return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)