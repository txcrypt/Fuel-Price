import sys
import os
import json
import pandas as pd
import numpy as np
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
import station_fairness, cycle_prediction, tgp_forecast, station_metadata, route_optimizer

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
    daily_df = await run_in_threadpool(cycle_prediction.load_daily_data)
    if daily_df is None or daily_df.empty: return clean_nan({"status": "UNKNOWN", "ticker": {"mogas": 0, "tgp": 0}})
    
    avg_len, avg_relent, last_hike = await run_in_threadpool(cycle_prediction.analyze_cycles, daily_df)
    status_obj = cycle_prediction.predict_status(avg_relent, last_hike)
    live_df = await run_in_threadpool(load_live_data_latest)
    
    hike_status = await run_in_threadpool(cycle_prediction.detect_leader_hike, live_df) if not live_df.empty else "STABLE"
    final_status = "HIKE_STARTED" if hike_status == "HIKE_STARTED" else status_obj['status']
    
    rec_map = {'HIKE': "FILL NOW", 'HIKE_STARTED': "FILL NOW", 'OVERDUE': "WARNING", 'BOTTOM': "BUY", 'DROPPING': "WAIT"}
    trend = await run_in_threadpool(tgp_forecast.analyze_trend)

    # History for Graph
    history = {}
    if daily_df is not None and not daily_df.empty:
        try:
            hist_data = daily_df.sort_index().tail(45)
            # Ensure DatetimeIndex
            if not isinstance(hist_data.index, pd.DatetimeIndex):
                 hist_data.index = pd.to_datetime(hist_data.index, errors='coerce')
                 
            # Filter out NaT if conversion failed
            hist_data = hist_data[hist_data.index.notnull()]
            
            history = {
                "dates": hist_data.index.strftime('%Y-%m-%d').tolist(),
                "prices": hist_data['price_cpl'].tolist()
            }
        except Exception as e:
            print(f"History generation error: {e}")
            history = {}
    
    return clean_nan({
        "status": final_status,
        "advice": rec_map.get(final_status, "Hold"),
        "next_hike_est": (last_hike + timedelta(days=avg_len)).strftime("%Y-%m-%d"),
        "days_elapsed": int(status_obj['days_elapsed']),
        "ticker": {"mogas": trend.get('current_mogas'), "tgp": trend.get('current_tgp'), "oil": trend.get('current_oil')},
        "history": history
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
        sentiment = await run_in_threadpool(market_news.get_market_sentiment)
        
        # Enriched Sentiment with TGP Trend
        trend = await run_in_threadpool(tgp_forecast.analyze_trend)
        oil_trend = trend.get('oil_trend_pct', 0)
        
        # Adjust score: +1 for decreasing oil/tgp (Good), -1 for increasing (Bad)
        if oil_trend > 1.0: sentiment['score'] -= 2 # Strong Price Pressure
        elif oil_trend < -1.0: sentiment['score'] += 2 # Strong Relief
        
        # Recalculate Mood string based on new score
        s = sentiment['score']
        if s <= -3: sentiment['mood'] = "Market Stress (Prices Rising)"
        elif s >= 3: sentiment['mood'] = "Consumer Relief (Prices Falling)"
        elif s < 0: sentiment['mood'] = "Slightly Inflationary"
        else: sentiment['mood'] = "Stable / Mixed"
        
        return clean_nan(sentiment)
    except Exception as e:
        print(f"Sentiment Error: {e}")
        return {"score": 0, "mood": "Error", "color": "#64748b", "articles": []}

from pydantic import BaseModel
class RouteRequest(BaseModel): start: str; end: str

@app.post("/api/planner")
async def plan_route(req: RouteRequest):
    res = await run_in_threadpool(route_optimizer.optimize_route, req.start, req.end)
    if res and 'stations' in res and not res['stations'].empty: res['stations'] = res['stations'].to_dict(orient='records')
    return clean_nan(res if res else {"error": "Route not found"})

app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def read_root(): return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)