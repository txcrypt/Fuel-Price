import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import lru_cache
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Import Local Modules ---
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness, cycle_prediction, tgp_forecast, data_collector, station_metadata, route_optimizer, backtester

app = FastAPI(title="Brisbane Fuel AI API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    print("🚀 Startup: Checking data integrity...")
    if not os.path.exists(data_collector.COLLECTION_FILE) or os.stat(data_collector.COLLECTION_FILE).st_size < 100:
        print("⚠️ Data missing. Collecting now...")
        try: data_collector.collect_live_data()
        except Exception as e: print(f"❌ Collection failed: {e}")
            
    print("🗺️ Generating Metadata & Ratings...")
    try: station_metadata.generate_metadata()
    except Exception as e: print(f"❌ Metadata failed: {e}")
    try: station_fairness.main()
    except Exception as e: print(f"❌ Ratings failed: {e}")
    print("✅ System Ready.")

def clean_nan(obj):
    if isinstance(obj, float): return None if (np.isnan(obj) or np.isinf(obj)) else obj
    if isinstance(obj, dict): return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list): return [clean_nan(v) for v in obj]
    return obj

@lru_cache(maxsize=1)
def get_cached_metadata():
    if not os.path.exists(station_metadata.METADATA_FILE): station_metadata.generate_metadata()
    return pd.read_csv(station_metadata.METADATA_FILE, dtype={'site_id': str, 'postcode': str}) if os.path.exists(station_metadata.METADATA_FILE) else pd.DataFrame()

def load_live_data_latest():
    if not os.path.exists(data_collector.COLLECTION_FILE): return pd.DataFrame()
    try:
        df = pd.read_csv(data_collector.COLLECTION_FILE)
        if df.empty: return pd.DataFrame()
        if 'site_id' in df.columns: df['site_id'] = df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
        ts_col = 'scraped_at' if 'scraped_at' in df.columns else 'reported_at'
        if ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
            recent = df[df[ts_col] > (pd.Timestamp.now() - pd.Timedelta(hours=48))]
            if recent.empty: recent = df
            return recent.sort_values(ts_col).drop_duplicates('site_id', keep='last')
        return df.drop_duplicates('site_id', keep='last')
    except: return pd.DataFrame()

@app.get("/api/market-status")
def get_market_status():
    daily_df = cycle_prediction.load_daily_data()
    if daily_df is None or daily_df.empty: return clean_nan({"status": "UNKNOWN", "ticker": {"mogas": 0, "tgp": 0}})
    
    avg_len, avg_relent, last_hike = cycle_prediction.analyze_cycles(daily_df)
    status_obj = cycle_prediction.predict_status(avg_relent, last_hike)
    live_df = load_live_data_latest()
    
    hike_status = cycle_prediction.detect_leader_hike(live_df) if not live_df.empty else "STABLE"
    final_status = "HIKE_STARTED" if hike_status == "HIKE_STARTED" else status_obj['status']
    
    rec_map = {'HIKE': "FILL NOW", 'HIKE_STARTED': "FILL NOW", 'OVERDUE': "WARNING", 'BOTTOM': "BUY", 'DROPPING': "WAIT"}
    trend = tgp_forecast.analyze_trend()
    
    return clean_nan({
        "status": final_status,
        "advice": rec_map.get(final_status, "Hold"),
        "next_hike_est": (last_hike + timedelta(days=avg_len)).strftime("%Y-%m-%d"),
        "days_elapsed": int(status_obj['days_elapsed']),
        "ticker": {"mogas": trend.get('current_mogas'), "tgp": trend.get('current_tgp'), "oil": trend.get('current_oil')}
    })

@app.get("/api/stations")
def get_stations():
    live_df = load_live_data_latest()
    if live_df.empty: return []
    meta = get_cached_metadata()
    if not meta.empty: live_df = live_df.merge(meta, on='site_id', how='left', suffixes=('', '_meta'))
    
    ratings_file = os.path.join(os.path.dirname(__file__), "station_ratings.csv")
    if os.path.exists(ratings_file):
        r_df = pd.read_csv(ratings_file, dtype={'site_id': str})
        live_df = live_df.merge(r_df[['site_id', 'fairness_score', 'rating']], on='site_id', how='left')

    stations = []
    market_avg = live_df['price_cpl'].median() if not live_df.empty else 180.0
    for _, row in live_df.iterrows():
        try:
            if pd.isna(row.get('latitude')): continue
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
                "is_cheap": float(row['price_cpl']) < (market_avg - 2.0)
            })
        except: continue
    return clean_nan(stations)

@app.get("/api/analytics")
def get_analytics():
    return clean_nan({"trend": tgp_forecast.analyze_trend(), "suburb_ranking": [], "price_distribution": []})

@app.get("/api/collect-status")
def get_collect_status():
    f = data_collector.COLLECTION_FILE
    return {"last_run": datetime.fromtimestamp(os.path.getmtime(f)).strftime("%Y-%m-%d %H:%M") if os.path.exists(f) else "Never"}

@app.get("/api/sentiment")
def get_sentiment():
    try:
        import market_news
        return clean_nan(market_news.get_market_sentiment())
    except Exception as e:
        print(f"Sentiment Error: {e}")
        return {"score": 0, "mood": "Error", "color": "#64748b", "articles": []}

@app.post("/api/trigger-collect")
def trigger_collect():
    c = data_collector.collect_live_data()
    station_metadata.generate_metadata()
    try: station_fairness.main()
    except: pass
    return {"success": True, "count": c}

from pydantic import BaseModel
class RouteRequest(BaseModel): start: str; end: str

@app.post("/api/planner")
def plan_route(req: RouteRequest):
    res = route_optimizer.optimize_route(req.start, req.end)
    if res and 'stations' in res and not res['stations'].empty: res['stations'] = res['stations'].to_dict(orient='records')
    return clean_nan(res if res else {"error": "Route not found"})

app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def read_root(): return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
