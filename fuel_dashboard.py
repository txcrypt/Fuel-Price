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
from neural_forecast import NeuralForecaster

# --- Import Local Modules ---
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness, market_physics, tgp_forecast, station_metadata, route_optimizer, market_news
from savings_calculator import SavingsCalculator

app = FastAPI(title="Brisbane Fuel AI API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Snapshot File ---
SNAPSHOT_FILE = "live_snapshot.csv"

def fetch_snapshot():
    print("📸 Fetching Live Snapshot for all states...")
    try:
        token = os.getenv("FUEL_API_TOKEN")
        all_dfs = []
        
        for state_code in config.STATES.keys():
            print(f"   Fetching {state_code}...")
            engine = FuelEngine(token=token, state=state_code)
            df = engine.get_market_snapshot()
            if df is not None and not df.empty:
                all_dfs.append(df)
        
        if not all_dfs:
            print("❌ No data fetched for any state.")
            return False
            
        df = pd.concat(all_dfs, ignore_index=True)
        
        # 1. Save Live Snapshot (Current State)
        df.to_csv(SNAPSHOT_FILE, index=False)
        
        # 2. Append to Historical Collection (Rate Limited to ~1 hour)
        history_file = "brisbane_fuel_live_collection.csv" # Kept name for compatibility or could rename later
        should_append = False
        
        if not os.path.exists(history_file):
            should_append = True
        else:
            try:
                # Check last scrape for ANY state to see if it's been an hour
                last_rows = pd.read_csv(history_file).tail(1)
                if not last_rows.empty and 'scraped_at' in last_rows.columns:
                    last_ts = pd.to_datetime(last_rows['scraped_at'].iloc[0])
                    if (datetime.now() - last_ts).total_seconds() > 3600:
                        should_append = True
                else:
                    should_append = True
            except:
                should_append = True

        if should_append:
            # Enforce schema consistency with historical file
            cols_to_save = ['site_id', 'price_cpl', 'reported_at', 'region', 'state', 'latitude', 'longitude', 'scraped_at']
            
            # Ensure columns exist in df
            available_cols = [c for c in cols_to_save if c in df.columns]
            
            if available_cols:
                header = not os.path.exists(history_file)
                df[available_cols].to_csv(history_file, mode='a', header=header, index=False)
                print(f"📜 History appended at {datetime.now().strftime('%H:%M')}")
                
                # Refresh Cache for all states
                try:
                    for state_code in config.STATES.keys():
                        market_physics.load_daily_data(force_refresh=True, state=state_code)
                except Exception as e: print(f"⚠️ Cache refresh failed: {e}")
        else:
            print(f"⏳ History skip (Rate limit)")
        
        print(f"✅ Snapshot saved: {len(df)} records across {len(all_dfs)} states.")
        return True
    except Exception as e:
        print(f"❌ Snapshot failed: {e}")
    return False

async def background_refresher():
    """Background task to refresh data every 30 minutes"""
    while True:
        try:
            await asyncio.sleep(1800) # 30 minutes
            print("🔄 Periodic Refresh Triggered...")
            await run_in_threadpool(fetch_snapshot)
        except Exception as e:
            print(f"⚠️ Background refresh error: {e}")
            await asyncio.sleep(60) # Retry after 1 minute if crash occurs

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
    
    # 3. Start Background Refresher
    asyncio.create_task(background_refresher())
    
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

def load_live_data_latest(state="QLD"):
    # Priority 1: Live Snapshot (Most recent attempt)
    if os.path.exists(SNAPSHOT_FILE):
        try:
            df = pd.read_csv(SNAPSHOT_FILE)
            if not df.empty:
                # Filter by state if provided
                if state and 'state' in df.columns:
                    df = df[df['state'] == state].copy()
                elif state and state != "QLD":
                    return pd.DataFrame()
                
                # Type enforcement
                if 'site_id' in df.columns: 
                    df['site_id'] = df['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
                return df
        except Exception as e:
            print(f"⚠️ Live snapshot load error: {e}")

    # Priority 2: Historical Fallback (Last known good state)
    history_file = "brisbane_fuel_live_collection.csv"
    if os.path.exists(history_file):
        try:
            print(f"⚠️ Using Historical Fallback for Live Data (State: {state})")
            df_hist = pd.read_csv(history_file)
            if not df_hist.empty:
                if state and 'state' in df_hist.columns:
                    df_hist = df_hist[df_hist['state'] == state].copy()
                elif state and state != "QLD":
                    return pd.DataFrame()

                if not df_hist.empty and 'scraped_at' in df_hist.columns:
                    last_scrape = df_hist['scraped_at'].max()
                    latest_slice = df_hist[df_hist['scraped_at'] == last_scrape].copy()
                    
                    if 'site_id' in latest_slice.columns:
                        latest_slice['site_id'] = latest_slice['site_id'].apply(lambda x: str(int(float(x))) if pd.notnull(x) and str(x).replace('.','',1).isdigit() else str(x))
                    
                    return latest_slice
        except Exception as e:
            print(f"⚠️ Historical fallback error: {e}")

    return pd.DataFrame()

@app.get("/api/market-status")
async def get_market_status(state: str = "QLD"):
    try:
        state = state.upper()
        # 1. Load Data
        daily_df = await run_in_threadpool(market_physics.load_daily_data, state=state)
        live_df = await run_in_threadpool(load_live_data_latest, state=state)
        
        capital_city = config.STATES.get(state, config.STATES["QLD"])["capital"]
        trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital_city)
        
        current_tgp = trend.get('current_tgp', 165.0)
        if not isinstance(current_tgp, (int, float)): current_tgp = 165.0
        
        current_median = live_df['price_cpl'].median() if not live_df.empty else 0.0
        if pd.isna(current_median): current_median = 0.0

        current_avg = live_df['price_cpl'].mean() if not live_df.empty else 0.0
        if pd.isna(current_avg): current_avg = 0.0
        
        # Extract last updated time from live data
        last_updated = "Unknown"
        if not live_df.empty and 'scraped_at' in live_df.columns:
            last_updated = str(live_df['scraped_at'].iloc[0])
        
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
            if projected_date > (now + timedelta(days=3)):
                 next_hike_str = "Within 3 Days"
        elif projected_date < now:
            next_hike_str = "Overdue (Imminent)"

        # 4. Construct Graph Data
        history = {"dates": [], "prices": []}
        if daily_df is not None and not daily_df.empty:
            h_df = daily_df.sort_values('day')
            
            # FIX: Inject Today's Live Data
            last_history_date = h_df['day'].max().date()
            today_date = pd.Timestamp.now().date()
            
            if last_history_date < today_date and current_median > 0:
                today_row = pd.DataFrame([{'day': pd.Timestamp(today_date), 'price_cpl': current_median}])
                h_df = pd.concat([h_df, today_row], ignore_index=True)

            h_df = h_df.tail(60)
            history = {
                "dates": h_df['day'].dt.strftime('%Y-%m-%d').tolist(),
                "prices": h_df['price_cpl'].tolist()
            }
            
        # Forecast
        forecast = {"dates": [], "prices": []}
        if history['dates']:
            last_price = history['prices'][-1]
            last_hist_date = pd.to_datetime(history['dates'][-1])
            
            forecaster = NeuralForecaster(
                tgp=current_tgp,
                current_price=last_price,
                days_since_hike=days_elapsed,
                status=status_obj['status'],
                cycle_length=int(avg_len)
            )
            forecast = forecaster.predict_next_14_days(start_date=last_hist_date)

        # 5. Calculate Savings Insight
        savings_insight = "Market is stable."
        try:
            calc = SavingsCalculator(
                current_avg_price=current_median,
                best_local_price=live_df['price_cpl'].min() if not live_df.empty else current_median,
                cycle_phase="Restoration" if status in ["HIKE_STARTED", "HIKE_IMMINENT"] else "Relenting",
                predicted_bottom=current_tgp + 2.0,
                tank_size=50
            )
            opp = calc.calculate_opportunity()
            
            if status in ["HIKE_STARTED", "HIKE_IMMINENT", "WARNING"]:
                savings_insight = f"⚡ Fill 50L now to save ~${opp:.2f} vs coming peak."
            elif status == "DROPPING":
                savings_insight = f"📉 Wait to save ~${opp:.2f} on 50L."
            elif status == "BOTTOM":
                 savings_insight = "✅ Prices at bottom. Great time to fill."
            else:
                savings_insight = f"ℹ️ Market stable. Potential variance ~${opp:.2f}."
        except Exception as e:
            print(f"Savings calc error: {e}")

        return clean_nan({
            "status": status_obj['status'],
            "advice": status_obj['advice'],
            "advice_type": "success" if status_obj['advice'] == "Buy" else ("error" if "FILL" in status_obj['advice'] else "info"),
            "hike_probability": status_obj['hike_probability'],
            "next_hike_est": next_hike_str,
            "days_elapsed": days_elapsed,
            "last_updated": last_updated,
            "savings_insight": savings_insight,
            "current_avg": current_avg,
            "ticker": {
                "tgp": current_tgp, 
                "oil": trend.get('current_oil', 0),
                "mogas": trend.get('current_mogas', 0),
                "import_parity_lag": trend.get('import_parity_lag', 'Neutral'),
                "excise": 0.496
            },
            "history": history,
            "forecast": forecast
        })
    except Exception as e:
        print(f"CRITICAL MARKET STATUS ERROR: {e}")
        # Return Safe Fallback
        return {
            "status": "OFFLINE",
            "advice": "Check Connection",
            "advice_type": "error",
            "hike_probability": 0,
            "next_hike_est": "--",
            "days_elapsed": 0,
            "last_updated": "Never",
            "savings_insight": "System is offline.",
            "ticker": {"tgp": 0, "oil": 0, "mogas": 0},
            "history": {"dates": [], "prices": []},
            "forecast": {"dates": [], "prices": []}
        }

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
    live_df = await run_in_threadpool(load_live_data_latest, state=None)
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
                    "suburb": str(row.get('suburb', '')),
                    "state": str(row.get('state', ''))
                })
        except: continue
        
    # Sort: Primary = Price (Asc), Secondary = Distance (Asc)
    # This fulfills "Cheapest fuel near me" - strict cheapest first.
    results.sort(key=lambda x: (x['price'], x['distance']))
    
    return clean_nan(results[:10])

@app.get("/api/sentiment")
async def get_sentiment():
    try:
        # Use run_in_threadpool for potentially blocking network calls
        news = await run_in_threadpool(market_news.get_market_news)
        return news
    except Exception as e:
        print(f"Sentiment Error: {e}")
        return {"global": [], "domestic": []}

@app.get("/api/stations")
async def get_stations(state: str = "QLD"):
    """Returns the full station list with ratings for the map and tables."""
    try:
        state = state.upper()
        ratings_file = "station_ratings.csv"
        if os.path.exists(ratings_file):
            df = pd.read_csv(ratings_file)
            # Filter by state
            if 'state' in df.columns:
                df = df[df['state'] == state].copy()
            
            # Standardize columns for frontend
            df = df.rename(columns={
                "price_cpl": "price",
                "latitude": "lat", 
                "longitude": "lng"
            })
            
            # Helper for cheap/expensive flag (simple median logic if not present)
            if 'price' in df.columns:
                median = df['price'].median()
                df['is_cheap'] = df['price'] < median
            
            # Handle NaN
            return clean_nan(df.to_dict(orient='records'))
        else:
            # Fallback to live snapshot if ratings not ready
            live_df = await run_in_threadpool(load_live_data_latest, state=state)
            if not live_df.empty:
                # Merge metadata for coords
                meta = get_cached_metadata()
                if not meta.empty:
                    live_df = live_df.merge(meta[['site_id', 'latitude', 'longitude', 'name', 'brand', 'suburb']], on='site_id', how='left', suffixes=('', '_meta'))
                    # Coalesce
                    for col in ['latitude', 'longitude', 'name', 'brand', 'suburb']:
                        if f'{col}_meta' in live_df.columns:
                            live_df[col] = live_df[col].fillna(live_df[f'{col}_meta'])
                
                live_df = live_df.rename(columns={"price_cpl": "price", "latitude": "lat", "longitude": "lng"})
                live_df = live_df.dropna(subset=['lat', 'lng'])
                if 'price' in live_df.columns:
                    median = live_df['price'].median()
                    live_df['is_cheap'] = live_df['price'] < median
                    # Add dummy fairness score
                    live_df['fairness_score'] = live_df['price'] - median
                
                return clean_nan(live_df.to_dict(orient='records'))
            
            return []
    except Exception as e:
        print(f"Stations Error: {e}")
        return []

@app.get("/api/analytics")
async def get_analytics(state: str = "QLD"):
    try:
        state = state.upper()
        live_df = await run_in_threadpool(load_live_data_latest, state=state)
        suburb_stats = []
        
        if not live_df.empty:
            # Merge suburb if missing
            if 'suburb' not in live_df.columns:
                 meta = get_cached_metadata()
                 if not meta.empty:
                     live_df = live_df.merge(meta[['site_id', 'suburb']], on='site_id', how='left')
            
            if 'suburb' in live_df.columns:
                # Group by Suburb
                grp = live_df.groupby('suburb')['price_cpl'].mean().reset_index()
                grp = grp.sort_values('price_cpl').head(10)
                suburb_stats = grp.to_dict(orient='records')

        # Forecast Data
        capital = config.STATES.get(state, config.STATES["QLD"])["capital"]
        trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital)
        current_tgp = trend.get('current_tgp', 165.0)
        
        # We need a proper forecast structure matching frontend expectations:
        # data.trend.history.dates, data.trend.history.values
        # data.trend.sarimax.forecast_dates, data.trend.sarimax.forecast_mean
        
        # Use NeuralForecaster for consistency
        forecaster = NeuralForecaster(
            tgp=current_tgp,
            current_price=current_tgp, # Approx
            days_since_hike=0,
            status="STABLE"
        )
        # Predict TGP trend (simplified)
        # Actually tgp_forecast.analyze_trend returns history. 
        # Let's project TGP flat or slightly trending for the visual.
        
        fc_dates = []
        fc_values = []
        last_date_str = trend['history']['dates'][-1] if trend['history']['dates'] else datetime.now().strftime('%Y-%m-%d')
        last_date = pd.to_datetime(last_date_str)
        
        for i in range(1, 15):
            d = last_date + timedelta(days=i)
            fc_dates.append(d.strftime('%Y-%m-%d'))
            # Simple projection based on trend direction
            direction = 1 if trend['trend_direction'] == "RISING" else (-1 if trend['trend_direction'] == "FALLING" else 0)
            val = current_tgp + (i * 0.1 * direction)
            fc_values.append(round(val, 2))

        return clean_nan({
            "suburb_ranking": suburb_stats,
            "trend": {
                "history": trend['history'],
                "sarimax": { # Frontend calls it sarimax, we provide our simple forecast
                    "forecast_dates": fc_dates,
                    "forecast_mean": fc_values
                }
            }
        })

    except Exception as e:
        print(f"Analytics Error: {e}")
        return {"suburb_ranking": [], "trend": {}}

app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def read_root(): return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)