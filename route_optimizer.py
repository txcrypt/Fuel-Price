import sys
import os
import pandas as pd
import numpy as np
import requests
import polyline
from math import radians, cos, sin, asin, sqrt
import config

try:
    from fuel_engine import FuelEngine 
except ImportError:
    print("❌ Error: Could not import FuelEngine. Check path.")
    sys.exit(1)

def load_local_data():
    """Fallback: Load from local CSV if API fails."""
    if os.path.exists(config.COLLECTION_FILE):
        try:
            df = pd.read_csv(config.COLLECTION_FILE)
            df = df.dropna(subset=['latitude', 'longitude', 'price_cpl'])
            return df
        except Exception as e:
            print(f"Warning: Error loading local data: {e}")
    return pd.DataFrame()

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r

def get_coords_from_address(address):
    params = {'q': f"{address}, Australia", 'format': 'json', 'limit': 1}
    headers = {'User-Agent': config.USER_AGENT}
    try:
        r = requests.get(config.NOMINATIM_BASE_URL, params=params, headers=headers, timeout=5)
        if r.status_code == 200 and r.json():
            data = r.json()[0]
            return float(data['lat']), float(data['lon']), data['display_name']
    except Exception:
        pass
    return None, None, None

def get_osrm_route(lat1, lon1, lat2, lon2):
    url = f"{config.OSRM_BASE_URL}/{lon1},{lat1};{lon2},{lat2}?overview=full&geometries=geojson"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data['code'] == 'Ok' and len(data['routes']) > 0:
                return [[p[1], p[0]] for p in data['routes'][0]['geometry']['coordinates']], data['routes'][0]['distance'] / 1000.0
    except Exception:
        pass
    return None, None

def calculate_detour_utility(station, route_dist_km, market_avg_price, tank_capacity=50, current_fuel=10, km_per_liter=10, hourly_wage=30.0):
    price = station['price_cpl']
    detour_km = (station['dist_score'] * 111.0) * 2.0
    if detour_km > 5.0: return -999.0
    
    fill_vol = tank_capacity - current_fuel
    if fill_vol <= 0: return -999.0
    
    gross_save = (market_avg_price - price) / 100.0 * fill_vol
    time_cost = ((detour_km / 40.0) + 0.08) * hourly_wage
    fuel_cost = (detour_km / km_per_liter) * (price / 100.0)
    
    return gross_save - (time_cost + fuel_cost)

def optimize_route(start_address, end_address, tank_capacity=50, current_fuel=10, km_per_liter=10, hourly_wage=30.0):
    lat1, lon1, name1 = get_coords_from_address(start_address)
    lat2, lon2, name2 = get_coords_from_address(end_address)
    if lat1 is None or lat2 is None: return None

    route_path, route_dist = get_osrm_route(lat1, lon1, lat2, lon2)
    if not route_path: route_path, route_dist = [[lat1, lon1], [lat2, lon2]], 0

    # Try Live API first
    df = pd.DataFrame()
    try:
        engine = FuelEngine()
        df = engine.get_market_snapshot()
    except: pass
    
    # Fallback to local
    if df is None or df.empty:
        df = load_local_data()
        
    if df.empty: return None
    
    market_avg = df['price_cpl'].median()
    
    # Filter candidates
    path_lats, path_lons = zip(*route_path)
    candidates = df[
        (df['latitude'].between(min(path_lats)-0.05, max(path_lats)+0.05)) & 
        (df['longitude'].between(min(path_lons)-0.05, max(path_lons)+0.05))
    ].copy()

    if not candidates.empty:
        # Vectorized distance check
        route_arr = np.array(route_path[::5])
        def get_dist(row):
            return np.sqrt(np.min(np.sum((route_arr - np.array([row['latitude'], row['longitude']]))**2, axis=1)))
        
        candidates['dist_score'] = candidates.apply(get_dist, axis=1)
        best = candidates[candidates['dist_score'] < 0.05].copy()
        
        if not best.empty:
            best['net_utility'] = best.apply(lambda r: calculate_detour_utility(r, route_dist, market_avg, tank_capacity, current_fuel, km_per_liter, hourly_wage), axis=1)
            best = best.sort_values('net_utility', ascending=False).head(15)
            return {'start': {'lat': lat1, 'lon': lon1, 'name': name1}, 'end': {'lat': lat2, 'lon': lon2, 'name': name2}, 'stations': best, 'route_path': route_path, 'distance_km': route_dist, 'market_avg': market_avg}

    return {'start': {'lat': lat1, 'lon': lon1, 'name': name1}, 'end': {'lat': lat2, 'lon': lon2, 'name': name2}, 'stations': pd.DataFrame(), 'route_path': route_path, 'distance_km': route_dist, 'market_avg': market_avg}
